// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A remote operation can be cancelled while its result is still returning.
// Stop must cancel the wait and join the caller before its directory is reused.
type stoppingS3Store struct {
	*testS3BlockStore
	mode      string
	blocked   atomic.Bool
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
}

func (s *stoppingS3Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if s.mode == "payload_outage" && strings.HasSuffix(key, ".blk") && s.blocked.CompareAndSwap(false, true) {
		close(s.entered)
		<-ctx.Done()
		close(s.cancelled)
		return false, ctx.Err()
	}
	return s.testS3BlockStore.PutIfAbsent(ctx, key, data)
}

func (s *stoppingS3Store) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.testS3BlockStore.Get(ctx, key)
	if err == nil && s.mode == "committed_root_response" && strings.Contains(key, "/manifests/") && s.blocked.CompareAndSwap(false, true) {
		close(s.entered)
		<-ctx.Done()
		close(s.cancelled)
		<-s.release
	}
	return data, err
}

func TestFileStoreS3TierShouldCancelAndJoinRemoteWritesBeforeStopReturns(t *testing.T) {
	for _, mode := range []string{"payload_outage", "committed_root_response"} {
		t.Run(mode, func(t *testing.T) {
			remote := &stoppingS3Store{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, mode: mode, entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
			fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 16 * 1024, S3Tier: &S3TierConfig{Store: remote, Prefix: "stop/" + mode, LocalHighBytes: 1024 * 1024, LocalLowBytes: 512 * 1024, RemoteFlushMaxAge: time.Millisecond, Timeout: 30 * time.Second}}
			cfg := StreamConfig{Name: "STOP", Subjects: []string{"events"}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
			fs, err := newFileStore(fcfg, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if fs != nil {
					fs.Stop()
				}
			}()
			released := false
			defer func() {
				if !released {
					close(remote.release)
				}
			}()
			if err := fs.StoreRawMsg("events", nil, []byte("preserve this message"), 1, time.Now().Add(-time.Hour).UnixNano(), 0, false); err != nil {
				t.Fatal(err)
			}
			select {
			case <-remote.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("remote operation did not start")
			}
			stopped := make(chan error, 1)
			go func() { stopped <- fs.Stop() }()
			select {
			case <-remote.cancelled:
			case <-time.After(time.Second):
				t.Fatal("Stop did not cancel the remote wait before its 30-second request timeout")
			}
			if mode == "committed_root_response" {
				select {
				case err := <-stopped:
					t.Fatalf("Stop returned before the in-flight committed-root response finished: %v", err)
				case <-time.After(30 * time.Millisecond):
				}
			}
			close(remote.release)
			released = true
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Stop did not finish after the remote operation exited")
			}
			// Reopening immediately must not race a late old-worker sidecar write.
			moved := fcfg.StoreDir + "-stopped"
			if err := os.Rename(fcfg.StoreDir, moved); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(moved) })
			fcfg.StoreDir = moved
			fs, err = newFileStore(fcfg, cfg)
			if err != nil {
				t.Fatal(err)
			}
			message, err := fs.LoadMsg(1, nil)
			if err != nil || string(message.msg) != "preserve this message" {
				t.Fatalf("reopened message=%v error=%v", message, err)
			}
			if state := fs.State(); state.Msgs != 1 || state.LastSeq != 1 {
				t.Fatalf("reopened state=%+v", state)
			}
		})
	}
}

func TestFileStoreS3TierShouldCloseBlockedColdReadWhenStoreStops(t *testing.T) {
	remote := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	fs, fcfg, cfg := testTieredFileStore(t, remote, t.TempDir(), "stop/cold-read")
	defer func() {
		if fs != nil {
			fs.Stop()
		}
	}()
	fs.tier.stop()
	payload := []byte(strings.Repeat("x", 10*1024))
	for range 4 {
		if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	remote.blocking = true
	remote.mu.Unlock()
	readDone := make(chan error, 1)
	go func() { _, err := fs.LoadMsg(1, nil); readDone <- err }()
	select {
	case <-remote.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cold read did not reach object storage")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- fs.Stop() }()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel and join the blocked cold read")
	}
	if err := <-readDone; err != ErrStoreClosed {
		t.Fatalf("read interrupted by Stop returned %v, want ErrStoreClosed", err)
	}
	remote.mu.Lock()
	remote.blocking = false
	remote.mu.Unlock()
	var err error
	fs, err = newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	message, err := fs.LoadMsg(1, nil)
	if err != nil || string(message.msg) != string(payload) {
		t.Fatalf("cold read after restart: message=%v error=%v", message, err)
	}
}
