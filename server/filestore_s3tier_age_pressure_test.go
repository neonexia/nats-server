// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Hold the first aged payload while publishers consume the remaining headroom.
// The first payload beyond one bounded batch must observe reclaimed local space.
type agePressureS3Store struct {
	S3TierObjectStore
	payloads     atomic.Int64
	entered      chan struct{}
	release      chan struct{}
	nextPayload  chan bool
	firstPayload string
}

func (s *agePressureS3Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if strings.HasSuffix(key, ".blk") {
		switch s.payloads.Add(1) {
		case 1:
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		case s3TierUploadBatchSize + 1:
			_, err := os.Stat(s.firstPayload)
			s.nextPayload <- os.IsNotExist(err)
		}
	}
	return s.S3TierObjectStore.PutIfAbsent(ctx, key, data)
}

func TestFileStoreS3TierShouldReclaimCoverageBeforeContinuingAgedBacklog(t *testing.T) {
	testS3TierAgePressure(t, &testS3BlockStore{objects: make(map[string][]byte)})
}

func TestFileStoreS3TierAgePressureCompatibleEndpoint(t *testing.T) {
	testS3TierAgePressure(t, testS3TierCompatibleStore(t))
}

func testS3TierAgePressure(t *testing.T, store S3TierObjectStore) {
	t.Helper()
	dir := t.TempDir()
	remote := &agePressureS3Store{S3TierObjectStore: store, entered: make(chan struct{}), release: make(chan struct{}), nextPayload: make(chan bool, 1), firstPayload: filepath.Join(dir, msgDir, "1.blk")}
	fcfg := FileStoreConfig{StoreDir: dir, BlockSize: 16 * 1024, S3Tier: &S3TierConfig{Store: remote, Prefix: fmt.Sprintf("age-pressure/%d", time.Now().UnixNano()), LocalHighBytes: 256 * 1024, LocalLowBytes: 128 * 1024, RemoteHighBytes: 1024 * 1024, RemoteLowBytes: 512 * 1024}}
	cfg := StreamConfig{Name: "AGE_PRESSURE", Subjects: []string{"events"}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fs, err := newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if fs != nil {
			fs.Stop()
		}
	}()
	payload := bytes.Repeat([]byte("x"), 10*1024)
	old := time.Now().Add(-time.Hour).UnixNano()
	for sequence := uint64(1); sequence <= 12; sequence++ {
		if err := fs.StoreRawMsg("events", nil, payload, sequence, old, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	fcfg.S3Tier.RemoteFlushMaxAge = time.Millisecond
	fs, err = newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			close(remote.release)
		}
	}()
	select {
	case <-remote.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("aged coverage did not start")
	}
	for range 14 {
		if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	close(remote.release)
	released = true
	select {
	case reclaimed := <-remote.nextPayload:
		if !reclaimed {
			t.Fatal("aged backlog started another upload batch before reclaiming committed local data")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("aged coverage did not resume after releasing the first payload")
	}
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if fs.S3TierStats().LocalBytes >= fcfg.S3Tier.LocalHighBytes {
			return fmt.Errorf("waiting for reclaimed publication headroom")
		}
		return nil
	})
	if sequence, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil || sequence != 27 {
		t.Fatalf("publication after reclamation: sequence=%d error=%v", sequence, err)
	}
	message, err := fs.LoadMsg(1, nil)
	if err != nil || !bytes.Equal(message.msg, payload) {
		t.Fatalf("reclaimed message replay: message=%v error=%v", message, err)
	}
	if state := fs.State(); state.Msgs != 27 || state.LastSeq != 27 {
		t.Fatalf("reclamation changed logical history: %+v", state)
	}
}
