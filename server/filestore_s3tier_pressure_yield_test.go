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

type pressureYieldS3Store struct {
	*testS3BlockStore
	payloads     atomic.Int64
	entered      chan struct{}
	release      chan struct{}
	nextPayload  chan bool
	firstPayload string
}

func (s *pressureYieldS3Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if strings.HasSuffix(key, ".blk") {
		count := s.payloads.Add(1)
		if count <= s3TierUploadBatchSize {
			s.entered <- struct{}{}
			select {
			case <-s.release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		} else if count == s3TierUploadBatchSize+1 {
			_, err := os.Stat(s.firstPayload)
			s.nextPayload <- os.IsNotExist(err)
		}
	}
	return s.testS3BlockStore.PutIfAbsent(ctx, key, data)
}

func TestFileStoreS3TierShouldReclaimCoveredBlocksWhenPressureArrivesDuringCoverage(t *testing.T) {
	dir := t.TempDir()
	remote := &pressureYieldS3Store{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, entered: make(chan struct{}, s3TierUploadBatchSize), release: make(chan struct{}), nextPayload: make(chan bool, 1), firstPayload: filepath.Join(dir, msgDir, "1.blk")}
	fs, _, _ := testTieredFileStore(t, remote, dir, "test/pressure-yield")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.LocalHighBytes = 256 * 1024
	fs.tier.cfg.LocalLowBytes = 16 * 1024
	fs.tier.cfg.RemoteHighBytes = 16 * 1024
	fs.tier.cfg.RemoteLowBytes = 8 * 1024
	payload := bytes.Repeat([]byte("x"), 10*1024)
	for range 12 {
		if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- fs.tier.evictToBudget() }()
	released := false
	finished := false
	defer func() {
		if !released {
			close(remote.release)
		}
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	for range s3TierUploadBatchSize {
		select {
		case <-remote.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("initial coverage batch did not start")
		}
	}
	// The pass began below the local watermark. Fill headroom while its first
	// remote batch is blocked, leaving multiple batches of uncovered backlog.
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
			t.Fatal("started more remote payload uploads before reclaiming already covered data under pressure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coverage did not proceed after releasing initial batch")
	}
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pressure pass did not finish")
	}
	message, err := fs.LoadMsg(1, nil)
	if err != nil || !bytes.Equal(message.msg, payload) {
		t.Fatalf("reclaimed payload replay=%v error=%v", message, err)
	}
	if state := fs.State(); state.Msgs != 26 || state.LastSeq != 26 {
		t.Fatalf("reclamation changed logical history: %+v", state)
	}
}

func TestFileStoreS3TierShouldCoverFinalBurstWithoutAnotherPublish(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 16 * 1024, S3Tier: &S3TierConfig{Store: remote, Prefix: "test/final-burst", LocalHighBytes: 1024 * 1024, LocalLowBytes: 512 * 1024, RemoteHighBytes: 16 * 1024, RemoteLowBytes: 8 * 1024}}
	cfg := StreamConfig{Name: "FINAL", Subjects: []string{"events"}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fs, err := newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Stop()
	for burst := 0; burst < 2; burst++ {
		for range 5 {
			if _, _, err := fs.StoreMsg("events", nil, make([]byte, 10*1024), 0); err != nil {
				t.Fatal(err)
			}
		}
		// The worker must retain deferred work after the last publisher exits.
		checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
			stats := fs.S3TierStats()
			if stats.BacklogBlocks != 0 || stats.RemoteBlocks < uint64((burst+1)*5-1) {
				return fmt.Errorf("waiting for final burst coverage: %+v", stats)
			}
			return nil
		})
	}
	if state := fs.State(); state.Msgs != 10 {
		t.Fatalf("final burst state=%+v", state)
	}
}
