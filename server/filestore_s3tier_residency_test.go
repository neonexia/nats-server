// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFileStoreS3TierResidencyAdmissionSurvivesRestartAndColdReplay(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, fcfg, cfg := testTieredFileStore(t, remote, t.TempDir(), "test/residency-admission")
	fs.tier.stop()
	// Stop reclamation to observe admission independently of upload timing.
	fs.tier.cfg.LocalHighBytes = 32 * 1024
	fs.tier.cfg.LocalLowBytes = 16 * 1024
	fcfg.S3Tier.LocalHighBytes = 32 * 1024
	fcfg.S3Tier.LocalLowBytes = 16 * 1024
	payload := make([]byte, 10*1024)
	for i := 0; i < 4; i++ {
		if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := fs.StoreMsg("events", nil, payload, 0); !errors.Is(err, errS3TierLocalCapacity) {
		t.Fatalf("over-capacity publish=%v", err)
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	var err error
	fs, err = newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Stop()
	fs.tier.stop()
	if _, _, err := fs.StoreMsg("events", nil, payload, 0); !errors.Is(err, errS3TierLocalCapacity) {
		t.Fatalf("restart lost resident charge: %v", err)
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	fs, err = newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Stop()
	fs.tier.stop()
	// Persisted remote descriptors must not be counted as local payloads on
	// restart, and failed cold fetches must release their projected residency.
	remote.mu.Lock()
	remote.failGet = true
	remote.mu.Unlock()
	for i := 0; i < 3; i++ {
		if _, err := fs.LoadMsg(1, nil); !errors.Is(err, errS3TierUnavailable) {
			t.Fatalf("failed cold replay=%v", err)
		}
	}
	remote.mu.Lock()
	remote.failGet = false
	remote.mu.Unlock()
	if seq, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil || seq != 5 {
		t.Fatalf("reclaimed publish=(%d,%v)", seq, err)
	}
	// Cold history must become resident again without changing logical sequence.
	for seq := uint64(1); seq <= 4; seq++ {
		if _, err := fs.LoadMsg(seq, nil); err != nil {
			t.Fatalf("replay %d: %v", seq, err)
		}
	}
	if _, _, err := fs.StoreMsg("events", nil, payload, 0); !errors.Is(err, errS3TierLocalCapacity) {
		t.Fatalf("cold replay did not consume residency: %v", err)
	}
	if state := fs.State(); state.Msgs != 5 || state.LastSeq != 5 {
		t.Fatalf("capacity rejection changed state: %+v", state)
	}
}

func TestFileStoreS3TierFailedFetchDoesNotRestoreEvictedResidency(t *testing.T) {
	remote := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	dir := t.TempDir()
	fs, _, _ := testTieredFileStore(t, remote, dir, "test/fetch-unlink-residency")
	defer fs.Stop()
	fs.tier.stop()
	if _, _, err := fs.StoreMsg("events", nil, make([]byte, 40*1024), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.DrainS3Tier(); err != nil {
		t.Fatal(err)
	}
	fs.tier.mu.Lock()
	descriptor := fs.tier.desc[1]
	fs.tier.mu.Unlock()
	remote.mu.Lock()
	remote.blocking = true
	remote.mu.Unlock()
	result := make(chan error, 1)
	release := sync.OnceFunc(func() { close(remote.release) })
	defer release()
	// Arrange the state reached when another fetch installs a file between a
	// reader's absent-file Stat and its acquisition of the single-flight slot.
	go func() { result <- fs.tier.fetchBlock(descriptor, filepath.Join(dir, msgDir, "1.blk")) }()
	select {
	case <-remote.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch did not start")
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	remote.testS3BlockStore.mu.Lock()
	remote.failGet = true
	remote.testS3BlockStore.mu.Unlock()
	release()
	select {
	case err := <-result:
		if !errors.Is(err, errS3TierUnavailable) {
			t.Fatalf("fetch error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetch did not fail")
	}
	for range 4 {
		if _, _, err := fs.StoreMsg("events", nil, make([]byte, 10*1024), 0); err != nil {
			t.Fatalf("failed fetch restored phantom residency: %v", err)
		}
	}
	if _, _, err := fs.StoreMsg("events", nil, []byte("over capacity"), 0); !errors.Is(err, errS3TierLocalCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
	// The covered payload remains readable from the remote store itself.
	remote.testS3BlockStore.mu.Lock()
	remote.failGet = false
	remote.testS3BlockStore.mu.Unlock()
	if _, err := remote.Get(context.Background(), descriptor.Key); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreS3TierConcurrentPublishersShareResidencyHeadroom(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, remote, t.TempDir(), "test/concurrent-residency")
	defer fs.Stop()
	fs.tier.stop()
	payload := make([]byte, 10*1024)
	start := make(chan struct{})
	results := make(chan error, 64)
	for range cap(results) {
		go func() { <-start; _, _, err := fs.StoreMsg("events", nil, payload, 0); results <- err }()
	}
	close(start)
	accepted := 0
	for range cap(results) {
		err := <-results
		if err == nil {
			accepted++
		} else if !errors.Is(err, errS3TierLocalCapacity) {
			t.Fatalf("publish error=%v", err)
		}
	}
	// 32 KiB permits four 10 KiB records under the legacy one-record overshoot.
	// Every concurrent contender after the fourth must receive backpressure.
	if accepted != 4 {
		t.Fatalf("accepted=%d want=4", accepted)
	}
	if state := fs.State(); state.Msgs != 4 || state.LastSeq != 4 {
		t.Fatalf("rejected contenders changed history: %+v", state)
	}
}
