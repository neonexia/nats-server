// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//go:build !skip_js_tests

package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type descriptorBarrierStore struct {
	S3TierObjectStore
	entered chan string
	release chan struct{}
	roots   atomic.Int32
}

func (s *descriptorBarrierStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, "/descriptors/") {
		select {
		case s.entered <- key:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.S3TierObjectStore.Get(ctx, key)
}

func (s *descriptorBarrierStore) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if strings.Contains(key, "/manifests/") {
		s.roots.Add(1)
	}
	return s.S3TierObjectStore.PutIfAbsent(ctx, key, data)
}

func TestFileStoreS3TierPressureBatchCompatibleEndpoint(t *testing.T) {
	store := &descriptorBarrierStore{S3TierObjectStore: testS3TierCompatibleStore(t), entered: make(chan string, 4), release: make(chan struct{})}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), fmt.Sprintf("pressure-batch/%d", time.Now().UnixNano()))
	defer fs.Stop()
	stopS3TierWorker(t, fs.tier)
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	payload := bytes.Repeat([]byte("batch"), 3277)
	for i := 0; i < 5; i++ {
		if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
	var once sync.Once
	release := func() { once.Do(func() { close(store.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() { done <- fs.tier.evictToBudget() }()
	// All four independent descriptors can be verifying concurrently. Until
	// every verification finishes, no root or local eviction may expose coverage.
	for i := 0; i < 4; i++ {
		select {
		case <-store.entered:
		case err := <-done:
			t.Fatalf("batch ended before four verifications: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("descriptor verification serialized")
		}
	}
	if roots := store.roots.Load(); roots != 0 {
		t.Fatalf("published %d roots before verification", roots)
	}
	for i := 1; i <= 4; i++ {
		if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf("%d.blk", i))); err != nil {
			t.Fatalf("reclaimed uncommitted block %d: %v", i, err)
		}
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pressure batch did not finish")
	}
	if roots := store.roots.Load(); roots != 1 {
		t.Fatalf("pressure batch wrote %d roots, want 1", roots)
	}
	for i := 1; i <= 4; i++ {
		if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf("%d.blk", i))); !os.IsNotExist(err) {
			t.Fatalf("covered block %d not reclaimed: %v", i, err)
		}
	}
	for seq := uint64(1); seq <= 5; seq++ {
		m, err := fs.LoadMsg(seq, nil)
		if err != nil || !bytes.Equal(m.msg, payload) {
			t.Fatalf("cold replay sequence %d: %v", seq, err)
		}
	}
}
