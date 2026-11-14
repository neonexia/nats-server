// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestJetStreamS3TierWarmupCacheVerifiesPayloadBeforeReuse(t *testing.T) {
	payload := []byte("confirmed immutable history")
	store := &testS3BlockStore{objects: map[string][]byte{"block": payload}}
	tier := newFileS3Tier(nil, S3TierConfig{Store: store, Prefix: "warmup"})
	defer tier.remoteCancel()
	dir := t.TempDir()
	block := s3BlockDescriptor{Index: 1, Key: "block", Size: len(payload), SHA256: checksumS3Block(payload)}
	require_NoError(t, tier.stageRemoteRestoreBlock(context.Background(), dir, block))
	store.mu.Lock()
	store.failGet = true
	store.mu.Unlock()
	// Verified cached bytes require no remote read at the final barrier.
	require_NoError(t, tier.stageRemoteRestoreBlock(context.Background(), dir, block))
	require_NoError(t, os.WriteFile(filepath.Join(dir, "1.blk"), []byte("corrupt"), 0600))
	if err := tier.stageRemoteRestoreBlock(context.Background(), dir, block); err == nil {
		t.Fatal("corrupt cache accepted while remote repair unavailable")
	}
	store.mu.Lock()
	store.failGet = false
	store.mu.Unlock()
	require_NoError(t, tier.stageRemoteRestoreBlock(context.Background(), dir, block))
	data, err := os.ReadFile(filepath.Join(dir, "1.blk"))
	require_NoError(t, err)
	if string(data) != string(payload) {
		t.Fatalf("repaired payload=%q", data)
	}
	require_NoError(t, os.WriteFile(filepath.Join(dir, "99.blk"), []byte("stale warmup history"), 0600))
	require_NoError(t, pruneRemoteRestoreBlocks(dir, []s3BlockDescriptor{block}))
	if _, err := os.Stat(filepath.Join(dir, "99.blk")); !os.IsNotExist(err) {
		t.Fatalf("stale cache remains: %v", err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "1.blk"))
	require_NoError(t, err)
	if string(data) != string(payload) {
		t.Fatal("pruning changed retained payload")
	}
}
