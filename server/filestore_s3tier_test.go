// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//go:build !skip_js_tests

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
)

type testS3BlockStore struct {
	mu             sync.Mutex
	objects        map[string][]byte
	failPut        bool
	failDescriptor bool
	failManifest   bool
	failCheckpoint bool
	failGet        bool
	blockPut       bool
}

type countingS3BlockStore struct {
	store S3TierObjectStore
	mu    sync.Mutex
	gets  []string
	lists int
}

func (s *countingS3BlockStore) PutIfAbsent(ctx context.Context, key string, buf []byte) (bool, error) {
	return s.store.PutIfAbsent(ctx, key, buf)
}

func (s *countingS3BlockStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets = append(s.gets, key)
	s.mu.Unlock()
	return s.store.Get(ctx, key)
}

func (s *countingS3BlockStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.store.List(ctx, prefix)
}

func (s *countingS3BlockStore) counts() (lists, descriptorGets, blockGets int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range s.gets {
		if strings.Contains(key, "/descriptors/") {
			descriptorGets++
		} else if strings.Contains(key, "/blocks/") {
			blockGets++
		}
	}
	return s.lists, descriptorGets, blockGets
}

func (s *countingS3BlockStore) reset() {
	s.mu.Lock()
	s.gets = nil
	s.lists = 0
	s.mu.Unlock()
}

type blockingS3BlockStore struct {
	*testS3BlockStore
	mu        sync.Mutex
	blocking  bool
	blockGets int
	entered   chan struct{}
	release   chan struct{}
}

func (s *blockingS3BlockStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	blocking := s.blocking && strings.Contains(key, "/blocks/")
	if strings.Contains(key, "/blocks/") {
		s.blockGets++
	}
	s.mu.Unlock()
	if blocking {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.testS3BlockStore.Get(ctx, key)
}

func (s *blockingS3BlockStore) resetBlockGets() {
	s.mu.Lock()
	s.blockGets = 0
	s.mu.Unlock()
}

func (s *blockingS3BlockStore) blockGetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blockGets
}

func (s *testS3BlockStore) Put(_ context.Context, key string, buf []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failPut || (s.failDescriptor && strings.Contains(key, "/descriptors/")) || (s.failManifest && strings.Contains(key, "/manifests/")) || (s.failCheckpoint && strings.Contains(key, "/checkpoints/")) {
		return errors.New("injected PUT failure")
	}
	s.objects[key] = append([]byte(nil), buf...)
	return nil
}

func (s *testS3BlockStore) PutIfAbsent(ctx context.Context, key string, buf []byte) (bool, error) {
	s.mu.Lock()
	if _, ok := s.objects[key]; ok {
		s.mu.Unlock()
		return false, nil
	}
	blockPut := s.blockPut
	s.mu.Unlock()
	if blockPut {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; ok {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.failPut || (s.failDescriptor && strings.Contains(key, "/descriptors/")) || (s.failManifest && strings.Contains(key, "/manifests/")) || (s.failCheckpoint && strings.Contains(key, "/checkpoints/")) {
		return false, errors.New("injected PUT failure")
	}
	s.objects[key] = append([]byte(nil), buf...)
	return true, nil
}

func (s *testS3BlockStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failGet {
		return nil, errors.New("injected GET failure")
	}
	buf, ok := s.objects[key]
	if !ok {
		return nil, errors.New("missing object")
	}
	return append([]byte(nil), buf...), nil
}

func (s *testS3BlockStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func testTieredFileStore(t *testing.T, store S3TierObjectStore, dir, prefix string) (*fileStore, FileStoreConfig, StreamConfig) {
	t.Helper()
	fcfg := FileStoreConfig{StoreDir: dir, BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: prefix, LocalHighBytes: 32 * 1024, LocalLowBytes: 16 * 1024,
	}}
	cfg := StreamConfig{Name: "TEST", Subjects: []string{"events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fs, err := newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return fs, fcfg, cfg
}

func TestJetStreamS3TierServerConfig(t *testing.T) {
	t.Setenv("NATS_S3_TIER_ACCESS", "minio-access")
	t.Setenv("NATS_S3_TIER_SECRET", "minio-secret")
	config := createConfFile(t, []byte(fmt.Sprintf(`
port: -1
jetstream {
  store_dir: %q
  s3_tier {
    endpoint: "minio.internal:9000"
    bucket: "nodus-history"
    region: "us-east-1"
    tls: false
    prefix: "instances/test-instance"
    block_size: 1M
    local_high_bytes: 8M
    local_low_bytes: 4M
    remote_high_bytes: 2M
    remote_low_bytes: 1M
    timeout: "2s"
    retry_min: "10ms"
    retry_max: "100ms"
    credentials {
      provider: "env"
      access_key_env: "NATS_S3_TIER_ACCESS"
      secret_key_env: "NATS_S3_TIER_SECRET"
    }
  }
}
`, t.TempDir())))
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatal(err)
	}
	tier := opts.jetStreamS3Tier
	if tier == nil || tier.Endpoint != "minio.internal:9000" || tier.Bucket != "nodus-history" || tier.store == nil {
		t.Fatalf("tier config = %#v", tier)
	}
	if tier.BlockSize != 1_000_000 || tier.LocalHighBytes != 8_000_000 || tier.RemoteLowBytes != 1_000_000 || tier.Timeout != 2*time.Second || tier.RetryMax != 100*time.Millisecond {
		t.Fatalf("unexpected tier settings: %#v", tier)
	}
	streamTier := tier.streamConfig("$G", "TEST")
	if streamTier == nil || streamTier.Prefix != "instances/test-instance/streams/JEc/VEVTVA" || streamTier.Store != tier.store {
		t.Fatalf("derived stream tier = %#v", streamTier)
	}
	s := RunServer(opts)
	defer s.Shutdown()
	stream := &StreamConfig{Name: "TEST", Subjects: []string{"events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	mset, err := s.GlobalAccount().addStream(stream)
	if err != nil {
		t.Fatal(err)
	}
	fs := mset.store.(*fileStore)
	if fs.tier == nil || fs.tier.cfg.Prefix != streamTier.Prefix || fs.tier.cfg.BlockSize != streamTier.BlockSize {
		t.Fatalf("stream did not inherit server tier configuration: %#v", fs.tier)
	}
	changed := *opts
	changedTier := *tier
	changedTier.Prefix = "instances/reloaded"
	changed.jetStreamS3Tier = &changedTier
	if _, err := s.diffOptions(&changed); err == nil {
		t.Fatal("expected S3 tier configuration reload to be rejected")
	}
}

func TestJetStreamS3TierServerConfigDefaultsAndCredentialValidation(t *testing.T) {
	t.Setenv("NATS_S3_TIER_DEFAULT_ACCESS", "minio-access")
	t.Setenv("NATS_S3_TIER_DEFAULT_SECRET", "minio-secret")
	config := createConfFile(t, []byte(`
jetstream {
  s3_tier {
    endpoint: "minio.internal:9000"
    bucket: "nodus-history"
    prefix: "instances/defaults"
    credentials {
      provider: "environment"
      access_key_env: "NATS_S3_TIER_DEFAULT_ACCESS"
      secret_key_env: "NATS_S3_TIER_DEFAULT_SECRET"
    }
  }
}
`))
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatal(err)
	}
	tier := opts.jetStreamS3Tier
	if tier == nil || tier.BlockSize != defaultS3TierBlockSize || tier.LocalHighBytes != defaultS3TierLocalHigh || tier.RemoteLowBytes != defaultS3TierRemoteLow || tier.Timeout != defaultS3TierTimeout || tier.RetryMax != defaultS3TierRetryMax {
		t.Fatalf("tier defaults = %#v", tier)
	}
	badConfig := createConfFile(t, []byte(`
jetstream {
  s3_tier {
    endpoint: "minio.internal:9000"
    bucket: "nodus-history"
    prefix: "instances/bad-credentials"
    credentials { provider: "env", access_key_env: "MISSING_ACCESS", secret_key_env: "MISSING_SECRET" }
  }
}
`))
	if _, err := ProcessConfigFile(badConfig); err == nil {
		t.Fatal("expected missing credential environment variables to reject configuration")
	}
}

func TestFileStoreS3TierReplayAndRestart(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, fcfg, cfg := testTieredFileStore(t, store, t.TempDir(), "test/stream")
	for i := 1; i <= 100; i++ {
		if seq, _, err := fs.StoreMsg("events", nil, []byte(fmt.Sprintf("message-%04d-%s", i, strings.Repeat("x", 1000))), 0); err != nil || seq != uint64(i) {
			t.Fatalf("store %d: seq=%d err=%v", i, seq, err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, "1.blk")); !os.IsNotExist(err) {
		t.Fatalf("oldest block still local: %v", err)
	}
	for seq := uint64(1); seq <= 100; seq++ {
		msg, err := fs.LoadMsg(seq, nil)
		if err != nil || msg.seq != seq {
			t.Fatalf("cold load %d: msg=%v err=%v", seq, msg, err)
		}
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"1.blk", "2.blk"} {
		_ = os.Remove(filepath.Join(fcfg.StoreDir, msgDir, path))
	}
	fs, err := newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatalf("restart with remote-only blocks: %v", err)
	}
	defer fs.Stop()
	if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 1 || state.LastSeq != 100 {
		t.Fatalf("recovered state: %+v", state)
	}
	for seq := uint64(1); seq <= 100; seq++ {
		msg, err := fs.LoadMsg(seq, nil)
		if err != nil || msg.seq != seq {
			t.Fatalf("restart load %d: msg=%v err=%v", seq, msg, err)
		}
	}
}

func TestFileStoreS3TierColdReadFetchesOnlySelectedBlock(t *testing.T) {
	base := &testS3BlockStore{objects: make(map[string][]byte)}
	store := &countingS3BlockStore{store: base}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/selected-block")
	defer fs.Stop()
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	store.reset()
	msg, next, err := fs.LoadNextMsg(_EMPTY_, false, 1, nil)
	if err != nil || msg.seq != 1 || next != 1 {
		t.Fatalf("first message = (%v, %d, %v)", msg, next, err)
	}
	_, _, blockGets := store.counts()
	if blockGets != 1 {
		t.Fatalf("cold read fetched %d payload blocks, want 1", blockGets)
	}
}

func TestFileStoreS3TierRestartDoesNotContactObjectStore(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, fcfg, cfg := testTieredFileStore(t, store, t.TempDir(), "test/offline-restart")
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failGet = true
	store.mu.Unlock()
	fs, err := newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatalf("restart while object store is unavailable: %v", err)
	}
	defer fs.Stop()
	if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 1 || state.LastSeq != 100 {
		t.Fatalf("recovered state: %+v", state)
	}
}

func TestFileStoreS3TierCoalescesConcurrentColdReads(t *testing.T) {
	store := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)},
		entered: make(chan struct{}, 1), release: make(chan struct{})}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/coalesced-read")
	defer fs.Stop()
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	store.resetBlockGets()
	store.mu.Lock()
	store.blocking = true
	store.mu.Unlock()
	readDone := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := fs.LoadMsg(1, nil)
			readDone <- err
		}()
	}
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cold read did not reach S3")
	}
	close(store.release)
	for range 2 {
		select {
		case err := <-readDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cold reads did not complete")
		}
	}
	if gets := store.blockGetCount(); gets != 1 {
		t.Fatalf("payload GETs=%d, want 1", gets)
	}
}

func TestFileStoreS3TierReclaimsHydratedCacheBeforeLocalBlocks(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/cache-bound")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{1, 20} {
		if _, err := fs.LoadMsg(seq, nil); err != nil {
			t.Fatalf("load %d: %v", seq, err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	if got := fs.tier.localBytes(); got > fs.tier.cfg.LocalLowBytes {
		t.Fatalf("local cache bytes=%d, want <= %d", got, fs.tier.cfg.LocalLowBytes)
	}
}

func TestFileStoreS3TierSeparatesRemoteCoverageFromLocalResidency(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/overlap")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 256*1024, 128*1024
	fs.tier.cfg.RemoteHighBytes, fs.tier.cfg.RemoteLowBytes = 32*1024, 16*1024
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	fs.tier.mu.Lock()
	covered := len(fs.tier.desc)
	fs.tier.mu.Unlock()
	if covered == 0 {
		t.Fatal("expected sealed blocks to gain remote coverage")
	}
	if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")); err != nil {
		t.Fatalf("covered block should remain local below residency budget: %v", err)
	}
	if got := fs.tier.localBytes(); got > fs.tier.cfg.LocalHighBytes {
		t.Fatalf("local bytes=%d, want <= %d", got, fs.tier.cfg.LocalHighBytes)
	}
}

func TestFileStoreS3TierDrainSealsAndCoversTail(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/drain")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.RemoteHighBytes, fs.tier.cfg.RemoteLowBytes = 1<<30, 1<<29
	for i := 0; i < 10; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	drained, err := fs.DrainS3Tier()
	if err != nil || drained != 1 {
		t.Fatalf("drain = (%d, %v), want (1, nil)", drained, err)
	}
	fs.tier.mu.Lock()
	_, covered := fs.tier.desc[1]
	fs.tier.mu.Unlock()
	if !covered {
		t.Fatal("sealed tail block lacks remote coverage")
	}
	if fs.lmb.index != 2 {
		t.Fatalf("active block index=%d, want 2", fs.lmb.index)
	}
	if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")); err != nil {
		t.Fatalf("drain should preserve local payload: %v", err)
	}
	if drained, err := fs.DrainS3Tier(); err != nil || drained != 0 {
		t.Fatalf("second drain = (%d, %v), want (0, nil)", drained, err)
	}
}

func TestFileStoreS3TierDrainCreatesImmutableCheckpoint(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/checkpoint")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.RemoteHighBytes, fs.tier.cfg.RemoteLowBytes = 1<<30, 1<<29
	for i := 0; i < 10; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if drained, err := fs.DrainS3Tier(); err != nil || drained != 1 {
		t.Fatalf("drain = (%d, %v), want (1, nil)", drained, err)
	}
	fs.tier.mu.Lock()
	record := *fs.tier.record
	checkpoint := *fs.tier.checkpoint
	fs.tier.mu.Unlock()
	if checkpoint.CoveredThrough != 10 || checkpoint.Incarnation != record.Incarnation {
		t.Fatalf("checkpoint = %+v, record = %+v", checkpoint, record)
	}
	store.mu.Lock()
	recordData := append([]byte(nil), store.objects[fs.tier.streamRecordKey()]...)
	checkpointData := append([]byte(nil), store.objects[fs.tier.checkpointKey(10)]...)
	manifestData := append([]byte(nil), store.objects[checkpoint.ManifestKey]...)
	objectCount := len(store.objects)
	store.mu.Unlock()
	if checksumS3Block(manifestData) != checkpoint.ManifestSHA256 {
		t.Fatal("checkpoint manifest digest does not match remote manifest")
	}
	var storedRecord s3TierStreamRecord
	var storedCheckpoint s3TierCheckpoint
	var manifest s3TierManifest
	if err := json.Unmarshal(recordData, &storedRecord); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(checkpointData, &storedCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if storedRecord.Incarnation != manifest.Incarnation || manifest.Version != 2 || storedCheckpoint.ManifestGeneration != manifest.Generation {
		t.Fatalf("record=%+v checkpoint=%+v manifest=%+v", storedRecord, storedCheckpoint, manifest)
	}
	if drained, err := fs.DrainS3Tier(); err != nil || drained != 0 {
		t.Fatalf("repeat drain = (%d, %v), want (0, nil)", drained, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.objects) != objectCount {
		t.Fatalf("repeat drain wrote %d objects, want %d", len(store.objects), objectCount)
	}
}

func TestFileStoreS3TierDrainCheckpointFailurePreservesLocalPayload(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte), failCheckpoint: true}
	fs, fcfg, _ := testTieredFileStore(t, store, t.TempDir(), "test/checkpoint-failure")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.RemoteHighBytes, fs.tier.cfg.RemoteLowBytes = 1<<30, 1<<29
	for i := 0; i < 10; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fs.DrainS3Tier(); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	if _, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, "1.blk")); err != nil {
		t.Fatalf("checkpoint failure removed local payload: %v", err)
	}
	if _, err := os.Stat(fs.tier.checkpointFile()); !os.IsNotExist(err) {
		t.Fatalf("checkpoint sidecar present after failed checkpoint: %v", err)
	}
	store.mu.Lock()
	_, exists := store.objects[fs.tier.checkpointKey(10)]
	store.mu.Unlock()
	if exists {
		t.Fatal("checkpoint object present after injected failure")
	}
}

func TestFileStoreS3TierRejectsConflictingStreamRecord(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/record-conflict")
	defer fs.Stop()
	fs.tier.stop()
	foreign := s3TierStreamRecord{Version: 1, Incarnation: "other", Prefix: "test/record-conflict", StreamName: "OTHER", Created: 1, ConfigSHA256: "other"}
	data, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}
	store.objects[fs.tier.streamRecordKey()] = data
	if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.DrainS3Tier(); err == nil {
		t.Fatal("expected stream record conflict")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for key := range store.objects {
		if strings.Contains(key, "/blocks/") || strings.Contains(key, "/descriptors/") {
			t.Fatalf("conflicting stream record allowed remote data write: %q", key)
		}
	}
}

func TestJetStreamS3TierDrainRemoteAPI(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	cfg := &StreamConfig{Name: "DRAIN", Subjects: []string{"drain.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fcfg := &FileStoreConfig{BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: "test/drain-api", LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
		RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	mset, err := s.GlobalAccount().addStreamWithStore(cfg, fcfg)
	if err != nil {
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "drain.events", strings.Repeat("x", 1000))
	}
	response, err := nc.Request(fmt.Sprintf(JSApiStreamDrainRemoteT, "DRAIN"), nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp JSApiStreamDrainRemoteResponse
	if err := json.Unmarshal(response.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil || !resp.Success || resp.Drained != 1 {
		t.Fatalf("drain API response: %+v", resp)
	}
	fs := mset.store.(*fileStore)
	fs.tier.mu.Lock()
	_, covered := fs.tier.desc[1]
	fs.tier.mu.Unlock()
	if !covered {
		t.Fatal("drain API did not cover sealed tail")
	}
}

func TestJetStreamS3TierRemoteRestoreSourceAPI(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	cfg := &StreamConfig{Name: "MOVE", Subjects: []string{"move.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fcfg := &FileStoreConfig{BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: "test/remote-restore-api", LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
		RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	mset, err := s.GlobalAccount().addStreamWithStore(cfg, fcfg)
	if err != nil {
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "move.events", strings.Repeat("x", 1000))
	}
	request := func(subject string) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		body, err := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "move_1"})
		if err != nil {
			t.Fatal(err)
		}
		msg, err := nc.Request(subject, body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp JSApiStreamRemoteRestoreResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	prepare := request(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "MOVE"))
	if prepare.Error != nil || !prepare.Success || prepare.State != s3TierRemoteRestorePrepared || prepare.Epoch != 1 {
		t.Fatalf("prepare response: %+v", prepare)
	}
	// The source stays readable, but the persisted fence rejects a new publish.
	stored, err := mset.getMsg(1)
	if err != nil || stored == nil || len(stored.Data) != 1000 {
		t.Fatalf("source read while prepared = (%+v, %v)", stored, err)
	}
	msg, err := nc.Request("move.events", []byte("blocked"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var pubAck JSPubAckResponse
	if err := json.Unmarshal(msg.Data, &pubAck); err != nil {
		t.Fatal(err)
	}
	if pubAck.Error == nil || !strings.Contains(pubAck.Error.Description, "fenced for remote restore") {
		t.Fatalf("publish while prepared response: %+v", pubAck)
	}
	checkpoint := request(fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "MOVE"))
	if checkpoint.Error != nil || !checkpoint.Success || checkpoint.State != s3TierRemoteRestoreCheckpointed || checkpoint.CheckpointKey == "" || checkpoint.CheckpointSHA256 == "" {
		t.Fatalf("checkpoint response: %+v", checkpoint)
	}
	store.mu.Lock()
	checkpointData := append([]byte(nil), store.objects[checkpoint.CheckpointKey]...)
	store.mu.Unlock()
	var remoteCheckpoint s3TierRemoteRestoreCheckpoint
	if err := json.Unmarshal(checkpointData, &remoteCheckpoint); err != nil {
		t.Fatal(err)
	}
	if remoteCheckpoint.OperationID != "move_1" || remoteCheckpoint.Epoch != prepare.Epoch || remoteCheckpoint.MessageCheckpoint.CoveredThrough != 10 {
		t.Fatalf("remote checkpoint: %+v", remoteCheckpoint)
	}
	if remoteCheckpoint.StreamConfig.Name != "MOVE" || remoteCheckpoint.StreamState.LastSeq != 10 {
		t.Fatalf("remote checkpoint stream metadata: %+v", remoteCheckpoint)
	}
	if retired := request(fmt.Sprintf(JSApiStreamRetireRemoteSourceT, "MOVE")); retired.Error == nil || !strings.Contains(retired.Error.Description, "requires target activation") {
		t.Fatalf("retire before target activation response: %+v", retired)
	}
	statusMsg, err := nc.Request(fmt.Sprintf(JSApiStreamRemoteRestoreStatusT, "MOVE"), nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var status JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(statusMsg.Data, &status); err != nil {
		t.Fatal(err)
	}
	if status.Error != nil || status.State != s3TierRemoteRestoreCheckpointed || status.OperationID != "move_1" {
		t.Fatalf("status response: %+v", status)
	}
	if status.S3Tier == nil || status.S3Tier.RestorePrepares == 0 || status.S3Tier.RestoreCheckpoints == 0 {
		t.Fatalf("status is missing remote restore metrics: %+v", status)
	}
}

func TestJetStreamS3TierRemoteRestoreCheckpointFailureRetries(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	cfg := &StreamConfig{Name: "RETRY", Subjects: []string{"retry.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fcfg := &FileStoreConfig{BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: "test/remote-restore-retry", LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
		RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	if _, err := s.GlobalAccount().addStreamWithStore(cfg, fcfg); err != nil {
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	sendStreamMsg(t, nc, "retry.events", "one")
	request := func(subject string) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		body, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "retry_1"})
		msg, err := nc.Request(subject, body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp JSApiStreamRemoteRestoreResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := request(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "RETRY")); resp.Error != nil || resp.State != s3TierRemoteRestorePrepared {
		t.Fatalf("prepare response: %+v", resp)
	}
	store.failPut = true
	if resp := request(fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "RETRY")); resp.Error == nil {
		t.Fatal("expected checkpoint failure")
	}
	// The durable source fence survives a remote outage. It keeps writers out
	// while the coordinator retries the same operation ID after recovery.
	if resp := request(fmt.Sprintf(JSApiStreamRemoteRestoreStatusT, "RETRY")); resp.State != s3TierRemoteRestorePrepared || resp.S3Tier == nil || resp.S3Tier.RestoreErrors == 0 {
		t.Fatalf("status after failed checkpoint: %+v", resp)
	}
	store.failPut = false
	if resp := request(fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "RETRY")); resp.Error != nil || resp.State != s3TierRemoteRestoreCheckpointed {
		t.Fatalf("checkpoint retry response: %+v", resp)
	}
}

func TestJetStreamS3TierRemoteRestoreAbortResumesSource(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	cfg := &StreamConfig{Name: "ABORT", Subjects: []string{"abort.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fcfg := &FileStoreConfig{BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: "test/remote-restore-abort", LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
		RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	if _, err := s.GlobalAccount().addStreamWithStore(cfg, fcfg); err != nil {
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	call := func(subject string) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		body, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "abort_1"})
		msg, err := nc.Request(subject, body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp JSApiStreamRemoteRestoreResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := call(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "ABORT")); resp.Error != nil || resp.State != s3TierRemoteRestorePrepared {
		t.Fatalf("prepare response: %+v", resp)
	}
	if resp := call(fmt.Sprintf(JSApiStreamAbortRemoteRestoreT, "ABORT")); resp.Error != nil || resp.State != s3TierRemoteRestoreAborted {
		t.Fatalf("abort response: %+v", resp)
	}
	sendStreamMsg(t, nc, "abort.events", "accepted-after-abort")
}

func TestJetStreamS3TierRemoteRestoreCheckpointsConsumers(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	cfg := &StreamConfig{Name: "CONSUMERS", Subjects: []string{"consumers.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	fcfg := &FileStoreConfig{BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: "test/remote-restore-consumers", LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
		RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	mset, err := s.GlobalAccount().addStreamWithStore(cfg, fcfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "C", AckPolicy: AckExplicit}); err != nil {
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	sendStreamMsg(t, nc, "consumers.events", "one")
	request := func(subject string) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		body, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "consumers_1"})
		msg, err := nc.Request(subject, body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp JSApiStreamRemoteRestoreResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := request(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "CONSUMERS")); resp.Error != nil || resp.State != s3TierRemoteRestorePrepared {
		t.Fatalf("prepare with consumer response: %+v", resp)
	}
	checkpoint := request(fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "CONSUMERS"))
	if checkpoint.Error != nil || checkpoint.State != s3TierRemoteRestoreCheckpointed {
		t.Fatalf("checkpoint response: %+v", checkpoint)
	}
	store.mu.Lock()
	data := append([]byte(nil), store.objects[checkpoint.CheckpointKey]...)
	store.mu.Unlock()
	var remote s3TierRemoteRestoreCheckpoint
	if err := json.Unmarshal(data, &remote); err != nil {
		t.Fatal(err)
	}
	if len(remote.Consumers) != 1 || remote.Consumers[0].ConsumerConfig.Durable != "C" || remote.Consumers[0].ConsumerState == nil {
		t.Fatalf("checkpoint consumers: %+v", remote.Consumers)
	}
}

func TestJetStreamS3TierRemoteRestoreFenceSurvivesRestart(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/FENCED": {
		Store: store, Prefix: "test/remote-restore-restart", BlockSize: 16 * 1024, LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
		RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	s := RunServer(&opts)
	cfg := &StreamConfig{Name: "FENCED", Subjects: []string{"fenced.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	if _, err := s.GlobalAccount().addStream(cfg); err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	for i := 0; i < 3; i++ {
		sendStreamMsg(t, nc, "fenced.events", "before-fence")
	}
	body, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "restart_1"})
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "FENCED"), body, 5*time.Second)
	if err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatal(err)
	}
	var prepared JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(msg.Data, &prepared); err != nil || prepared.Error != nil || prepared.State != s3TierRemoteRestorePrepared {
		nc.Close()
		s.Shutdown()
		t.Fatalf("prepare response: %+v, %v", prepared, err)
	}
	nc.Close()
	s.Shutdown()

	s = RunServer(&opts)
	defer s.Shutdown()
	nc = clientConnectToServer(t, s)
	defer nc.Close()
	msg, err = nc.Request("fenced.events", []byte("after-restart"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var pubAck JSPubAckResponse
	if err := json.Unmarshal(msg.Data, &pubAck); err != nil {
		t.Fatal(err)
	}
	if pubAck.Error == nil || !strings.Contains(pubAck.Error.Description, "fenced for remote restore") {
		t.Fatalf("publish after restart response: %+v", pubAck)
	}
}

func TestJetStreamS3TierRemoteRestoreActivatesTarget(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	newOptions := func(dir string) Options {
		opts := DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = dir
		opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/MOVE": {
			Store: store, Prefix: "test/remote-restore-target", BlockSize: 16 * 1024, LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29,
			RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
		}}
		return opts
	}
	sourceOpts := newOptions(t.TempDir())
	source := RunServer(&sourceOpts)
	defer source.Shutdown()
	cfg := &StreamConfig{Name: "MOVE", Subjects: []string{"move.target"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	sourceMset, err := source.GlobalAccount().addStream(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceMset.addConsumer(&ConsumerConfig{Durable: "C", AckPolicy: AckExplicit}); err != nil {
		t.Fatal(err)
	}
	sourceNC := clientConnectToServer(t, source)
	defer sourceNC.Close()
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, sourceNC, "move.target", fmt.Sprintf("source-%d", i))
	}
	operation, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "handoff_1"})
	requestSource := func(subject string) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		msg, err := sourceNC.Request(subject, operation, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var response JSApiStreamRemoteRestoreResponse
		if err := json.Unmarshal(msg.Data, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	if response := requestSource(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "MOVE")); response.Error != nil || response.State != s3TierRemoteRestorePrepared {
		t.Fatalf("prepare response: %+v", response)
	}
	checkpoint := requestSource(fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "MOVE"))
	if checkpoint.Error != nil || checkpoint.State != s3TierRemoteRestoreCheckpointed {
		t.Fatalf("checkpoint response: %+v", checkpoint)
	}

	targetOpts := newOptions(t.TempDir())
	target := RunServer(&targetOpts)
	defer target.Shutdown()
	targetNC := clientConnectToServer(t, target)
	defer targetNC.Close()
	restoreRequest, _ := json.Marshal(JSApiStreamRestoreRemoteRequest{OperationID: "handoff_1", ExpectedEpoch: checkpoint.Epoch, TargetID: "node-b"})
	// A failed target download must leave no partial, inactive stream behind.
	// The coordinator can retry the same immutable checkpoint once S3 recovers.
	store.failGet = true
	msg, err := targetNC.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVE"), restoreRequest, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var failedRestore JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(msg.Data, &failedRestore); err != nil {
		t.Fatal(err)
	}
	if failedRestore.Error == nil {
		t.Fatalf("restore during S3 outage response: %+v", failedRestore)
	}
	if _, err := target.GlobalAccount().lookupStream("MOVE"); err == nil {
		t.Fatal("failed target restore retained a stream")
	}
	store.failGet = false
	msg, err = targetNC.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVE"), restoreRequest, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var restored JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(msg.Data, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Error != nil || !restored.Success || restored.State != "active" || restored.TargetID != "node-b" {
		t.Fatalf("restore response: %+v", restored)
	}
	mset, err := target.GlobalAccount().lookupStream("MOVE")
	if err != nil {
		t.Fatal(err)
	}
	if consumer := mset.lookupConsumer("C"); consumer == nil {
		t.Fatal("target did not restore durable consumer")
	}
	statusMsg, err := targetNC.Request(fmt.Sprintf(JSApiStreamRemoteRestoreStatusT, "MOVE"), nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var targetStatus JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(statusMsg.Data, &targetStatus); err != nil {
		t.Fatal(err)
	}
	if targetStatus.Error != nil || targetStatus.State != "active" || targetStatus.TargetID != "node-b" || targetStatus.Consumers != 1 || targetStatus.S3Tier == nil || targetStatus.S3Tier.RestoreActivations == 0 {
		t.Fatalf("target restore status: %+v", targetStatus)
	}
	stored, err := mset.getMsg(10)
	if err != nil || string(stored.Data) != "source-9" {
		t.Fatalf("target recovered message = (%+v, %v)", stored, err)
	}
	sendStreamMsg(t, targetNC, "move.target", "target-11")
	if state := mset.state(); state.LastSeq != 11 {
		t.Fatalf("target state after publish: %+v", state)
	}
	// A lost restore response can be retried by the same logical target.
	msg, err = targetNC.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVE"), restoreRequest, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var retried JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(msg.Data, &retried); err != nil {
		t.Fatal(err)
	}
	if retried.Error != nil || !retried.Success || retried.State != "active" || retried.TargetID != "node-b" {
		t.Fatalf("restore retry response: %+v", retried)
	}
	// A completed source remains readable but cannot become a second writer.
	sourceMset, err = source.GlobalAccount().lookupStream("MOVE")
	if err != nil {
		t.Fatal(err)
	}
	stored, err = sourceMset.getMsg(10)
	if err != nil || string(stored.Data) != "source-9" {
		t.Fatalf("source read after activation = (%+v, %v)", stored, err)
	}
	msg, err = sourceNC.Request("move.target", []byte("source-should-fail"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var sourceAck JSPubAckResponse
	if err := json.Unmarshal(msg.Data, &sourceAck); err != nil {
		t.Fatal(err)
	}
	if sourceAck.Error == nil || !strings.Contains(sourceAck.Error.Description, "fenced for remote restore") {
		t.Fatalf("source publish after activation response: %+v", sourceAck)
	}
	// Retirement requires the durable target activation receipt. It removes only
	// source-local resources; the immutable S3 checkpoint stays available for
	// later audit or restore work.
	retire := requestSource(fmt.Sprintf(JSApiStreamRetireRemoteSourceT, "MOVE"))
	if retire.Error != nil || !retire.Success || retire.State != s3TierRemoteRestoreRetired || retire.TargetID != "node-b" {
		t.Fatalf("retire response: %+v", retire)
	}
	if _, err := source.GlobalAccount().lookupStream("MOVE"); err == nil {
		t.Fatal("retired source still has a stream")
	}
	store.mu.Lock()
	_, checkpointRetained := store.objects[checkpoint.CheckpointKey]
	store.mu.Unlock()
	if !checkpointRetained {
		t.Fatal("source retirement deleted the remote checkpoint")
	}
	// A lost retirement response is idempotent after source-local removal.
	if retry := requestSource(fmt.Sprintf(JSApiStreamRetireRemoteSourceT, "MOVE")); retry.Error != nil || !retry.Success || retry.State != s3TierRemoteRestoreRetired {
		t.Fatalf("retire retry response: %+v", retry)
	}
	otherOpts := newOptions(t.TempDir())
	otherTarget := RunServer(&otherOpts)
	defer otherTarget.Shutdown()
	otherNC := clientConnectToServer(t, otherTarget)
	defer otherNC.Close()
	conflictRequest, _ := json.Marshal(JSApiStreamRestoreRemoteRequest{OperationID: "handoff_1", ExpectedEpoch: checkpoint.Epoch, TargetID: "node-c"})
	msg, err = otherNC.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVE"), conflictRequest, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var conflict JSApiStreamRemoteRestoreResponse
	if err := json.Unmarshal(msg.Data, &conflict); err != nil {
		t.Fatal(err)
	}
	if conflict.Error == nil || !strings.Contains(conflict.Error.Description, "another target") {
		t.Fatalf("conflicting target response: %+v", conflict)
	}
	if _, err := otherTarget.GlobalAccount().lookupStream("MOVE"); err == nil {
		t.Fatal("conflicting target retained an inactive stream")
	}
}

func TestFileStoreS3TierRemoteRestorePrepareAdoptsRemoteIntent(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, fcfg, cfg := testTieredFileStore(t, store, t.TempDir(), "test/remote-restore-intent")
	state, err := fs.tier.prepareRemoteRestore("intent_1")
	if err != nil || state.State != s3TierRemoteRestorePrepared {
		fs.Stop()
		t.Fatalf("prepare = (%+v, %v)", state, err)
	}
	if err := os.Remove(fs.tier.remoteRestoreFile()); err != nil {
		fs.Stop()
		t.Fatal(err)
	}
	fs.Stop()

	// Simulate a crash after S3 committed the intent but before the local
	// journal was durable. Retrying the same operation must adopt the marker.
	fs, err = newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Stop()
	state, err = fs.tier.prepareRemoteRestore("intent_1")
	if err != nil || state.State != s3TierRemoteRestorePrepared || state.Epoch != 1 {
		t.Fatalf("retry prepare = (%+v, %v)", state, err)
	}
}

func TestFileStoreS3TierRejectsWritesWhenOffloadCannotRelievePressure(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/local-pressure")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
	store.mu.Lock()
	store.failPut = true
	store.mu.Unlock()
	if _, _, err := fs.StoreMsg("events", nil, []byte("blocked"), 0); !errors.Is(err, errS3TierLocalCapacity) {
		t.Fatalf("write error = %v, want local capacity error", err)
	}
	store.mu.Lock()
	store.failPut = false
	store.mu.Unlock()
	if _, _, err := fs.StoreMsg("events", nil, []byte("recovered"), 0); err != nil {
		t.Fatalf("write after object store recovery: %v", err)
	}
}

func TestFileStoreS3TierFailureKeepsLocalCopy(t *testing.T) {
	for _, failDescriptor := range []bool{false, true} {
		t.Run(fmt.Sprintf("descriptor=%v", failDescriptor), func(t *testing.T) {
			store := &testS3BlockStore{objects: make(map[string][]byte)}
			fs, fcfg, _ := testTieredFileStore(t, store, t.TempDir(), "test/stream")
			defer fs.Stop()
			fs.tier.stop()
			fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
			for i := 0; i < 100; i++ {
				if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
					t.Fatal(err)
				}
			}
			store.mu.Lock()
			store.failPut, store.failDescriptor = !failDescriptor, failDescriptor
			store.mu.Unlock()
			fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = fcfg.S3Tier.LocalHighBytes, fcfg.S3Tier.LocalLowBytes
			if err := fs.tier.evictToBudget(); err == nil {
				t.Fatal("expected remote failure")
			}
			if _, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, "1.blk")); err != nil {
				t.Fatalf("local block lost after remote failure: %v", err)
			}
		})
	}
}

func TestFileStoreS3TierManifestCommitFailureKeepsLocalCopy(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/manifest-failure")
	defer fs.Stop()
	fs.tier.stop()
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
	store.mu.Lock()
	store.failManifest = true
	store.mu.Unlock()
	if err := fs.tier.evictToBudget(); err == nil {
		t.Fatal("expected manifest commit failure")
	}
	if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")); err != nil {
		t.Fatalf("local block lost after manifest failure: %v", err)
	}
}

func TestFileStoreS3TierWritesImmutableManifest(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/manifest")
	defer fs.Stop()
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	var latest s3TierManifest
	for key, data := range store.objects {
		if strings.Contains(key, "/manifests/") {
			if err := json.Unmarshal(data, &latest); err != nil {
				store.mu.Unlock()
				t.Fatal(err)
			}
		}
	}
	store.mu.Unlock()
	if latest.Version != 2 || latest.Incarnation == "" || latest.Generation == 0 || len(latest.Blocks) == 0 {
		t.Fatalf("invalid manifest: %#v", latest)
	}
	for _, d := range latest.Blocks {
		if !validS3BlockDescriptor(d) {
			t.Fatalf("invalid manifest descriptor: %#v", d)
		}
	}
}

func TestFileStoreS3TierStatsTrackPhysicalTierActivity(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/stats")
	defer fs.Stop()
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	before := fs.S3TierStats()
	if before == nil || before.RemoteBlocks == 0 || before.RemoteBytes == 0 || before.Evictions == 0 || before.Uploads == 0 {
		t.Fatalf("stats after offload: %#v", before)
	}
	if _, err := fs.LoadMsg(1, nil); err != nil {
		t.Fatal(err)
	}
	after := fs.S3TierStats()
	if after.Fetches != before.Fetches+1 || after.LocalBytes <= before.LocalBytes {
		t.Fatalf("stats after cold read: before=%#v after=%#v", before, after)
	}
}

func TestFileStoreS3TierRetriesUploadAndReportsBacklog(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte), failPut: true}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/retry")
	defer fs.Stop()
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	fs.tier.cfg.RemoteHighBytes, fs.tier.cfg.RemoteLowBytes = 1, 0
	fs.tier.cfg.RetryMin, fs.tier.cfg.RetryMax = 5*time.Millisecond, 20*time.Millisecond
	for i := 0; i < 40; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	fs.tier.kick()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := fs.S3TierStats()
		if stats.RetryAttempts > 0 && stats.BacklogBlocks > 0 && stats.BacklogBytes > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stats := fs.S3TierStats()
	if stats.RetryAttempts == 0 || stats.BacklogBlocks == 0 || stats.BacklogBytes == 0 {
		t.Fatalf("retry/backlog metrics = %+v", stats)
	}
	store.mu.Lock()
	store.failPut = false
	store.mu.Unlock()
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats = fs.S3TierStats()
		if stats.BacklogBlocks == 0 && stats.RemoteBlocks > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("remote coverage did not recover after outage: %+v", fs.S3TierStats())
}

func TestFileStoreS3TierUploadTimeoutRetriesAndRecovers(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte), blockPut: true}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/upload-timeout")
	defer fs.Stop()
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	fs.tier.cfg.RemoteHighBytes, fs.tier.cfg.RemoteLowBytes = 1, 0
	fs.tier.cfg.Timeout = 10 * time.Millisecond
	fs.tier.cfg.RetryMin, fs.tier.cfg.RetryMax = 5*time.Millisecond, 20*time.Millisecond
	for i := 0; i < 40; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	fs.tier.kick()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stats := fs.S3TierStats(); stats.RetryAttempts > 0 && stats.UploadErrors > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if stats := fs.S3TierStats(); stats.RetryAttempts == 0 || stats.UploadErrors == 0 {
		t.Fatalf("timeout did not schedule retry: %+v", stats)
	}
	store.mu.Lock()
	store.blockPut = false
	store.mu.Unlock()
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stats := fs.S3TierStats(); stats.BacklogBlocks == 0 && stats.RemoteBlocks > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("remote coverage did not recover after timeout: %+v", fs.S3TierStats())
}

func TestFileStoreS3TierCorruptionAndMissingConfigFailClosed(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, fcfg, cfg := testTieredFileStore(t, store, t.TempDir(), "test/corruption")
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	firstRemote, ok := fs.tier.desc[1]
	if !ok {
		t.Fatal("first block was not offloaded")
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	withoutTier := fcfg
	withoutTier.S3Tier = nil
	if _, err := newFileStore(withoutTier, cfg); err == nil {
		t.Fatal("remote-only stream accepted without tier configuration")
	}
	store.mu.Lock()
	store.objects[firstRemote.Key][0] ^= 0xff
	store.mu.Unlock()
	fs, err := newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatalf("remote-only restart should not require S3: %v", err)
	}
	defer fs.Stop()
	if _, err := fs.LoadMsg(1, nil); !errors.Is(err, errS3TierUnavailable) {
		t.Fatalf("corrupt remote block read error = %v, want unavailable", err)
	}
}

func TestFileStoreS3TierColdReadDoesNotHoldWriteLock(t *testing.T) {
	store := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)},
		entered: make(chan struct{}, 1), release: make(chan struct{})}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/lock")
	defer fs.Stop()
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.blocking = true
	store.mu.Unlock()
	readDone := make(chan error, 1)
	go func() {
		_, _, err := fs.LoadNextMsg("", false, 1, nil)
		readDone <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cold read did not reach S3")
	}
	writeDone := make(chan error, 1)
	go func() {
		_, _, err := fs.StoreMsg("events", nil, []byte("new"), 0)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cold GET blocked a local write")
	}
	close(store.release)
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cold read did not complete")
	}
}

func TestFileStoreS3TierLastBySubjectColdFailure(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	fs, _, _ := testTieredFileStore(t, store, t.TempDir(), "test/last-subject")
	defer fs.Stop()
	if _, _, err := fs.StoreMsg("older", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 99; i++ {
		if _, _, err := fs.StoreMsg("newer", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failGet = true
	store.mu.Unlock()
	if _, err := fs.LoadLastMsg("older", nil); !errors.Is(err, errS3TierUnavailable) {
		t.Fatalf("old subject should report storage outage: %v", err)
	}
	if _, err := fs.LoadLastMsg("newer", nil); err != nil {
		t.Fatalf("new subject should remain local: %v", err)
	}
}

func TestJetStreamS3TierNativePullConsumer(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	cfg := &StreamConfig{Name: "TIER", Subjects: []string{"tier.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true, AllowDirect: true}
	fcfg := &FileStoreConfig{BlockSize: 16 * 1024, S3Tier: &S3TierConfig{
		Store: store, Prefix: "test/native", LocalHighBytes: 32 * 1024, LocalLowBytes: 16 * 1024,
	}}
	mset, err := s.GlobalAccount().addStreamWithStore(cfg, fcfg)
	if err != nil {
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	for i := 1; i <= 100; i++ {
		sendStreamMsg(t, nc, "tier.events", fmt.Sprintf("message-%04d-%s", i, strings.Repeat("x", 1000)))
	}
	fs := mset.store.(*fileStore)
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")); !os.IsNotExist(err) {
		t.Fatalf("oldest block not evicted: %v", err)
	}
	o, err := mset.addConsumer(&ConsumerConfig{Durable: "REPLAY", AckPolicy: AckExplicit, DeliverPolicy: DeliverAll})
	if err != nil {
		t.Fatal(err)
	}
	defer o.delete()
	store.mu.Lock()
	store.failGet = true
	store.mu.Unlock()
	if _, err := fs.LoadMsg(1, nil); !errors.Is(err, errS3TierUnavailable) {
		t.Fatalf("cold read outage should be retryable storage error: %v", err)
	}
	inbox := nats.NewInbox()
	replies := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) { replies <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.PublishRequest(fmt.Sprintf(JSDirectMsgGetT, "TIER"), inbox, []byte(`{"seq":1}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case get := <-replies:
		if get.Header.Get("Status") != "503" || get.Header.Get("Description") != "Storage Unavailable" {
			t.Fatalf("direct get outage should report storage 503: reply=%v", get)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("direct get outage response timed out")
	}
	o.mu.Lock()
	_, _, readErr := o.getNextMsg()
	nextSeq := o.sseq
	o.mu.Unlock()
	if !errors.Is(readErr, errS3TierUnavailable) || nextSeq != 1 {
		t.Fatalf("consumer outage result: next sequence %d, error %v", nextSeq, readErr)
	}
	store.mu.Lock()
	store.failGet = false
	store.mu.Unlock()
	for i := 1; i <= 100; i++ {
		msg, err := nc.Request(o.requestNextMsgSubject(), nil, 5*time.Second)
		if err != nil {
			t.Fatalf("pull %d: %v", i, err)
		}
		if !strings.HasPrefix(string(msg.Data), fmt.Sprintf("message-%04d-", i)) {
			t.Fatalf("pull %d returned %q", i, msg.Data)
		}
		if err := nc.Publish(msg.Reply, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFileStoreS3TierCompatibleEndpoint(t *testing.T) {
	endpoint := os.Getenv("TEST_S3_TIER_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TEST_S3_TIER_ENDPOINT to an S3-compatible endpoint")
	}
	store, err := NewS3TierMinIOStore(endpoint, "testaccess", "testsecret123", "tier-test", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.client.MakeBucket(context.Background(), store.bucket, minio.MakeBucketOptions{}); err != nil {
		if exists, existsErr := store.client.BucketExists(context.Background(), store.bucket); existsErr != nil || !exists {
			t.Fatal(err)
		}
	}
	fs, fcfg, cfg := testTieredFileStore(t, store, t.TempDir(), fmt.Sprintf("integration/%d", time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, "1.blk")); !os.IsNotExist(err) {
		t.Fatalf("expected S3-backed block eviction, stat: %v", err)
	}
	if _, err := fs.LoadMsg(1, nil); err != nil {
		t.Fatalf("S3 cold read: %v", err)
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	fs, err = newFileStore(fcfg, cfg)
	if err != nil {
		t.Fatalf("S3 restart: %v", err)
	}
	defer fs.Stop()
	if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 1 {
		t.Fatalf("S3 recovered state: %+v", state)
	}
}

func TestJetStreamS3TierServerRestart(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/RESTART": {
		Store: store, Prefix: "test/server-restart", BlockSize: 16 * 1024, LocalHighBytes: 32 * 1024, LocalLowBytes: 16 * 1024,
	}}
	s := RunServer(&opts)
	cfg := &StreamConfig{Name: "RESTART", Subjects: []string{"restart.events"}, Storage: FileStorage,
		Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
	mset, err := s.GlobalAccount().addStream(cfg)
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	nc := clientConnectToServer(t, s)
	for i := 1; i <= 100; i++ {
		sendStreamMsg(t, nc, "restart.events", fmt.Sprintf("message-%04d-%s", i, strings.Repeat("x", 1000)))
	}
	fs := mset.store.(*fileStore)
	if err := fs.tier.evictToBudget(); err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")); !os.IsNotExist(err) {
		nc.Close()
		s.Shutdown()
		t.Fatalf("oldest block not evicted before restart: %v", err)
	}
	nc.Close()
	s.Shutdown()
	s = RunServer(&opts)
	defer s.Shutdown()
	mset, err = s.GlobalAccount().lookupStream("RESTART")
	if err != nil {
		t.Fatalf("recovered stream: %v", err)
	}
	if state := mset.state(); state.Msgs != 100 || state.FirstSeq != 1 {
		t.Fatalf("recovered stream state: %+v", state)
	}
	o, err := mset.addConsumer(&ConsumerConfig{Durable: "REPLAY", AckPolicy: AckExplicit, DeliverPolicy: DeliverAll})
	if err != nil {
		t.Fatal(err)
	}
	defer o.delete()
	nc = clientConnectToServer(t, s)
	defer nc.Close()
	for i := 1; i <= 100; i++ {
		msg, err := nc.Request(o.requestNextMsgSubject(), nil, 5*time.Second)
		if err != nil {
			t.Fatalf("restarted pull %d: %v", i, err)
		}
		if !strings.HasPrefix(string(msg.Data), fmt.Sprintf("message-%04d-", i)) {
			t.Fatalf("restarted pull %d returned %q", i, msg.Data)
		}
		if err := nc.Publish(msg.Reply, nil); err != nil {
			t.Fatal(err)
		}
	}
}
