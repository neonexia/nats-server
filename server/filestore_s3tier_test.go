// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

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
