// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"context"
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
	failGet        bool
}

type blockingS3BlockStore struct {
	*testS3BlockStore
	mu       sync.Mutex
	blocking bool
	entered  chan struct{}
	release  chan struct{}
}

func (s *blockingS3BlockStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	blocking := s.blocking && strings.Contains(key, "/blocks/")
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

func (s *testS3BlockStore) Put(_ context.Context, key string, buf []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failPut || (s.failDescriptor && strings.Contains(key, "/descriptors/")) {
		return errors.New("injected PUT failure")
	}
	s.objects[key] = append([]byte(nil), buf...)
	return nil
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

func TestFileStoreS3TierFailureKeepsLocalCopy(t *testing.T) {
	for _, failDescriptor := range []bool{false, true} {
		t.Run(fmt.Sprintf("descriptor=%v", failDescriptor), func(t *testing.T) {
			store := &testS3BlockStore{objects: make(map[string][]byte), failPut: !failDescriptor, failDescriptor: failDescriptor}
			fs, fcfg, _ := testTieredFileStore(t, store, t.TempDir(), "test/stream")
			defer fs.Stop()
			for i := 0; i < 100; i++ {
				if _, _, err := fs.StoreMsg("events", nil, []byte(strings.Repeat("x", 1000)), 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := fs.tier.evictToBudget(); err == nil {
				t.Fatal("expected remote failure")
			}
			if _, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, "1.blk")); err != nil {
				t.Fatalf("local block lost after remote failure: %v", err)
			}
		})
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
	if _, err := newFileStore(fcfg, cfg); err == nil {
		t.Fatal("corrupt remote block accepted during recovery")
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
