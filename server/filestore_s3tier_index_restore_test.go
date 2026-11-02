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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The same read faults wrap either the deterministic store or a real endpoint;
// successful operations always use the selected store's ordinary S3 path.
type s3IndexRestoreFaultStore struct {
	S3TierObjectStore
	mu        sync.Mutex
	key, mode string
	offline   bool
	reads     int
}

func (s *s3IndexRestoreFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.reads++
	offline, mode := s.offline, ""
	if key == s.key {
		mode = s.mode
	}
	s.mu.Unlock()
	if offline || mode == "missing" {
		return nil, errors.New("injected unavailable index object")
	}
	data, err := s.S3TierObjectStore.Get(ctx, key)
	if err == nil && mode == "corrupt" {
		data = append([]byte(nil), data...)
		if len(data) > 0 {
			data[len(data)-1] ^= 1
		}
	}
	return data, err
}

func (s *s3IndexRestoreFaultStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.reads++
	offline := s.offline
	s.mu.Unlock()
	if offline {
		return nil, errors.New("injected object-store outage")
	}
	return s.S3TierObjectStore.List(ctx, prefix)
}

func TestJetStreamS3TierPagedIndexRestoreFailures(t *testing.T) {
	testJetStreamS3TierPagedIndexRestoreFailures(t, &testS3BlockStore{objects: make(map[string][]byte)})
}

func TestJetStreamS3TierPagedIndexRestoreFailuresCompatibleEndpoint(t *testing.T) {
	testJetStreamS3TierPagedIndexRestoreFailures(t, testS3TierCompatibleStore(t))
}

func testJetStreamS3TierPagedIndexRestoreFailures(t *testing.T, base S3TierObjectStore) {
	t.Helper()
	store := &s3IndexRestoreFaultStore{S3TierObjectStore: base}
	prefix := fmt.Sprintf("integration/paged-restore/%d", time.Now().UnixNano())
	options := func() Options {
		opts := DefaultTestOptions
		opts.Port, opts.JetStream, opts.StoreDir = -1, true, t.TempDir()
		opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/PAGED": {
			Store: store, Prefix: prefix, BlockSize: 16 * 1024,
			LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29, RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
		}}
		return opts
	}
	sourceOpts, targetOpts := options(), options()
	source, target := RunServer(&sourceOpts), RunServer(&targetOpts)
	defer source.Shutdown()
	defer func() { target.Shutdown() }()
	sourceNC, targetNC := clientConnectToServer(t, source), clientConnectToServer(t, target)
	defer sourceNC.Close()
	defer func() { targetNC.Close() }()
	sourceJS, err := sourceNC.JetStream()
	require_NoError(t, err)
	_, err = sourceJS.AddStream(&nats.StreamConfig{Name: "PAGED", Subjects: []string{"paged"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	_, err = sourceJS.AddConsumer("PAGED", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	const messages = 80
	payload := strings.Repeat("paged-index-", 1600)
	for i := 0; i < messages; i++ {
		ack, err := sourceJS.Publish("paged", []byte(fmt.Sprintf("%d:%s", i, payload)))
		require_NoError(t, err)
		if ack.Sequence != uint64(i+1) {
			t.Fatalf("publication sequence = %d, want %d", ack.Sequence, i+1)
		}
	}
	call := func(nc *nats.Conn, subject string, body any) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		data, err := json.Marshal(body)
		require_NoError(t, err)
		message, err := nc.Request(subject, data, time.Minute)
		require_NoError(t, err)
		var response JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(message.Data, &response))
		return response
	}
	request := JSApiStreamRemoteRestoreRequest{OperationID: "paged-failure-retry"}
	prepared := call(sourceNC, fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "PAGED"), request)
	if prepared.Error != nil || prepared.State != s3TierRemoteRestorePrepared {
		t.Fatalf("prepare: %+v", prepared)
	}
	checkpoint := call(sourceNC, fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "PAGED"), request)
	if checkpoint.Error != nil || checkpoint.State != s3TierRemoteRestoreCheckpointed {
		t.Fatalf("checkpoint: %+v", checkpoint)
	}
	checkpointData, err := base.Get(context.Background(), checkpoint.CheckpointKey)
	require_NoError(t, err)
	var frozen s3TierRemoteRestoreCheckpoint
	require_NoError(t, json.Unmarshal(checkpointData, &frozen))
	rootData, err := base.Get(context.Background(), frozen.MessageCheckpoint.ManifestKey)
	require_NoError(t, err)
	var root s3TierManifest
	require_NoError(t, json.Unmarshal(rootData, &root))
	if root.Root == nil || root.Root.Level == 0 {
		t.Fatal("fixture did not produce a multi-page index")
	}
	restore := JSApiStreamRestoreRemoteRequest{OperationID: request.OperationID, ExpectedEpoch: checkpoint.Epoch, TargetID: "paged-target"}
	targetJS, err := targetNC.JetStream()
	require_NoError(t, err)
	for _, mode := range []string{"missing", "corrupt"} {
		store.mu.Lock()
		store.key, store.mode = root.Root.Key, mode
		store.mu.Unlock()
		rejected := call(targetNC, fmt.Sprintf(JSApiStreamRestoreRemoteT, "PAGED"), restore)
		if rejected.Error == nil || !strings.Contains(rejected.Error.Description, "index") {
			t.Fatalf("restore with %s page: %+v", mode, rejected)
		}
		if _, err := targetJS.StreamInfo("PAGED"); err != nats.ErrStreamNotFound {
			t.Fatalf("failed restore exposed stream: %v", err)
		}
	}
	store.mu.Lock()
	store.mode = ""
	store.mu.Unlock()
	restored := call(targetNC, fmt.Sprintf(JSApiStreamRestoreRemoteT, "PAGED"), restore)
	if restored.Error != nil || !restored.Success || restored.State != "active" {
		t.Fatalf("restore after repair: %+v", restored)
	}
	// A lost successful response must not repeat activation or mutate the cursor.
	retried := call(targetNC, fmt.Sprintf(JSApiStreamRestoreRemoteT, "PAGED"), restore)
	if retried.Error != nil || !retried.Success || retried.Epoch != restored.Epoch {
		t.Fatalf("restore retry: %+v", retried)
	}
	for i := 0; i < messages; i++ {
		message, err := targetJS.GetMsg("PAGED", uint64(i+1))
		require_NoError(t, err)
		if string(message.Data) != fmt.Sprintf("%d:%s", i, payload) {
			t.Fatalf("restored payload %d differs", i+1)
		}
	}
	if _, err := targetJS.ConsumerInfo("PAGED", "C"); err != nil {
		t.Fatalf("restored durable: %v", err)
	}
	// New publication may build a different root, but the old checkpoint remains
	// a self-contained read view. No latest-manifest listing participates in it.
	ack, err := targetJS.Publish("paged", []byte("after-checkpoint"))
	require_NoError(t, err)
	if ack.Sequence != messages+1 {
		t.Fatalf("post-restore sequence = %d", ack.Sequence)
	}
	drain, err := targetNC.Request(fmt.Sprintf(JSApiStreamDrainRemoteT, "PAGED"), nil, time.Minute)
	require_NoError(t, err)
	var drained JSApiStreamDrainRemoteResponse
	require_NoError(t, json.Unmarshal(drain.Data, &drained))
	if drained.Error != nil {
		t.Fatalf("drain new root: %+v", drained)
	}
	reader := &fileS3Tier{cfg: S3TierConfig{Store: store, Prefix: prefix, Timeout: time.Second * 30}}
	blocks, _, err := reader.loadManifestIndex(root)
	require_NoError(t, err)
	if len(blocks) == 0 || blocks[len(blocks)-1].LastSeq != messages {
		t.Fatalf("old root includes newer tail: %+v", blocks)
	}
	unchanged, err := base.Get(context.Background(), frozen.MessageCheckpoint.ManifestKey)
	require_NoError(t, err)
	if string(unchanged) != string(rootData) {
		t.Fatal("new upload changed checkpoint root")
	}
	targetNC.Close()
	target.Shutdown()
	store.mu.Lock()
	store.offline = true
	store.reads = 0
	store.mu.Unlock()
	target = RunServer(&targetOpts)
	targetNC = clientConnectToServer(t, target)
	targetJS, err = targetNC.JetStream()
	require_NoError(t, err)
	message, err := targetJS.GetMsg("PAGED", messages+1)
	require_NoError(t, err)
	if string(message.Data) != "after-checkpoint" {
		t.Fatalf("restarted tail = %q", message.Data)
	}
	store.mu.Lock()
	reads := store.reads
	store.offline = false
	store.mu.Unlock()
	if reads != 0 {
		t.Fatalf("ordinary restart contacted object store %d times", reads)
	}
}
