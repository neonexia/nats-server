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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type delayedRestoreStore struct {
	S3TierObjectStore
	once             sync.Once
	entered, release chan struct{}
}

func (s *delayedRestoreStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, "/manifests/") {
		s.once.Do(func() { close(s.entered); <-s.release })
	}
	return s.S3TierObjectStore.Get(ctx, key)
}

func TestJetStreamS3TierConcurrentRestoreRetries(t *testing.T) {
	testConcurrentRestoreRetries(t, &testS3BlockStore{objects: make(map[string][]byte)})
}
func TestJetStreamS3TierConcurrentRestoreRetriesCompatibleEndpoint(t *testing.T) {
	testConcurrentRestoreRetries(t, testS3TierCompatibleStore(t))
}
func testConcurrentRestoreRetries(t *testing.T, base S3TierObjectStore) {
	store := &delayedRestoreStore{S3TierObjectStore: base, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(store.release) })
	prefix := fmt.Sprintf("integration/concurrent-restore/%d", time.Now().UnixNano())
	options := func(remote S3TierObjectStore) Options {
		o := DefaultTestOptions
		o.Port = -1
		o.JetStream = true
		o.StoreDir = t.TempDir()
		o.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/RETRY": {Store: remote, Prefix: prefix, BlockSize: 16384, LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29, RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29}}
		return o
	}
	so, to := options(base), options(store)
	source, target := RunServer(&so), RunServer(&to)
	defer source.Shutdown()
	defer func() { release.Do(func() { close(store.release) }); target.Shutdown() }()
	sn, tn := clientConnectToServer(t, source), clientConnectToServer(t, target)
	defer sn.Close()
	defer tn.Close()
	js, err := sn.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "RETRY", Subjects: []string{"retry"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	_, err = js.Publish("retry", []byte("preserved"))
	require_NoError(t, err)
	body, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "retry"})
	var checkpoint JSApiStreamRemoteRestoreResponse
	for _, subject := range []string{JSApiStreamPrepareRemoteRestoreT, JSApiStreamCheckpointRemoteRestoreT} {
		msg, err := sn.Request(fmt.Sprintf(subject, "RETRY"), body, 5*time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &checkpoint))
		if checkpoint.Error != nil {
			t.Fatal(checkpoint.Error)
		}
	}
	body, _ = json.Marshal(JSApiStreamRestoreRemoteRequest{OperationID: "retry", ExpectedEpoch: checkpoint.Epoch, TargetID: "target"})
	results := make(chan error, 9)
	request := func() {
		msg, err := tn.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "RETRY"), body, 10*time.Second)
		if err == nil {
			var r JSApiStreamRemoteRestoreResponse
			err = json.Unmarshal(msg.Data, &r)
			if err == nil && (r.Error != nil || !r.Success || r.State != "active") {
				err = fmt.Errorf("restore response: %+v", r)
			}
		}
		results <- err
	}
	go request()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not reach delayed storage read")
	}
	for i := 0; i < 8; i++ {
		go request()
	}
	// Repeated client requests overlap the still-running target activation.
	time.Sleep(100 * time.Millisecond)
	release.Do(func() { close(store.release) })
	for i := 0; i < 9; i++ {
		require_NoError(t, <-results)
	}
	msg, err := js.GetMsg("RETRY", 1)
	require_NoError(t, err)
	if string(msg.Data) != "preserved" {
		t.Fatalf("source payload %q", msg.Data)
	}
	targetJS, err := tn.JetStream()
	require_NoError(t, err)
	msg, err = targetJS.GetMsg("RETRY", 1)
	require_NoError(t, err)
	if string(msg.Data) != "preserved" {
		t.Fatalf("restored payload %q", msg.Data)
	}
}
