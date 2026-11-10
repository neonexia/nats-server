// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"bytes"
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

func TestJetStreamS3TierRestoreRespectsLocalBudget(t *testing.T) {
	testRemoteRestoreLocalBudget(t, &testS3BlockStore{objects: make(map[string][]byte)})
}

func TestJetStreamS3TierRestoreRespectsLocalBudgetCompatibleEndpoint(t *testing.T) {
	testRemoteRestoreLocalBudget(t, testS3TierCompatibleStore(t))
}

func testRemoteRestoreLocalBudget(t *testing.T, store S3TierObjectStore, beforeRestore ...func(*nats.Conn, []byte)) {
	prefix := fmt.Sprintf("restore-budget/%d", time.Now().UnixNano())
	options := func(high uint64) Options {
		o := DefaultTestOptions
		o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
		o.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/BUDGET": {Store: store, Prefix: prefix, BlockSize: 16 * 1024, LocalHighBytes: high, LocalLowBytes: high / 2, RemoteHighBytes: 16 << 20, RemoteLowBytes: 8 << 20}}
		return o
	}
	so, to := options(16<<20), options(128<<10)
	source, target := RunServer(&so), RunServer(&to)
	defer source.Shutdown()
	defer target.Shutdown()
	sn, tn := clientConnectToServer(t, source), clientConnectToServer(t, target)
	defer sn.Close()
	defer tn.Close()
	js, err := sn.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "BUDGET", Subjects: []string{"budget"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	payload := bytes.Repeat([]byte("b"), 8<<10)
	for i := 0; i < 64; i++ {
		_, err = js.Publish("budget", payload)
		require_NoError(t, err)
	}
	body, _ := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: "move"})
	var checkpoint JSApiStreamRemoteRestoreResponse
	for _, subject := range []string{JSApiStreamPrepareRemoteRestoreT, JSApiStreamCheckpointRemoteRestoreT} {
		msg, err := sn.Request(fmt.Sprintf(subject, "BUDGET"), body, 5*time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &checkpoint))
		if checkpoint.Error != nil {
			t.Fatal(checkpoint.Error)
		}
	}
	body, _ = json.Marshal(JSApiStreamRestoreRemoteRequest{OperationID: "move", ExpectedEpoch: checkpoint.Epoch, TargetID: "target"})
	for _, hook := range beforeRestore {
		hook(tn, body)
	}
	msg, err := tn.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "BUDGET"), body, 10*time.Second)
	require_NoError(t, err)
	var restored JSApiStreamRemoteRestoreResponse
	require_NoError(t, json.Unmarshal(msg.Data, &restored))
	if restored.Error != nil || !restored.Success {
		t.Fatalf("restore: %+v", restored)
	}
	targetJS, err := tn.JetStream()
	require_NoError(t, err)
	// An active target must admit a new message immediately, even when retained
	// history exceeds its residency budget. Background eviction is not readiness.
	ack, err := targetJS.Publish("budget", payload)
	require_NoError(t, err)
	if ack.Sequence != 65 {
		t.Fatalf("new sequence = %d", ack.Sequence)
	}
	for seq := uint64(1); seq <= 65; seq++ {
		m, err := targetJS.GetMsg("BUDGET", seq)
		require_NoError(t, err)
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("corrupt sequence %d", seq)
		}
	}
}

// The first read fails only after another is in flight. Restore must cancel
// those reads and finish cleanup before responding, so a retry is safe.
type restoreInterruptedReads struct {
	S3TierObjectStore
	mu                        sync.Mutex
	armed                     bool
	started, active, canceled int
	overlapped                chan struct{}
}

func (s *restoreInterruptedReads) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	if !s.armed || !strings.HasSuffix(key, ".blk") {
		s.mu.Unlock()
		return s.S3TierObjectStore.Get(ctx, key)
	}
	s.started++
	first := s.started == 1
	s.active++
	if s.started == 2 {
		close(s.overlapped)
	}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	if first {
		select {
		case <-s.overlapped:
			return nil, errors.New("injected payload read failure")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	<-ctx.Done()
	s.mu.Lock()
	s.canceled++
	s.mu.Unlock()
	return nil, ctx.Err()
}

func TestJetStreamS3TierRestoreCancelsPayloadReadsBeforeRetry(t *testing.T) {
	store := &restoreInterruptedReads{S3TierObjectStore: &testS3BlockStore{objects: make(map[string][]byte)}, overlapped: make(chan struct{})}
	testRemoteRestoreLocalBudget(t, store, func(nc *nats.Conn, body []byte) {
		store.mu.Lock()
		store.armed = true
		store.mu.Unlock()
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "BUDGET"), body, 10*time.Second)
		require_NoError(t, err)
		var response JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(msg.Data, &response))
		if response.Success || response.Error == nil || !strings.Contains(response.Error.Description, "injected payload read failure") {
			t.Fatalf("restore should report payload failure: %+v", response)
		}
		store.mu.Lock()
		active, canceled := store.active, store.canceled
		store.armed = false
		store.mu.Unlock()
		if active != 0 || canceled == 0 {
			t.Fatalf("restore returned before canceling reads: active=%d canceled=%d", active, canceled)
		}
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.StreamInfo("BUDGET")
		if !errors.Is(err, nats.ErrStreamNotFound) {
			t.Fatalf("failed restore exposed target: %v", err)
		}
	})
}
