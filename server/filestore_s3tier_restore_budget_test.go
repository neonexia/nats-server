// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func testRemoteRestoreLocalBudget(t *testing.T, store S3TierObjectStore) {
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
