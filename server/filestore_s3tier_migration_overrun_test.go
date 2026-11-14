// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Consumer metadata counts against migration admission just like payload files.
// A failed staging attempt must not borrow the occupied engine's sibling budget.
func TestJetStreamS3TierMigrationMetadataOverrunPreservesSiblingCompatibleEndpoint(t *testing.T) {
	store := testS3TierCompatibleStore(t)
	prefix := fmt.Sprintf("migration-metadata-overrun/%d", time.Now().UnixNano())
	o := DefaultTestOptions
	o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
	o.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/MOVING": {Store: store, Prefix: prefix, BlockSize: 16 << 10, LocalHighBytes: 128 << 10, LocalLowBytes: 64 << 10, RemoteHighBytes: 256 << 10, RemoteLowBytes: 128 << 10}}
	source := RunServer(&o)
	t.Cleanup(source.Shutdown)
	sn := clientConnectToServer(t, source)
	defer sn.Close()
	sj, err := sn.JetStream()
	require_NoError(t, err)
	targetDir := t.TempDir()
	url, killTarget := startStagedRestoreProcess(t, targetDir, prefix)
	tn, err := nats.Connect(url)
	require_NoError(t, err)
	defer tn.Close()
	tj, err := tn.JetStream()
	require_NoError(t, err)
	for name, js := range map[string]nats.JetStreamContext{"MOVING": sj, "SIBLING": tj} {
		_, err = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
		require_NoError(t, err)
	}
	_, err = sj.Publish("MOVING", []byte("small-payload"))
	require_NoError(t, err)
	for i := 0; i < 64; i++ {
		_, err = sj.AddConsumer("MOVING", &nats.ConsumerConfig{Durable: fmt.Sprintf("worker%d", i), Description: strings.Repeat("x", 2048), AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}
	call := func(nc *nats.Conn, subject string, body any) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		b, err := json.Marshal(body)
		require_NoError(t, err)
		msg, err := nc.Request(subject, b, 20*time.Second)
		require_NoError(t, err)
		var r JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(msg.Data, &r))
		return r
	}
	var epoch uint64
	op := JSApiStreamRemoteRestoreRequest{OperationID: "metadata-overrun"}
	for _, subject := range []string{JSApiStreamPrepareRemoteRestoreT, JSApiStreamCheckpointRemoteRestoreT} {
		r := call(sn, fmt.Sprintf(subject, "MOVING"), op)
		if r.Error != nil || !r.Success {
			t.Fatalf("source barrier: %+v", r)
		}
		if subject == JSApiStreamCheckpointRemoteRestoreT {
			epoch = r.Epoch
		}
	}
	r := call(tn, JSApiMigrationReserve, JSApiMigrationReservationRequest{OperationID: op.OperationID, Streams: []string{"MOVING"}, LocalBytes: 64 << 10, SiblingHeadroomBytes: 1 << 20})
	if r.Error != nil || !r.Success {
		t.Fatalf("reserve: %+v", r)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if attempt == 1 {
			// Restart must retain the same finite ceiling, not turn a failed
			// attempt into an unreserved restore on retry.
			killTarget()
			tn.Close()
			url, _ = startStagedRestoreProcess(t, targetDir, prefix)
			tn, err = nats.Connect(url)
			require_NoError(t, err)
			defer tn.Close()
			tj, err = tn.JetStream()
			require_NoError(t, err)
		}
		r = call(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), map[string]any{"operation_id": op.OperationID, "expected_epoch": epoch, "target_id": "occupied", "stage_only": true, "restore_limits": S3TierRestoreLimits{PayloadBytes: 64 << 10, BytesPerSecond: 1 << 20, BurstBytes: 4096, Concurrency: 1}})
		if r.Success || r.Error == nil {
			t.Fatalf("over-budget staging accepted: %+v", r)
		}
		if !strings.Contains(r.Error.Description, ErrMigrationBudgetExceeded.Error()) {
			t.Fatalf("expected budget refusal: %+v", r.Error)
		}
		payload := []byte(fmt.Sprintf("sibling-%d", attempt))
		ack, err := tj.Publish("SIBLING", payload)
		require_NoError(t, err)
		message, err := tj.GetMsg("SIBLING", ack.Sequence)
		require_NoError(t, err)
		if string(message.Data) != string(payload) {
			t.Fatalf("sibling payload: %q", message.Data)
		}
		if _, err := tj.Publish("MOVING", []byte("must-remain-fenced"), nats.AckWait(100*time.Millisecond)); err == nil {
			t.Fatal("failed migration became writable")
		}
	}
}
