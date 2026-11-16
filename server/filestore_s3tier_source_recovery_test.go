// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamS3TierPreparedSourceRecoversConsumersAfterCrashCompatibleEndpoint(t *testing.T) {
	testS3TierCompatibleStore(t)
	prefix := fmt.Sprintf("source-consumer-recovery/%d", time.Now().UnixNano())
	dir := t.TempDir()
	url, kill := startStagedRestoreProcess(t, dir, prefix)
	nc, err := nats.Connect(url, nats.NoReconnect())
	require_NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "MOVING", Subjects: []string{"MOVING"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = js.Publish("MOVING", []byte(fmt.Sprintf("message-%d", i)))
		require_NoError(t, err)
	}
	_, err = js.AddConsumer("MOVING", &nats.ConsumerConfig{Durable: "worker", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Hour})
	require_NoError(t, err)
	reader, err := js.PullSubscribe("MOVING", "worker", nats.Bind("MOVING", "worker"))
	require_NoError(t, err)
	batch, err := reader.Fetch(2, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_NoError(t, batch[0].AckSync())
	call := func(connection *nats.Conn, subject string, body any) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		data, err := json.Marshal(body)
		require_NoError(t, err)
		reply, err := connection.Request(subject, data, 10*time.Second)
		require_NoError(t, err)
		var receipt JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(reply.Data, &receipt))
		if receipt.Error != nil || !receipt.Success {
			t.Fatalf("%s: %+v", subject, receipt)
		}
		return receipt
	}
	prepared := call(nc, fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "MOVING"), map[string]any{"operation_id": "source-restart"})
	kill()
	url, _ = startStagedRestoreProcess(t, dir, prefix)
	recovered, err := nats.Connect(url, nats.NoReconnect())
	require_NoError(t, err)
	defer recovered.Close()
	rjs, err := recovered.JetStream()
	require_NoError(t, err)
	info, err := rjs.ConsumerInfo("MOVING", "worker")
	require_NoError(t, err)
	if info.AckFloor.Stream != 1 || info.Delivered.Stream != 2 || info.NumAckPending != 1 {
		t.Fatalf("recovered consumer changed boundary: %+v", info)
	}
	_, err = rjs.AddConsumer("MOVING", &nats.ConsumerConfig{Durable: "foreign", AckPolicy: nats.AckExplicitPolicy})
	if err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("expected consumer fence, got %v", err)
	}
	// Old ACK tokens must not advance the recovered source behind the fence.
	_, err = recovered.Request(batch[1].Reply, nil, 50*time.Millisecond)
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("late ACK unexpectedly settled: %v", err)
	}
	info, err = rjs.ConsumerInfo("MOVING", "worker")
	require_NoError(t, err)
	if info.AckFloor.Stream != 1 || info.NumAckPending != 1 {
		t.Fatalf("late ACK crossed recovered source fence: %+v", info)
	}
	checkpoint := call(recovered, fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "MOVING"), map[string]any{"operation_id": "source-restart"})
	if checkpoint.Epoch != prepared.Epoch {
		t.Fatal("checkpoint changed source epoch")
	}
	targetURL, _ := startStagedRestoreProcess(t, t.TempDir(), prefix)
	target, err := nats.Connect(targetURL)
	require_NoError(t, err)
	defer target.Close()
	call(target, fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), map[string]any{"operation_id": "source-restart", "expected_epoch": checkpoint.Epoch, "target_id": "target"})
	tjs, err := target.JetStream()
	require_NoError(t, err)
	info, err = tjs.ConsumerInfo("MOVING", "worker")
	require_NoError(t, err)
	if info.AckFloor.Stream != 1 || info.Delivered.Stream != 2 || info.NumAckPending != 1 {
		t.Fatalf("checkpoint omitted recovered consumer: %+v", info)
	}
}
