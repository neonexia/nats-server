// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"strings"
	"testing"
	"time"
)

func TestJetStreamS3TierSourceLimitsSurviveRestartAndIsolateSibling(t *testing.T) {
	store := testS3TierCompatibleStore(t)
	o := DefaultTestOptions
	o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
	o.JetStreamS3Tiers = map[string]*S3TierConfig{}
	prefix := fmt.Sprintf("source-limits/%d", time.Now().UnixNano())
	for _, name := range []string{"MOVING", "SIBLING", "CANCEL"} {
		o.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: store, Prefix: prefix + "/" + name, BlockSize: 1 << 20, LocalHighBytes: 64 << 20, LocalLowBytes: 32 << 20, RemoteHighBytes: 64 << 20, RemoteLowBytes: 32 << 20}
	}
	source := RunServer(&o)
	defer func() { source.Shutdown() }()
	nc := clientConnectToServer(t, source)
	defer func() { nc.Close() }()
	js, err := nc.JetStream()
	require_NoError(t, err)
	for _, name := range []string{"MOVING", "SIBLING", "CANCEL"} {
		_, err = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
		require_NoError(t, err)
	}
	_, err = js.Publish("MOVING", []byte(strings.Repeat("x", 256<<10)))
	require_NoError(t, err)
	call := func(stream string, prepare bool, request JSApiStreamRemoteRestoreRequest) (JSApiStreamRemoteRestoreResponse, error) {
		subject := fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, stream)
		if prepare {
			subject = fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, stream)
		}
		body, _ := json.Marshal(request)
		reply, err := nc.Request(subject, body, 15*time.Second)
		var result JSApiStreamRemoteRestoreResponse
		if err != nil {
			return result, err
		}
		if err = json.Unmarshal(reply.Data, &result); err != nil {
			return result, err
		}
		if result.Error != nil {
			return result, result.Error
		}
		return result, nil
	}
	limits := S3TierSourceLimits{BytesPerSecond: 128 << 10, BurstBytes: 4096}
	moving := JSApiStreamRemoteRestoreRequest{OperationID: "limited", SourceLimits: &limits}
	shared := limits
	shared.Concurrency = 1
	if _, err := call("MOVING", true, JSApiStreamRemoteRestoreRequest{OperationID: "limited", SourceLimits: &shared}); err == nil || !strings.Contains(err.Error(), "shared source migration budget required") {
		t.Fatalf("aggregate ceiling accepted without shared disk budget: %v", err)
	}
	_, err = call("MOVING", true, moving)
	require_NoError(t, err)
	nc.Close()
	source.Shutdown()
	source = RunServer(&o)
	nc = clientConnectToServer(t, source)
	js, err = nc.JetStream()
	require_NoError(t, err)
	// Restart cannot silently remove or widen the already prepared ceiling.
	invalid := moving
	invalid.SourceLimits = nil
	if _, err = call("MOVING", true, invalid); err == nil {
		t.Fatal("removed durable source ceiling")
	}
	widened := limits
	widened.BytesPerSecond *= 2
	invalid.SourceLimits = &widened
	if _, err = call("MOVING", true, invalid); err == nil {
		t.Fatal("widened durable source ceiling")
	}
	_, err = call("MOVING", true, moving)
	require_NoError(t, err)
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := call("MOVING", false, JSApiStreamRemoteRestoreRequest{OperationID: "limited"})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	_, err = js.Publish("SIBLING", []byte("unaffected"))
	require_NoError(t, err)
	sibling := JSApiStreamRemoteRestoreRequest{OperationID: "sibling"}
	_, err = call("SIBLING", true, sibling)
	require_NoError(t, err)
	_, err = call("SIBLING", false, sibling)
	require_NoError(t, err)
	select {
	case err := <-done:
		t.Fatalf("source completed before sibling checkpoint: %v", err)
	default:
	}
	require_NoError(t, <-done)
	if elapsed := time.Since(started); elapsed < 1700*time.Millisecond {
		t.Fatalf("256KiB checkpoint escaped 128KiB/s ceiling: %s", elapsed)
	}
	message, err := js.GetMsg("MOVING", 1)
	require_NoError(t, err)
	if len(message.Data) != 256<<10 {
		t.Fatalf("payload size=%d", len(message.Data))
	}

	// Shutdown must cancel a rate-limited checkpoint instead of waiting hours
	// for a deliberately tiny operator ceiling.
	_, err = js.Publish("CANCEL", []byte(strings.Repeat("y", 256<<10)))
	require_NoError(t, err)
	cancelRequest := JSApiStreamRemoteRestoreRequest{OperationID: "cancel", SourceLimits: &S3TierSourceLimits{BytesPerSecond: 32, BurstBytes: 4096}}
	_, err = call("CANCEL", true, cancelRequest)
	require_NoError(t, err)
	cancelled := make(chan error, 1)
	go func() {
		_, err := call("CANCEL", false, JSApiStreamRemoteRestoreRequest{OperationID: "cancel"})
		cancelled <- err
	}()
	time.Sleep(100 * time.Millisecond)
	stopped := make(chan struct{})
	go func() { source.Shutdown(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked on source upload limiter")
	}
	nc.Close()
	if err := <-cancelled; err == nil {
		t.Fatal("cancelled checkpoint reported success")
	}
}

// Two streams of the same operation must share the rate after restart; otherwise
// overlapping final checkpoints multiply the operator's configured ceiling.
func TestJetStreamS3TierSharedSourceCeilingSurvivesRestart(t *testing.T) {
	store := testS3TierCompatibleStore(t)
	o := DefaultTestOptions
	o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
	o.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 128 << 20, ReserveBytes: 8 << 20, MinFreeBytes: 1 << 20}
	o.JetStreamS3Tiers = map[string]*S3TierConfig{}
	prefix := fmt.Sprintf("shared-source/%d", time.Now().UnixNano())
	for _, name := range []string{"FIRST", "SECOND", "SIBLING"} {
		o.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: store, Prefix: prefix + "/" + name, BlockSize: 1 << 20, LocalHighBytes: 64 << 20, LocalLowBytes: 32 << 20, RemoteHighBytes: 64 << 20, RemoteLowBytes: 32 << 20}
	}
	s := RunServer(&o)
	defer func() { s.Shutdown() }()
	nc := clientConnectToServer(t, s)
	defer func() { nc.Close() }()
	js, err := nc.JetStream()
	require_NoError(t, err)
	limits := &S3TierSourceLimits{BytesPerSecond: 64 << 10, BurstBytes: 4096, Concurrency: 1}
	call := func(subject string, request JSApiStreamRemoteRestoreRequest) error {
		body, _ := json.Marshal(request)
		reply, err := nc.Request(subject, body, 15*time.Second)
		if err != nil {
			return err
		}
		var response JSApiStreamRemoteRestoreResponse
		if err := json.Unmarshal(reply.Data, &response); err != nil {
			return err
		}
		if response.Error != nil {
			return response.Error
		}
		return nil
	}
	for _, name := range []string{"FIRST", "SECOND", "SIBLING"} {
		_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
		require_NoError(t, err)
		if name == "SIBLING" {
			continue
		}
		_, err = js.Publish(name, []byte(strings.Repeat("x", 128<<10)))
		require_NoError(t, err)
		require_NoError(t, call(fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, name), JSApiStreamRemoteRestoreRequest{OperationID: "shared", SourceLimits: limits}))
	}
	nc.Close()
	s.Shutdown()
	s = RunServer(&o)
	nc = clientConnectToServer(t, s)
	js, err = nc.JetStream()
	require_NoError(t, err)
	started := time.Now()
	done := make(chan error, 2)
	for _, name := range []string{"FIRST", "SECOND"} {
		go func(name string) {
			done <- call(fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, name), JSApiStreamRemoteRestoreRequest{OperationID: "shared"})
		}(name)
	}
	_, err = js.Publish("SIBLING", []byte("unaffected"))
	require_NoError(t, err)
	require_NoError(t, <-done)
	require_NoError(t, <-done)
	if elapsed := time.Since(started); elapsed < 3500*time.Millisecond {
		t.Fatalf("parallel streams exceeded shared 64KiB/s ceiling: %s", elapsed)
	}
	for _, name := range []string{"FIRST", "SECOND"} {
		msg, err := js.GetMsg(name, 1)
		require_NoError(t, err)
		if len(msg.Data) != 128<<10 {
			t.Fatalf("%s payload changed: %d", name, len(msg.Data))
		}
	}
}
