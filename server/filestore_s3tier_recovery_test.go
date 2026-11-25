// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamS3TierRecoveryCheckpointPreservesBoundaryWithoutGrantingOwnership(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	testRecoveryCheckpoint(t, store)
}

func TestJetStreamS3TierRecoveryCheckpointCompatibleEndpoint(t *testing.T) {
	testRecoveryCheckpoint(t, testS3TierCompatibleStore(t))
}

func testRecoveryCheckpoint(t *testing.T, store S3TierObjectStore) {
	prefix := fmt.Sprintf("recovery-checkpoint/%d", time.Now().UnixNano())
	opts := DefaultTestOptions
	opts.Port, opts.JetStream, opts.StoreDir = -1, true, t.TempDir()
	opts.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 128 << 20, ReserveBytes: 8 << 20, MinFreeBytes: 1 << 20}
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/RECOVER": {
		Store: store, Prefix: prefix, BlockSize: 16 * 1024,
		LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29, RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	s := RunServer(&opts)
	defer s.Shutdown()
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	js, err := nc.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "RECOVER", Subjects: []string{"messages"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	for i := range 3 {
		_, err = js.Publish("messages", []byte(fmt.Sprintf("message-%d", i)))
		require_NoError(t, err)
	}
	_, err = js.AddConsumer("RECOVER", &nats.ConsumerConfig{Durable: "worker", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Hour})
	require_NoError(t, err)
	reader, err := js.PullSubscribe("messages", "worker", nats.Bind("RECOVER", "worker"))
	require_NoError(t, err)
	batch, err := reader.Fetch(2, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_NoError(t, batch[0].AckSync())
	call := func(subject string, request any) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		data, err := json.Marshal(request)
		require_NoError(t, err)
		reply, err := nc.Request(subject, data, 10*time.Second)
		require_NoError(t, err)
		var result JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(reply.Data, &result))
		return result
	}
	req := JSApiStreamRemoteRestoreRequest{OperationID: "backup-1", SourceGeneration: 7, SourceLimits: &S3TierSourceLimits{BytesPerSecond: 64 << 20, BurstBytes: 1 << 20, Concurrency: 1}}
	subject := fmt.Sprintf(JSApiStreamRecoveryCheckpointT, "RECOVER")
	for _, invalid := range []JSApiStreamRemoteRestoreRequest{
		{},
		{OperationID: "../escape", SourceGeneration: 7, SourceLimits: req.SourceLimits},
		{OperationID: "zero", SourceLimits: req.SourceLimits},
		{OperationID: "unbounded", SourceGeneration: 7},
	} {
		if response := call(subject, invalid); response.Error == nil || response.Success {
			t.Fatalf("invalid checkpoint accepted: %+v", response)
		}
	}
	if faulty, ok := store.(*testS3BlockStore); ok {
		faulty.mu.Lock()
		faulty.failPut = true
		faulty.mu.Unlock()
		failed := req
		failed.OperationID = "upload-fails"
		response := call(subject, failed)
		faulty.mu.Lock()
		faulty.failPut = false
		faulty.mu.Unlock()
		if response.Error == nil || response.Success {
			t.Fatalf("failed upload reported checkpoint: %+v", response)
		}
		// Failure must not leave a source write/ACK fence behind.
		info, err := js.StreamInfo("RECOVER")
		require_NoError(t, err)
		require_Equal(t, info.State.Msgs, uint64(3))
	}
	checkpoint := call(subject, req)
	if checkpoint.Error != nil || !checkpoint.Success || checkpoint.CoveredThrough != 3 || checkpoint.Epoch != 7 || checkpoint.CheckpointSHA256 == "" {
		t.Fatalf("checkpoint: %+v", checkpoint)
	}
	// Both write admission and consumer progress resume, without an abort RPC.
	_, err = js.Publish("messages", []byte("after-checkpoint"))
	require_NoError(t, err)
	require_NoError(t, batch[1].AckSync())
	retried := call(subject, req)
	if retried.Error != nil || retried.CheckpointSHA256 != checkpoint.CheckpointSHA256 || retried.CoveredThrough != 3 {
		t.Fatalf("retry changed checkpoint: %+v", retried)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, err := store.Get(ctx, checkpoint.CheckpointKey)
	require_NoError(t, err)
	var backup s3TierRemoteRestoreCheckpoint
	require_NoError(t, json.Unmarshal(data, &backup))
	if len(backup.Consumers) != 1 || backup.Consumers[0].ConsumerState.AckFloor.Stream != 1 || backup.Consumers[0].ConsumerState.Delivered.Stream != 2 {
		t.Fatalf("consumer checkpoint: %+v", backup.Consumers)
	}
	req.SourceGeneration++
	if response := call(subject, req); response.Error == nil || response.Success {
		t.Fatalf("generation reuse accepted: %+v", response)
	}
	// No migration checkpoint is published: a backup cannot be mistaken for
	// evidence that a still-serving source has relinquished ownership.
	targetOpts := opts
	targetOpts.StoreDir, targetOpts.Port = t.TempDir(), -1
	target := RunServer(&targetOpts)
	defer target.Shutdown()
	targetNC := clientConnectToServer(t, target)
	defer targetNC.Close()
	request, err := json.Marshal(JSApiStreamRestoreRemoteRequest{OperationID: "backup-1", ExpectedEpoch: 7, TargetID: "target"})
	require_NoError(t, err)
	reply, err := targetNC.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "RECOVER"), request, time.Second)
	require_NoError(t, err)
	var restored JSApiStreamRemoteRestoreResponse
	require_NoError(t, json.Unmarshal(reply.Data, &restored))
	if restored.Error == nil || restored.Success {
		t.Fatalf("live backup authorized migration restore: %+v", restored)
	}
	// Commit remote objects beyond the selected recovery point. Restoration
	// must fork the object namespace rather than overwriting this newer tail.
	later := req
	later.SourceGeneration, later.OperationID = 7, "backup-later"
	if response := call(subject, later); response.Error != nil || response.CoveredThrough != 4 {
		t.Fatalf("later checkpoint: %+v", response)
	}
	s.Shutdown()
	s.WaitForShutdown()
	require_NoError(t, os.RemoveAll(opts.StoreDir))
	nc = targetNC
	reservation := JSApiMigrationReservationRequest{OperationID: "recover-1", Streams: []string{"RECOVER"}, LocalBytes: 8 << 20, SiblingHeadroomBytes: 1 << 20}
	if response := call(JSApiMigrationReserve, reservation); response.Error != nil || !response.Success {
		t.Fatalf("recovery reservation: %+v", response)
	}
	recovery := JSApiStreamRestoreRemoteRequest{OperationID: "recover-1", ExpectedEpoch: 7, TargetID: "target", StageOnly: true,
		RecoveryCheckpointID: "backup-1", RecoveryCheckpointSHA256: checkpoint.CheckpointSHA256,
		RestoreLimits: &S3TierRestoreLimits{PayloadBytes: 1 << 20, BytesPerSecond: 64 << 20, BurstBytes: 1 << 20, Concurrency: 1}}
	recoverySubject := fmt.Sprintf(JSApiStreamRecoverRemoteT, "RECOVER")
	if response := call(recoverySubject, recovery); response.Error == nil || response.Success {
		t.Fatalf("unfenced recovery accepted: %+v", response)
	}
	recovery.SourceFenced = true
	bad := recovery
	bad.ExpectedEpoch++
	if response := call(recoverySubject, bad); response.Error == nil || response.Success {
		t.Fatalf("wrong generation accepted: %+v", response)
	}
	bad = recovery
	bad.RecoveryCheckpointSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	if response := call(recoverySubject, bad); response.Error == nil || response.Success {
		t.Fatalf("wrong checksum accepted: %+v", response)
	}
	if faulty, ok := store.(*testS3BlockStore); ok {
		faulty.mu.Lock()
		original := append([]byte(nil), faulty.objects[checkpoint.CheckpointKey]...)
		faulty.objects[checkpoint.CheckpointKey] = []byte("corrupt checkpoint")
		faulty.mu.Unlock()
		response := call(recoverySubject, recovery)
		faulty.mu.Lock()
		faulty.objects[checkpoint.CheckpointKey] = original
		faulty.mu.Unlock()
		if response.Error == nil || response.Success {
			t.Fatalf("corrupt checkpoint accepted: %+v", response)
		}
	}
	if response := call(recoverySubject, recovery); response.Error != nil || !response.Success || response.State != "staged" {
		t.Fatalf("stage recovery: %+v", response)
	}
	if response := call(recoverySubject, bad); response.Error == nil || response.Success {
		t.Fatalf("staged receipt accepted a different backup digest: %+v", response)
	}
	tjs, err := targetNC.JetStream()
	require_NoError(t, err)
	if _, err := tjs.Publish("messages", []byte("before-ownership"), nats.AckWait(100*time.Millisecond)); err == nil {
		t.Fatal("staged recovery accepted a publish")
	}
	activate := JSApiStreamRestoreRemoteRequest{OperationID: "recover-1", ExpectedEpoch: 7, TargetID: "target", ActivateStaged: true, StorageGeneration: "recover-1"}
	if response := call(fmt.Sprintf(JSApiStreamRestoreRemoteT, "RECOVER"), activate); response.Error != nil || response.State != "active" {
		t.Fatalf("activate recovery: %+v", response)
	}
	for i := uint64(1); i <= 3; i++ {
		msg, err := tjs.GetMsg("RECOVER", i)
		require_NoError(t, err)
		require_Equal(t, string(msg.Data), fmt.Sprintf("message-%d", i-1))
	}
	if _, err := tjs.GetMsg("RECOVER", 4); err == nil {
		t.Fatal("recovery included an uncheckpointed tail")
	}
	consumer, err := tjs.ConsumerInfo("RECOVER", "worker")
	require_NoError(t, err)
	require_Equal(t, consumer.AckFloor.Stream, uint64(1))
	require_Equal(t, consumer.NumAckPending, 1)
	ack, err := tjs.Publish("messages", []byte("new-history"))
	require_NoError(t, err)
	require_Equal(t, ack.Sequence, uint64(4))
	// Re-upload must succeed even though the old owner also uploaded sequence 4.
	later.OperationID, later.SourceGeneration = "replacement-backup", 8
	later.StorageGeneration = "recover-1"
	if response := call(subject, later); response.Error != nil || response.CoveredThrough != 4 {
		t.Fatalf("replacement checkpoint collided with old history: %+v", response)
	}
	target.Shutdown()
	target.WaitForShutdown()
	restarted := RunServer(&targetOpts)
	defer restarted.Shutdown()
	restartedNC := clientConnectToServer(t, restarted)
	defer restartedNC.Close()
	restartedJS, err := restartedNC.JetStream()
	require_NoError(t, err)
	msg, err := restartedJS.GetMsg("RECOVER", 4)
	require_NoError(t, err)
	require_Equal(t, string(msg.Data), "new-history")
	ack, err = restartedJS.Publish("messages", []byte("after-replacement-restart"))
	require_NoError(t, err)
	require_Equal(t, ack.Sequence, uint64(5))
}
