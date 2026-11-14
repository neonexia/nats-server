// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// A staged destination must survive a restart without becoming a second writer.
// The unrelated target stream stays usable throughout the ownership barrier.
func TestJetStreamS3TierStagedRestoreCompatibleEndpoint(t *testing.T) {
	testStagedRestoreOwnership(t, false)
}

func TestJetStreamS3TierStagedRestoreAbortCompatibleEndpoint(t *testing.T) {
	testStagedRestoreOwnership(t, true)
}

func testStagedRestoreOwnership(t *testing.T, abort bool) {
	store := testS3TierCompatibleStore(t)
	prefix := fmt.Sprintf("staged-restore/%d", time.Now().UnixNano())
	options := func() Options {
		o := DefaultTestOptions
		o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
		o.JetStreamS3Tiers = map[string]*S3TierConfig{}
		for _, name := range []string{"MOVING", "SIBLING"} {
			o.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: store, Prefix: prefix, BlockSize: 16 << 10, LocalHighBytes: 128 << 10, LocalLowBytes: 64 << 10, RemoteHighBytes: 256 << 10, RemoteLowBytes: 128 << 10}
		}
		return o
	}
	so, to := options(), options()
	source := RunServer(&so)
	t.Cleanup(source.Shutdown)
	targetURL, killTarget := startStagedRestoreProcess(t, to.StoreDir, prefix)
	sn := clientConnectToServer(t, source)
	tn, err := nats.Connect(targetURL)
	require_NoError(t, err)
	defer sn.Close()
	defer tn.Close()
	sj, err := sn.JetStream()
	require_NoError(t, err)
	tj, err := tn.JetStream()
	require_NoError(t, err)
	for name, js := range map[string]nats.JetStreamContext{"MOVING": sj, "SIBLING": tj} {
		_, err = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
		require_NoError(t, err)
	}
	_, err = sj.Publish("MOVING", []byte("before-cutover"))
	require_NoError(t, err)
	_, err = sj.Publish("MOVING", []byte("pending-at-cutover"))
	require_NoError(t, err)
	call := func(nc *nats.Conn, subject string, body any) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		b, e := json.Marshal(body)
		require_NoError(t, e)
		m, e := nc.Request(subject, b, 10*time.Second)
		require_NoError(t, e)
		var r JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(m.Data, &r))
		if r.Error != nil || !r.Success {
			t.Fatalf("%s: %+v", subject, r)
		}
		return r
	}
	_, err = sj.AddConsumer("MOVING", &nats.ConsumerConfig{Durable: "worker", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Hour})
	require_NoError(t, err)
	reader, err := sj.PullSubscribe("MOVING", "worker", nats.Bind("MOVING", "worker"))
	require_NoError(t, err)
	pending, err := reader.Fetch(2, nats.MaxWait(time.Second))
	require_NoError(t, err)

	// The management client must prove the engine understands staging before
	// sending an additive flag that older engines would silently ignore.
	infoMsg, err := tn.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	require_NoError(t, json.Unmarshal(infoMsg.Data, &info))
	if info.Error != nil || info.RemoteRestoreStagingVersion != 1 {
		t.Fatalf("missing staged-restore capability: %+v", info)
	}
	reject := func(body any) {
		t.Helper()
		b, err := json.Marshal(body)
		require_NoError(t, err)
		msg, err := tn.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), b, time.Second)
		require_NoError(t, err)
		var result JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(msg.Data, &result))
		if result.Success || result.Error == nil {
			t.Fatalf("unsafe restore request was accepted: %+v", result)
		}
	}
	reject(map[string]any{"operation_id": "staged-move", "expected_epoch": 1, "target_id": "occupied-target", "activate_staged": true})
	// Warmup runs before the source barrier: subscriptions and acknowledgements
	// remain live, and the destination has no registered messaging stream.
	warmOp := JSApiStreamRemoteRestoreRequest{OperationID: "staged-move", SourceLimits: &S3TierSourceLimits{BytesPerSecond: 1 << 20, BurstBytes: 4096}}
	call(sn, fmt.Sprintf(JSApiStreamWarmupRemoteRestoreT, "MOVING"), warmOp)
	warmReservation := JSApiMigrationReservationRequest{OperationID: warmOp.OperationID, Streams: []string{"MOVING"}, LocalBytes: 2 << 20, SiblingHeadroomBytes: 1 << 20}
	call(tn, JSApiMigrationReserve, warmReservation)
	call(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), map[string]any{"operation_id": warmOp.OperationID, "target_id": "occupied-target", "warmup_only": true, "restore_limits": S3TierRestoreLimits{PayloadBytes: 1 << 20, BytesPerSecond: 1 << 20, BurstBytes: 4096, Concurrency: 1}})
	if _, err := tj.StreamInfo("MOVING"); err != nats.ErrStreamNotFound {
		t.Fatalf("warmup exposed target: %v", err)
	}
	warmAck, err := sj.Publish("MOVING", []byte("while-warming"))
	require_NoError(t, err)
	if warmAck.Sequence != 3 {
		t.Fatalf("warmup interrupted source publishing: %+v", warmAck)
	}
	require_NoError(t, pending[0].AckSync())
	pending = pending[1:]
	op := JSApiStreamRemoteRestoreRequest{OperationID: "staged-move"}
	call(sn, fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "MOVING"), op)
	cp := call(sn, fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "MOVING"), op)
	if err := pending[0].AckSync(nats.AckWait(100 * time.Millisecond)); err == nil {
		t.Fatal("late source ACK confirmed after checkpoint")
	}

	reservation := JSApiMigrationReservationRequest{OperationID: op.OperationID, Streams: []string{"MOVING"}, LocalBytes: 2 << 20, SiblingHeadroomBytes: 1 << 20}
	call(tn, JSApiMigrationReserve, reservation)
	for _, bad := range []JSApiMigrationReservationRequest{
		{OperationID: "other-move", Streams: []string{"MOVING"}, LocalBytes: 2 << 20, SiblingHeadroomBytes: 1 << 20},
		{OperationID: op.OperationID, Streams: []string{"SIBLING"}, LocalBytes: 2 << 20, SiblingHeadroomBytes: 1 << 20},
	} {
		body, e := json.Marshal(bad)
		require_NoError(t, e)
		msg, e := tn.Request(JSApiMigrationReserve, body, time.Second)
		require_NoError(t, e)
		var response JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(msg.Data, &response))
		if response.Success || response.Error == nil {
			t.Fatalf("conflicting reservation accepted: %+v", response)
		}
	}
	stage := map[string]any{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "occupied-target", "stage_only": true}
	reject(map[string]any{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "occupied-target", "stage_only": true, "restore_limits": S3TierRestoreLimits{PayloadBytes: 1, BytesPerSecond: 1 << 20, BurstBytes: 4096, Concurrency: 1}})
	stage["restore_limits"] = S3TierRestoreLimits{PayloadBytes: 1 << 20, BytesPerSecond: 1 << 20, BurstBytes: 4096, Concurrency: 1}
	for range 2 {
		r := call(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), stage)
		if r.State != "staged" {
			t.Fatalf("stage must not activate target: state=%q", r.State)
		}
	}

	// Conflicting retries must neither overwrite staging nor activate it.
	for _, invalid := range []map[string]any{
		{"operation_id": "wrong-operation", "expected_epoch": cp.Epoch, "target_id": "occupied-target", "activate_staged": true},
		{"operation_id": op.OperationID, "expected_epoch": cp.Epoch + 1, "target_id": "occupied-target", "activate_staged": true},
		{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "wrong-target", "activate_staged": true},
		{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "occupied-target", "activate_staged": true, "stage_only": true},
		{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "occupied-target"},
	} {
		reject(invalid)
	}
	body, e := json.Marshal(reservation)
	require_NoError(t, e)
	releaseMsg, e := tn.Request(JSApiMigrationRelease, body, time.Second)
	require_NoError(t, e)
	var earlyRelease JSApiStreamRemoteRestoreResponse
	require_NoError(t, json.Unmarshal(releaseMsg.Data, &earlyRelease))
	if earlyRelease.Success || earlyRelease.Error == nil {
		t.Fatal("released disk hold before activation")
	}
	if _, err = tj.Publish("MOVING", []byte("too-early"), nats.AckWait(100*time.Millisecond)); err == nil {
		t.Fatal("staged target accepted a publish")
	}
	if _, err := tn.Request(pending[0].Reply, []byte("+ACK"), 100*time.Millisecond); err == nil {
		t.Fatal("staged target confirmed an ACK before activation")
	}
	if m, err := tn.Request("$JS.API.CONSUMER.MSG.NEXT.MOVING.worker", []byte(`{"batch":1,"expires":100000000}`), 200*time.Millisecond); err == nil && (string(m.Data) == "before-cutover" || string(m.Data) == "pending-at-cutover") {
		t.Fatal("staged target delivered before activation")
	}
	_, err = tj.Publish("SIBLING", []byte("during-stage"))
	require_NoError(t, err)
	// SIGKILL deliberately skips every graceful store/consumer flush.
	killTarget()
	targetURL, _ = startStagedRestoreProcess(t, to.StoreDir, prefix)
	rn, err := nats.Connect(targetURL)
	require_NoError(t, err)
	defer rn.Close()
	rj, err := rn.JetStream()
	require_NoError(t, err)
	if _, err = rj.Publish("MOVING", []byte("too-early-after-restart"), nats.AckWait(100*time.Millisecond)); err == nil {
		t.Fatal("target restart lost staged fence")
	}
	if _, err := rn.Request(pending[0].Reply, []byte("+ACK"), 100*time.Millisecond); err == nil {
		t.Fatal("restarted staged target confirmed an ACK before activation")
	}
	_, err = rj.Publish("SIBLING", []byte("after-restart"))
	require_NoError(t, err)
	activate := map[string]any{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "occupied-target", "activate_staged": true}
	if abort {
		// Abort must win against delayed activation even after the target has
		// persisted a complete checkpoint and restarted with it.
		call(sn, fmt.Sprintf(JSApiStreamAbortRemoteRestoreT, "MOVING"), op)
		for _, delayed := range []any{stage, activate, activate} {
			b, err := json.Marshal(delayed)
			require_NoError(t, err)
			m, err := rn.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), b, time.Second)
			require_NoError(t, err)
			var response JSApiStreamRemoteRestoreResponse
			require_NoError(t, json.Unmarshal(m.Data, &response))
			if response.Success || response.Error == nil {
				t.Fatal("delayed activation accepted after source resumed")
			}
		}
		ack, err := sj.Publish("MOVING", []byte("after-rollback"))
		require_NoError(t, err)
		if ack.Sequence != 4 {
			t.Fatalf("rollback sequence=%d, want 4", ack.Sequence)
		}
		require_NoError(t, pending[0].AckSync())
		ci, err := sj.ConsumerInfo("MOVING", "worker")
		require_NoError(t, err)
		if ci.AckFloor.Stream != 2 || ci.NumAckPending != 0 {
			t.Fatalf("source ACK recovery: %+v", ci)
		}
		if _, err := rj.Publish("MOVING", []byte("stale-target"), nats.AckWait(100*time.Millisecond)); err == nil {
			t.Fatal("aborted destination became writable")
		}
		discard := map[string]any{"operation_id": op.OperationID, "expected_epoch": cp.Epoch, "target_id": "occupied-target", "discard_staged": true}
		for range 2 {
			call(rn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), discard)
		}
		if _, err := rj.StreamInfo("MOVING"); err != nats.ErrStreamNotFound {
			t.Fatalf("discarded stream still present: %v", err)
		}
		call(rn, JSApiMigrationRelease, reservation)
		// A delayed stage retry must not recreate the stream after its disk hold
		// has been released. The abort decision survives all local cleanup.
		b, err := json.Marshal(stage)
		require_NoError(t, err)
		m, err := rn.Request(fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), b, time.Second)
		require_NoError(t, err)
		var delayed JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(m.Data, &delayed))
		if delayed.Success || delayed.Error == nil {
			t.Fatal("aborted stage recreated after cleanup")
		}
		_, err = rj.Publish("SIBLING", []byte("after-other-namespace-rollback"))
		require_NoError(t, err)
		return
	}
	for range 2 {
		r := call(rn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "MOVING"), activate)
		if r.State != "active" {
			t.Fatalf("activation state=%q", r.State)
		}
	}
	// Once activation owns the shared decision, a stale rollback must never
	// reopen the old source, even if its local checkpoint is still present.
	abortBody, err := json.Marshal(op)
	require_NoError(t, err)
	abortMsg, err := sn.Request(fmt.Sprintf(JSApiStreamAbortRemoteRestoreT, "MOVING"), abortBody, time.Second)
	require_NoError(t, err)
	var abortResponse JSApiStreamRemoteRestoreResponse
	require_NoError(t, json.Unmarshal(abortMsg.Data, &abortResponse))
	if abortResponse.Success || abortResponse.Error == nil {
		t.Fatal("source rollback accepted after target activation")
	}
	if _, err := sj.Publish("MOVING", []byte("old-source"), nats.AckWait(100*time.Millisecond)); err == nil {
		t.Fatal("old source resumed after activation")
	}
	ci, err := rj.ConsumerInfo("MOVING", "worker")
	require_NoError(t, err)
	if ci.AckFloor.Stream != 1 || ci.NumAckPending != 1 {
		t.Fatalf("consumer checkpoint changed: %+v", ci)
	}
	ack, err := rj.Publish("MOVING", []byte("after-cutover"))
	require_NoError(t, err)
	if ack.Sequence != 4 {
		t.Fatalf("target sequence=%d; fenced writes must not enter history", ack.Sequence)
	}
	tail, err := rj.GetMsg("MOVING", 3)
	require_NoError(t, err)
	if string(tail.Data) != "while-warming" {
		t.Fatal("final checkpoint missed live warmup tail")
	}
	first, err := rj.GetMsg("MOVING", 1)
	require_NoError(t, err)
	if string(first.Data) != "before-cutover" {
		t.Fatal("restored payload changed")
	}
	call(rn, JSApiMigrationRelease, reservation)
	secondMove := JSApiStreamRemoteRestoreRequest{OperationID: "move-again"}
	call(rn, fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "MOVING"), secondMove)
	status := call(rn, fmt.Sprintf(JSApiStreamRemoteRestoreStatusT, "MOVING"), secondMove)
	if status.State != "prepared" || status.OperationID != secondMove.OperationID {
		t.Fatalf("previous activation hid the new source fence: %+v", status)
	}
}

// Run the target in a separate process so restart coverage cannot accidentally
// rely on Shutdown flushing the checkpoint that staging promised was durable.
func startStagedRestoreProcess(t *testing.T, dir, prefix string) (string, func()) {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "ready")
	log, err := os.Create(filepath.Join(t.TempDir(), "engine.log"))
	require_NoError(t, err)
	t.Cleanup(func() { log.Close() })
	cmd := exec.Command(os.Args[0], "-test.run=^TestJetStreamS3TierStagedRestoreProcess$")
	cmd.Env = append(os.Environ(), "NEOND_STAGE_TEST_DIR="+dir, "NEOND_STAGE_TEST_PREFIX="+prefix, "NEOND_STAGE_TEST_READY="+ready)
	cmd.Stdout, cmd.Stderr = log, log
	require_NoError(t, cmd.Start())
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(ready); err == nil && len(b) > 0 {
			return string(b), stop
		}
		time.Sleep(10 * time.Millisecond)
	}
	b, _ := os.ReadFile(log.Name())
	t.Fatalf("target process did not become ready: %s", b)
	return "", stop
}

func TestJetStreamS3TierStagedRestoreProcess(t *testing.T) {
	dir := os.Getenv("NEOND_STAGE_TEST_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	store := testS3TierCompatibleStore(t)
	o := DefaultTestOptions
	o.Port, o.JetStream, o.StoreDir = -1, true, dir
	o.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 8 << 20, ReserveBytes: 512 << 10, MinFreeBytes: 1}
	o.JetStreamS3Tiers = map[string]*S3TierConfig{}
	for _, name := range []string{"MOVING", "SIBLING"} {
		o.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: store, Prefix: os.Getenv("NEOND_STAGE_TEST_PREFIX"), BlockSize: 16 << 10, LocalHighBytes: 128 << 10, LocalLowBytes: 64 << 10, RemoteHighBytes: 256 << 10, RemoteLowBytes: 128 << 10}
	}
	s := RunServer(&o)
	require_NoError(t, os.WriteFile(os.Getenv("NEOND_STAGE_TEST_READY"), []byte(s.ClientURL()), 0600))
	select {}
}
