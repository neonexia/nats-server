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

func TestJetStreamS3TierParallelStageSharesDownloadCeiling(t *testing.T) {
	store := testS3TierCompatibleStore(t)
	prefix := fmt.Sprintf("shared-stage/%d", time.Now().UnixNano())
	options := func() Options {
		o := DefaultTestOptions
		o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
		o.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 128 << 20, ReserveBytes: 8 << 20, MinFreeBytes: 1 << 20}
		o.JetStreamS3Tiers = map[string]*S3TierConfig{}
		for _, name := range []string{"FIRST", "SECOND", "SIBLING"} {
			o.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: store, Prefix: prefix + "/" + name, BlockSize: 1 << 20, LocalHighBytes: 64 << 20, LocalLowBytes: 32 << 20, RemoteHighBytes: 64 << 20, RemoteLowBytes: 32 << 20}
		}
		return o
	}
	so, to := options(), options()
	source, target := RunServer(&so), RunServer(&to)
	defer source.Shutdown()
	defer target.Shutdown()
	sn, tn := clientConnectToServer(t, source), clientConnectToServer(t, target)
	defer sn.Close()
	defer tn.Close()
	sj, err := sn.JetStream()
	require_NoError(t, err)
	tj, err := tn.JetStream()
	require_NoError(t, err)
	call := func(nc *nats.Conn, subject string, request any) (JSApiStreamRemoteRestoreResponse, error) {
		body, _ := json.Marshal(request)
		reply, err := nc.Request(subject, body, 15*time.Second)
		var response JSApiStreamRemoteRestoreResponse
		if err != nil {
			return response, err
		}
		if err = json.Unmarshal(reply.Data, &response); err != nil {
			return response, err
		}
		if response.Error != nil {
			return response, response.Error
		}
		if !response.Success {
			return response, fmt.Errorf("unsuccessful %s: %+v", subject, response)
		}
		return response, nil
	}
	epochs := map[string]uint64{}
	for _, name := range []string{"FIRST", "SECOND"} {
		_, err = sj.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
		require_NoError(t, err)
		_, err = sj.Publish(name, []byte(strings.Repeat("p", 128<<10)))
		require_NoError(t, err)
		request := JSApiStreamRemoteRestoreRequest{OperationID: "shared"}
		_, err = call(sn, fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, name), request)
		require_NoError(t, err)
		cp, err := call(sn, fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, name), request)
		require_NoError(t, err)
		epochs[name] = cp.Epoch
	}
	_, err = tj.AddStream(&nats.StreamConfig{Name: "SIBLING", Subjects: []string{"SIBLING"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	_, err = call(tn, JSApiMigrationReserve, JSApiMigrationReservationRequest{OperationID: "shared", Streams: []string{"FIRST", "SECOND"}, LocalBytes: 8 << 20, SiblingHeadroomBytes: 1 << 20})
	require_NoError(t, err)
	limits := S3TierRestoreLimits{PayloadBytes: 1 << 20, BytesPerSecond: 64 << 10, BurstBytes: 4096, Concurrency: 1}
	done := make(chan error, 2)
	started := time.Now()
	for _, name := range []string{"FIRST", "SECOND"} {
		go func(name string) {
			_, err := call(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, name), map[string]any{"operation_id": "shared", "expected_epoch": epochs[name], "target_id": "target", "stage_only": true, "restore_limits": limits})
			done <- err
		}(name)
	}
	_, err = tj.Publish("SIBLING", []byte("still-live"))
	require_NoError(t, err)
	require_NoError(t, <-done)
	require_NoError(t, <-done)
	if elapsed := time.Since(started); elapsed < 3500*time.Millisecond {
		t.Fatalf("parallel staging exceeded shared ceiling: %s", elapsed)
	}
	for _, name := range []string{"FIRST", "SECOND"} {
		if _, err := tj.Publish(name, []byte("must-not-write"), nats.AckWait(100*time.Millisecond)); err == nil {
			t.Fatalf("staging accepted a write to %s", name)
		}
		_, err = call(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, name), map[string]any{"operation_id": "shared", "expected_epoch": epochs[name], "target_id": "target", "activate_staged": true})
		require_NoError(t, err)
		msg, err := tj.GetMsg(name, 1)
		require_NoError(t, err)
		if string(msg.Data) != strings.Repeat("p", 128<<10) {
			t.Fatalf("%s payload changed", name)
		}
	}
}
