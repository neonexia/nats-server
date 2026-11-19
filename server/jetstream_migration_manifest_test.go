// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestJetStreamMigrationWholeShardManifestBoundaries(t *testing.T) {
	o := DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.StoreDir = t.TempDir()
	o.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 32 << 20, ReserveBytes: 1 << 20, MinFreeBytes: 1}
	s := RunServer(&o)
	defer s.Shutdown()
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	for _, count := range []int{0, 257, 4096, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			req := JSApiMigrationReservationRequest{OperationID: fmt.Sprintf("manifest-%d", count), LocalBytes: 8 << 20, SiblingHeadroomBytes: 1 << 20}
			for i := 0; i < count; i++ {
				req.Streams = append(req.Streams, fmt.Sprintf("STREAM_%04d", i))
			}
			b, err := json.Marshal(req)
			require_NoError(t, err)
			msg, err := nc.Request(JSApiMigrationReserve, b, 10*time.Second)
			require_NoError(t, err)
			var r JSApiStreamRemoteRestoreResponse
			require_NoError(t, json.Unmarshal(msg.Data, &r))
			if count == 0 || count > 4096 {
				if r.Error == nil || r.Success {
					t.Fatalf("invalid manifest accepted: %+v", r)
				}
				return
			}
			if r.Error != nil || !r.Success {
				t.Fatalf("whole-shard manifest refused: %+v", r)
			}
			msg, err = nc.Request(JSApiMigrationRelease, b, 10*time.Second)
			require_NoError(t, err)
			require_NoError(t, json.Unmarshal(msg.Data, &r))
			if r.Error != nil || !r.Success {
				t.Fatalf("release: %+v", r)
			}
		})
	}
}
