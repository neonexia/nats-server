// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestFileStoreS3TierShouldReclaimDurableCacheWhileUploadPassIsBlocked(t *testing.T) {
	testS3TierCacheAdmission(t, &testS3BlockStore{objects: make(map[string][]byte)})
}

func TestFileStoreS3TierCacheAdmissionCompatibleEndpoint(t *testing.T) {
	testS3TierCacheAdmission(t, testS3TierCompatibleStore(t))
}

func testS3TierCacheAdmission(t *testing.T, remote S3TierObjectStore) {
	t.Helper()
	for _, budgeted := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk_budget=%v", budgeted), func(t *testing.T) {
			fs, fcfg, cfg := testTieredFileStore(t, remote, t.TempDir(), fmt.Sprintf("test/cache-admission/%d", time.Now().UnixNano()))
			t.Cleanup(func() { fs.Stop() })
			stopS3TierWorker(t, fs.tier)
			fs.tier.cfg.LocalHighBytes = 1 << 20
			payload := bytes.Repeat([]byte("a"), 1000)
			for i := 0; i < 100; i++ {
				if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := fs.DrainS3Tier(); err != nil {
				t.Fatal(err)
			}
			fs.tier.cfg.LocalHighBytes = 32 << 10
			fs.tier.cfg.LocalLowBytes = 16 << 10
			if budgeted {
				b, err := newFileDiskBudget(fcfg.StoreDir, DiskBudgetConfig{LimitBytes: 1 << 20, ReserveBytes: 32 << 10, MinFreeBytes: 1})
				if err != nil {
					t.Fatal(err)
				}
				fs.dios.budget.Store(b)
			}
			// Remote coverage can be stalled while disposable cache already exists.
			// Publishing must not wait on that pass or discard uncommitted data.
			fs.tier.runMu.Lock()
			done := make(chan error, 1)
			go func() {
				seq, _, err := fs.StoreMsg("events", nil, payload, 0)
				if err == nil && seq != 101 {
					err = fmt.Errorf("sequence %d, want101", seq)
				}
				done <- err
			}()
			select {
			case err := <-done:
				fs.tier.runMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				fs.tier.runMu.Unlock()
				<-done
				t.Fatal("publish waited for remote coverage")
			}
			if fs.tier.stats.evictions.Load() == 0 {
				t.Fatal("did not exercise cache reclamation")
			}
			if err := fs.Stop(); err != nil {
				t.Fatal(err)
			}
			var err error
			fs, err = newFileStore(fcfg, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for seq := uint64(1); seq <= 101; seq++ {
				m, err := fs.LoadMsg(seq, nil)
				if err != nil || !bytes.Equal(m.msg, payload) {
					t.Fatalf("replay%d: %v", seq, err)
				}
			}
		})
	}
}
