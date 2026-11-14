// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskMigrationMaximumRootsPreserveCeilingAfterReload(t *testing.T) {
	root := t.TempDir()
	cfg := DiskBudgetConfig{LimitBytes: 8 << 20, ReserveBytes: 512 << 10, MinFreeBytes: 1}
	budget, err := newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	reservation := diskMigrationReservation{Version: 1, OperationID: "maximum-roots", LimitBytes: 2 << 20}
	for i := 0; i < 256; i++ {
		reservation.Roots = append(reservation.Roots, fmt.Sprintf("stage/%03d", i), fmt.Sprintf("streams/%03d", i))
	}
	require_NoError(t, budget.reserveMigration(reservation, 1<<20))
	for pass := 0; pass < 2; pass++ {
		for _, path := range []string{"stage/000/blocks/1.blk", "stage/255", "streams/255/consumers/name/state.dat"} {
			if _, err := budget.reserveGrowth(filepath.Join(root, path), 3<<20, true); !errors.Is(err, ErrMigrationBudgetExceeded) {
				t.Fatalf("reserved subtree escaped migration limit (%s, reload=%d): %v", path, pass, err)
			}
		}
		for _, path := range []string{"stage/255-extra/file", "other/stage/255/file", "stage-parent/file"} {
			rollback, err := budget.reserveGrowth(filepath.Join(root, path), 3<<20, false)
			require_NoError(t, err)
			rollback()
		}
		budget, err = newFileDiskBudget(root, cfg)
		require_NoError(t, err)
	}
}

func TestDiskMigrationReservationSurvivesRestartAndProtectsSiblings(t *testing.T) {
	root := t.TempDir()
	cfg := DiskBudgetConfig{LimitBytes: 8 << 20, ReserveBytes: 512 << 10, MinFreeBytes: 1}
	b, err := newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	reservation := diskMigrationReservation{Version: 1, OperationID: "namespace-move", Roots: []string{"stage/orders", "streams/orders"}, LimitBytes: 2 << 20}
	require_NoError(t, b.reserveMigration(reservation, 1<<20))
	// A lexical prefix is not subtree ownership. This sibling may use more
	// than the migration ceiling while still respecting the engine's hold.
	rollback, err := b.reserveGrowth(filepath.Join(root, "stage/orders-extra/1.blk"), 3<<20, false)
	require_NoError(t, err)
	rollback()
	// An unrelated writer cannot borrow bytes promised to the pending move.
	if _, err := b.reserveGrowth(filepath.Join(root, "sibling-too-large.blk"), 6<<20, false); !errors.Is(err, ErrDiskBudgetExceeded) {
		t.Fatalf("sibling consumed reserved capacity: %v", err)
	}
	require_NoError(t, b.reconcile())
	// Reload the ledger as a new process would; no payload file exists yet.
	b, err = newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	if b.snapshot().ReservedBytes != 2<<20 {
		t.Fatalf("restart lost hold: %+v", b.snapshot())
	}
	require_NoError(t, b.reserveMigration(reservation, 1<<20))
	path := filepath.Join(root, "stage/orders/1.blk")
	require_NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	_, err = b.reserveGrowth(path, 1<<20, false)
	require_NoError(t, err)
	require_NoError(t, os.WriteFile(path, make([]byte, 1<<20), 0600))
	b.completed(path)
	require_NoError(t, b.reconcile())
	if _, err = b.reserveGrowth(path, 3<<20, true); !errors.Is(err, ErrMigrationBudgetExceeded) {
		t.Fatalf("maintenance bypassed migration ceiling: %v", err)
	}
	// Migration rejection must not mark the entire engine unavailable.
	_, err = b.reserveGrowth(filepath.Join(root, "sibling.blk"), 1<<20, false)
	require_NoError(t, err)
	require_NoError(t, os.MkdirAll(filepath.Join(root, "streams"), 0700))
	require_NoError(t, os.Rename(filepath.Join(root, "stage/orders"), filepath.Join(root, "streams/orders")))
	b.renamedTree(filepath.Join(root, "stage/orders"), filepath.Join(root, "streams/orders"))
	b, err = newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	if _, err = b.reserveGrowth(filepath.Join(root, "streams/orders/2.blk"), 2<<20, false); !errors.Is(err, ErrMigrationBudgetExceeded) {
		t.Fatalf("rename/restart lost ceiling: %v", err)
	}
	if err = b.releaseMigration("different-operation"); err == nil {
		t.Fatal("foreign operation released reservation")
	}
	used := b.snapshot().UsedBytes
	require_NoError(t, b.releaseMigration("namespace-move"))
	if after := b.snapshot(); after.UsedBytes != used || after.ReservedBytes != 0 {
		t.Fatalf("release discarded resident accounting: %+v", after)
	}
	b, err = newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	if b.snapshot().ReservedBytes != 0 {
		t.Fatal("released reservation returned after restart")
	}
}

func TestDiskMigrationReservationRejectsConflictAndUnsafeRoots(t *testing.T) {
	for _, root := range []string{".", "..", "../outside", "/absolute", "stage/../other"} {
		t.Run(root, func(t *testing.T) {
			b, err := newFileDiskBudget(t.TempDir(), DiskBudgetConfig{LimitBytes: 8 << 20, ReserveBytes: 512 << 10, MinFreeBytes: 1})
			require_NoError(t, err)
			if err := b.reserveMigration(diskMigrationReservation{Version: 1, OperationID: "move", Roots: []string{root}, LimitBytes: 1 << 20}, 1<<20); err == nil {
				t.Fatal("unsafe root accepted")
			}
		})
	}
	b, err := newFileDiskBudget(t.TempDir(), DiskBudgetConfig{LimitBytes: 8 << 20, ReserveBytes: 512 << 10, MinFreeBytes: 1})
	require_NoError(t, err)
	r := diskMigrationReservation{Version: 1, OperationID: "move", Roots: []string{"stage"}, LimitBytes: 1 << 20}
	require_NoError(t, b.reserveMigration(r, 1<<20))
	r.LimitBytes++
	if err := b.reserveMigration(r, 1<<20); err == nil {
		t.Fatal("retry changed immutable reservation")
	}
	r.OperationID = "other"
	if err := b.reserveMigration(r, 1<<20); err == nil {
		t.Fatal("concurrent migration accepted")
	}
}

func TestDiskMigrationJournalFailureNeverReturnsSuccessfulAdmission(t *testing.T) {
	root := t.TempDir()
	cfg := DiskBudgetConfig{LimitBytes: 8 << 20, ReserveBytes: 512 << 10, MinFreeBytes: 1}
	b, err := newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	// A non-empty directory at the journal destination forces atomic rename to fail.
	journal := filepath.Join(root, migrationBudgetFile)
	require_NoError(t, os.Mkdir(journal, 0700))
	require_NoError(t, os.WriteFile(filepath.Join(journal, "occupied"), []byte("x"), 0600))
	r := diskMigrationReservation{Version: 1, OperationID: "move", Roots: []string{"stage"}, LimitBytes: 1 << 20}
	for range 2 {
		if err = b.reserveMigration(r, 1<<20); err == nil {
			t.Fatal("failed journal became successful admission on retry")
		}
	}
	require_NoError(t, os.RemoveAll(journal))
	require_NoError(t, b.reserveMigration(r, 1<<20))
	recovered, err := newFileDiskBudget(root, cfg)
	require_NoError(t, err)
	if recovered.snapshot().ReservedBytes != 1<<20 {
		t.Fatal("repaired journal did not retain reservation")
	}
	require_NoError(t, os.WriteFile(journal, []byte(`{"version":1,"roots":["../escape"]}`), 0600))
	if _, err = newFileDiskBudget(root, cfg); err == nil {
		t.Fatal("corrupt reservation was ignored during startup")
	}
}
