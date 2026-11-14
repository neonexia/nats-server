// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

var ErrMigrationBudgetExceeded = errors.New("migration disk reservation exhausted")

const migrationBudgetFile = "migration-reservation.json"
const migrationJournalHeadroom = 128 << 10

// One namespace migration may own an engine at a time. Roots include both the
// private staging trees and final stream trees, so rename never frees the hold.
// Relative paths make the durable reservation independent of the mount location.
type diskMigrationReservation struct {
	Version     int      `json:"version"`
	OperationID string   `json:"operation_id"`
	Roots       []string `json:"roots"`
	LimitBytes  uint64   `json:"limit_bytes"`
}

func (r *diskMigrationReservation) validate() error {
	if r == nil || r.Version != 1 || !validS3TierRemoteRestoreOperationID(r.OperationID) || r.LimitBytes == 0 || len(r.Roots) == 0 || len(r.Roots) > 512 {
		return errors.New("invalid migration disk reservation")
	}
	for i, root := range r.Roots {
		if filepath.IsAbs(root) || filepath.Clean(root) != root || root == "." || root == ".." || strings.HasPrefix(root, ".."+string(os.PathSeparator)) {
			return errors.New("migration root must be a relative subtree")
		}
		for _, previous := range r.Roots[:i] {
			if withinDiskTree(root, previous) || withinDiskTree(previous, root) {
				return errors.New("migration roots overlap")
			}
		}
	}
	if data, err := json.Marshal(r); err != nil || len(data) > 64<<10 {
		return errors.New("migration reservation exceeds journal limit")
	}
	return nil
}
func withinDiskTree(path, root string) bool {
	return path == root || (len(path) > len(root) && path[len(root)] == os.PathSeparator && strings.HasPrefix(path, root))
}
func (b *fileDiskBudget) migrationOwnsLocked(path string) bool {
	if b.migration == nil {
		return false
	}
	return b.migrationOwnsRoots(path, b.migrationRoots)
}

func migrationRootIndex(roots []string) map[string]struct{} {
	index := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		index[root] = struct{}{}
	}
	return index
}

func (b *fileDiskBudget) migrationOwnsRoots(path string, roots map[string]struct{}) bool {
	// Ledger paths are absolute/clean and journal roots are validated relative
	// subtrees. Strip the common prefix once: joining and cleaning each root
	// for every file made reconciliation expensive at the 256-stream boundary.
	prefix := b.root
	if !strings.HasSuffix(prefix, string(os.PathSeparator)) {
		prefix += string(os.PathSeparator)
	}
	relative, inside := strings.CutPrefix(path, prefix)
	if !inside {
		return false
	}
	// Check ancestors rather than every reserved stream. Reconciliation and
	// publisher accounting share the budget lock; its hold time must not grow
	// with (number of files × number of migration roots).
	for {
		if _, found := roots[relative]; found {
			return true
		}
		end := strings.LastIndexByte(relative, os.PathSeparator)
		if end < 0 {
			return false
		}
		relative = relative[:end]
	}
}
func (b *fileDiskBudget) migrationHoldLocked() uint64 {
	if b.migration == nil || b.migrationUsed >= b.migration.LimitBytes {
		return 0
	}
	return b.migration.LimitBytes - b.migrationUsed
}
func (b *fileDiskBudget) recordFileLocked(path string, entry diskBudgetFile) {
	if b.scanChanges != nil {
		b.scanChanges[path] = struct{}{}
	}
	if b.migrationOwnsLocked(path) {
		old := b.files[path]
		b.migrationUsed -= max(old.actual, old.planned)
		b.migrationUsed = addSaturate(b.migrationUsed, max(entry.actual, entry.planned))
	}
	b.files[path] = entry
}
func (b *fileDiskBudget) forgetFileLocked(path string) {
	if b.scanChanges != nil {
		b.scanChanges[path] = struct{}{}
	}
	if b.migrationOwnsLocked(path) {
		old := b.files[path]
		b.migrationUsed -= max(old.actual, old.planned)
	}
	delete(b.files, path)
}
func (b *fileDiskBudget) loadMigrationReservation() error {
	data, err := os.ReadFile(filepath.Join(b.root, migrationBudgetFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var r diskMigrationReservation
	if len(data) > 64<<10 {
		return errors.New("migration reservation exceeds journal limit")
	}
	if err = json.Unmarshal(data, &r); err != nil {
		return err
	}
	if err = r.validate(); err != nil {
		return err
	}
	if r.LimitBytes > b.cfg.LimitBytes-b.cfg.ReserveBytes {
		return errors.New("configured disk budget is smaller than durable migration reservation")
	}
	b.migration = &r
	b.migrationRoots = migrationRootIndex(r.Roots)
	return nil
}

// Reserve the entire ceiling before source fencing. Unused reservation remains
// unavailable to sibling allocations across scans and process restarts.
func (b *fileDiskBudget) reserveMigration(r diskMigrationReservation, siblingHeadroom uint64) error {
	if b == nil {
		return errors.New("migration requires an enforced engine disk budget")
	}
	if err := r.validate(); err != nil {
		return err
	}
	if siblingHeadroom == 0 {
		return errors.New("migration requires protected sibling headroom")
	}
	if b.cfg.ReserveBytes < migrationJournalHeadroom {
		return errors.New("migration requires at least 128 KiB of engine maintenance reserve")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.migration != nil {
		if reflect.DeepEqual(*b.migration, r) {
			return b.persistMigrationLocked(r)
		}
		return errors.New("engine already has a different migration reservation")
	}
	if b.measurementErr != nil || b.blocked {
		return ErrDiskBudgetExceeded
	}
	var resident uint64
	roots := migrationRootIndex(r.Roots)
	for path, e := range b.files {
		if b.migrationOwnsRoots(path, roots) {
			resident = addSaturate(resident, max(e.actual, e.planned))
		}
	}
	if resident > r.LimitBytes {
		return ErrMigrationBudgetExceeded
	}
	hold := r.LimitBytes - resident
	if err := b.refreshFreeLocked(); err != nil {
		return err
	}
	need := addSaturate(hold, addSaturate(siblingHeadroom, migrationJournalHeadroom))
	total := addSaturate(b.used, b.reserved)
	ceiling := b.cfg.LimitBytes - b.cfg.ReserveBytes
	if total > ceiling || need > ceiling-total || b.available < addSaturate(b.cfg.MinFreeBytes, addSaturate(b.reserved, need)) {
		return ErrDiskBudgetExceeded
	}
	copy := r
	copy.Roots = append([]string(nil), r.Roots...)
	b.migration = &copy
	b.migrationRoots = roots
	b.migrationUsed = resident
	// Keep the hold on an ambiguous journal sync failure. Retrying the identical
	// reservation repeats the durable write before returning success.
	return b.persistMigrationLocked(r)
}

func (b *fileDiskBudget) persistMigrationLocked(r diskMigrationReservation) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(data) > 64<<10 {
		return errors.New("migration reservation exceeds journal limit")
	}
	if err = writeAtomically(newDiskIOSemaphore(4), filepath.Join(b.root, migrationBudgetFile), data, defaultFilePerms, true); err != nil {
		return err
	}
	for _, path := range []string{b.root, filepath.Join(b.root, migrationBudgetFile)} {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		old := b.files[path]
		b.used -= old.actual
		b.reserved -= max(old.actual, old.planned) - old.actual
		actual := diskBudgetAllocated(info)
		b.recordFileLocked(path, diskBudgetFile{actual: actual, planned: actual})
		b.used += actual
	}

	b.updatePressureLocked()
	return nil
}

// Release only after activation/cleanup is durably complete. Existing resident
// files remain charged as ordinary allocations; removing the hold never frees data.
func (b *fileDiskBudget) releaseMigration(operationID string) error {
	if b == nil {
		return errors.New("migration requires an enforced engine disk budget")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.migration == nil {
		return nil
	}
	if b.migration.OperationID != operationID {
		return fmt.Errorf("migration operation does not own reservation")
	}
	if err := os.Remove(filepath.Join(b.root, migrationBudgetFile)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if canFsyncDirectories {
		if err := syncDir(b.root); err != nil {
			return err
		}
	}
	b.migration = nil
	b.migrationDownloads = nil
	b.migrationRoots = nil
	b.migrationUsed = 0
	b.updatePressureLocked()
	return nil
}

func (b *fileDiskBudget) requireMigration(operationID string, paths ...string) error {
	if b == nil {
		return errors.New("staged restore requires an enforced migration disk reservation")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.migration == nil || b.migration.OperationID != operationID {
		return errors.New("staged restore has no matching migration reservation")
	}
	if b.migrationUsed > b.migration.LimitBytes {
		return ErrMigrationBudgetExceeded
	}
	for _, path := range paths {
		if !b.migrationOwnsLocked(path) {
			return errors.New("staged stream is outside migration reservation")
		}
	}
	return nil
}
