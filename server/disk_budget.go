// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DiskBudgetConfig limits this server's local files independently of logical
// stream retention. Dedicated volumes or filesystem quotas are still required
// to isolate this allocation from unrelated filesystem writers.
type DiskBudgetConfig struct {
	LimitBytes   uint64 `json:"limit_bytes"`
	ReserveBytes uint64 `json:"reserve_bytes"`
	MinFreeBytes uint64 `json:"min_free_bytes"`
}

// DiskBudgetStats exposes admission separately from server liveness.
type DiskBudgetStats struct {
	DiskBudgetConfig
	UsedBytes       uint64 `json:"used_bytes"`
	ReservedBytes   uint64 `json:"reserved_bytes"`
	AvailableBytes  uint64 `json:"available_bytes"`
	AvailableInodes uint64 `json:"available_inodes"`
	Pressure        bool   `json:"pressure"`
	Admission       bool   `json:"admission"`
	State           string `json:"state"`
	Error           string `json:"error,omitempty"`
	Rejections      uint64 `json:"rejections"`
}

var ErrDiskBudgetExceeded = errors.New("local disk budget or filesystem headroom exhausted")

type diskBudgetFile struct{ actual, planned uint64 }

type fileDiskBudget struct {
	mu                                sync.Mutex
	root                              string
	cfg                               DiskBudgetConfig
	files                             map[string]diskBudgetFile
	tiers                             map[*fileS3Tier]struct{}
	used, reserved, available, inodes uint64
	pressure, blocked                 bool
	measurementErr                    error
	rejections                        uint64
	wake                              chan struct{}
}

func validateDiskBudget(cfg *DiskBudgetConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.LimitBytes == 0 || cfg.ReserveBytes == 0 || cfg.ReserveBytes >= cfg.LimitBytes/5 || cfg.MinFreeBytes == 0 {
		return errors.New("disk_budget requires positive limit_bytes, min_free_bytes and reserve_bytes below 20% of limit_bytes")
	}
	return nil
}

func newFileDiskBudget(root string, cfg DiskBudgetConfig) (*fileDiskBudget, error) {
	if err := validateDiskBudget(&cfg); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	b := &fileDiskBudget{root: root, cfg: cfg, files: make(map[string]diskBudgetFile), tiers: make(map[*fileS3Tier]struct{}), wake: make(chan struct{}, 1)}
	if err = b.reconcile(); err != nil {
		return nil, err
	}
	return b, nil
}

func diskBudgetRound(size uint64) uint64 {
	if size == 0 {
		return 0
	}
	if size > ^uint64(0)-4095 {
		return ^uint64(0)
	}
	return (size + 4095) &^ uint64(4095)
}

func (b *fileDiskBudget) kick() {
	if b != nil {
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}
}
func (b *fileDiskBudget) owns(path string) bool {
	rel, err := filepath.Rel(b.root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// reserveGrowth records projected file length before changing buffered state.
// Scans never erase this promise while the accepted bytes await a flush.
// Lock ordering is stream/block -> budget; the controller never acquires a
// stream or block lock while holding its own lock.
func (b *fileDiskBudget) reserveGrowth(path string, size uint64, maintenance bool) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	path = filepath.Clean(path)
	if !b.owns(path) {
		return nil, fmt.Errorf("%w: allocation outside assigned storage root", ErrDiskBudgetExceeded)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	old := b.files[path]
	if !maintenance && (b.blocked || b.measurementErr != nil) {
		return nil, b.rejectLocked(ErrDiskBudgetExceeded)
	}
	planned := max(old.planned, diskBudgetRound(size))
	growth := planned - max(old.actual, old.planned)
	if planned < max(old.actual, old.planned) {
		growth = 0
	}
	if growth > 0 {
		if err := b.refreshFreeLocked(); err != nil {
			return nil, b.rejectLocked(err)
		}
		ceiling := b.cfg.LimitBytes
		if !maintenance {
			ceiling -= b.cfg.ReserveBytes
		}
		total := addSaturate(b.used, b.reserved)
		freeNeed := addSaturate(b.cfg.MinFreeBytes, addSaturate(b.reserved, growth))
		if (!maintenance && b.blocked) || total > ceiling || growth > ceiling-total || b.available < freeNeed {
			return nil, b.rejectLocked(ErrDiskBudgetExceeded)
		}
	}
	b.files[path] = diskBudgetFile{actual: old.actual, planned: planned}
	b.reserved += growth
	b.updatePressureLocked()
	return func() {
		b.mu.Lock()
		e := b.files[path]
		before := max(e.actual, e.planned) - e.actual
		e.planned = max(old.planned, e.actual)
		b.files[path] = e
		b.reserved -= before - (max(e.actual, e.planned) - e.actual)
		b.mu.Unlock()
	}, nil
}

func (b *fileDiskBudget) rejectLocked(err error) error {
	b.rejections++
	b.blocked = true
	b.pressure = true
	b.kick()
	return fmt.Errorf("%w: %v", ErrDiskBudgetExceeded, err)
}
func (b *fileDiskBudget) refreshFreeLocked() error {
	available, inodes, err := diskBudgetFree(b.root)
	b.available, b.inodes, b.measurementErr = available, inodes, err
	return err
}
func (b *fileDiskBudget) updatePressureLocked() {
	total := addSaturate(b.used, b.reserved)
	if total >= b.cfg.LimitBytes/5*4 {
		b.pressure = true
	}
	if total <= b.cfg.LimitBytes/5*3 && b.measurementErr == nil && b.available >= addSaturate(b.cfg.MinFreeBytes, b.cfg.ReserveBytes) {
		b.pressure = false
		b.blocked = false
	}
	if b.pressure {
		b.kick()
	}
}

// completed is used only after a write/delete/rename is finished. Unlike a
// reconciliation scan it may release a buffered reservation for that file.
func (b *fileDiskBudget) completed(paths ...string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, path := range paths {
		old := b.files[path]
		b.used -= old.actual
		b.reserved -= max(old.actual, old.planned) - old.actual
		if info, err := os.Stat(path); err == nil {
			n := diskBudgetAllocated(info)
			b.files[path] = diskBudgetFile{actual: n, planned: n}
			b.used += n
		} else if os.IsNotExist(err) {
			delete(b.files, path)
		} else {
			b.files[path] = old
			b.used += old.actual
			b.reserved += max(old.actual, old.planned) - old.actual
			b.measurementErr = err
		}
	}
	b.updatePressureLocked()
}

func (b *fileDiskBudget) reconcile() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	observed := make(map[string]uint64)
	err := filepath.Walk(b.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("disk budget does not support symlink %s", path)
		}
		observed[path] = diskBudgetAllocated(info)
		return nil
	})
	if err != nil {
		b.measurementErr = err
		b.blocked = true
		return err
	}
	// Preserve projected growth even when a newly reserved file has not yet
	// been created. A scan cannot grant another writer those bytes.
	for path, old := range b.files {
		actual, exists := observed[path]
		if !exists && old.planned <= old.actual {
			delete(b.files, path)
			continue
		}
		old.actual = actual
		b.files[path] = old
		delete(observed, path)
	}
	for path, actual := range observed {
		b.files[path] = diskBudgetFile{actual: actual, planned: actual}
	}
	b.used, b.reserved = 0, 0
	for _, e := range b.files {
		b.used += e.actual
		b.reserved += max(e.actual, e.planned) - e.actual
	}
	if err = b.refreshFreeLocked(); err != nil {
		b.blocked = true
		return err
	}
	if b.available < b.cfg.MinFreeBytes {
		b.blocked = true
		b.pressure = true
	}
	b.updatePressureLocked()
	return nil
}

func (b *fileDiskBudget) snapshot() *DiskBudgetStats {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state := "normal"
	if b.pressure {
		state = "pressure"
	}
	if b.blocked {
		state = "blocked"
	}
	if b.measurementErr != nil {
		state = "unavailable"
	}
	value := &DiskBudgetStats{DiskBudgetConfig: b.cfg, UsedBytes: b.used, ReservedBytes: b.reserved, AvailableBytes: b.available, AvailableInodes: b.inodes, Pressure: b.pressure, Admission: !b.blocked && b.measurementErr == nil, State: state, Rejections: b.rejections}
	if b.measurementErr != nil {
		value.Error = b.measurementErr.Error()
	}
	return value
}

func (b *fileDiskBudget) register(t *fileS3Tier) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tiers[t] = struct{}{}
	b.mu.Unlock()
	b.kick()
}
func (b *fileDiskBudget) unregister(t *fileS3Tier) {
	if b == nil {
		return
	}
	b.mu.Lock()
	delete(b.tiers, t)
	b.mu.Unlock()
}
func (b *fileDiskBudget) run(quit <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-quit:
			return
		case <-ticker.C:
		case <-b.wake:
		}
		_ = b.reconcile()
		b.mu.Lock()
		pressure := b.pressure
		tiers := make([]*fileS3Tier, 0, len(b.tiers))
		for tier := range b.tiers {
			tiers = append(tiers, tier)
		}
		b.mu.Unlock()
		if !pressure {
			continue
		}
		for _, tier := range tiers {
			select {
			case <-quit:
				return
			default:
			}
			if !b.snapshot().Pressure {
				break
			}
			_ = tier.reclaimDiskBudgetBlock()
			_ = b.reconcile()
		}
		// Rate limit persistent full/outage conditions independently of write rate.
		select {
		case <-quit:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// observed transfers flushed bytes from projected growth to allocated usage,
// without discarding reservations for a later buffered tail of the same file.
func (b *fileDiskBudget) observed(path string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	info, err := os.Stat(path)
	if err != nil {
		b.measurementErr = err
		return
	}
	e := b.files[path]
	b.used -= e.actual
	b.reserved -= max(e.actual, e.planned) - e.actual
	e.actual = diskBudgetAllocated(info)
	b.files[path] = e
	b.used += e.actual
	b.reserved += max(e.actual, e.planned) - e.actual
	b.updatePressureLocked()
}

// renamedTree transfers the allocation ledger without temporarily granting the
// restored bytes to concurrent publishers. The destination must be absent before
// the filesystem rename, as required by the restore activation protocol.
func (b *fileDiskBudget) renamedTree(oldDir, newDir string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for path, entry := range b.files {
		if path == oldDir || strings.HasPrefix(path, oldDir+string(os.PathSeparator)) {
			destination := newDir + strings.TrimPrefix(path, oldDir)
			previous := b.files[destination]
			// A scan may already have observed the renamed destination.
			b.used -= previous.actual
			b.reserved -= max(previous.actual, previous.planned) - previous.actual
			b.files[destination] = entry
			delete(b.files, path)
		}
	}
}

func (b *fileDiskBudget) forgetTree(dir string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for path, e := range b.files {
		if path == dir || strings.HasPrefix(path, dir+string(os.PathSeparator)) {
			b.used -= e.actual
			b.reserved -= max(e.actual, e.planned) - e.actual
			delete(b.files, path)
		}
	}
	b.updatePressureLocked()
}

func mkdirAllWithDiskBudget(dios *diskIOSemaphore, path string, mode os.FileMode) error {
	b := dios.budget.Load()
	if b == nil {
		return os.MkdirAll(path, mode)
	}
	var reserved []string
	for parent := filepath.Clean(path); b.owns(parent); parent = filepath.Dir(parent) {
		if _, err := os.Stat(parent); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return err
		}
		if _, err := b.reserveGrowth(parent, 4096, true); err != nil {
			for _, dir := range reserved {
				b.completed(dir)
			}
			return err
		}
		reserved = append(reserved, parent)
		if parent == b.root {
			break
		}
	}
	defer func() {
		for _, dir := range reserved {
			b.completed(dir)
		}
	}()
	return os.MkdirAll(path, mode)
}
