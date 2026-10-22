// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDiskBudgetPreservesBufferedReservationsAcrossScan(t *testing.T) {
	root := t.TempDir()
	b, err := newFileDiskBudget(root, DiskBudgetConfig{LimitBytes: 256 * 1024, ReserveBytes: 32 * 1024, MinFreeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	baseline := b.snapshot().UsedBytes
	path := filepath.Join(root, "buffered.blk")
	if _, err = b.reserveGrowth(path, 32*1024, false); err != nil {
		t.Fatal(err)
	}
	if err = b.reconcile(); err != nil {
		t.Fatal(err)
	}
	if got := b.snapshot(); got.ReservedBytes != 32*1024 || got.UsedBytes != baseline {
		t.Fatalf("scan erased pending allocation: %+v", got)
	}
	if err = os.WriteFile(path, make([]byte, 16*1024), 0600); err != nil {
		t.Fatal(err)
	}
	b.observed(path)
	if got := b.snapshot(); got.ReservedBytes != 16*1024 || got.UsedBytes != baseline+16*1024 {
		t.Fatalf("flush transfer=%+v", got)
	}
	if err = os.WriteFile(path, make([]byte, 32*1024), 0600); err != nil {
		t.Fatal(err)
	}
	b.observed(path)
	if got := b.snapshot(); got.ReservedBytes != 0 {
		t.Fatalf("flushed reservation retained: %+v", got)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	b.completed(path)
	if got := b.snapshot(); got.UsedBytes != baseline {
		t.Fatalf("unlink accounting=%+v", got)
	}
}

func TestDiskBudgetConcurrentReservationsCannotOverspend(t *testing.T) {
	root := t.TempDir()
	b, err := newFileDiskBudget(root, DiskBudgetConfig{LimitBytes: 128 * 1024, ReserveBytes: 16 * 1024, MinFreeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for index := 0; index < 32; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b.reserveGrowth(filepath.Join(root, fmt.Sprintf("%d.blk", index)), 8192, false)
			if err != nil && !errors.Is(err, ErrDiskBudgetExceeded) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got := b.snapshot()
	if got.UsedBytes+got.ReservedBytes > got.LimitBytes-got.ReserveBytes || got.Rejections == 0 || got.Admission {
		t.Fatalf("overspent or failed to block: %+v", got)
	}
	if err := b.reconcile(); err != nil {
		t.Fatal(err)
	}
	if after := b.snapshot(); after.ReservedBytes != got.ReservedBytes {
		t.Fatalf("scan freed unflushed bytes: before=%+v after=%+v", got, after)
	}
}

func TestDiskBudgetMeasurementFailureFailsClosedAndRecovers(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	b, err := newFileDiskBudget(root, DiskBudgetConfig{LimitBytes: 128 * 1024, ReserveBytes: 16 * 1024, MinFreeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(parent, "moved")
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if _, err = b.reserveGrowth(filepath.Join(root, "message.blk"), 4096, false); !errors.Is(err, ErrDiskBudgetExceeded) {
		t.Fatalf("missing volume admission=%v", err)
	}
	if got := b.snapshot(); got.Admission || got.State != "unavailable" {
		t.Fatalf("missing volume state=%+v", got)
	}
	if err := os.Rename(moved, root); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcile(); err != nil {
		t.Fatal(err)
	}
	if _, err = b.reserveGrowth(filepath.Join(root, "message.blk"), 4096, false); err != nil {
		t.Fatalf("healthy volume did not resume: %v", err)
	}
}

func TestJetStreamDiskBudgetAggregatesStreamsAndRejectsTransiently(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 256 * 1024, ReserveBytes: 32 * 1024, MinFreeBytes: 1}
	s := RunServer(&opts)
	defer s.Shutdown()
	var stores []*fileStore
	for _, name := range []string{"A", "B"} {
		m, err := s.GlobalAccount().addStream(&StreamConfig{Name: name, Subjects: []string{name}, Storage: FileStorage})
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, m.store.(*fileStore))
	}
	payload := make([]byte, 8192)
	var rejected bool
	for index := 0; index < 100; index++ {
		fs := stores[index%2]
		before := fs.State()
		_, _, err := fs.StoreMsg("events", nil, payload, 0)
		if errors.Is(err, ErrDiskBudgetExceeded) {
			rejected = true
			after := fs.State()
			if before.Msgs != after.Msgs || before.LastSeq != after.LastSeq {
				t.Fatalf("rejected message advanced state: before=%+v after=%+v", before, after)
			}
			if fs.werr != nil {
				t.Fatalf("capacity poisoned stream: %v", fs.werr)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !rejected {
		t.Fatal("aggregate budget never rejected")
	}
	stats, err := s.Jsz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DiskBudget == nil || stats.DiskBudget.Admission || stats.DiskBudget.UsedBytes+stats.DiskBudget.ReservedBytes > stats.DiskBudget.LimitBytes {
		t.Fatalf("budget stats=%+v", stats.DiskBudget)
	}
	for _, fs := range stores {
		if err := fs.FlushAllPending(); err != nil {
			t.Fatalf("accepted buffered data could not flush: %v", err)
		}
		if state := fs.State(); state.Msgs > 0 {
			if _, err := fs.LoadMsg(state.FirstSeq, nil); err != nil {
				t.Fatalf("local read during rejection: %v", err)
			}
		}
	}
}

func TestJetStreamDiskBudgetPressureCoversPartialBlocks(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 512 * 1024, ReserveBytes: 64 * 1024, MinFreeBytes: 1}
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{}
	for _, name := range []string{"A", "B"} {
		opts.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: remote, Prefix: "pressure/" + name, BlockSize: 1024 * 1024, LocalHighBytes: 16 * 1024 * 1024, LocalLowBytes: 8 * 1024 * 1024, RemoteHighBytes: 16 * 1024 * 1024, RemoteLowBytes: 8 * 1024 * 1024}
	}
	s := RunServer(&opts)
	defer s.Shutdown()
	var stores []*fileStore
	for _, name := range []string{"A", "B"} {
		m, err := s.GlobalAccount().addStream(&StreamConfig{Name: name, Subjects: []string{name}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true})
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, m.store.(*fileStore))
	}
	for index := 0; index < 6; index++ {
		if _, _, err := stores[index%2].StoreMsg("events", nil, make([]byte, 64*1024), 0); err != nil {
			if errors.Is(err, ErrDiskBudgetExceeded) {
				break
			}
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var evicted uint64
		for _, fs := range stores {
			evicted += fs.tier.stats.evictions.Load()
		}
		stats := s.diskIOSemaphore().budget.Load().snapshot()
		if evicted > 0 && !stats.Pressure && stats.Admission {
			if _, _, err := stores[0].StoreMsg("events", nil, []byte("resumed"), 0); err != nil {
				t.Fatalf("publish did not resume: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("partial blocks not reclaimed: %+v", s.diskIOSemaphore().budget.Load().snapshot())
}

func TestDiskBudgetRestoreReservesWholeFootprintBeforeFetch(t *testing.T) {
	root := t.TempDir()
	b, err := newFileDiskBudget(root, DiskBudgetConfig{LimitBytes: 128 * 1024, ReserveBytes: 16 * 1024, MinFreeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	counted := &countingS3BlockStore{store: &testS3BlockStore{objects: map[string][]byte{}}}
	tier := newFileS3Tier(nil, S3TierConfig{Store: counted, Prefix: "restore"})
	material := &s3TierRemoteRestoreMaterial{manifest: s3TierManifest{Blocks: []s3BlockDescriptor{{Index: 1, Size: 256 * 1024, Key: "remote-block"}}}}
	err = tier.stageRemoteRestore(filepath.Join(root, "stage"), material, b)
	if !errors.Is(err, ErrDiskBudgetExceeded) {
		t.Fatalf("oversized restore error=%v", err)
	}
	counted.mu.Lock()
	gets := len(counted.gets)
	counted.mu.Unlock()
	if gets != 0 {
		t.Fatalf("downloaded %d blocks before admitting complete restore", gets)
	}
	if got := b.snapshot(); got.ReservedBytes != 0 {
		t.Fatalf("restore rejection leaked reservations: %+v", got)
	}
}

func TestJetStreamDiskBudgetConfigurationAndStartupAccounting(t *testing.T) {
	root := t.TempDir()
	config := createConfFile(t, []byte(fmt.Sprintf(`port:-1
jetstream { store_dir: %q, disk_budget { limit_bytes: 1M, reserve_bytes: 64K, min_free_bytes: 1K } }
`, root)))
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatal(err)
	}
	if opts.JetStreamDiskBudget == nil || opts.JetStreamDiskBudget.LimitBytes != 1000000 {
		t.Fatalf("config=%+v", opts.JetStreamDiskBudget)
	}
	jsroot := filepath.Join(root, JetStreamStoreDir)
	if err := os.MkdirAll(jsroot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jsroot, "existing.data"), make([]byte, 128*1024), 0600); err != nil {
		t.Fatal(err)
	}
	s := RunServer(opts)
	defer s.Shutdown()
	stats, err := s.Jsz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DiskBudget == nil || stats.DiskBudget.UsedBytes < 128*1024 {
		t.Fatalf("startup ignored existing files: %+v", stats.DiskBudget)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("unchanged disk budget reload: %v", err)
	}
	if err := os.WriteFile(config, []byte(fmt.Sprintf(`port:-1
jetstream { store_dir: %q, disk_budget { limit_bytes: 2M, reserve_bytes: 64K, min_free_bytes: 1K } }
`, root)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Reload(); err == nil || !strings.Contains(err.Error(), "reload not supported for JetStream disk_budget") {
		t.Fatalf("changed disk budget reload error=%v", err)
	}
	stats, err = s.Jsz(nil)
	if err != nil || stats.DiskBudget.LimitBytes != 1000000 {
		t.Fatalf("rejected reload changed enforced budget: stats=%+v err=%v", stats, err)
	}
}

func TestJetStreamDiskBudgetPreservesStreamResidencyLimit(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 4 * 1024 * 1024, ReserveBytes: 512 * 1024, MinFreeBytes: 1}
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/A": {Store: remote, Prefix: "residency/A", BlockSize: 1024 * 1024, LocalHighBytes: 128 * 1024, LocalLowBytes: 64 * 1024, RemoteHighBytes: 2 * 1024 * 1024, RemoteLowBytes: 1024 * 1024}}
	s := RunServer(&opts)
	defer s.Shutdown()
	m, err := s.GlobalAccount().addStream(&StreamConfig{Name: "A", Subjects: []string{"A"}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true})
	if err != nil {
		t.Fatal(err)
	}
	fs := m.store.(*fileStore)
	payload := make([]byte, 80*1024)
	// The first partial block fits. The next publish must be rejected until
	// asynchronous coverage/reclamation creates room, despite global headroom.
	for cycle := 0; cycle < 3; cycle++ {
		if _, _, err := fs.StoreMsg("A", nil, payload, 0); err != nil {
			t.Fatalf("cycle %d first publish: %v", cycle, err)
		}
		if _, _, err := fs.StoreMsg("A", nil, payload, 0); !errors.Is(err, errS3TierLocalCapacity) {
			t.Fatalf("cycle %d residency admission error=%v", cycle, err)
		}
		checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
			if fs.tier.stats.evictions.Load() < uint64(cycle+1) {
				return fmt.Errorf("waiting for partial-block eviction %d", cycle+1)
			}
			return nil
		})
	}
	if s.diskIOSemaphore().budget.Load().snapshot().Pressure {
		t.Fatal("per-stream residency unexpectedly required aggregate pressure")
	}
	if state := fs.State(); state.Msgs != 3 || state.LastSeq != 3 {
		t.Fatalf("rejected messages changed stream state: %+v", state)
	}
}

func TestJetStreamDiskBudgetRejectsNativeSnapshotRestore(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 1024 * 1024, ReserveBytes: 128 * 1024, MinFreeBytes: 1}
	s := RunServer(&opts)
	defer s.Shutdown()
	_, err := s.GlobalAccount().RestoreStream(&StreamConfig{Name: "SNAP", Storage: FileStorage}, strings.NewReader("must not be extracted"))
	if err == nil || !strings.Contains(err.Error(), "native snapshot restore is unavailable with disk_budget") {
		t.Fatalf("native restore admission error=%v", err)
	}
	if _, err := s.GlobalAccount().lookupStream("SNAP"); err == nil {
		t.Fatal("rejected snapshot restore created a stream")
	}
}

func TestJetStreamDiskBudgetPressureHonorsUploadBackoff(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamDiskBudget = &DiskBudgetConfig{LimitBytes: 512 * 1024, ReserveBytes: 64 * 1024, MinFreeBytes: 1}
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/A": {Store: remote, Prefix: "backoff/A", BlockSize: 1024 * 1024, LocalHighBytes: 4 * 1024 * 1024, LocalLowBytes: 2 * 1024 * 1024, RemoteHighBytes: 4 * 1024 * 1024, RemoteLowBytes: 2 * 1024 * 1024, RetryMin: 500 * time.Millisecond, RetryMax: time.Second}}
	s := RunServer(&opts)
	defer s.Shutdown()
	m, err := s.GlobalAccount().addStream(&StreamConfig{Name: "A", Subjects: []string{"A"}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true})
	if err != nil {
		t.Fatal(err)
	}
	fs := m.store.(*fileStore)
	remote.mu.Lock()
	remote.failPut = true
	remote.mu.Unlock()
	for index := 0; index < 8; index++ {
		if _, _, err := fs.StoreMsg("A", nil, make([]byte, 64*1024), 0); err != nil {
			if !errors.Is(err, ErrDiskBudgetExceeded) {
				t.Fatal(err)
			}
			break
		}
	}
	checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
		if fs.tier.stats.uploadErrors.Load() == 0 {
			return errors.New("waiting for failed upload")
		}
		return nil
	})
	// Repeated pressure and publisher kicks must not turn the 500ms retry
	// policy into a 100ms loop. Observe actual failed remote attempts.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		fs.tier.kick()
		s.diskIOSemaphore().budget.Load().kick()
		if failures := fs.tier.stats.uploadErrors.Load(); failures != 1 {
			t.Fatalf("backoff bypassed: %d remote failures", failures)
		}
		time.Sleep(10 * time.Millisecond)
	}
	remote.mu.Lock()
	remote.failPut = false
	remote.mu.Unlock()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if fs.tier.stats.evictions.Load() == 0 {
			return errors.New("waiting for recovery eviction")
		}
		return nil
	})
}

func TestDiskBudgetFilesystemHeadroomRejectsDespiteAssignedCapacity(t *testing.T) {
	root := t.TempDir()
	available, _, err := diskBudgetFree(root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newFileDiskBudget(root, DiskBudgetConfig{LimitBytes: 1024 * 1024, ReserveBytes: 128 * 1024, MinFreeBytes: available + 1<<40})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.reserveGrowth(filepath.Join(root, "message.blk"), 4096, false); !errors.Is(err, ErrDiskBudgetExceeded) {
		t.Fatalf("filesystem headroom admission error=%v", err)
	}
	if stats := b.snapshot(); stats.Admission || stats.ReservedBytes != 0 {
		t.Fatalf("insufficient filesystem headroom admitted allocation: %+v", stats)
	}
}

func TestDiskBudgetColdDownloadReservesBeforeRemoteRead(t *testing.T) {
	root := t.TempDir()
	b, err := newFileDiskBudget(root, DiskBudgetConfig{LimitBytes: 128 * 1024, ReserveBytes: 16 * 1024, MinFreeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	counted := &countingS3BlockStore{store: &testS3BlockStore{objects: make(map[string][]byte)}}
	fs := &fileStore{dios: &diskIOSemaphore{}}
	fs.dios.budget.Store(b)
	tier := newFileS3Tier(fs, S3TierConfig{Store: counted, Prefix: "cold"})
	err = tier.fetchBlock(s3BlockDescriptor{Index: 1, Size: 256 * 1024, Key: "cold-block"}, filepath.Join(root, "1.blk"))
	if !errors.Is(err, ErrDiskBudgetExceeded) {
		t.Fatalf("cold block admission error=%v", err)
	}
	counted.mu.Lock()
	gets := len(counted.gets)
	counted.mu.Unlock()
	if gets != 0 {
		t.Fatalf("downloaded %d remote objects before admission", gets)
	}
	if stats := b.snapshot(); stats.ReservedBytes != 0 {
		t.Fatalf("cold rejection leaked reservation: %+v", stats)
	}
}
