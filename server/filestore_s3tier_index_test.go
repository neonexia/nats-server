// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Faults surround the real object store, including a persisted write whose
// response is lost. They do not replace conditional-write or verification I/O.
type indexFaultStore struct {
	S3TierObjectStore
	mu           sync.Mutex
	failPrefix   string
	loseResponse bool
	conflict     bool
	failed       bool
	offline      bool
	gets         int
	puts         map[string]int
}

func (s *indexFaultStore) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	s.mu.Lock()
	s.puts[key]++
	fail := !s.failed && s.failPrefix != "" && strings.Contains(key, s.failPrefix)
	if fail {
		s.failed = true
	}
	lost := s.loseResponse
	conflict := s.conflict
	offline := s.offline
	s.mu.Unlock()
	if offline {
		return false, errors.New("injected object store outage")
	}
	if fail && conflict {
		_, err := s.S3TierObjectStore.PutIfAbsent(ctx, key, []byte("conflicting immutable object"))
		return false, err
	}
	if fail && !lost {
		return false, errors.New("injected object publication failure")
	}
	created, err := s.S3TierObjectStore.PutIfAbsent(ctx, key, data)
	if fail && err == nil {
		return false, errors.New("injected lost publication response")
	}
	return created, err
}
func (s *indexFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets++
	offline := s.offline
	s.mu.Unlock()
	if offline {
		return nil, errors.New("injected object store outage")
	}
	return s.S3TierObjectStore.Get(ctx, key)
}
func (s *indexFaultStore) List(ctx context.Context, key string) ([]string, error) {
	s.mu.Lock()
	offline := s.offline
	s.mu.Unlock()
	if offline {
		return nil, errors.New("injected object store outage")
	}
	return s.S3TierObjectStore.List(ctx, key)
}
func TestFileStoreS3TierIndexPublicationFailuresCompatibleEndpoint(t *testing.T) {
	real := testS3TierCompatibleStore(t)
	for _, stage := range []string{"descriptor-put", "descriptor-response", "page-put", "root-response", "local-root", "local-descriptor", "batch-root-response", "batch-local-descriptor", "pending-membership"} {
		t.Run(stage, func(t *testing.T) {
			store := &indexFaultStore{S3TierObjectStore: real, puts: make(map[string]int)}
			dir := t.TempDir()
			prefix := fmt.Sprintf("paged-failure/%d/%s", time.Now().UnixNano(), stage)
			fs, fcfg, cfg := testTieredFileStore(t, store, dir, prefix)
			stopS3TierWorker(t, fs.tier)
			fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
			fcfg.S3Tier.LocalHighBytes, fcfg.S3Tier.LocalLowBytes = 1<<30, 1<<29
			payload := bytes.Repeat([]byte("x"), 16*1024)
			for i := 0; i < 4; i++ {
				if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
					t.Fatal(err)
				}
			}
			var localFault string
			switch stage {
			case "descriptor-put", "descriptor-response":
				store.failPrefix = "/descriptors/"
				store.loseResponse = stage == "descriptor-response"
			case "page-put":
				store.failPrefix = "/index/pages/"
			case "root-response", "batch-root-response", "pending-membership":
				store.failPrefix = "/manifests/"
				store.loseResponse = true
			case "local-root":
				localFault = fs.tier.manifestFile()
			case "local-descriptor":
				localFault = fs.tier.descriptorFile(1)
			case "batch-local-descriptor":
				localFault = fs.tier.descriptorFile(2)
			}
			if localFault != "" {
				if err := os.Mkdir(localFault, defaultDirPerms); err != nil {
					t.Fatal(err)
				}
			}
			fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
			var publishErr error
			if strings.HasPrefix(stage, "batch-") {
				_, publishErr = fs.DrainS3Tier()
			} else {
				publishErr = fs.tier.evictToBudget()
			}
			if publishErr == nil {
				t.Fatal("expected publication failure")
			}
			if stage == "pending-membership" {
				path := fs.tier.indexPendingFile()
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var pending s3IndexPending
				if err = json.Unmarshal(original, &pending); err != nil {
					t.Fatal(err)
				}
				pending.Added[0].Size++
				tampered, err := json.Marshal(pending)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, tampered, defaultFilePerms); err != nil {
					t.Fatal(err)
				}
				if err = fs.tier.evictToBudget(); err == nil || !strings.Contains(err.Error(), "absent from index") {
					t.Fatalf("mismatched pending descriptor accepted: %v", err)
				}
				if err = os.WriteFile(path, original, defaultFilePerms); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(filepath.Join(dir, msgDir, "1.blk")); err != nil {
				t.Fatalf("failed publication reclaimed payload: %v", err)
			}
			if err := fs.Stop(); err != nil {
				t.Fatal(err)
			}
			if localFault != "" {
				if err := os.Remove(localFault); err != nil {
					t.Fatal(err)
				}
			}
			store.mu.Lock()
			store.offline = true
			gets := store.gets
			store.mu.Unlock()
			reopened, err := newFileStore(fcfg, cfg)
			if err != nil {
				t.Fatalf("offline restart: %v", err)
			}
			defer reopened.Stop()
			stopS3TierWorker(t, reopened.tier)
			store.mu.Lock()
			if store.gets != gets {
				t.Errorf("same-node restart contacted object store: %d reads", store.gets-gets)
			}
			store.offline = false
			store.mu.Unlock()
			for seq := uint64(1); seq <= 4; seq++ {
				msg, err := reopened.LoadMsg(seq, nil)
				if err != nil || !bytes.Equal(msg.msg, payload) {
					t.Fatalf("local recovery sequence %d: %v", seq, err)
				}
			}
			if seq, _, err := reopened.StoreMsg("events", nil, payload, 0); err != nil || seq != 5 {
				t.Fatalf("new publish after restart: sequence=%d error=%v", seq, err)
			}
			if _, err := reopened.DrainS3Tier(); err != nil {
				t.Fatalf("retry drain: %v", err)
			}
			rootData, err := os.ReadFile(reopened.tier.manifestFile())
			if err != nil {
				t.Fatal(err)
			}
			var root s3TierManifest
			if err = json.Unmarshal(rootData, &root); err != nil {
				t.Fatal(err)
			}
			blocks, _, err := reopened.tier.loadManifestIndex(root)
			if err != nil {
				t.Fatalf("remote committed index: %v", err)
			}
			if len(blocks) != 5 {
				t.Fatalf("committed blocks=%d, want5", len(blocks))
			}
			if _, err := os.Stat(reopened.tier.indexPendingFile()); !os.IsNotExist(err) {
				t.Fatalf("pending journal not retired: %v", err)
			}
			reopened.tier.cfg.LocalHighBytes, reopened.tier.cfg.LocalLowBytes = 32*1024, 16*1024
			if err := reopened.tier.evictToBudget(); err != nil {
				t.Fatal(err)
			}
			for seq := uint64(1); seq <= 5; seq++ {
				msg, err := reopened.LoadMsg(seq, nil)
				if err != nil || !bytes.Equal(msg.msg, payload) {
					t.Fatalf("cold recovery sequence %d: %v", seq, err)
				}
			}
			if stage == "root-response" || stage == "batch-root-response" {
				store.mu.Lock()
				retries := store.puts[reopened.tier.manifestKey(1)]
				store.mu.Unlock()
				if retries < 2 {
					t.Fatalf("lost root response did not retry same immutable key: %d", retries)
				}
			}
		})
	}
}

func TestFileStoreS3TierPagedIndexBoundsCompatibleEndpoint(t *testing.T) {
	testFileStoreS3TierPagedIndexBounds(t, 130, 1)
}

func TestFileStoreS3TierPagedIndexDeepBoundsCompatibleEndpoint(t *testing.T) {
	testFileStoreS3TierPagedIndexBounds(t, 2100, 2)
}

func testFileStoreS3TierPagedIndexBounds(t *testing.T, count int, minimumLevel uint8) {
	t.Helper()
	store := testS3TierCompatibleStore(t)
	fs, fcfg, cfg := testTieredFileStore(t, store, t.TempDir(), fmt.Sprintf("paged-bounds/%d", time.Now().UnixNano()))
	defer func() { fs.Stop() }()
	stopS3TierWorker(t, fs.tier)
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
	fcfg.S3Tier.LocalHighBytes, fcfg.S3Tier.LocalLowBytes = 1<<30, 1<<29
	payload := bytes.Repeat([]byte("b"), 16*1024)
	for i := 0; i < count; i++ {
		if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fs.DrainS3Tier(); err != nil {
		t.Fatal(err)
	}
	rootData, err := os.ReadFile(fs.tier.manifestFile())
	if err != nil {
		t.Fatal(err)
	}
	if len(rootData) > 2048 {
		t.Fatalf("root grew with descriptor history: %d bytes", len(rootData))
	}
	var root s3TierManifest
	if err = json.Unmarshal(rootData, &root); err != nil {
		t.Fatal(err)
	}
	blocks, pages, err := fs.tier.loadManifestIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != count || root.Root == nil || root.Root.Level < minimumLevel {
		t.Fatalf("expected level%d tree for%dblocks: root=%+v blocks=%d", minimumLevel, count, root, len(blocks))
	}
	for _, data := range pages {
		var p s3IndexPage
		if err = json.Unmarshal(data, &p); err != nil {
			t.Fatal(err)
		}
		if len(p.Blocks) > s3IndexPageSize || len(p.Children) > s3IndexPageSize {
			t.Fatal("unbounded index page")
		}
	}
	cached, err := os.ReadDir(filepath.Dir(fs.tier.indexPageFile("")))
	if err != nil {
		t.Fatal(err)
	}
	if len(cached) != len(pages) {
		t.Fatalf("local cache retained obsolete generations: %d files, %d reachable pages", len(cached), len(pages))
	}
	fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
	if err := fs.tier.evictToBudget(); err != nil {
		t.Fatal(err)
	}
	// Reopen the committed local pages before cold reads; a deeper root must
	// retain the same bounded cache and sequence routing after restart.
	if count > 130 {
		if err := fs.Stop(); err != nil {
			t.Fatal(err)
		}
		fs, err = newFileStore(fcfg, cfg)
		if err != nil {
			t.Fatal(err)
		}
		stopS3TierWorker(t, fs.tier)
	}
	for _, seq := range []uint64{1, 64, 65, uint64(count - 1), uint64(count)} {
		m, err := fs.LoadMsg(seq, nil)
		if err != nil || !bytes.Equal(m.msg, payload) {
			t.Fatalf("page boundary read%d: %v", seq, err)
		}
	}
}

func TestFileStoreS3TierIndexConflictKeepsLocalCompatibleEndpoint(t *testing.T) {
	real := testS3TierCompatibleStore(t)
	for _, kind := range []string{"/index/pages/", "/manifests/"} {
		t.Run(strings.Trim(kind, "/"), func(t *testing.T) {
			store := &indexFaultStore{S3TierObjectStore: real, failPrefix: kind, conflict: true, puts: make(map[string]int)}
			fs, _, _ := testTieredFileStore(t, store, t.TempDir(), fmt.Sprintf("paged-conflict/%d", time.Now().UnixNano()))
			defer fs.Stop()
			stopS3TierWorker(t, fs.tier)
			fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 1<<30, 1<<29
			payload := bytes.Repeat([]byte("conflict"), 4096)
			for i := 0; i < 3; i++ {
				if _, _, err := fs.StoreMsg("events", nil, payload, 0); err != nil {
					t.Fatal(err)
				}
			}
			fs.tier.cfg.LocalHighBytes, fs.tier.cfg.LocalLowBytes = 32*1024, 16*1024
			for retry := 0; retry < 2; retry++ {
				if err := fs.tier.evictToBudget(); err == nil {
					t.Fatal("conflicting immutable index object accepted")
				}
				if _, err := os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")); err != nil {
					t.Fatalf("conflict reclaimed local payload: %v", err)
				}
				m, err := fs.LoadMsg(1, nil)
				if err != nil || !bytes.Equal(m.msg, payload) {
					t.Fatalf("local read after conflict: %v", err)
				}
			}
		})
	}
}
