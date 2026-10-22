// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func archiveTestRequest(t *testing.T, nc *nats.Conn, operation string) JSApiStreamArchiveRemoteResponse {
	t.Helper()
	data, err := json.Marshal(JSApiStreamRemoteRestoreRequest{OperationID: operation})
	require_NoError(t, err)
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamArchiveRemoteT, "ARCHIVE"), data, 10*time.Second)
	require_NoError(t, err)
	var response JSApiStreamArchiveRemoteResponse
	require_NoError(t, json.Unmarshal(msg.Data, &response))
	return response
}

func archiveTestServer(t *testing.T, store S3TierObjectStore) (*Server, Options, *nats.Conn, nats.JetStreamContext) {
	t.Helper()
	opts := DefaultTestOptions
	opts.Port, opts.JetStream, opts.StoreDir = -1, true, t.TempDir()
	opts.JetStreamS3Tiers = map[string]*S3TierConfig{"$G/ARCHIVE": {
		Store: store, Prefix: "test/archive", BlockSize: 16 * 1024,
		LocalHighBytes: 1 << 30, LocalLowBytes: 1 << 29, RemoteHighBytes: 1 << 30, RemoteLowBytes: 1 << 29,
	}}
	s := RunServer(&opts)
	t.Cleanup(s.Shutdown)
	nc := clientConnectToServer(t, s)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "ARCHIVE", Subjects: []string{"archive"}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "SIBLING", Subjects: []string{"sibling"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	return s, opts, nc, js
}

func sealArchiveTestStream(t *testing.T, js nats.JetStreamContext) {
	t.Helper()
	info, err := js.StreamInfo("ARCHIVE")
	require_NoError(t, err)
	info.Config.Sealed = true
	_, err = js.UpdateStream(&info.Config)
	require_NoError(t, err)
}

func TestJetStreamS3TierArchiveRemovesOnlyRetiredFiles(t *testing.T) {
	for _, messages := range []int{0, 20} {
		t.Run(fmt.Sprintf("messages-%d", messages), func(t *testing.T) {
			store := &testS3BlockStore{objects: make(map[string][]byte)}
			s, opts, nc, js := archiveTestServer(t, store)
			for i := 0; i < messages; i++ {
				_, err := js.Publish("archive", []byte(strings.Repeat("payload", 1024)))
				require_NoError(t, err)
			}
			if response := archiveTestRequest(t, nc, "retire-1"); response.Error == nil || !strings.Contains(response.Error.Description, "sealed") {
				t.Fatalf("unsealed archive: %+v", response)
			}
			sealArchiveTestStream(t, js)
			response := archiveTestRequest(t, nc, "retire-1")
			if response.Error != nil || !response.Success || response.CoveredThrough != uint64(messages) || response.ArchiveKey != "test/archive/archive.json" {
				t.Fatalf("archive: %+v", response)
			}
			dir := filepath.Join(opts.StoreDir, "jetstream", "$G", streamsDir, "ARCHIVE")
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("archive directory remains: %v", err)
			}
			if _, err := js.StreamInfo("ARCHIVE"); err != nats.ErrStreamNotFound {
				t.Fatalf("archived stream visible: %v", err)
			}
			_, err := js.Publish("sibling", []byte("still-serving"))
			require_NoError(t, err)
			got, err := js.GetMsg("SIBLING", 1)
			require_NoError(t, err)
			if string(got.Data) != "still-serving" {
				t.Fatalf("sibling payload: %q", got.Data)
			}
			if response := archiveTestRequest(t, nc, "different-operation"); response.Error == nil {
				t.Fatal("conflicting operation succeeded")
			}
			_, err = js.AddStream(&nats.StreamConfig{Name: "ARCHIVE", Subjects: []string{"archive"}, Storage: nats.FileStorage})
			if err == nil || !strings.Contains(err.Error(), "archived") {
				t.Fatalf("archived name reused: %v", err)
			}
			store.mu.Lock()
			var receipt s3TierArchive
			require_NoError(t, json.Unmarshal(store.objects[response.ArchiveKey], &receipt))
			var manifest s3TierManifest
			require_NoError(t, json.Unmarshal(store.objects[receipt.Checkpoint.ManifestKey], &manifest))
			for _, block := range manifest.Blocks {
				if checksumS3Block(store.objects[block.Key]) != block.SHA256 {
					t.Errorf("lost remote block %s", block.Key)
				}
			}
			store.mu.Unlock()
			// Lost response and simultaneous retries must not change sibling
			// state or release the same stream reservation multiple times.
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if r := archiveTestRequest(t, nc, "retire-1"); r.Error != nil || !r.Success {
						t.Errorf("retry: %+v", r)
					}
				}()
			}
			wg.Wait()
			s.Shutdown()
			s.WaitForShutdown()
			s = RunServer(&opts)
			defer s.Shutdown()
			nc = clientConnectToServer(t, s)
			defer nc.Close()
			if r := archiveTestRequest(t, nc, "retire-1"); r.Error != nil || !r.Success {
				t.Fatalf("restart retry: %+v", r)
			}
		})
	}
}

func TestJetStreamS3TierArchiveFailurePreservesLocalData(t *testing.T) {
	for _, failure := range []string{"object-store", "corrupt-block", "tombstone-directory"} {
		t.Run(failure, func(t *testing.T) {
			store := &testS3BlockStore{objects: make(map[string][]byte)}
			s, _, nc, js := archiveTestServer(t, store)
			_, err := js.Publish("archive", []byte("preserve-this"))
			require_NoError(t, err)
			sealArchiveTestStream(t, js)
			_, jsa, err := s.GlobalAccount().checkForJetStream()
			require_NoError(t, err)
			var savedKey string
			var saved []byte
			if failure == "object-store" {
				store.mu.Lock()
				store.failPut = true
				store.mu.Unlock()
			}
			if failure == "corrupt-block" {
				msg, err := nc.Request(fmt.Sprintf(JSApiStreamDrainRemoteT, "ARCHIVE"), nil, 5*time.Second)
				require_NoError(t, err)
				var r JSApiStreamDrainRemoteResponse
				require_NoError(t, json.Unmarshal(msg.Data, &r))
				if r.Error != nil {
					t.Fatal(r.Error)
				}
				store.mu.Lock()
				for key, data := range store.objects {
					if strings.HasSuffix(key, ".blk") {
						savedKey, saved = key, append([]byte(nil), data...)
						store.objects[key] = []byte("corrupt")
						break
					}
				}
				store.mu.Unlock()
				if savedKey == "" {
					t.Fatal("no uploaded payload")
				}
			}
			parent := filepath.Dir(jsa.archiveTombstoneFile("ARCHIVE"))
			if failure == "tombstone-directory" {
				require_NoError(t, os.WriteFile(parent, []byte("blocked"), 0600))
			}
			if r := archiveTestRequest(t, nc, "retire-1"); r.Error == nil || r.Success {
				t.Fatalf("failure succeeded: %+v", r)
			}
			info, err := js.StreamInfo("ARCHIVE")
			require_NoError(t, err)
			if !info.Config.Sealed {
				t.Fatal("failed archive unsealed stream")
			}
			got, err := js.GetMsg("ARCHIVE", 1)
			require_NoError(t, err)
			if string(got.Data) != "preserve-this" {
				t.Fatalf("local data: %q", got.Data)
			}
			store.mu.Lock()
			store.failPut = false
			if savedKey != "" {
				store.objects[savedKey] = saved
			}
			store.mu.Unlock()
			if failure == "tombstone-directory" {
				require_NoError(t, os.Remove(parent))
			}
			if r := archiveTestRequest(t, nc, "retire-1"); r.Error != nil || !r.Success {
				t.Fatalf("retry: %+v", r)
			}
		})
	}
}

func TestJetStreamS3TierArchiveRestartFinishesPartialLocalCleanup(t *testing.T) {
	store := &testS3BlockStore{objects: make(map[string][]byte)}
	s, opts, nc, js := archiveTestServer(t, store)
	_, err := js.Publish("archive", []byte("preserved-remotely"))
	require_NoError(t, err)
	sealArchiveTestStream(t, js)
	_, jsa, err := s.GlobalAccount().checkForJetStream()
	require_NoError(t, err)
	dir := filepath.Join(jsa.storeDir, streamsDir, "ARCHIVE")
	if r := archiveTestRequest(t, nc, "retire-1"); r.Error != nil {
		t.Fatal(r.Error)
	}
	s.Shutdown()
	s.WaitForShutdown()
	// Model a process exit midway through removal: metadata can be missing,
	// but the separately durable tombstone and some payload files remain.
	require_NoError(t, os.MkdirAll(filepath.Join(dir, msgDir), 0700))
	require_NoError(t, os.WriteFile(filepath.Join(dir, msgDir, "1.blk"), []byte("leftover"), 0600))
	store.mu.Lock()
	store.failGet = true
	store.mu.Unlock()
	s = RunServer(&opts)
	defer s.Shutdown()
	nc = clientConnectToServer(t, s)
	defer nc.Close()
	js, err = nc.JetStream()
	require_NoError(t, err)
	if _, err = js.StreamInfo("ARCHIVE"); err != nats.ErrStreamNotFound {
		t.Fatalf("archive resurrected: %v", err)
	}
	if r := archiveTestRequest(t, nc, "retire-1"); r.Error != nil || !r.Success {
		t.Fatalf("offline cleanup retry: %+v", r)
	}
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("partial directory remains: %v", err)
	}
	_, err = js.Publish("sibling", []byte("survived-restart"))
	require_NoError(t, err)
}

func TestJetStreamS3TierArchiveWaitsForColdReadBeforeRemovingFiles(t *testing.T) {
	store := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, _, nc, js := archiveTestServer(t, store)
	_, err := js.Publish("archive", []byte("cold-payload"))
	require_NoError(t, err)
	sealArchiveTestStream(t, js)
	mset, err := s.GlobalAccount().lookupStream("ARCHIVE")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	_, err = fs.DrainS3Tier()
	require_NoError(t, err)
	// Evict the covered payload to exercise a read that has left the store
	// locks and is downloading from S3 when retirement starts.
	path := filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")
	fs.mu.Lock()
	mb := fs.blks[0]
	mb.mu.Lock()
	mb.closeFDsLockedNoCheck()
	mb.cache = nil
	err = os.Remove(path)
	mb.mu.Unlock()
	fs.mu.Unlock()
	require_NoError(t, err)
	store.mu.Lock()
	store.blocking = true
	store.mu.Unlock()
	readDone := make(chan error, 1)
	go func() {
		msg, err := fs.LoadMsg(1, nil)
		if err == nil && string(msg.msg) != "cold-payload" {
			err = fmt.Errorf("cold read returned %q", msg.msg)
		}
		readDone <- err
	}()
	select {
	case <-store.entered:
	case err := <-readDone:
		t.Fatalf("cold read finished without reaching the object store: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("cold read did not reach object store")
	}
	store.mu.Lock()
	store.blocking = false
	store.mu.Unlock()
	archiveDone := make(chan JSApiStreamArchiveRemoteResponse, 1)
	go func() { archiveDone <- archiveTestRequest(t, nc, "retire-1") }()
	_, jsa, err := s.GlobalAccount().checkForJetStream()
	require_NoError(t, err)
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error { _, err := os.Stat(jsa.archiveTombstoneFile("ARCHIVE")); return err })
	select {
	case response := <-archiveDone:
		t.Fatalf("cleanup finished with an inflight cold read: %+v", response)
	default:
	}
	close(store.release)
	if err := <-readDone; err != nil && err != ErrStoreClosed {
		t.Fatalf("cold read during retirement: %v", err)
	}
	select {
	case response := <-archiveDone:
		if response.Error != nil || !response.Success {
			t.Fatalf("archive: %+v", response)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("archive did not finish after the read")
	}
	if _, err := os.Stat(fs.fcfg.StoreDir); !os.IsNotExist(err) {
		t.Fatalf("cold read recreated local archive files: %v", err)
	}
	if _, err := fs.LoadMsg(1, nil); err != ErrStoreClosed {
		t.Fatalf("late hydration: %v", err)
	}
}
