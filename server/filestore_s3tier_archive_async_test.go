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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func archiveAsyncRequest(t *testing.T, nc *nats.Conn, stream, operation string, status bool) JSApiStreamArchiveRemoteResponse {
	t.Helper()
	data, err := json.Marshal(JSApiStreamArchiveRemoteRequest{OperationID: operation, Async: true, StatusOnly: status})
	require_NoError(t, err)
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamArchiveRemoteT, stream), data, time.Second)
	require_NoError(t, err)
	var r JSApiStreamArchiveRemoteResponse
	require_NoError(t, json.Unmarshal(msg.Data, &r))
	if r.OperationID != operation {
		t.Fatalf("operation ID = %q, want %q", r.OperationID, operation)
	}
	return r
}

func awaitArchived(t *testing.T, nc *nats.Conn, operation string) JSApiStreamArchiveRemoteResponse {
	t.Helper()
	var response JSApiStreamArchiveRemoteResponse
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		response = archiveAsyncRequest(t, nc, "ARCHIVE", operation, true)
		if response.Error != nil {
			return fmt.Errorf("archive failed: %s", response.Error.Description)
		}
		if !response.Success || response.State != "archived" || response.ArchiveKey == "" {
			return fmt.Errorf("archive not complete: %+v", response)
		}
		return nil
	})
	return response
}

func TestJetStreamS3TierArchiveAsyncKeepsPollingAndSiblingTrafficResponsive(t *testing.T) {
	remote := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, opts, nc, js := archiveTestServer(t, remote)
	for i := 0; i < 20; i++ {
		_, err := js.Publish("archive", []byte(strings.Repeat("payload", 1024)))
		require_NoError(t, err)
	}
	sealArchiveTestStream(t, js)
	// Pre-cover the stream so the barrier delays final integrity verification,
	// rather than the ordinary background upload path.
	data, err := nc.Request(fmt.Sprintf(JSApiStreamDrainRemoteT, "ARCHIVE"), nil, time.Second)
	require_NoError(t, err)
	if strings.Contains(string(data.Data), `"error"`) {
		t.Fatalf("drain: %s", data.Data)
	}
	remote.resetBlockGets()
	remote.mu.Lock()
	remote.blocking = true
	remote.mu.Unlock()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(remote.release) }) })
	r := archiveAsyncRequest(t, nc, "ARCHIVE", "async-1", false)
	if r.Error != nil || r.Success || r.State != "running" {
		t.Fatalf("start: %+v", r)
	}
	select {
	case <-remote.entered:
	case <-time.After(time.Second):
		t.Fatal("archive did not begin verification")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(status bool) {
			defer wg.Done()
			r := archiveAsyncRequest(t, nc, "ARCHIVE", "async-1", status)
			if r.Error != nil || r.Success || r.State != "running" {
				t.Errorf("duplicate request: %+v", r)
			}
		}(i%2 == 0)
	}
	wg.Wait()
	if got := remote.blockGetCount(); got != 1 {
		t.Fatalf("duplicate requests started additional verification: gets=%d, want 1", got)
	}
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "conflict", false); r.Error == nil {
		t.Fatal("conflicting operation accepted")
	}
	if r := archiveAsyncRequest(t, nc, "SIBLING", "other", false); r.Error != nil || r.State != "busy" || r.Success {
		t.Fatalf("busy account: %+v", r)
	}
	if r := archiveAsyncRequest(t, nc, "SIBLING", "other", true); r.Error != nil || r.State != "not_started" || r.Success {
		t.Fatalf("unstarted status: %+v", r)
	}
	for i := 1; i <= 20; i++ {
		payload := []byte(fmt.Sprintf("sibling-%d", i))
		_, err := js.Publish("sibling", payload)
		require_NoError(t, err)
		got, err := js.GetMsg("SIBLING", uint64(i))
		require_NoError(t, err)
		if string(got.Data) != string(payload) {
			t.Fatalf("sibling message %d = %q", i, got.Data)
		}
	}
	release.Do(func() { close(remote.release) })
	r = awaitArchived(t, nc, "async-1")
	if r.CoveredThrough != 20 {
		t.Fatalf("coverage=%d, want 20", r.CoveredThrough)
	}
	s.Shutdown()
	s.WaitForShutdown()
	remote.testS3BlockStore.mu.Lock()
	remote.failGet = true
	remote.testS3BlockStore.mu.Unlock()
	s = RunServer(&opts)
	defer s.Shutdown()
	nc = clientConnectToServer(t, s)
	defer nc.Close()
	r = archiveAsyncRequest(t, nc, "ARCHIVE", "async-1", true)
	if r.Error != nil || !r.Success || r.State != "archived" || r.CoveredThrough != 20 {
		t.Fatalf("restart receipt: %+v", r)
	}
}

func TestJetStreamS3TierArchiveAsyncReportsFailureAndRetriesSameOperation(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	_, _, nc, js := archiveTestServer(t, remote)
	_, err := js.Publish("archive", []byte("preserve-on-failure"))
	require_NoError(t, err)
	sealArchiveTestStream(t, js)
	remote.mu.Lock()
	remote.failGet = true
	remote.mu.Unlock()
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "retry-1", false); r.Error != nil || r.State != "running" {
		t.Fatalf("start: %+v", r)
	}
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		r := archiveAsyncRequest(t, nc, "ARCHIVE", "retry-1", true)
		if r.State != "failed" || r.Error == nil || r.Success {
			return fmt.Errorf("failure not observable: %+v", r)
		}
		return nil
	})
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "retry-1", true); r.State != "failed" || r.Error == nil {
		t.Fatalf("status restarted failed work: %+v", r)
	}
	// Only the last transient task is retained per account. Another failed
	// stream can replace it, but must never turn an unknown result into success.
	if r := archiveAsyncRequest(t, nc, "SIBLING", "replacement", false); r.State != "running" || r.Error != nil {
		t.Fatalf("replacement start: %+v", r)
	}
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		r := archiveAsyncRequest(t, nc, "SIBLING", "replacement", true)
		if r.State != "failed" || r.Error == nil {
			return fmt.Errorf("replacement not failed: %+v", r)
		}
		return nil
	})
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "retry-1", true); r.State != "not_started" || r.Success || r.Error != nil {
		t.Fatalf("evicted transient result: %+v", r)
	}
	got, err := js.GetMsg("ARCHIVE", 1)
	require_NoError(t, err)
	if string(got.Data) != "preserve-on-failure" {
		t.Fatalf("payload=%q", got.Data)
	}
	remote.mu.Lock()
	remote.failGet = false
	remote.mu.Unlock()
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "retry-1", false); r.Error != nil || r.State != "running" {
		t.Fatalf("retry: %+v", r)
	}
	awaitArchived(t, nc, "retry-1")
}

func TestJetStreamS3TierArchiveAsyncShutdownLeavesResumableStream(t *testing.T) {
	remote := &blockingS3BlockStore{testS3BlockStore: &testS3BlockStore{objects: make(map[string][]byte)}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, opts, nc, js := archiveTestServer(t, remote, 100*time.Millisecond)
	_, err := js.Publish("archive", []byte("shutdown-safe"))
	require_NoError(t, err)
	sealArchiveTestStream(t, js)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamDrainRemoteT, "ARCHIVE"), nil, time.Second)
	require_NoError(t, err)
	remote.mu.Lock()
	remote.blocking = true
	remote.mu.Unlock()
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "shutdown-1", false); r.State != "running" || r.Error != nil {
		t.Fatalf("start: %+v", r)
	}
	select {
	case <-remote.entered:
	case <-time.After(time.Second):
		t.Fatal("archive did not reach blocked verification")
	}
	done := make(chan struct{})
	go func() { s.Shutdown(); s.WaitForShutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("background archive prevented bounded shutdown")
	}
	remote.mu.Lock()
	remote.blocking = false
	remote.mu.Unlock()
	close(remote.release)
	s = RunServer(&opts)
	defer s.Shutdown()
	nc = clientConnectToServer(t, s)
	defer nc.Close()
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "shutdown-1", true); r.State != "not_started" || r.Error != nil || r.Success {
		t.Fatalf("interrupted status: %+v", r)
	}
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "shutdown-1", false); r.State != "running" || r.Error != nil {
		t.Fatalf("resume: %+v", r)
	}
	awaitArchived(t, nc, "shutdown-1")
}

func TestJetStreamS3TierArchiveAsyncStatusRecognizesSynchronousRecovery(t *testing.T) {
	remote := &testS3BlockStore{objects: make(map[string][]byte)}
	_, _, nc, js := archiveTestServer(t, remote)
	sealArchiveTestStream(t, js)
	remote.mu.Lock()
	remote.failGet = true
	remote.mu.Unlock()
	archiveAsyncRequest(t, nc, "ARCHIVE", "sync-retry", false)
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		r := archiveAsyncRequest(t, nc, "ARCHIVE", "sync-retry", true)
		if r.State != "failed" || r.Error == nil {
			return fmt.Errorf("failure not recorded: %+v", r)
		}
		return nil
	})
	remote.mu.Lock()
	remote.failGet = false
	remote.mu.Unlock()
	if r := archiveTestRequest(t, nc, "sync-retry"); r.Error != nil || !r.Success {
		t.Fatalf("synchronous recovery: %+v", r)
	}
	if r := archiveAsyncRequest(t, nc, "ARCHIVE", "sync-retry", true); r.Error != nil || !r.Success || r.State != "archived" {
		t.Fatalf("stale asynchronous failure hid durable recovery: %+v", r)
	}
}
