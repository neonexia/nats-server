// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Qualify the stream-level primitive on occupied source and target engines. This
// does not assert namespace ownership, coordinator admission or an activation
// barrier: the current restore API activates immediately after verification.
func TestJetStreamS3TierSharedEngineRestoreCompatibleEndpoint(t *testing.T) {
	store := testS3TierCompatibleStore(t)
	prefix := fmt.Sprintf("shared-restore/%d", time.Now().UnixNano())
	start := func() (*Server, *Options) {
		o := DefaultTestOptions
		o.Port, o.JetStream, o.StoreDir = -1, true, t.TempDir()
		o.JetStreamS3Tiers = make(map[string]*S3TierConfig)
		for _, name := range []string{"ORDERS", "AUDIT", "BILLING"} {
			o.JetStreamS3Tiers["$G/"+name] = &S3TierConfig{Store: store, Prefix: prefix, BlockSize: 16 * 1024, LocalHighBytes: 128 << 10, LocalLowBytes: 64 << 10, RemoteHighBytes: 256 << 10, RemoteLowBytes: 128 << 10}
		}
		s := RunServer(&o)
		t.Cleanup(s.Shutdown)
		o.Port = s.Addr().(*net.TCPAddr).Port
		return s, &o
	}
	source, sourceOpts := start()
	target, targetOpts := start()
	sn, tn := clientConnectToServer(t, source), clientConnectToServer(t, target)
	defer sn.Close()
	defer tn.Close()
	sj, err := sn.JetStream()
	require_NoError(t, err)
	tj, err := tn.JetStream()
	require_NoError(t, err)
	create := func(js nats.JetStreamContext, name string) {
		_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage, Discard: nats.DiscardNew, DenyDelete: true, DenyPurge: true})
		require_NoError(t, err)
	}
	create(sj, "ORDERS")
	create(sj, "AUDIT")
	create(tj, "BILLING")
	for n := 1; n <= 32; n++ {
		_, err = sj.Publish("ORDERS", []byte(fmt.Sprintf("order-%d", n)))
		require_NoError(t, err)
	}
	_, err = sj.AddConsumer("ORDERS", &nats.ConsumerConfig{Durable: "worker", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Hour})
	require_NoError(t, err)
	reader, err := sj.PullSubscribe("ORDERS", "worker", nats.Bind("ORDERS", "worker"))
	require_NoError(t, err)
	messages, err := reader.Fetch(2, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_NoError(t, messages[0].AckSync())
	stop := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(stop) })
	type traffic struct {
		name  string
		js    nats.JetStreamContext
		count uint64
		err   error
	}
	siblings := []*traffic{{name: "AUDIT", js: sj}, {name: "BILLING", js: tj}}
	var wg sync.WaitGroup
	defer func() { once.Do(func() { close(stop) }); wg.Wait() }()
	for _, s := range siblings {
		wg.Add(1)
		go func(s *traffic) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				payload := []byte(fmt.Sprintf("%s-%d", s.name, s.count+1))
				ack, e := s.js.Publish(s.name, payload)
				if e != nil {
					s.err = e
					return
				}
				if ack.Sequence != s.count+1 {
					s.err = fmt.Errorf("sequence %d after %d", ack.Sequence, s.count)
					return
				}
				s.count++
				time.Sleep(5 * time.Millisecond)
			}
		}(s)
	}
	request := func(nc *nats.Conn, subject string, body any) JSApiStreamRemoteRestoreResponse {
		t.Helper()
		b, e := json.Marshal(body)
		require_NoError(t, e)
		m, e := nc.Request(subject, b, 20*time.Second)
		require_NoError(t, e)
		var r JSApiStreamRemoteRestoreResponse
		require_NoError(t, json.Unmarshal(m.Data, &r))
		if r.Error != nil || !r.Success {
			t.Fatalf("%s: %+v", subject, r)
		}
		return r
	}
	op := JSApiStreamRemoteRestoreRequest{OperationID: "shared-move"}
	request(sn, fmt.Sprintf(JSApiStreamPrepareRemoteRestoreT, "ORDERS"), op)
	if err := messages[1].AckSync(nats.AckWait(100 * time.Millisecond)); err == nil {
		t.Fatal("late source ACK falsely confirmed")
	}
	cp := request(sn, fmt.Sprintf(JSApiStreamCheckpointRemoteRestoreT, "ORDERS"), op)
	body := JSApiStreamRestoreRemoteRequest{OperationID: op.OperationID, ExpectedEpoch: cp.Epoch, TargetID: "shared-target"}
	restored := request(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "ORDERS"), body)
	if restored.State != "active" {
		t.Fatalf("restore state %q", restored.State)
	}
	request(tn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "ORDERS"), body)
	ci, err := tj.ConsumerInfo("ORDERS", "worker")
	require_NoError(t, err)
	if ci.AckFloor.Stream != 1 || ci.NumAckPending != 1 {
		t.Fatalf("consumer checkpoint: %+v", ci)
	}
	for n := uint64(1); n <= 32; n++ {
		m, e := tj.GetMsg("ORDERS", n)
		require_NoError(t, e)
		if string(m.Data) != fmt.Sprintf("order-%d", n) {
			t.Fatalf("corrupt order %d", n)
		}
	}
	ack, err := tj.Publish("ORDERS", []byte("order-33"))
	require_NoError(t, err)
	if ack.Sequence != 33 {
		t.Fatalf("new sequence %d", ack.Sequence)
	}
	if _, err = sj.Publish("ORDERS", []byte("old-owner")); err == nil {
		t.Fatal("source remained writable")
	}
	once.Do(func() { close(stop) })
	wg.Wait()
	// Restart both occupied engines after handoff. The source fence and target
	// durable ACK position must survive independently of any client connection.
	source.Shutdown()
	target.Shutdown()
	source = RunServer(sourceOpts)
	t.Cleanup(source.Shutdown)
	target = RunServer(targetOpts)
	t.Cleanup(target.Shutdown)
	rsn, rtn := clientConnectToServer(t, source), clientConnectToServer(t, target)
	defer rsn.Close()
	defer rtn.Close()
	rsj, e := rsn.JetStream()
	require_NoError(t, e)
	rtj, e := rtn.JetStream()
	require_NoError(t, e)
	siblings[0].js, siblings[1].js = rsj, rtj
	ci, err = rtj.ConsumerInfo("ORDERS", "worker")
	require_NoError(t, err)
	if ci.AckFloor.Stream != 1 || ci.NumAckPending != 1 {
		t.Fatalf("consumer checkpoint changed after restart: %+v", ci)
	}
	request(rtn, fmt.Sprintf(JSApiStreamRestoreRemoteT, "ORDERS"), body)
	if _, err = rsj.Publish("ORDERS", []byte("old-owner-after-restart")); err == nil {
		t.Fatal("source fence disappeared after restart")
	}
	for _, s := range siblings {
		if s.err != nil || s.count == 0 {
			t.Fatalf("sibling %s count=%d error=%v", s.name, s.count, s.err)
		}
		info, e := s.js.StreamInfo(s.name)
		require_NoError(t, e)
		if info.State.Msgs != s.count {
			t.Fatalf("sibling %s lost messages", s.name)
		}
		for n := uint64(1); n <= s.count; n++ {
			m, e := s.js.GetMsg(s.name, n)
			require_NoError(t, e)
			if string(m.Data) != fmt.Sprintf("%s-%d", s.name, n) {
				t.Fatalf("corrupt sibling %s/%d", s.name, n)
			}
		}
		t.Logf("sibling %s: %d confirmed messages preserved", s.name, s.count)
	}
	t.Log("current restore API activates before coordinator cutover; namespace admission barrier requires separate design/implementation")
}
