// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const placementTestToken = "placement-test-control-token"

func runPlacementServer(t *testing.T) *Server {
	t.Helper()
	opts := DefaultTestOptions
	opts.Port = -1
	opts.Users = []*User{{Username: "control", Password: "control-secret"}, {Username: "app", Password: "app-secret"}}
	opts.Placement = &PlacementConfig{ControlUser: "control", ControlListen: "127.0.0.1:0", ControlToken: placementTestToken}
	s := RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

func placementRequest(t *testing.T, s *Server, path, token string, body any, want int) PlacementStatus {
	t.Helper()
	method := http.MethodGet
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
		method = http.MethodPost
	}
	request, err := http.NewRequest(method, "http://"+s.PlacementControlAddr().String()+"/v1/placement"+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("%s status=%d want=%d body=%s", path, response.StatusCode, want, data)
	}
	var status PlacementStatus
	if want < 300 {
		if err := json.Unmarshal(data, &status); err != nil {
			t.Fatal(err)
		}
	}
	return status
}

func TestPlacementAdmissionLifecycle(t *testing.T) {
	s := runPlacementServer(t)
	control, err := nats.Connect(s.ClientURL(), nats.UserInfo("control", "control-secret"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	connect := func(name, password string) (*nats.Conn, error) {
		return nats.Connect(s.ClientURL(), nats.UserInfo("app", password), nats.Name(name), nats.NoReconnect(), nats.Timeout(time.Second))
	}
	name := "alini/1/shard-a/placement-a"
	if c, err := connect(name, "app-secret"); err == nil {
		c.Close()
		t.Fatal("unbound process admitted application")
	}
	status := placementRequest(t, s, "", placementTestToken, nil, 200)
	binding := PlacementBinding{BootID: status.BootID, ShardID: "shard-a", Incarnation: "placement-a", OperationID: "operation-a"}
	placementRequest(t, s, "", "incorrect", nil, 401)
	stale := binding
	stale.BootID = "old-boot"
	placementRequest(t, s, "/bind", placementTestToken, stale, 409)
	placementRequest(t, s, "/activate", placementTestToken, binding, 409)
	bound := placementRequest(t, s, "/bind", placementTestToken, binding, 200)
	if bound.Active || bound.Binding == nil || *bound.Binding != binding {
		t.Fatalf("unexpected inactive binding: %+v", bound)
	}
	placementRequest(t, s, "/bind", placementTestToken, binding, 200)
	conflicting := binding
	conflicting.Incarnation = "other"
	placementRequest(t, s, "/bind", placementTestToken, conflicting, 409)
	if c, err := connect(name, "app-secret"); err == nil {
		c.Close()
		t.Fatal("inactive process admitted application")
	}
	placementRequest(t, s, "/activate", placementTestToken, binding, 200)
	placementRequest(t, s, "/activate", placementTestToken, binding, 200)
	for _, invalid := range []string{"", "alini/2/shard-a/placement-a", "alini/1/shard-a/stale", "alini/1/other/placement-a", "alini/1/shard-a/placement-a/extra"} {
		if c, err := connect(invalid, "app-secret"); err == nil {
			c.Close()
			t.Fatalf("admitted invalid name %q", invalid)
		}
	}
	if c, err := connect(name, "wrong"); err == nil {
		c.Close()
		t.Fatal("placement bypassed password authentication")
	}
	application, err := connect(name, "app-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	sub, err := control.SubscribeSync("placement.messages")
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := application.Publish("placement.messages", []byte("correct")); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil || string(msg.Data) != "correct" {
		t.Fatalf("message=%v err=%v", msg, err)
	}
	fenced := placementRequest(t, s, "/fence", placementTestToken, binding, 200)
	if fenced.Active || !fenced.Fenced {
		t.Fatalf("unexpected fence state %+v", fenced)
	}
	placementRequest(t, s, "/fence", placementTestToken, binding, 200)
	placementRequest(t, s, "/activate", placementTestToken, binding, 409)
	placementRequest(t, s, "/bind", placementTestToken, binding, 409)
	deadline := time.Now().Add(time.Second)
	for !application.IsClosed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !application.IsClosed() {
		t.Fatal("fence left application connection open")
	}
	if err := control.FlushTimeout(time.Second); err != nil {
		t.Fatalf("fence closed control connection: %v", err)
	}
	if c, err := connect(name, "app-secret"); err == nil {
		c.Close()
		t.Fatal("fenced process admitted application")
	}
}

func TestPlacementRejectsPipelinedApplicationTraffic(t *testing.T) {
	s := runPlacementServer(t)
	control, err := nats.Connect(s.ClientURL(), nats.UserInfo("control", "control-secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	sub, err := control.SubscribeSync("forbidden")
	if err != nil {
		t.Fatal(err)
	}
	control.Flush()
	for _, user := range []string{"app", "control"} {
		connection, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		password := "app-secret"
		if user == "control" {
			password = "wrong"
		}
		fmt.Fprintf(connection, "CONNECT {\"user\":%q,\"pass\":%q,\"name\":\"alini/1/shard/stale\"}\r\nPUB forbidden 4\r\noops\r\nPING\r\n", user, password)
		connection.SetReadDeadline(time.Now().Add(time.Second))
		io.ReadAll(connection)
		connection.Close()
	}
	if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("rejected pipeline delivered %v, error=%v", msg, err)
	}
}

func TestPlacementResetRequiresNewProcess(t *testing.T) {
	s := runPlacementServer(t)
	status := placementRequest(t, s, "", placementTestToken, nil, 200)
	placementRequest(t, s, "/reset", placementTestToken, PlacementBinding{BootID: status.BootID}, 202)
	s.WaitForShutdown()
	replacement := runPlacementServer(t)
	next := placementRequest(t, replacement, "", placementTestToken, nil, 200)
	if next.BootID == status.BootID || next.Active || next.Binding != nil {
		t.Fatalf("reset process resurrected binding: %+v", next)
	}
	old := PlacementBinding{BootID: status.BootID, ShardID: "shard", Incarnation: "placement", OperationID: "operation"}
	placementRequest(t, replacement, "/bind", placementTestToken, old, 409)
}

func TestPlacementConfigValidation(t *testing.T) {
	file := createConfFile(t, []byte(`port: -1
 authorization { users: [{user: control, password: secret}] }
 placement { control_user: control, control_listen: "127.0.0.1:0", control_token: "placement-test-control-token" }
 `))
	opts, err := ProcessConfigFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Placement == nil || opts.Placement.ControlUser != "control" {
		t.Fatalf("missing parsed placement: %+v", opts.Placement)
	}
	server, err := NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	server.Shutdown()
	for _, test := range []struct {
		name   string
		config PlacementConfig
		want   string
	}{
		{"missing control principal", PlacementConfig{ControlUser: "missing", ControlListen: "127.0.0.1:0", ControlToken: placementTestToken}, "configured password user"},
		{"short token", PlacementConfig{ControlUser: "control", ControlListen: "127.0.0.1:0", ControlToken: "short"}, "at least 16"},
		{"invalid listen", PlacementConfig{ControlUser: "control", ControlListen: "bad", ControlToken: placementTestToken}, "control_listen"},
	} {
		t.Run(test.name, func(t *testing.T) {
			copy := opts.Clone()
			copy.Placement = &test.config
			_, err := NewServer(copy)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %s", err, test.want)
			}
		})
	}
}

func TestPlacementHTTPBoundaries(t *testing.T) {
	s := runPlacementServer(t)
	status := placementRequest(t, s, "", placementTestToken, nil, 200)
	binding := PlacementBinding{BootID: status.BootID, ShardID: strings.Repeat("s", 128), Incarnation: strings.Repeat("i", 128), OperationID: strings.Repeat("o", 128)}
	for _, test := range []struct {
		name   string
		modify func(*PlacementBinding)
	}{
		{"empty operation", func(b *PlacementBinding) { b.OperationID = "" }},
		{"oversized shard", func(b *PlacementBinding) { b.ShardID += "x" }},
		{"oversized incarnation", func(b *PlacementBinding) { b.Incarnation += "x" }},
		{"invalid identifier", func(b *PlacementBinding) { b.ShardID = "shard/other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := binding
			test.modify(&invalid)
			placementRequest(t, s, "/bind", placementTestToken, invalid, 400)
		})
	}
	placementRequest(t, s, "/bind", placementTestToken, map[string]any{"bootId": status.BootID, "shardId": "s", "incarnation": "i", "operationId": "o", "bypass": true}, 400)
	placementRequest(t, s, "/bind", placementTestToken, binding, 200)
	placementRequest(t, s, "/activate", placementTestToken, binding, 200)
	connection, err := nats.Connect(s.ClientURL(), nats.UserInfo("app", "app-secret"), nats.Name("alini/1/"+binding.ShardID+"/"+binding.Incarnation), nats.NoReconnect())
	if err != nil {
		t.Fatalf("maximum identifiers rejected: %v", err)
	}
	connection.Close()
	placementRequest(t, s, "/reset", placementTestToken, PlacementBinding{BootID: status.BootID}, 409)
	placementRequest(t, s, "/fence", placementTestToken, binding, 200)
	placementRequest(t, s, "/reset", placementTestToken, binding, 202)
	s.WaitForShutdown()
}

func TestPlacementResetRejectsRetainedStreams(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.Users = []*User{{Username: "control", Password: "control-secret"}, {Username: "app", Password: "app-secret"}}
	opts.Placement = &PlacementConfig{ControlUser: "control", ControlListen: "127.0.0.1:0", ControlToken: placementTestToken}
	s := RunServer(&opts)
	defer s.Shutdown()
	control, err := nats.Connect(s.ClientURL(), nats.UserInfo("control", "control-secret"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	js, err := control.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "RETAINED", Subjects: []string{"retained"}, Storage: nats.FileStorage}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish("retained", []byte("must survive rejected reset")); err != nil {
		t.Fatal(err)
	}
	status := placementRequest(t, s, "", placementTestToken, nil, 200)
	placementRequest(t, s, "/reset", placementTestToken, PlacementBinding{BootID: status.BootID}, 409)
	msg, err := js.GetMsg("RETAINED", 1)
	if err != nil || string(msg.Data) != "must survive rejected reset" {
		t.Fatalf("retained message=%+v error=%v", msg, err)
	}
	after := placementRequest(t, s, "", placementTestToken, nil, 200)
	if after.BootID != status.BootID {
		t.Fatalf("rejected reset changed boot %s to %s", status.BootID, after.BootID)
	}
	if err := js.DeleteStream("RETAINED"); err != nil {
		t.Fatal(err)
	}
	placementRequest(t, s, "/reset", placementTestToken, PlacementBinding{BootID: status.BootID}, 202)
	s.WaitForShutdown()
}

func TestPlacementConfigurationReload(t *testing.T) {
	base := `port: -1
 authorization { users: [{user: control, password: control-secret}, {user: alternate, password: alternate-secret}] }
 `
	placement := fmt.Sprintf(`placement { control_user: control, control_listen: "127.0.0.1:0", control_token: %q }`, placementTestToken)
	for _, tc := range []struct{ name, initial, replacement string }{
		{"unchanged disabled", "", ""},
		{"unchanged enabled", placement, placement},
		{"enable", "", placement},
		{"disable", placement, ""},
		{"control user", placement, strings.Replace(placement, "control_user: control", "control_user: alternate", 1)},
		{"control address", placement, strings.Replace(placement, "127.0.0.1:0", "127.0.0.1:4229", 1)},
		{"control token", placement, strings.Replace(placement, placementTestToken, "replacement-control-token", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := createConfFile(t, []byte(base+tc.initial))
			opts, err := ProcessConfigFile(path)
			if err != nil {
				t.Fatal(err)
			}
			s := RunServer(opts)
			defer s.Shutdown()
			before := s.PlacementStatus()
			if err := os.WriteFile(path, []byte(base+tc.replacement), 0600); err != nil {
				t.Fatal(err)
			}
			err = s.Reload()
			if tc.initial == tc.replacement {
				if err != nil {
					t.Fatalf("unchanged configuration reload: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "config reload not supported for placement") {
				t.Fatalf("changed placement reload error=%v", err)
			}
			after := s.PlacementStatus()
			if after.BootID != before.BootID || after.Active != before.Active || after.Fenced != before.Fenced {
				t.Fatalf("reload changed placement status: before=%+v after=%+v", before, after)
			}
			if tc.initial != "" {
				placementRequest(t, s, "", placementTestToken, nil, 200)
				placementRequest(t, s, "", "replacement-control-token", nil, 401)
			}
		})
	}
}
