// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package diskfull

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type pubAckResponse struct {
	Stream string `json:"stream"`
	Seq    uint64 `json:"seq"`
	Error  *struct {
		Description string `json:"description"`
		ErrCode     uint16 `json:"err_code"`
	} `json:"error,omitempty"`
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func runDocker(t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func waitForNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url, nats.Timeout(250*time.Millisecond))
		if err == nil {
			return nc
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", url)
	return nil
}

func TestS3TierDiskFullReturnsPublishError(t *testing.T) {
	if os.Getenv("TEST_S3_TIER_DISK_FULL") == "" {
		t.Skip("set TEST_S3_TIER_DISK_FULL=1 to run the Docker tmpfs integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	port := reservePort(t)
	name := fmt.Sprintf("nats-s3-tier-disk-full-%d", time.Now().UnixNano())
	image := fmt.Sprintf("nats-s3-tier-disk-full:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", "-f", image).Run() })
	configPath := filepath.Join(tmp, "server.conf")
	config := `port: 4222
jetstream {
  store_dir: "/data"
  max_file_store: 128M
  s3_tier {
    endpoint: "127.0.0.1:1"
    bucket: "unavailable"
    tls: false
    prefix: "disk-full"
    block_size: 64K
    local_high_bytes: 96M
    local_low_bytes: 80M
    remote_high_bytes: 64K
    remote_low_bytes: 32K
    timeout: "100ms"
    retry_min: "50ms"
    retry_max: "100ms"
    credentials {
      provider: "env"
      access_key_env: "NATS_S3_TIER_ACCESS"
      secret_key_env: "NATS_S3_TIER_SECRET"
    }
  }
}
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	runDocker(t, "build", "-f", filepath.Join(repoRoot, "lab/s3-tier-disk-full/Dockerfile"), "-t", image, repoRoot)
	runDocker(t, "run", "-d", "--rm", "--name", name, "--tmpfs", "/data:rw,size=12m", "-p", fmt.Sprintf("127.0.0.1:%d:4222", port), "-v", configPath+":/server.conf:ro", "-e", "NATS_S3_TIER_ACCESS=test", "-e", "NATS_S3_TIER_SECRET=test", image, "-c", "/server.conf")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	nc := waitForNATS(t, fmt.Sprintf("nats://127.0.0.1:%d", port))
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:       "DISK",
		Subjects:   []string{"disk.events"},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardNew,
		DenyDelete: true,
		DenyPurge:  true,
	}); err != nil {
		logs, logErr := exec.Command("docker", "logs", name).CombinedOutput()
		if logErr != nil {
			t.Fatalf("create stream: %v (also could not read server logs: %v)", err, logErr)
		}
		t.Fatalf("create stream: %v\n%s", err, logs)
	}

	payload := []byte(strings.Repeat("x", 64*1024))
	var accepted int
	for ; accepted < 1000; accepted++ {
		response, err := nc.Request("disk.events", payload, 3*time.Second)
		if err != nil {
			t.Fatalf("publish %d timed out instead of returning a store error: %v", accepted+1, err)
		}
		var ack pubAckResponse
		if err := json.Unmarshal(response.Data, &ack); err != nil {
			t.Fatalf("publish %d response: %v", accepted+1, err)
		}
		if ack.Error != nil {
			if accepted == 0 {
				t.Fatal("disk-full condition occurred before any publish was acknowledged")
			}
			if ack.Error.ErrCode != 10077 {
				t.Fatalf("publish %d error: %+v", accepted+1, ack.Error)
			}
			break
		}
		if ack.Stream != "DISK" || ack.Seq != uint64(accepted+1) {
			t.Fatalf("publish %d acknowledgment: %+v", accepted+1, ack)
		}
	}
	if accepted == 1000 {
		t.Fatal("tmpfs did not reach disk full")
	}
	if _, err := js.GetMsg("DISK", 1); err != nil {
		t.Fatalf("accepted message was not readable after ENOSPC: %v", err)
	}
	logs := runDocker(t, "logs", name)
	if !strings.Contains(logs, "no space left on device") {
		t.Fatalf("server did not report ENOSPC:\n%s", logs)
	}
}
