// s3-tier-reference is a local, process-isolated reference application for the
// experimental S3-tier recovery protocol. It intentionally uses only normal
// NATS clients and the public management subjects that an external coordinator
// such as Nodus would use.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/nats-io/nats.go"
)

const (
	streamName  = "EVENTS"
	subject     = "events.raw"
	operation   = "reference-handoff-1"
	targetID    = "reference-target"
	accessKey   = "reference-access"
	secretKey   = "reference-secret"
	rustFSImage = "rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c"
)

type restoreResponse struct {
	Success     bool   `json:"success"`
	OperationID string `json:"operation_id"`
	Epoch       uint64 `json:"epoch"`
	State       string `json:"state"`
	TargetID    string `json:"target_id"`
	NextAction  string `json:"next_action"`
	Consumers   int    `json:"consumers"`
	Error       *struct {
		Description string `json:"description"`
	} `json:"error,omitempty"`
}

type sourceRequest struct {
	OperationID string `json:"operation_id"`
}

type targetRequest struct {
	OperationID   string `json:"operation_id"`
	ExpectedEpoch uint64 `json:"expected_epoch"`
	TargetID      string `json:"target_id"`
}

type process struct {
	cmd    *exec.Cmd
	log    *os.File
	stopFn func()
}

func reservePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func start(name, path string, args []string, env []string, logDir string) (*process, error) {
	log, err := os.Create(filepath.Join(logDir, name+".log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	return &process{cmd: cmd, log: log}, nil
}

func (p *process) stop() {
	if p == nil {
		return
	}
	if p.stopFn != nil {
		p.stopFn()
		p.stopFn = nil
		return
	}
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
	_ = p.log.Close()
}

func startDockerRustFS(name, hostPort, dataDir string) (*process, error) {
	args := []string{
		"run", "-d", "--name", name,
		"-p", fmt.Sprintf("127.0.0.1:%s:9000", hostPort),
		"-v", dataDir + ":/data",
		"-e", "RUSTFS_ACCESS_KEY=" + accessKey,
		"-e", "RUSTFS_SECRET_KEY=" + secretKey,
		"-e", "RUSTFS_ADDRESS=:9000",
		"-e", "RUSTFS_CONSOLE_ENABLE=false",
		rustFSImage, "/data",
	}
	if output, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("start RustFS container: %w: %s", err, output)
	}
	return &process{stopFn: func() { _ = exec.Command("docker", "rm", "-f", name).Run() }}, nil
}

func waitFor(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := http.Get(url) // #nosec G107 -- local child process endpoint.
		if err == nil {
			response.Body.Close()
			if response.StatusCode < 500 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", url)
}

func waitForNATS(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url, nats.Timeout(250*time.Millisecond))
		if err == nil {
			nc.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", url)
}

func request(nc *nats.Conn, subject string, value any) (restoreResponse, error) {
	var response restoreResponse
	var data []byte
	var err error
	if value != nil {
		data, err = json.Marshal(value)
		if err != nil {
			return response, err
		}
	}
	msg, err := nc.Request(subject, data, 10*time.Second)
	if err != nil {
		return response, err
	}
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		return response, err
	}
	return response, nil
}

func mustActive(response restoreResponse, want string) error {
	if response.Error != nil {
		return errors.New(response.Error.Description)
	}
	if !response.Success || response.State != want {
		return fmt.Errorf("expected successful %s state, got %+v", want, response)
	}
	return nil
}

func consumeAndAck(js nats.JetStreamContext, durable string, expected int) error {
	sub, err := js.PullSubscribe("", durable, nats.BindStream(streamName))
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, expected)
	for len(seen) < expected {
		msgs, err := sub.Fetch(min(25, expected-len(seen)), nats.MaxWait(10*time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			return err
		}
		for _, msg := range msgs {
			if _, ok := seen[string(msg.Data)]; ok {
				return fmt.Errorf("consumer %s received duplicate payload %q", durable, msg.Data)
			}
			seen[string(msg.Data)] = struct{}{}
			if err := msg.AckSync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	var natsServer, objectStoreServer, objectStoreMode string
	var publishers, messagesPerPublisher int
	flag.StringVar(&natsServer, "nats-server", "./nats-server", "path to the forked nats-server binary")
	flag.StringVar(&objectStoreServer, "object-store", "rustfs", "path to the RustFS binary when -object-store-mode=process")
	flag.StringVar(&objectStoreMode, "object-store-mode", "docker", "object-store runtime: docker or process")
	flag.IntVar(&publishers, "publishers", 3, "concurrent publisher count")
	flag.IntVar(&messagesPerPublisher, "messages", 100, "messages per publisher")
	flag.Parse()

	// Docker Desktop can mount /tmp reliably; the per-user temporary directory
	// may be private to the host process and unreadable by the VM.
	root, err := os.MkdirTemp("/tmp", "s3-tier-reference-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)
	fmt.Printf("reference run directory: %s\n", root)

	objectStorePort, _ := reservePort()
	sourcePort, _ := reservePort()
	targetPort, _ := reservePort()
	objectStoreData := filepath.Join(root, "object-store")
	logs := filepath.Join(root, "logs")
	// Docker runs RustFS under its container user. The parent temporary directory
	// is mode 0700 on macOS, so make only the bind-mounted data directory writable.
	if err := os.MkdirAll(objectStoreData, 0o777); err != nil {
		panic(err)
	}
	if err := os.Chmod(objectStoreData, 0o777); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(logs, 0o755); err != nil {
		panic(err)
	}
	objectStoreEnv := []string{
		"RUSTFS_ACCESS_KEY=" + accessKey,
		"RUSTFS_SECRET_KEY=" + secretKey,
		fmt.Sprintf("RUSTFS_ADDRESS=127.0.0.1:%d", objectStorePort),
		"RUSTFS_CONSOLE_ENABLE=false",
	}
	startObjectStore := func(name string) (*process, error) {
		switch objectStoreMode {
		case "docker":
			return startDockerRustFS("s3-tier-reference-"+fmt.Sprint(objectStorePort), fmt.Sprint(objectStorePort), objectStoreData)
		case "process":
			return start(name, objectStoreServer, []string{objectStoreData}, objectStoreEnv, logs)
		default:
			return nil, fmt.Errorf("unknown object-store mode %q", objectStoreMode)
		}
	}
	objectStoreProcess, err := startObjectStore("rustfs")
	if err != nil {
		panic(err)
	}
	defer func() { objectStoreProcess.stop() }()
	objectStoreHealth := fmt.Sprintf("http://127.0.0.1:%d/health", objectStorePort)
	if err := waitFor(objectStoreHealth, 45*time.Second); err != nil {
		logs, _ := exec.Command("docker", "logs", "s3-tier-reference-"+fmt.Sprint(objectStorePort)).CombinedOutput()
		panic(fmt.Errorf("%w; RustFS logs: %s", err, logs))
	}
	endpoint := fmt.Sprintf("127.0.0.1:%d", objectStorePort)
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := client.MakeBucket(ctx, "reference", minio.MakeBucketOptions{}); err != nil && minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
		panic(err)
	}

	config := func(name string, port int) string {
		store := filepath.Join(root, name, "store")
		return fmt.Sprintf(`port: %d
jetstream {
  store_dir: %q
  s3_tier {
    endpoint: %q
    bucket: "reference"
    tls: false
    prefix: "reference-run"
    block_size: 16K
    local_high_bytes: 64K
    local_low_bytes: 32K
    remote_high_bytes: 32K
    remote_low_bytes: 16K
    timeout: "2s"
    retry_min: "50ms"
    retry_max: "250ms"
    credentials {
      provider: "env"
      access_key_env: "NATS_S3_TIER_ACCESS"
      secret_key_env: "NATS_S3_TIER_SECRET"
    }
  }
}
`, port, store, endpoint)
	}
	writeConfig := func(name string, port int) string {
		path := filepath.Join(root, name+".conf")
		if err := os.WriteFile(path, []byte(config(name, port)), 0o600); err != nil {
			panic(err)
		}
		return path
	}
	env := []string{"NATS_S3_TIER_ACCESS=" + accessKey, "NATS_S3_TIER_SECRET=" + secretKey}
	sourceConfig := writeConfig("source", sourcePort)
	source, err := start("source", natsServer, []string{"-c", sourceConfig}, env, logs)
	if err != nil {
		panic(err)
	}
	defer func() { source.stop() }()
	sourceURL := fmt.Sprintf("nats://127.0.0.1:%d", sourcePort)
	if err := waitForNATS(sourceURL, 15*time.Second); err != nil {
		output, _ := os.ReadFile(filepath.Join(logs, "source.log"))
		panic(fmt.Errorf("%w; source log: %s", err, output))
	}
	nc, err := nats.Connect(sourceURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: streamName, Subjects: []string{subject}, Storage: nats.FileStorage,
		Retention: nats.LimitsPolicy, Discard: nats.DiscardNew,
		DenyDelete: true, DenyPurge: true,
	}); err != nil {
		output, _ := os.ReadFile(filepath.Join(logs, "source.log"))
		panic(fmt.Errorf("create source stream: %w; source log: %s", err, output))
	}
	for _, durable := range []string{"audit-a", "audit-b"} {
		if _, err := js.AddConsumer(streamName, &nats.ConsumerConfig{Durable: durable, AckPolicy: nats.AckExplicitPolicy, DeliverPolicy: nats.DeliverAllPolicy}); err != nil {
			panic(err)
		}
	}
	// A remote outage must apply bounded backpressure once local residency is
	// exhausted, then resume draining after RustFS returns. Keep this separate
	// from the handoff stream so its intentionally rejected publishes do not
	// affect the recovery sequence assertions below.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: "PRESSURE", Subjects: []string{"pressure.raw"}, Storage: nats.FileStorage,
		Retention: nats.LimitsPolicy, Discard: nats.DiscardNew,
		DenyDelete: true, DenyPurge: true,
	}); err != nil {
		panic(err)
	}
	objectStoreProcess.stop()
	accepted := 0
	for i := 0; i < 512; i++ {
		if _, err := js.Publish("pressure.raw", []byte(strings.Repeat("p", 4096))); err != nil {
			break
		}
		accepted++
	}
	if accepted == 0 || accepted == 512 {
		panic(fmt.Errorf("upload outage did not apply bounded local pressure: accepted=%d", accepted))
	}
	objectStoreProcess, err = startObjectStore("rustfs-upload-recovered")
	if err != nil {
		panic(err)
	}
	if err := waitFor(objectStoreHealth, 45*time.Second); err != nil {
		panic(err)
	}
	drain, err := request(nc, "$JS.API.STREAM.DRAIN_REMOTE.PRESSURE", nil)
	if err != nil || !drain.Success || drain.Error != nil {
		panic(fmt.Errorf("drain after upload outage: %v %+v", err, drain))
	}
	pressureInfo, err := js.StreamInfo("PRESSURE")
	if err != nil || int(pressureInfo.State.Msgs) != accepted {
		panic(fmt.Errorf("pressure stream state: %v %+v", err, pressureInfo))
	}

	total := publishers * messagesPerPublisher
	published := make([]string, 0, total)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for publisher := 0; publisher < publishers; publisher++ {
		publisher := publisher
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; seq < messagesPerPublisher; seq++ {
				payload := fmt.Sprintf("publisher=%d sequence=%d", publisher, seq)
				if _, err := js.Publish(subject, []byte(payload)); err != nil {
					panic(err)
				}
				mu.Lock()
				published = append(published, payload)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Strings(published)
	if err := consumeAndAck(js, "audit-a", total/3); err != nil {
		panic(err)
	}
	if err := consumeAndAck(js, "audit-b", total/4); err != nil {
		panic(err)
	}

	prepare, err := request(nc, "$JS.API.STREAM.PREPARE_REMOTE_RESTORE."+streamName, sourceRequest{operation})
	if err != nil || mustActive(prepare, "prepared") != nil || prepare.NextAction != "checkpoint_remote_restore" {
		panic(fmt.Errorf("prepare: %v %+v", err, prepare))
	}
	// Simulate source-process loss after the durable fence. The restarted source
	// must continue at checkpointing, with writers still fenced.
	source.stop()
	source, err = start("source-restarted", natsServer, []string{"-c", sourceConfig}, env, logs)
	if err != nil {
		panic(err)
	}
	if err := waitForNATS(sourceURL, 15*time.Second); err != nil {
		panic(err)
	}
	nc.Close()
	nc, err = nats.Connect(sourceURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	// A source crash must retain its logical block map even when old local blocks
	// were reclaimed. This read exercises precise cold hydration before the
	// checkpoint moves ownership to the target.
	sourceJS, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	if _, err := sourceJS.GetMsg(streamName, 1); err != nil {
		panic(fmt.Errorf("source cold read after restart: %w", err))
	}
	checkpoint, err := request(nc, "$JS.API.STREAM.CHECKPOINT_REMOTE_RESTORE."+streamName, sourceRequest{operation})
	if err != nil || mustActive(checkpoint, "checkpointed") != nil || checkpoint.NextAction != "restore_remote" {
		panic(fmt.Errorf("checkpoint: %v %+v", err, checkpoint))
	}

	targetConfig := writeConfig("target", targetPort)
	target, err := start("target", natsServer, []string{"-c", targetConfig}, env, logs)
	if err != nil {
		panic(err)
	}
	defer target.stop()
	if err := waitForNATS(fmt.Sprintf("nats://127.0.0.1:%d", targetPort), 15*time.Second); err != nil {
		panic(err)
	}
	targetNC, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", targetPort))
	if err != nil {
		panic(err)
	}
	defer targetNC.Close()

	// An object-store outage produces a retryable restore error and leaves no
	// active target. Restart RustFS, then use the same operation and target IDs.
	objectStoreProcess.stop()
	failed, err := request(targetNC, "$JS.API.STREAM.RESTORE_REMOTE."+streamName, targetRequest{operation, checkpoint.Epoch, targetID})
	if err != nil || failed.Error == nil {
		panic(fmt.Errorf("expected target restore failure during object-store outage: %v %+v", err, failed))
	}
	objectStoreProcess, err = startObjectStore("rustfs-restarted")
	if err != nil {
		panic(err)
	}
	if err := waitFor(objectStoreHealth, 45*time.Second); err != nil {
		panic(err)
	}
	var restored restoreResponse
	deadline := time.Now().Add(15 * time.Second)
	for {
		restored, err = request(targetNC, "$JS.API.STREAM.RESTORE_REMOTE."+streamName, targetRequest{operation, checkpoint.Epoch, targetID})
		if err == nil && restored.Error == nil && restored.State == "active" && restored.NextAction == "retire_remote_source" {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || mustActive(restored, "active") != nil || restored.NextAction != "retire_remote_source" {
		output, _ := os.ReadFile(filepath.Join(logs, "target.log"))
		description := ""
		if restored.Error != nil {
			description = restored.Error.Description
		}
		panic(fmt.Errorf("restore: %v %+v (%s); target log: %s", err, restored, description, output))
	}
	targetJS, err := targetNC.JetStream()
	if err != nil {
		panic(err)
	}
	info, err := targetJS.StreamInfo(streamName)
	if err != nil || int(info.State.Msgs) != total {
		panic(fmt.Errorf("target stream state: %v %+v", err, info))
	}
	if err := consumeAndAck(targetJS, "audit-a", total-total/3); err != nil {
		panic(err)
	}
	if err := consumeAndAck(targetJS, "audit-b", total-total/4); err != nil {
		panic(err)
	}

	retired, err := request(nc, "$JS.API.STREAM.RETIRE_REMOTE_SOURCE."+streamName, sourceRequest{operation})
	if err != nil || mustActive(retired, "retired") != nil || retired.NextAction != "none" {
		panic(fmt.Errorf("retire: %v %+v", err, retired))
	}
	status, err := request(nc, "$JS.API.STREAM.REMOTE_RESTORE_STATUS."+streamName, sourceRequest{operation})
	if err != nil || mustActive(status, "retired") != nil || status.NextAction != "none" {
		panic(fmt.Errorf("retired status: %v %+v", err, status))
	}
	fmt.Printf("PASS: %d messages, %d publishers, bounded upload pressure, two restored durable consumers; source restart and object-store outages recovered. Logs: %s\n", total, publishers, logs)
}
