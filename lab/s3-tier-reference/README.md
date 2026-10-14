# S3-tier reference handoff

This is a process-isolated reference application for the experimental S3-tier
remote recovery protocol. It starts a local RustFS S3-compatible server and
independent source and target `nats-server` processes. The client uses only normal NATS publishing,
durable consumers, and the public remote-restore management subjects.

The run exercises:

- concurrent raw-message publishers;
- ordinary source-node restart, with publishers and durable consumers reconnecting to the same server and local store;
- two durable pull consumers with different acknowledged positions;
- a RustFS upload outage that reaches bounded local pressure, followed by remote drain;
- `prepare`, a source-process restart, and `checkpoint`;
- a RustFS outage during target restore followed by the same-operation retry;
- target restart after activation and source restart after checkpoint, before source retirement;
- source publishes rejected by the durable handoff fence, then live publisher reconnection to the target;
- restored consumer delivery and acknowledgement state, including post-handoff messages;
- source retirement and retired-state polling after local source removal.

Build the forked server and run the harness:

```sh
go build -o ./nats-server-reference .
go run ./lab/s3-tier-reference -nats-server ./nats-server-reference
```

The executable accepts `-publishers` and `-messages` to increase the concurrent
load. It creates an isolated temporary directory and prints the retained process
logs path. A successful run ends with `PASS`.

## Real object-store test gate

Run the compatible-endpoint tests against RustFS before relying on the reference
simulation. This pulls the Neonexia-owned image, whose Dockerfile pins RustFS by digest, waits for it to be ready,
runs the server tests through real S3 request signing and object operations, and
removes the temporary container.

```sh
docker rm -f nats-s3-tier-rustfs 2>/dev/null || true
docker run -d --rm --name nats-s3-tier-rustfs \
  -p 127.0.0.1:19000:9000 \
  -e RUSTFS_ACCESS_KEY=testaccess \
  -e RUSTFS_SECRET_KEY=testsecret123 \
  -e RUSTFS_ADDRESS=:9000 \
  -e RUSTFS_CONSOLE_ENABLE=false \
  ghcr.io/neonexia/nats-server-s3-tier-rustfs:latest

until curl --fail --silent http://127.0.0.1:19000/health >/dev/null; do sleep 1; done

TEST_S3_TIER_ENDPOINT=127.0.0.1:19000 go test ./server \
  -run 'Test(FileStoreS3TierCompatibleEndpoint|FileStoreS3TierGarbageCompatibleEndpoint|JetStreamS3TierRemoteRestoreCompatibleEndpoint)$' \
  -count=1 -timeout=10m -vet=off

docker rm -f nats-s3-tier-rustfs
```

By default the harness starts a pinned official RustFS Docker image. This is a local
S3-compatible functional qualification, not an authentication test. Use
`-object-store-mode process -object-store /path/to/rustfs` to start a standalone server
process instead.
