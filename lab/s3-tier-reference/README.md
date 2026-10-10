# S3-tier reference handoff

This is a process-isolated reference application for the experimental S3-tier
remote recovery protocol. It starts a local RustFS S3-compatible server and
independent source and target `nats-server` processes. The client uses only normal NATS publishing,
durable consumers, and the public remote-restore management subjects.

The run exercises:

- concurrent raw-message publishers;
- two durable pull consumers with different acknowledged positions;
- `prepare`, a source-process restart, and `checkpoint`;
- a RustFS outage during target restore followed by the same-operation retry;
- restored consumer delivery and acknowledgement state;
- source retirement and retired-state polling after local source removal.

Build the forked server and run the harness:

```sh
go build -o ./nats-server-reference .
go run ./lab/s3-tier-reference -nats-server ./nats-server-reference
```

The executable accepts `-publishers` and `-messages` to increase the concurrent
load. It creates an isolated temporary directory and prints the retained process
logs path. A successful run ends with `PASS`.

By default the harness starts a pinned official RustFS Docker image. This is a local
S3-compatible functional qualification, not an authentication test. Use
`-object-store-mode process -object-store /path/to/rustfs` to start a standalone server
process instead.
