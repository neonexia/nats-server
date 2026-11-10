# Process-scoped placement admission

This fork provides an opt-in placement gate for externally managed, pre-deployed
server slots. It is independent of routing and does not implement placement policy.
Without the `placement` block, upstream connection behavior is unchanged.
Placement-enabled slots reject routes, gateways, leaf nodes and MQTT configuration:
those ingress paths do not use the application CONNECT placement check.

```conf
authorization {
  users: [
    {user: node-agent, password: $NODE_AGENT_PASSWORD},
    {user: application, password: $APPLICATION_PASSWORD}
  ]
}
placement {
  control_user: node-agent
  control_listen: "127.0.0.1:8223"
  control_token: $PLACEMENT_CONTROL_TOKEN
}
```

The private HTTP listener uses a separate bearer token of at least 16 bytes.
Keep it on loopback or a protected management network; it does not provide TLS.
Do not expose it through a public messaging gateway. Configuration changes to the
placement block require a process restart. Tokens are never returned by status.

Ordinary NATS authentication runs first. The configured password-authenticated
control user bypasses only placement admission and can prepare/checkpoint/restore
streams in its normal account. Normal NATS permissions still apply. A matching
username or client name alone does not grant an exemption.

Applications supply `name: "alini/1/<shard-id>/<incarnation>"` in CONNECT. Both IDs
must be 1–128 ASCII letters/digits, hyphens or underscores. A malformed, missing,
inactive, fenced or mismatching placement produces `-ERR 'Placement unavailable'`
and closes the connection before application commands are parsed. The name is not
a credential. TLS-first shared-port routing is configured separately.

## Private management API

Every operation requires `Authorization: Bearer <PLACEMENT_CONTROL_TOKEN>`.

`GET /v1/placement` returns:

```json
{
  "bootId": "fresh-server-identity",
  "binding": {
    "bootId": "fresh-server-identity",
    "shardId": "shard-a",
    "incarnation": "placement-a",
    "operationId": "move-a"
  },
  "active": false,
  "fenced": false
}
```

The binding is omitted on a fresh process. It is never persisted locally. Use the
returned boot ID, not the configured server name. Restart produces a different ID.

All POST bodies identify the exact four-field binding shown above:

| Endpoint | Behavior |
| --- | --- |
| `/v1/placement/bind` | Install inactive binding once. Identical retry succeeds; conflicting binding fails. |
| `/v1/placement/activate` | Activate the exact binding. Identical retry succeeds unless fenced. |
| `/v1/placement/fence` | Irrevocably disable application admission and close application connections; keep control connections. Identical retry succeeds. |
| `/v1/placement/reset` | Fence, respond with HTTP 202, then shut down the process. Deployment supervisor must restart it. On an unbound process only, `{ "bootId": "..." }` is sufficient. |

Success returns the current status. Invalid JSON/identifiers return 400, bearer
authentication failures 401, stale boot/conflicting binding/invalid lifecycle 409,
unknown endpoints 404 and unsupported operation methods 405. Bodies are limited
to 4 KiB and unknown fields are rejected.

The caller must durably authorize activation before calling it. This server does
not contact a coordinator. Never free or reassign the slot merely because a fence
or reset request was sent: observe shutdown and a new process boot identity first.
Reset refuses a populated JetStream store with HTTP 409; first complete durable
retirement/archive cleanup. Generic stream deletion is not supported for tiered
streams. An aborted target that cannot be safely archived remains unavailable.
The lifecycle owner serializes management operations; reset is not a data-wipe API.

A lost reset response is recovered by observing that new identity. Old commands
cannot bind or activate the replacement process.

Fencing is not a stream checkpoint and does not replace the existing remote restore
write-fence protocol. Complete the required durable checkpoint/retirement ordering
before resetting. This API neither moves data nor guarantees partition-safe failover.
