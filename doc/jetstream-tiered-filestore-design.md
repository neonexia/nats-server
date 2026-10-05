# Experimental JetStream file/S3 tier

Status: experimental fork implementation, 2026-10-04. The first runnable slice is on `codex/jetstream-s3-tier-spike`; it is not enabled for Nodus or ready for production. This document concerns native JetStream stream and consumer reads. Nodus history services, audit archives, and analytical range APIs are outside this experiment.

## What the spike actually implements

- `FileStoreConfig.S3Tier` enables an append-only block tier. `Options.JetStreamS3Tiers` maps `account/stream` to a tier config when the normal server path creates or recovers a stream. The option is programmatic only; the NATS config parser has no S3 stanza. Credentials are supplied by the caller rather than persisted in stream metadata.
- The S3 adapter uses `minio-go/v7` against an S3-compatible endpoint. Object keys contain a caller-supplied unique stream prefix, block index, and SHA-256 digest. `RemoteHighBytes` and `RemoteLowBytes` control when sealed blocks gain S3 coverage. `LocalHighBytes` and `LocalLowBytes` separately control when covered local payloads are reclaimed. The active block stays local.
- Each eviction uploads block bytes, reads them back and compares bytes, uploads a JSON descriptor, reads it back and compares bytes, writes and verifies an immutable manifest generation, syncs local descriptor and manifest sidecars, then unlinks the local `.blk` under file-store and block locks. The S3 prefix is opaque to NATS; Nodus supplies its stream identity and a separate history service can later interpret its own namespace.
- Restart uses local descriptor sidecars to retain remote-only block metadata without listing or reading S3. A cold reader selects one missing block, releases the normal file-store and block locks, coalesces a fetch for that block, verifies it, and retries the native read. Missing tier configuration still fails startup. Corrupt or unavailable remote data fails the read rather than being treated as missing history.
- Native durable pull consumers and direct get use the existing JetStream storage methods. Cold reads hydrate a missing block. Direct get returns `503 Storage Unavailable` on remote fetch failure instead of `404`. The normal `nats.go` request helper maps a 503 status to its `ErrNoResponders` error; raw headers distinguish the storage failure.
- The experiment rejects config changes that would enable retention expiry, automatic old-message discard, per-subject limits, rollups, multiple replicas, compression, sources/mirrors, and asynchronous persist. Explicit delete, erase, purge, compact, truncate, stream deletion, and snapshots are rejected for tiered stores. These restrictions protect immutable remote block assumptions; they are not a complete durability or lifecycle design.

## Observed results

- Focused Go tests cover block eviction, readback, remote-only file-store restart while the object store rejects reads, full server restart, 100-message native durable pull replay, exact single-block cold reads, coalesced concurrent cold reads, cache re-eviction, manifest commit failure, and pressure-driven write rejection. PUT, descriptor, and manifest failures leave the local block present. A blocked cold GET does not hold a local `StoreMsg` in the retry path. An injected GET failure leaves the consumer sequence at 1, and direct get returns the storage-specific 503 header.
- The S3 adapter passed the same eviction, cold-read, and restart test against a local Moto S3 endpoint. The installed MinIO binary reported that S3 operations were disabled without a license; an older container image was unavailable in this environment. A MinIO or AWS endpoint and network-partition tests remain unverified.
- `go test -race` passed the focused tier tests. These are correctness spikes, not latency or throughput benchmarks.

## Revised assessment and remaining gaps

The block, descriptor, and manifest protocol is feasible for a narrow append-only stream. Native consumer sequence continuity survives eviction and remote-only restart in the tested setup. Restart does not contact S3; it keeps the existing full-state sequence and subject metadata and marks descriptor-backed missing payloads as readable remotely.

Remote coverage and local residency use separate watermark pairs. A node can therefore copy sealed blocks to S3 continuously while retaining a much larger SSD-backed local window for live channels. Local reclamation prefers hydrated or otherwise covered blocks by least-recent block access; a read reaches S3 only after that local payload was reclaimed. At the next publish after an unsuccessful asynchronous eviction, the server attempts synchronously to reach the local budget and returns a transient local-capacity error if it cannot. `/jsz` stream detail exposes local and remote bytes, remote block count, transfer and cache counters, capacity errors, and both watermark pairs.

The remaining gaps are runtime configuration and credential sources, upload retry scheduling and backlog limits, object garbage collection, remote stream deletion, replica support, encryption recovery, durable consumer and stream metadata backup, full fault injection at every crash point, and benchmarks. Read-path coverage is focused on direct gets and pull consumers; broad upstream coverage is still required. Do not enable this for production or for Nodus's current 64 MiB logical `MaxBytes` streams without those gates and a separate logical-retention decision.

History serving remains outside this fork. A future service may consume the immutable per-stream descriptor and manifest objects, apply Nodus authorization and workspace semantics, and offer range reads without giving clients unrestricted bucket access.

## Target design and validation gates

The sections below preserve the design target. Where they differ from the runnable spike, the observed-results and remaining-gaps sections above describe current behavior.

### Goal and decision

Keep a file-backed JetStream stream as one logical sequence. Write new messages to the node's local file store. When a separately configured **local disk budget** is exceeded, copy the oldest eligible sealed message blocks to S3, verify and commit their remote references, then remove their local payload files. Native consumers and direct stream reads continue using the same stream and sequence numbers. A cold read fetches the block into a bounded local cache. The current write block and recently used blocks remain local.

This is a tier of `FileStorage`, not an independent `S3Storage` type. `StreamConfig.MaxBytes` remains a **logical retention/quota** limit; it must not be reused as the local cache budget. Nodus currently configures a 64 MiB logical stream `MaxBytes` with `DiscardNew`; simply moving bytes to S3 would not extend that limit. Any later Nodus integration must deliberately set logical retention independently of the node's physical budget.

## Evidence from code and a disposable spike

- `server/store.go` defines the internal `StreamStore` contract. Native consumers call `LoadMsg` and `LoadNextMsg` (`server/consumer.go`); direct stream reads use the same store (`server/stream.go`, `server/jetstream_api.go`). A tier-aware file store can therefore preserve the native APIs, provided its state and error semantics remain correct.
- `server/filestore.go` recovery enumerates local `*.blk` files, and reads can load an entire block under locks. Rolling to a new block can start asynchronous recompression. Old blocks may later be rewritten by erase, compaction, or retention.
- A temporary test wrote 100 one-KiB messages into 32-KiB blocks, then erased sequence 1. The SHA-256 of sealed `1.blk` changed (`2056a2…` to `769b79…`). **Sealed does not mean immutable.**
- The same test stopped the store, removed local `1.blk`, and reopened it. Recovery reported only 70 messages with `FirstSeq=31`, rather than the original 100. **A missing local block is currently interpreted as lost history, not as remote placement.** The temporary test was removed after the run.
- A second temporary test held the old block's lock while a `LoadNextMsg` ran. A concurrent `StoreMsg` remained blocked until that read released the block. **A remote GET in the current load path would couple cold-read latency to local publish latency.** This temporary test was also removed.
- Focused upstream tests for file recovery, erase, age recovery, and `DiscardNew` byte limits passed locally. They establish a baseline; they do not test S3 tiering.

## Restricted first experiment

The first runnable slice should admit only a **single-replica, append-only, limits-retention file stream** with explicit delete and purge disabled, no TTL, no rollups, no per-subject retention, no age-based expiry, no automatic `DiscardOld`, and no compression during the trial. Reject unsupported configurations at stream creation and on updates. Guard internal purge, truncate, erase, and compaction paths as well; API flags alone are not a storage invariant. This restriction avoids uploading a block that can subsequently be changed. General JetStream compatibility requires a later design for immutable remote generations plus durable deletion overlays or copy-on-write replacement.

The file store needs a tier manager with an object-store interface. Use a real S3 provider and a deterministic local test provider behind the same interface. The initial runtime configuration should specify bucket/prefix, region or endpoint, high and low local byte watermarks, cold-read timeout, upload/fetch concurrency, and cache capacity. Credentials remain outside stream metadata. Invalid configuration fails closed. No silent fallback to local-only operation once local capacity is exhausted.

### Offload state machine

1. A block becomes eligible only after the next block is active, pending writes are flushed and synced according to the stream's durability policy, and compression/rewrite tasks have completed. Keep the active block and a configurable recent reserve local.
2. When local bytes exceed the high watermark, choose the oldest eligible local blocks until projected usage is below the low watermark. Coalesce concurrent work per block; bound background I/O so live writes are not starved.
3. Upload a new immutable object key containing a generation or content digest. Verify object length and checksum by an independent read or equivalent strong checksum check. An upload alone does not make a block remote-readable.
4. Commit an immutable remote descriptor with stream identity, block index, sequence bounds, object key/version, checksum, key-material reference, and format version. Commit an immutable manifest generation that includes the descriptor. The local synced descriptor and manifest sidecars speed restart. Commit ordering is **block bytes → verification → remote descriptor → remote manifest → local manifest and descriptor → local unlink**. A crash between steps leaves either a still-local block or an orphan remote object, never a local deletion with no committed remote reference.
5. Reclaim the local payload only after the descriptor is durable and the block has no active readers or writers. Keep logical stream/account byte counters unchanged. Record separate local payload, local cache, remote retained, and transfer backlog metrics.

The remote descriptor is not a complete stream backup. Consumer state, stream metadata, encryption keys, and deletion/retention state require their own recovery design before claiming node-loss restoration.

### Cold read and recovery

Keep enough sequence/subject metadata locally to select a remote block without scanning S3. On a miss, coalesce concurrent fetches, retrieve into a temporary cache file, verify checksum, then atomically expose it to the existing decoder. Do not advance a consumer position or return `not found` on timeout, unavailable object, or checksum failure; expose a retryable storage error. Evict cache entries by a bounded policy without deleting the remote descriptor.

`LoadNextMsg` holds the file-store read lock while searching blocks, and block load holds a block lock. The tiered implementation returns a typed local miss from that locked path, fetches one descriptor-backed block outside those locks, then retries. Direct get maps a tier fetch error to a distinct retryable `503`. Recovery uses local descriptor sidecars to retain remote-only blocks without listing S3. A full-state snapshot still does not include the remote manifest, so node-loss recovery remains outside the supported claims.

## Failure scenarios to prove

| Failure point | Required outcome |
| --- | --- |
| S3 PUT fails or stalls | Local block remains; lag grows; uploads retry with bounds. If disk fills, publishes fail visibly. |
| Upload succeeds but descriptor commit fails | Local block remains; orphan object is safe to collect later. |
| Descriptor commits, process crashes before unlink | Restart sees both copies and prefers verified local, without duplicate sequences. |
| Local block unlinked, process or node restarts | Recovery retains its original sequence range and reads the remote object. |
| Cold GET times out or S3 is unavailable | Read fails retryably; no false `not found`, skipped sequence, or ACK advancement. Writes to the local active block continue if capacity allows. |
| Remote bytes corrupt or wrong version | Checksum fails; fail closed, report the affected range, and retain/restore from another valid copy if available. |
| Read races eviction, cache eviction, or stream stop | Reader pins one object generation; no partial file or use-after-delete. |
| Configuration changes or older binary starts | Incompatible binary refuses a remote-only stream rather than discarding it. |
| Purge/delete/TTL/retention requested | Reject in restricted slice; later design must update remote generations and logical indexes safely. |
| Replica leader changes | Not supported in first slice; later version must agree on descriptor ownership and recovery across replicas. |

## Validation gates before enabling eviction

Run a local S3-compatible service and exercise real PUT/GET/list/delete behavior, not only an in-memory mock. Publish enough messages to roll multiple blocks, lower the local budget, verify that old local payload bytes are reclaimed, and use a **native durable consumer** plus direct `Get` to replay every original sequence across the local/S3 boundary. Repeat after restart and after deleting the node's local block cache. Inject failures at each commit step and during cold reads. Compare stream state, consumer ACK/redelivery state, and checksums before and after. Then benchmark cold-read latency, concurrent consumers, local-write latency during misses, upload lag, cache hit rate, and S3 request volume.

The first implementation should remain explicitly experimental until these gates pass. Do not enable automatic eviction for Nodus based solely on successful upload or a single happy-path replay.
