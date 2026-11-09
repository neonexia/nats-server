// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3TierConfig is an experimental, programmatic file-store option. Prefix must
// uniquely identify one stream incarnation in the bucket.
type S3TierConfig struct {
	Store          S3TierObjectStore
	Prefix         string
	BlockSize      uint64 // Optional experimental block size override.
	LocalHighBytes uint64
	LocalLowBytes  uint64
	// RemoteHighBytes and RemoteLowBytes control when sealed local blocks gain
	// remote coverage. Zero values retain the original behavior of using the
	// local watermarks for both policies.
	RemoteHighBytes   uint64
	RemoteLowBytes    uint64
	RemoteFlushMaxAge time.Duration // Zero disables age-driven remote coverage.
	Timeout           time.Duration
	RetryMin          time.Duration
	RetryMax          time.Duration
	// GCGrace delays deletion of a known failed payload upload. This is only
	// for protocol debris recorded before a descriptor was committed; it never
	// applies to stream history.
	GCGrace time.Duration
}

const (
	defaultS3TierBlockSize  = 8_000_000
	defaultS3TierLocalHigh  = 512_000_000
	defaultS3TierLocalLow   = 384_000_000
	defaultS3TierRemoteHigh = 128_000_000
	defaultS3TierRemoteLow  = 64_000_000
	defaultS3TierTimeout    = 30 * time.Second
	defaultS3TierRetryMin   = time.Second
	defaultS3TierRetryMax   = 30 * time.Second
	defaultS3TierGCGrace    = 24 * time.Hour
	s3TierUploadBatchSize   = 4
	s3TierUploadSlots       = 16
)

// Bound speculative payload buffers across all streams, not only within each
// batch. A busy stream must not multiply staging memory by the channel count.
var s3TierUploads = make(chan struct{}, s3TierUploadSlots)

// S3TierServerConfig is parsed from the server's JetStream configuration.
// Nodus supplies one opaque Prefix per NATS server instance; stream paths are
// derived from normal NATS identities and never contain workspace semantics.
type S3TierServerConfig struct {
	Endpoint string
	Bucket   string
	Region   string
	TLS      bool
	Prefix   string

	CredentialProvider string
	AccessKeyEnv       string
	SecretKeyEnv       string
	SessionTokenEnv    string

	BlockSize         uint64
	LocalHighBytes    uint64
	LocalLowBytes     uint64
	RemoteHighBytes   uint64
	RemoteLowBytes    uint64
	RemoteFlushMaxAge time.Duration // Zero disables age-driven remote coverage.
	Timeout           time.Duration
	RetryMin          time.Duration
	RetryMax          time.Duration
	GCGrace           time.Duration

	store S3TierObjectStore
}

// S3TierStats reports physical tier activity separately from the stream's
// logical retention counters. Values are exposed in the JetStream monitor view.
// S3TierIOStats counts logical object-store calls, including the time spent in
// SDK retries. SlowDown counts errors returned to us, not hidden SDK retries.
type S3TierIOStats struct {
	PutCalls      uint64  `json:"put_calls"`
	PutBytes      uint64  `json:"put_bytes"`
	PutSeconds    float64 `json:"put_seconds"`
	VerifyCalls   uint64  `json:"verify_calls"`
	VerifyBytes   uint64  `json:"verify_bytes"`
	VerifySeconds float64 `json:"verify_seconds"`
	Errors        uint64  `json:"errors"`
	SlowDown      uint64  `json:"slow_down"`
}

type s3TierIOCounters struct {
	puts, putBytes, putNanos, gets, getBytes, getNanos, errors, slowDown atomic.Uint64
}

type S3TierStats struct {
	ImmutableIO map[string]S3TierIOStats `json:"immutable_io"`

	LocalBytes         uint64 `json:"local_bytes"`
	RemoteBytes        uint64 `json:"remote_bytes"`
	RemoteBlocks       uint64 `json:"remote_blocks"`
	Fetches            uint64 `json:"fetches"`
	FetchErrors        uint64 `json:"fetch_errors"`
	CacheHits          uint64 `json:"cache_hits"`
	Uploads            uint64 `json:"uploads"`
	UploadErrors       uint64 `json:"upload_errors"`
	Evictions          uint64 `json:"evictions"`
	CapacityErrors     uint64 `json:"capacity_errors"`
	HighWatermark      uint64 `json:"high_watermark"`
	LowWatermark       uint64 `json:"low_watermark"`
	RemoteHigh         uint64 `json:"remote_high_watermark"`
	RemoteLow          uint64 `json:"remote_low_watermark"`
	CheckpointSeq      uint64 `json:"checkpoint_sequence"`
	BacklogBytes       uint64 `json:"backlog_bytes"`
	BacklogBlocks      uint64 `json:"backlog_blocks"`
	RetryAttempts      uint64 `json:"retry_attempts"`
	GCPending          uint64 `json:"gc_pending"`
	GCDeletes          uint64 `json:"gc_deletes"`
	GCErrors           uint64 `json:"gc_errors"`
	RestorePrepares    uint64 `json:"restore_prepares"`
	RestoreCheckpoints uint64 `json:"restore_checkpoints"`
	RestoreAborts      uint64 `json:"restore_aborts"`
	RestoreActivations uint64 `json:"restore_activations"`
	RestoreRetirements uint64 `json:"restore_retirements"`
	RestoreErrors      uint64 `json:"restore_errors"`
}

// S3TierObjectStore makes the block protocol testable with fault injection.
// Keys are relative to a bucket and must be treated as immutable once written.
type S3TierObjectStore interface {
	// PutIfAbsent creates an immutable object. A false result means the key
	// already existed and the caller must verify it contains the same bytes.
	PutIfAbsent(context.Context, string, []byte) (bool, error)
	Get(context.Context, string) ([]byte, error)
	List(context.Context, string) ([]string, error)
}

// s3TierObjectDeleter is optional so existing read/write test stores retain
// their original contract. An execute request fails closed when the configured
// store cannot provide idempotent deletion.
type s3TierObjectDeleter interface {
	Delete(context.Context, string) error
}

func newS3TierServerConfig() *S3TierServerConfig {
	return &S3TierServerConfig{
		CredentialProvider: "default",
		BlockSize:          defaultS3TierBlockSize,
		LocalHighBytes:     defaultS3TierLocalHigh,
		LocalLowBytes:      defaultS3TierLocalLow,
		RemoteHighBytes:    defaultS3TierRemoteHigh,
		RemoteLowBytes:     defaultS3TierRemoteLow,
		RemoteFlushMaxAge:  5 * time.Minute,
		Timeout:            defaultS3TierTimeout,
		RetryMin:           defaultS3TierRetryMin,
		RetryMax:           defaultS3TierRetryMax,
		GCGrace:            defaultS3TierGCGrace,
	}
}

func (c *S3TierServerConfig) resolveStore() error {
	if c == nil || c.Endpoint == "" || c.Bucket == "" || strings.Trim(c.Prefix, "/") == "" {
		return errors.New("S3 tier requires endpoint, bucket, and opaque prefix")
	}
	creds, err := c.credentials()
	if err != nil {
		return err
	}
	store, err := newS3TierStore(c.Endpoint, c.Region, c.Bucket, c.TLS, creds)
	if err != nil {
		return err
	}
	c.store = store
	return nil
}

func (c *S3TierServerConfig) credentials() (*credentials.Credentials, error) {
	switch c.CredentialProvider {
	case "default":
		if c.AccessKeyEnv != "" || c.SecretKeyEnv != "" || c.SessionTokenEnv != "" {
			return nil, errors.New("S3 tier default credential provider does not accept environment credential references")
		}
		return newS3TierAWSCredentials(c.Region, c.Timeout)
	case "iam":
		if c.AccessKeyEnv != "" || c.SecretKeyEnv != "" || c.SessionTokenEnv != "" {
			return nil, errors.New("S3 tier IAM credential provider does not accept environment credential references")
		}
		// Keep the provider on the client so expiring role credentials refresh
		// before signing requests. Resolving once would strand long-lived stores.
		return credentials.New(&credentials.IAM{Region: c.Region}), nil
	case "env", "environment":
		if c.AccessKeyEnv == "" || c.SecretKeyEnv == "" {
			return nil, errors.New("S3 tier environment credential provider requires access_key_env and secret_key_env")
		}
		accessKey := os.Getenv(c.AccessKeyEnv)
		if accessKey == "" {
			return nil, fmt.Errorf("S3 tier access key environment variable %q is not set", c.AccessKeyEnv)
		}
		secretKey := os.Getenv(c.SecretKeyEnv)
		if secretKey == "" {
			return nil, fmt.Errorf("S3 tier secret key environment variable %q is not set", c.SecretKeyEnv)
		}
		var sessionToken string
		if c.SessionTokenEnv != "" {
			sessionToken = os.Getenv(c.SessionTokenEnv)
		}
		return credentials.NewStaticV4(accessKey, secretKey, sessionToken), nil
	default:
		return nil, fmt.Errorf("S3 tier credential provider %q is not supported", c.CredentialProvider)
	}
}

func (c *S3TierServerConfig) streamConfig(account, stream string) *S3TierConfig {
	if c == nil || c.store == nil {
		return nil
	}
	segment := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	prefix := fmt.Sprintf("%s/streams/%s/%s", strings.Trim(c.Prefix, "/"), segment(account), segment(stream))
	return &S3TierConfig{
		Store: c.store, Prefix: prefix, BlockSize: c.BlockSize,
		LocalHighBytes: c.LocalHighBytes, LocalLowBytes: c.LocalLowBytes,
		RemoteHighBytes: c.RemoteHighBytes, RemoteLowBytes: c.RemoteLowBytes, RemoteFlushMaxAge: c.RemoteFlushMaxAge,
		Timeout: c.Timeout, RetryMin: c.RetryMin, RetryMax: c.RetryMax,
		GCGrace: c.GCGrace,
	}
}

func (c *S3TierServerConfig) equal(other *S3TierServerConfig) bool {
	if c == nil || other == nil {
		return c == other
	}
	return c.Endpoint == other.Endpoint && c.Bucket == other.Bucket && c.Region == other.Region && c.TLS == other.TLS && c.Prefix == other.Prefix &&
		c.CredentialProvider == other.CredentialProvider && c.AccessKeyEnv == other.AccessKeyEnv && c.SecretKeyEnv == other.SecretKeyEnv && c.SessionTokenEnv == other.SessionTokenEnv &&
		c.BlockSize == other.BlockSize && c.LocalHighBytes == other.LocalHighBytes && c.LocalLowBytes == other.LocalLowBytes && c.RemoteHighBytes == other.RemoteHighBytes && c.RemoteLowBytes == other.RemoteLowBytes && c.RemoteFlushMaxAge == other.RemoteFlushMaxAge &&
		c.Timeout == other.Timeout && c.RetryMin == other.RetryMin && c.RetryMax == other.RetryMax && c.GCGrace == other.GCGrace
}

// S3TierMinIOStore works with AWS S3 and S3-compatible endpoints such as MinIO.
type S3TierMinIOStore struct {
	client *minio.Client
	bucket string
}

func NewS3TierMinIOStore(endpoint, accessKey, secretKey, bucket string, secure bool) (*S3TierMinIOStore, error) {
	return newS3TierMinIOStore(endpoint, accessKey, secretKey, "", "", bucket, secure)
}

func newS3TierMinIOStore(endpoint, accessKey, secretKey, sessionToken, region, bucket string, secure bool) (*S3TierMinIOStore, error) {
	return newS3TierStore(endpoint, region, bucket, secure, credentials.NewStaticV4(accessKey, secretKey, sessionToken))
}

func newS3TierStore(endpoint, region, bucket string, secure bool, creds *credentials.Credentials) (*S3TierMinIOStore, error) {
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("S3 tier endpoint and bucket are required")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds: creds, Secure: secure, Region: region,
	})
	if err != nil {
		return nil, err
	}
	return &S3TierMinIOStore{client: client, bucket: bucket}, nil
}

func (s *S3TierMinIOStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	return err
}

func (s *S3TierMinIOStore) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), opts)
	if err == nil {
		return true, nil
	}
	if minio.ToErrorResponse(err).Code == "PreconditionFailed" {
		return false, nil
	}
	return false, err
}

func (s *S3TierMinIOStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

func (s *S3TierMinIOStore) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil
	}
	return err
}

func (s *S3TierMinIOStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

type s3BlockDescriptor struct {
	Version     uint8  `json:"version"`
	Index       uint32 `json:"index"`
	FirstSeq    uint64 `json:"first_seq"`
	LastSeq     uint64 `json:"last_seq"`
	Size        int    `json:"size"`
	SHA256      string `json:"sha256"`
	Key         string `json:"key"`
	Incarnation string `json:"incarnation,omitempty"`
}

// s3TierManifest pins an immutable, paged index of committed block descriptors.
// Readers can discover a stream's remote index without inspecting payloads;
// each new generation receives a new object key so a partial update can never
// replace an earlier complete view.
type s3TierManifest struct {
	Version     uint8               `json:"version"`
	Generation  uint64              `json:"generation"`
	Prefix      string              `json:"prefix"`
	Incarnation string              `json:"incarnation,omitempty"`
	Root        *s3IndexRef         `json:"root,omitempty"`
	Count       uint64              `json:"count"`
	Blocks      []s3BlockDescriptor `json:"-"`
}

// s3TierStreamRecord separates a stream incarnation from the caller supplied
// prefix. The name can be reused, while an immutable record cannot be reused.
type s3TierStreamRecord struct {
	Version      uint8  `json:"version"`
	Incarnation  string `json:"incarnation"`
	Prefix       string `json:"prefix"`
	StreamName   string `json:"stream_name"`
	Created      int64  `json:"created"`
	ConfigSHA256 string `json:"config_sha256"`
}

// s3TierCheckpoint is a message-block recovery boundary. Metadata references
// are intentionally deferred until stream/consumer recovery has a format.
type s3TierCheckpoint struct {
	Version            uint8  `json:"version"`
	Incarnation        string `json:"incarnation"`
	ManifestGeneration uint64 `json:"manifest_generation"`
	ManifestKey        string `json:"manifest_key"`
	ManifestSHA256     string `json:"manifest_sha256"`
	CoveredThrough     uint64 `json:"covered_through_sequence"`
	ConfigSHA256       string `json:"config_sha256"`
}

const (
	s3TierRemoteRestorePrepared     = "prepared"
	s3TierRemoteRestoreCheckpointed = "checkpointed"
	s3TierRemoteRestoreAborted      = "aborted"
	s3TierRemoteRestoreRetired      = "retired"
)

// s3TierRemoteRestoreState is the durable source-side fence for a remote
// restore operation. The local copy makes a source restart preserve its write
// fence; the immutable remote markers make a completed checkpoint discoverable
// when the source disk is later retired.
type s3TierRemoteRestoreState struct {
	Version     uint8     `json:"version"`
	OperationID string    `json:"operation_id"`
	Epoch       uint64    `json:"epoch"`
	State       string    `json:"state"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`

	CheckpointKey    string `json:"checkpoint_key,omitempty"`
	CheckpointSHA256 string `json:"checkpoint_sha256,omitempty"`
}

// s3TierRemoteRestoreCheckpoint binds one source fence to the immutable
// message boundary and stream identity needed by a future target restore. It
// intentionally does not create a writable target; activation needs a
// coordinator-owned lease and is implemented separately.
type s3TierRemoteRestoreCheckpoint struct {
	Version           uint8                   `json:"version"`
	OperationID       string                  `json:"operation_id"`
	Epoch             uint64                  `json:"epoch"`
	Created           time.Time               `json:"created"`
	StreamConfig      StreamConfig            `json:"stream_config"`
	StreamState       StreamState             `json:"stream_state"`
	MessageCheckpoint s3TierCheckpoint        `json:"message_checkpoint"`
	Consumers         []SnapshotConsumerState `json:"consumers,omitempty"`
}

// s3TierRemoteRestoreActivation is a conditionally-created remote receipt for
// one target identity. It turns a source fence epoch into a durable ownership
// decision, so two target nodes cannot both activate the same checkpoint.
type s3TierRemoteRestoreActivation struct {
	Version       uint8     `json:"version"`
	OperationID   string    `json:"operation_id"`
	Epoch         uint64    `json:"epoch"`
	TargetID      string    `json:"target_id"`
	CheckpointKey string    `json:"checkpoint_key"`
	Activated     time.Time `json:"activated"`
}

// s3TierRemoteRestoreRetirement is the immutable commit record that permits
// source cleanup only after a matching target activation has been verified.
type s3TierRemoteRestoreRetirement struct {
	Version       uint8     `json:"version"`
	OperationID   string    `json:"operation_id"`
	Epoch         uint64    `json:"epoch"`
	TargetID      string    `json:"target_id"`
	CheckpointKey string    `json:"checkpoint_key"`
	Retired       time.Time `json:"retired"`
}

// s3TierGCCandidate is written only after an immutable payload write succeeds
// but its descriptor cannot be committed. A descriptor is the first durable
// reachability edge to a payload, so these candidates are the only objects
// this implementation is permitted to delete.
type s3TierGCCandidate struct {
	Version uint8     `json:"version"`
	Key     string    `json:"key"`
	Created time.Time `json:"created"`
}

// S3TierGCResult reports a bounded, idempotent cleanup pass. Pending includes
// candidates still within the grace interval; it never counts stream history.
type S3TierGCResult struct {
	Scanned  uint64 `json:"scanned"`
	Pending  uint64 `json:"pending"`
	Eligible uint64 `json:"eligible"`
	Deleted  uint64 `json:"deleted"`
}

type fileS3Tier struct {
	// Append-only payload residency includes accepted buffered growth. Separate
	// from logical stream bytes, which also include remotely retained blocks.
	// Lock order is stream/block -> resident -> disk budget; a resident holder
	// never acquires stream/block locks. Accounting is rebuilt before exposure.
	residentMu       sync.Mutex
	resident         map[uint32]uint64
	residentBytes    uint64
	budgetRetryAt    time.Time
	budgetRetryDelay time.Duration
	budgetRetryErr   error
	localPressure    atomic.Bool
	fs               *fileStore
	remoteCtx        context.Context
	remoteCancel     context.CancelFunc
	cfg              S3TierConfig
	mu               sync.Mutex   // Protects descriptors and in-flight fetches.
	runMu            sync.Mutex   // One eviction pass at a time.
	hydrateMu        sync.RWMutex // Archive cleanup waits for cold reads before removing files.
	desc             map[uint32]s3BlockDescriptor
	fetch            map[uint32]*s3TierFetch
	gen              uint64
	manifestSHA256   string
	indexRoot        *s3IndexRef
	indexStaging     map[string][]byte // runMu: only final batch paths become durable pages.
	coverageBatch    *[]s3TierCoverage // runMu: payloads remain local until the batch root commits.
	record           *s3TierStreamRecord
	checkpoint       *s3TierCheckpoint
	restore          *s3TierRemoteRestoreState
	activation       *s3TierRemoteRestoreActivation
	gc               []s3TierGCCandidate
	stats            s3TierCounters
	wake             chan struct{}
	quit             chan struct{}
	once             sync.Once
}

type s3TierCounters struct {
	immutableIO                                                                                               [4]s3TierIOCounters
	fetches, fetchErrors, cacheHits, uploads, uploadErrors, evictions, capacityErrors, retryAttempts          atomic.Uint64
	gcDeletes, gcErrors                                                                                       atomic.Uint64
	restorePrepares, restoreCheckpoints, restoreAborts, restoreActivations, restoreRetirements, restoreErrors atomic.Uint64
}

type s3TierFetch struct {
	done chan struct{}
	err  error
}

func newFileS3Tier(fs *fileStore, cfg S3TierConfig) *fileS3Tier {
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultS3TierTimeout
	}
	if cfg.RetryMin == 0 {
		cfg.RetryMin = defaultS3TierRetryMin
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = defaultS3TierRetryMax
	}
	if cfg.GCGrace == 0 {
		cfg.GCGrace = defaultS3TierGCGrace
	}
	if cfg.RemoteHighBytes == 0 {
		cfg.RemoteHighBytes, cfg.RemoteLowBytes = cfg.LocalHighBytes, cfg.LocalLowBytes
	}
	remoteCtx, remoteCancel := context.WithCancel(context.Background())
	return &fileS3Tier{fs: fs, remoteCtx: remoteCtx, remoteCancel: remoteCancel, cfg: cfg, resident: make(map[uint32]uint64), desc: make(map[uint32]s3BlockDescriptor), fetch: make(map[uint32]*s3TierFetch), wake: make(chan struct{}, 1), quit: make(chan struct{})}
}

func validateS3Tier(fcfg FileStoreConfig, cfg StreamConfig) error {
	if fcfg.S3Tier == nil {
		return nil
	}
	t := fcfg.S3Tier
	if t.Timeout == 0 {
		t.Timeout = defaultS3TierTimeout
	}
	if t.RetryMin == 0 {
		t.RetryMin = defaultS3TierRetryMin
	}
	if t.RetryMax == 0 {
		t.RetryMax = defaultS3TierRetryMax
	}
	if t.Store == nil || strings.Trim(t.Prefix, "/") == "" || t.LocalLowBytes == 0 || t.LocalHighBytes <= t.LocalLowBytes ||
		(t.RemoteHighBytes > 0 && t.RemoteHighBytes <= t.RemoteLowBytes) || (t.RemoteHighBytes == 0 && t.RemoteLowBytes != 0) ||
		t.RetryMin <= 0 || t.RetryMax < t.RetryMin || t.RemoteFlushMaxAge < 0 {
		return fmt.Errorf("S3 tier requires store, unique prefix, valid watermarks, and retry max >= retry min > 0")
	}
	if cfg.Replicas > 1 || cfg.Retention != LimitsPolicy || cfg.Discard != DiscardNew || !cfg.DenyDelete || !cfg.DenyPurge ||
		cfg.AllowRollup || cfg.AllowMsgTTL || cfg.MaxAge != 0 || cfg.MaxMsgs > 0 || cfg.MaxMsgsPer > 0 || cfg.Compression != NoCompression ||
		fcfg.Compression != NoCompression || fcfg.AsyncFlush || cfg.PersistMode == AsyncPersistMode || cfg.SubjectDeleteMarkerTTL != 0 ||
		cfg.AllowMsgCounter || len(cfg.Sources) > 0 || cfg.Mirror != nil {
		return fmt.Errorf("experimental S3 tier requires a single-replica append-only LimitsPolicy stream with DiscardNew, DenyDelete, DenyPurge, and no expiry, rollup, compression, or source")
	}
	return nil
}

func (t *fileS3Tier) ctx() (context.Context, context.CancelFunc) {
	parent := t.remoteCtx
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, t.cfg.Timeout)
}

func (t *fileS3Tier) descriptorKey(index uint32) string {
	return fmt.Sprintf("%s/descriptors/%010d.json", strings.Trim(t.cfg.Prefix, "/"), index)
}

func (t *fileS3Tier) descriptorFile(index uint32) string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, fmt.Sprintf("%d.tier.json", index))
}

func (t *fileS3Tier) manifestKey(generation uint64) string {
	return fmt.Sprintf("%s/manifests/%020d.json", strings.Trim(t.cfg.Prefix, "/"), generation)
}

func (t *fileS3Tier) manifestFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.manifest.json")
}

func (t *fileS3Tier) streamRecordKey() string {
	return fmt.Sprintf("%s/stream-record.json", strings.Trim(t.cfg.Prefix, "/"))
}

func (t *fileS3Tier) streamRecordFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.stream.json")
}

func (t *fileS3Tier) checkpointKey(sequence uint64) string {
	return fmt.Sprintf("%s/checkpoints/%020d.json", strings.Trim(t.cfg.Prefix, "/"), sequence)
}

func (t *fileS3Tier) checkpointFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.checkpoint.json")
}

func (t *fileS3Tier) remoteRestoreFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.remote-restore.json")
}

func (t *fileS3Tier) remoteRestorePrefix(operationID string) string {
	return fmt.Sprintf("%s/restore/%s", strings.Trim(t.cfg.Prefix, "/"), operationID)
}

func (t *fileS3Tier) remoteRestoreIntentKey(operationID string) string {
	return t.remoteRestorePrefix(operationID) + "/intent.json"
}

func (t *fileS3Tier) remoteRestoreAbortKey(operationID string) string {
	return t.remoteRestorePrefix(operationID) + "/abort.json"
}

func (t *fileS3Tier) remoteRestoreCheckpointKey(operationID string) string {
	return t.remoteRestorePrefix(operationID) + "/checkpoint.json"
}

func (t *fileS3Tier) remoteRestoreActivationKey(operationID string) string {
	return t.remoteRestorePrefix(operationID) + "/activation.json"
}

func (t *fileS3Tier) remoteRestoreRetirementKey(operationID string) string {
	return t.remoteRestorePrefix(operationID) + "/retirement.json"
}

func (t *fileS3Tier) remoteRestoreActivationFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.remote-restore-target.json")
}

func (t *fileS3Tier) gcFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.gc.json")
}

// prepareS3Tier runs before the normal file-store recovery. Descriptor sidecars
// are durable local evidence that a missing payload belongs to the remote tier.
// Startup must not list or fetch S3 objects: that would turn an object-store
// outage and total history size into a restart dependency.
func (fs *fileStore) prepareS3Tier() error {
	marker := filepath.Join(fs.fcfg.StoreDir, "s3-tier.json")
	if fs.tier == nil {
		if _, err := os.Stat(marker); err == nil {
			return fmt.Errorf("S3-tiered store requires S3 tier configuration")
		}
		return nil
	}
	t := fs.tier
	var manifestIncarnation string
	if buf, err := os.ReadFile(marker); err == nil {
		if string(buf) != strings.Trim(t.cfg.Prefix, "/") {
			return fmt.Errorf("S3 tier prefix changed for existing store")
		}
	} else if !os.IsNotExist(err) {
		return err
	} else if err := writeAtomically(fs.dios, marker, []byte(strings.Trim(t.cfg.Prefix, "/")), defaultFilePerms, true); err != nil {
		return err
	}
	if buf, err := os.ReadFile(t.manifestFile()); err == nil {
		var manifest s3TierManifest
		if err := json.Unmarshal(buf, &manifest); err != nil || manifest.Version != 3 || manifest.Generation == 0 || manifest.Prefix != strings.Trim(t.cfg.Prefix, "/") {
			return fmt.Errorf("invalid local S3 tier manifest: %v", err)
		}
		t.gen = manifest.Generation
		t.manifestSHA256 = checksumS3Block(buf)
		t.indexRoot = manifest.Root
		blocks, err := t.loadLocalManifestIndex(manifest)
		if err != nil {
			return err
		}
		for _, d := range blocks {
			t.desc[d.Index] = d
		}
		manifestIncarnation = manifest.Incarnation
	} else if !os.IsNotExist(err) {
		return err
	}
	if buf, err := os.ReadFile(t.streamRecordFile()); err == nil {
		var record s3TierStreamRecord
		if err := json.Unmarshal(buf, &record); err != nil {
			return fmt.Errorf("invalid local S3 tier stream record: %w", err)
		}
		if !t.validStreamRecord(&record) {
			return errors.New("local S3 tier stream record does not match stream identity or configuration")
		}
		t.record = &record
		if manifestIncarnation != "" && manifestIncarnation != record.Incarnation {
			return errors.New("local S3 tier manifest and stream record disagree")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if manifestIncarnation != "" && t.record == nil {
		return errors.New("local S3 tier manifest has no stream record")
	}
	if buf, err := os.ReadFile(t.checkpointFile()); err == nil {
		var checkpoint s3TierCheckpoint
		if err := json.Unmarshal(buf, &checkpoint); err != nil || !t.validCheckpoint(&checkpoint) {
			return fmt.Errorf("invalid local S3 tier checkpoint: %v", err)
		}
		t.checkpoint = &checkpoint
		if t.record != nil && checkpoint.Incarnation != t.record.Incarnation {
			return errors.New("local S3 tier checkpoint and stream record disagree")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if buf, err := os.ReadFile(t.remoteRestoreFile()); err == nil {
		var restore s3TierRemoteRestoreState
		if err := json.Unmarshal(buf, &restore); err != nil || !validS3TierRemoteRestoreState(&restore) {
			return fmt.Errorf("invalid local S3 tier remote restore state: %v", err)
		}
		t.restore = &restore
	} else if !os.IsNotExist(err) {
		return err
	}
	if buf, err := os.ReadFile(t.remoteRestoreActivationFile()); err == nil {
		var activation s3TierRemoteRestoreActivation
		if err := json.Unmarshal(buf, &activation); err != nil || !validS3TierRemoteRestoreActivation(&activation) {
			return fmt.Errorf("invalid local S3 tier remote restore activation: %v", err)
		}
		t.activation = &activation
	} else if !os.IsNotExist(err) {
		return err
	}
	if buf, err := os.ReadFile(t.gcFile()); err == nil {
		var candidates []s3TierGCCandidate
		if err := json.Unmarshal(buf, &candidates); err != nil {
			return fmt.Errorf("invalid local S3 tier garbage journal: %v", err)
		}
		for _, candidate := range candidates {
			if !t.validGCCandidate(candidate) {
				return errors.New("invalid local S3 tier garbage journal candidate")
			}
		}
		t.gc = candidates
	} else if !os.IsNotExist(err) {
		return err
	}
	paths, err := filepath.Glob(filepath.Join(fs.fcfg.StoreDir, msgDir, "*.tier.json"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		buf, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var d s3BlockDescriptor
		if err := json.Unmarshal(buf, &d); err != nil || !validS3BlockDescriptor(d) || filepath.Clean(path) != t.descriptorFile(d.Index) {
			return fmt.Errorf("invalid local S3 tier descriptor %q: %v", path, err)
		}
		if committed, ok := t.desc[d.Index]; !ok || committed != d {
			return errors.New("local S3 descriptor is not in committed index")
		}
		if local, err := os.ReadFile(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, d.Index))); err == nil {
			if len(local) != d.Size || checksumS3Block(local) != d.SHA256 {
				return fmt.Errorf("S3 tier block %d local checksum mismatch during recovery", d.Index)
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	// The committed local tree is sufficient evidence after a crash between
	// root installation and per-block descriptor installation. Repair locally.
	for _, d := range t.desc {
		path := t.descriptorFile(d.Index)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if local, err := os.ReadFile(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, d.Index))); err == nil {
				if len(local) != d.Size || checksumS3Block(local) != d.SHA256 {
					return fmt.Errorf("S3 tier block %d local checksum mismatch during descriptor recovery", d.Index)
				}
			} else if !os.IsNotExist(err) {
				return err
			}
			buf, err := json.Marshal(d)
			if err != nil {
				return err
			}
			if err = writeAtomically(fs.dios, path, buf, defaultFilePerms, true); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	if err := t.recoverIndexCache(); err != nil {
		return err
	}
	// Recover admission evidence before publishers or reclamation can run. No
	// remote reads are required, and a measurement failure cannot imply zero.
	blocks, err := filepath.Glob(filepath.Join(fs.fcfg.StoreDir, msgDir, "*.blk"))
	if err != nil {
		return err
	}
	for _, path := range blocks {
		var index uint32
		if _, err := fmt.Sscanf(filepath.Base(path), blkScan, &index); err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		t.resident[index] = uint64(info.Size())
		t.residentBytes = addSaturate(t.residentBytes, uint64(info.Size()))
	}
	go t.loop()
	return nil
}

// recoverS3TierBlocks makes locally persisted descriptors part of file-store
// recovery without contacting S3. A crash may leave the normal full-state file
// behind an already committed remote block, because reclaim removes that block
// from local disk. Tiered streams are append-only, so descriptor sequence ranges
// are sufficient to restore the logical boundaries; payloads remain cold until
// a reader selects their block and the usual hydration path fetches it.
func (fs *fileStore) recoverS3TierBlocks() error {
	if fs.tier == nil {
		return nil
	}
	fs.tier.mu.Lock()
	descriptors := make([]s3BlockDescriptor, 0, len(fs.tier.desc))
	for _, d := range fs.tier.desc {
		descriptors = append(descriptors, d)
	}
	fs.tier.mu.Unlock()
	if len(descriptors) == 0 {
		return nil
	}
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].Index < descriptors[j].Index })

	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, d := range descriptors {
		mb := fs.bim[d.Index]
		if mb == nil {
			mb = fs.initMsgBlock(d.Index)
			fs.bim[d.Index] = mb
			fs.blks = append(fs.blks, mb)
		}
		mb.mu.Lock()
		atomic.StoreUint64(&mb.first.seq, d.FirstSeq)
		atomic.StoreUint64(&mb.last.seq, d.LastSeq)
		mb.msgs = d.LastSeq - d.FirstSeq + 1
		mb.rbytes = uint64(d.Size)
		mb.closed = true
		mb.mu.Unlock()
	}
	sort.Slice(fs.blks, func(i, j int) bool { return fs.blks[i].index < fs.blks[j].index })

	// The experimental tier forbids deletes, purges, expiry, and rollups. All
	// sequence ranges therefore represent live messages and can repair a stale
	// full-state snapshot after an unclean restart without reading remote data.
	first, last := atomic.LoadUint64(&fs.blks[0].first.seq), atomic.LoadUint64(&fs.blks[len(fs.blks)-1].last.seq)
	if first == 0 || last < first {
		return errors.New("S3 tier descriptors do not form a valid stream range")
	}
	fs.state.FirstSeq, fs.state.LastSeq = first, last
	fs.state.Msgs = last - first + 1
	return nil
}

func validS3BlockDescriptor(d s3BlockDescriptor) bool {
	return (d.Version == 1 || d.Version == 2) &&
		d.Index != 0 && d.FirstSeq != 0 && d.LastSeq >= d.FirstSeq && d.Size > 0 &&
		d.SHA256 != "" && d.Key != "" && (d.Version == 1 || d.Incarnation != "")
}

func (t *fileS3Tier) configSHA256() (string, error) {
	return s3TierStreamConfigSHA256(t.fs.cfg.StreamConfig)
}

func (t *fileS3Tier) validStreamRecord(record *s3TierStreamRecord) bool {
	if record == nil || record.Version != 1 || record.Incarnation == "" || record.Prefix != strings.Trim(t.cfg.Prefix, "/") || record.StreamName != t.fs.cfg.Name {
		return false
	}
	digest, err := t.configSHA256()
	return err == nil && record.ConfigSHA256 == digest
}

func (t *fileS3Tier) validCheckpoint(checkpoint *s3TierCheckpoint) bool {
	if checkpoint == nil || checkpoint.Version != 1 || checkpoint.Incarnation == "" || checkpoint.ManifestGeneration == 0 || checkpoint.ManifestKey == "" || checkpoint.ManifestSHA256 == "" {
		return false
	}
	digest, err := t.configSHA256()
	return err == nil && checkpoint.ConfigSHA256 == digest &&
		checkpoint.ManifestKey == t.manifestKey(checkpoint.ManifestGeneration)
}

func (t *fileS3Tier) validGCCandidate(candidate s3TierGCCandidate) bool {
	prefix := strings.Trim(t.cfg.Prefix, "/") + "/blocks/"
	return candidate.Version == 1 && candidate.Created.After(time.Unix(0, 0)) && strings.HasPrefix(candidate.Key, prefix) && strings.HasSuffix(candidate.Key, ".blk")
}

func (t *fileS3Tier) writeGCJournalLocked() error {
	buf, err := json.Marshal(t.gc)
	if err != nil {
		return err
	}
	return writeAtomically(t.fs.dios, t.gcFile(), buf, defaultFilePerms, true)
}

// recordGCCandidate persists a failed payload's identity before returning an
// error to the caller. A later successful descriptor commit removes it from
// the journal, and GC will not inspect or delete any other remote object.
func (t *fileS3Tier) recordGCCandidate(key string) error {
	candidate := s3TierGCCandidate{Version: 1, Key: key, Created: time.Now().UTC()}
	if !t.validGCCandidate(candidate) {
		return errors.New("invalid S3 tier garbage candidate")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, existing := range t.gc {
		if existing.Key == key {
			return nil
		}
	}
	t.gc = append(t.gc, candidate)
	if err := t.writeGCJournalLocked(); err != nil {
		t.gc = t.gc[:len(t.gc)-1]
		return err
	}
	return nil
}

func (t *fileS3Tier) removeGCCandidateLocked(key string) error {
	for i, candidate := range t.gc {
		if candidate.Key == key {
			t.gc = append(t.gc[:i], t.gc[i+1:]...)
			return t.writeGCJournalLocked()
		}
	}
	return nil
}

// collectGarbage only deletes payload keys durably journaled when descriptor
// creation failed. Stream records, descriptors, manifests, checkpoints, and
// every payload reachable from a descriptor are deliberately outside its
// authority. That makes stream deletion and history retention irrelevant to
// this operation.
func (t *fileS3Tier) collectGarbage(execute bool) (S3TierGCResult, error) {
	t.runMu.Lock()
	defer t.runMu.Unlock()
	if t.fs.isClosed() {
		return S3TierGCResult{}, ErrStoreClosed
	}

	t.mu.Lock()
	candidates := append([]s3TierGCCandidate(nil), t.gc...)
	referenced := make(map[string]struct{}, len(t.desc))
	for _, descriptor := range t.desc {
		referenced[descriptor.Key] = struct{}{}
	}
	t.mu.Unlock()

	var result S3TierGCResult
	var deleter s3TierObjectDeleter
	if execute {
		var ok bool
		if deleter, ok = t.cfg.Store.(s3TierObjectDeleter); !ok {
			return result, errors.New("S3 tier store does not support garbage collection deletion")
		}
	}
	for _, candidate := range candidates {
		result.Scanned++
		if _, ok := referenced[candidate.Key]; ok {
			t.mu.Lock()
			err := t.removeGCCandidateLocked(candidate.Key)
			t.mu.Unlock()
			if err != nil {
				return result, err
			}
			continue
		}
		if time.Since(candidate.Created) < t.cfg.GCGrace {
			result.Pending++
			continue
		}
		result.Eligible++
		if !execute {
			continue
		}
		ctx, cancel := t.ctx()
		err := deleter.Delete(ctx, candidate.Key)
		cancel()
		if err != nil {
			t.stats.gcErrors.Add(1)
			return result, fmt.Errorf("S3 tier garbage collection delete %q: %w", candidate.Key, err)
		}
		t.mu.Lock()
		err = t.removeGCCandidateLocked(candidate.Key)
		t.mu.Unlock()
		if err != nil {
			return result, err
		}
		t.stats.gcDeletes.Add(1)
		result.Deleted++
	}
	return result, nil
}

func validS3TierRemoteRestoreOperationID(operationID string) bool {
	if len(operationID) == 0 || len(operationID) > 128 {
		return false
	}
	for _, r := range operationID {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func validS3TierRemoteRestoreState(state *s3TierRemoteRestoreState) bool {
	if state == nil || state.Version != 1 || !validS3TierRemoteRestoreOperationID(state.OperationID) || state.Epoch == 0 || state.Created.IsZero() || state.Updated.IsZero() {
		return false
	}
	switch state.State {
	case s3TierRemoteRestorePrepared, s3TierRemoteRestoreAborted:
		return state.CheckpointKey == "" && state.CheckpointSHA256 == ""
	case s3TierRemoteRestoreCheckpointed:
		return state.CheckpointKey != "" && state.CheckpointSHA256 != ""
	default:
		return false
	}
}

func validS3TierRemoteRestoreActivation(activation *s3TierRemoteRestoreActivation) bool {
	return activation != nil && activation.Version == 1 && validS3TierRemoteRestoreOperationID(activation.OperationID) &&
		activation.Epoch > 0 && activation.TargetID != "" && activation.CheckpointKey != "" && !activation.Activated.IsZero()
}

func (state *s3TierRemoteRestoreState) fenced() bool {
	return state != nil && (state.State == s3TierRemoteRestorePrepared || state.State == s3TierRemoteRestoreCheckpointed)
}

func (t *fileS3Tier) remoteRestoreState() *s3TierRemoteRestoreState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.restore == nil {
		return nil
	}
	state := *t.restore
	return &state
}

func (t *fileS3Tier) remoteRestoreActivation() *s3TierRemoteRestoreActivation {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activation == nil {
		return nil
	}
	activation := *t.activation
	return &activation
}

func (t *fileS3Tier) writeRemoteRestoreState(state *s3TierRemoteRestoreState) error {
	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := writeAtomically(t.fs.dios, t.remoteRestoreFile(), buf, defaultFilePerms, true); err != nil {
		return err
	}
	t.mu.Lock()
	stateCopy := *state
	t.restore = &stateCopy
	t.mu.Unlock()
	return nil
}

func (t *fileS3Tier) writeRemoteRestoreActivation(activation *s3TierRemoteRestoreActivation) error {
	if !validS3TierRemoteRestoreActivation(activation) {
		return errors.New("invalid remote restore activation")
	}
	buf, err := json.Marshal(activation)
	if err != nil {
		return err
	}
	if err := writeAtomically(t.fs.dios, t.remoteRestoreActivationFile(), buf, defaultFilePerms, true); err != nil {
		return err
	}
	t.mu.Lock()
	activationCopy := *activation
	t.activation = &activationCopy
	t.mu.Unlock()
	return nil
}

// prepareRemoteRestore persists the source intent before the caller starts
// rejecting publishes. An operation ID makes retried control requests
// idempotent after a lost response or a source restart.
func (t *fileS3Tier) prepareRemoteRestore(operationID string) (*s3TierRemoteRestoreState, error) {
	if !validS3TierRemoteRestoreOperationID(operationID) {
		return nil, errors.New("remote restore operation ID must contain only letters, digits, '-' or '_'")
	}
	t.mu.Lock()
	if current := t.restore; current != nil {
		if current.OperationID == operationID {
			state := *current
			t.mu.Unlock()
			if state.fenced() {
				return &state, nil
			}
			return nil, errors.New("remote restore operation was aborted; use a new operation ID")
		}
		if current.fenced() {
			t.mu.Unlock()
			return nil, errors.New("another remote restore operation already fences this stream")
		}
	}
	epoch := uint64(1)
	if t.restore != nil {
		epoch = t.restore.Epoch + 1
	}
	t.mu.Unlock()

	if buf, found, err := t.getImmutableIfPresent(t.remoteRestoreIntentKey(operationID)); err != nil {
		return nil, err
	} else if found {
		var state s3TierRemoteRestoreState
		if err := json.Unmarshal(buf, &state); err != nil || !validS3TierRemoteRestoreState(&state) || state.OperationID != operationID || state.State != s3TierRemoteRestorePrepared {
			return nil, errors.New("remote restore intent conflicts with local operation")
		}
		if err := t.writeRemoteRestoreState(&state); err != nil {
			return nil, err
		}
		return &state, nil
	}
	now := time.Now().UTC()
	state := &s3TierRemoteRestoreState{Version: 1, OperationID: operationID, Epoch: epoch, State: s3TierRemoteRestorePrepared, Created: now, Updated: now}
	buf, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if err := t.putImmutable(t.remoteRestoreIntentKey(operationID), buf); err != nil {
		return nil, fmt.Errorf("S3 tier remote restore intent verification failed: %w", err)
	}
	if err := t.writeRemoteRestoreState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (t *fileS3Tier) abortRemoteRestore(operationID string) (*s3TierRemoteRestoreState, error) {
	t.mu.Lock()
	if t.restore == nil || t.restore.OperationID != operationID {
		t.mu.Unlock()
		return nil, errors.New("remote restore operation is not prepared")
	}
	state := *t.restore
	t.mu.Unlock()
	if state.State == s3TierRemoteRestoreAborted {
		return &state, nil
	}
	if state.State != s3TierRemoteRestorePrepared {
		return nil, errors.New("remote restore operation is not prepared")
	}
	state.State, state.Updated = s3TierRemoteRestoreAborted, time.Now().UTC()
	if buf, found, err := t.getImmutableIfPresent(t.remoteRestoreAbortKey(operationID)); err != nil {
		return nil, err
	} else if found {
		if err := json.Unmarshal(buf, &state); err != nil || !validS3TierRemoteRestoreState(&state) || state.OperationID != operationID || state.State != s3TierRemoteRestoreAborted {
			return nil, errors.New("remote restore abort conflicts with local operation")
		}
		if err := t.writeRemoteRestoreState(&state); err != nil {
			return nil, err
		}
		return &state, nil
	}
	buf, err := json.Marshal(&state)
	if err != nil {
		return nil, err
	}
	if err := t.putImmutable(t.remoteRestoreAbortKey(operationID), buf); err != nil {
		return nil, fmt.Errorf("S3 tier remote restore abort verification failed: %w", err)
	}
	if err := t.writeRemoteRestoreState(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (t *fileS3Tier) checkpointRemoteRestore(operationID string, checkpoint *s3TierRemoteRestoreCheckpoint) (*s3TierRemoteRestoreState, error) {
	if checkpoint == nil {
		return nil, errors.New("remote restore checkpoint is required")
	}
	t.mu.Lock()
	if t.restore == nil || t.restore.OperationID != operationID {
		t.mu.Unlock()
		return nil, errors.New("remote restore operation was not prepared")
	}
	if t.restore.State == s3TierRemoteRestoreCheckpointed {
		state := *t.restore
		t.mu.Unlock()
		return &state, nil
	}
	if t.restore.State != s3TierRemoteRestorePrepared {
		t.mu.Unlock()
		return nil, errors.New("remote restore operation is not prepared")
	}
	state := *t.restore
	t.mu.Unlock()

	key := t.remoteRestoreCheckpointKey(operationID)
	if buf, found, err := t.getImmutableIfPresent(key); err != nil {
		return nil, err
	} else if found {
		var existing s3TierRemoteRestoreCheckpoint
		if err := json.Unmarshal(buf, &existing); err != nil || existing.Version != 1 || existing.OperationID != state.OperationID || existing.Epoch != state.Epoch {
			return nil, errors.New("remote restore checkpoint conflicts with local operation")
		}
		state.State, state.Updated = s3TierRemoteRestoreCheckpointed, time.Now().UTC()
		state.CheckpointKey, state.CheckpointSHA256 = key, checksumS3Block(buf)
		if err := t.writeRemoteRestoreState(&state); err != nil {
			return nil, err
		}
		return &state, nil
	}
	checkpoint.Version, checkpoint.OperationID, checkpoint.Epoch = 1, state.OperationID, state.Epoch
	checkpoint.Created = time.Now().UTC()
	buf, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}
	if err := t.putImmutable(key, buf); err != nil {
		return nil, fmt.Errorf("S3 tier remote restore checkpoint verification failed: %w", err)
	}
	state.State, state.Updated = s3TierRemoteRestoreCheckpointed, time.Now().UTC()
	state.CheckpointKey, state.CheckpointSHA256 = key, checksumS3Block(buf)
	if err := t.writeRemoteRestoreState(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (t *fileS3Tier) putImmutable(key string, data []byte) error {
	kind := 3
	switch {
	case strings.Contains(key, "/blocks/"):
		kind = 0
	case strings.Contains(key, "/descriptors/"):
		kind = 1
	case strings.Contains(key, "/manifests/"), strings.Contains(key, "/index/pages/"):
		kind = 2
	}
	stats := &t.stats.immutableIO[kind]
	recordError := func(err error) {
		stats.errors.Add(1)
		if minio.ToErrorResponse(err).Code == "SlowDown" {
			stats.slowDown.Add(1)
		}
	}
	ctx, cancel := t.ctx()
	started := time.Now()
	stats.puts.Add(1)
	stats.putBytes.Add(uint64(len(data)))
	_, err := t.cfg.Store.PutIfAbsent(ctx, key, data)
	stats.putNanos.Add(uint64(time.Since(started)))
	cancel()
	if err != nil {
		recordError(err)
		return err
	}
	ctx, cancel = t.ctx()
	started = time.Now()
	stats.gets.Add(1)
	committed, err := t.cfg.Store.Get(ctx, key)
	stats.getNanos.Add(uint64(time.Since(started)))
	stats.getBytes.Add(uint64(len(committed)))
	cancel()
	if err != nil {
		recordError(err)
		return fmt.Errorf("S3 tier immutable object verification failed for %q: %w", key, err)
	}
	if !bytes.Equal(committed, data) {
		stats.errors.Add(1)
		return fmt.Errorf("S3 tier immutable object verification failed for %q: content mismatch", key)
	}
	return nil
}

// getImmutableIfPresent distinguishes an absent immutable marker from a
// transport failure without depending on object-store-specific not-found
// errors. Restore operation retries use it after a crash between remote and
// local durability steps.
func (t *fileS3Tier) getImmutableIfPresent(key string) ([]byte, bool, error) {
	ctx, cancel := t.ctx()
	keys, err := t.cfg.Store.List(ctx, key)
	cancel()
	if err != nil {
		return nil, false, err
	}
	for _, candidate := range keys {
		if candidate != key {
			continue
		}
		ctx, cancel = t.ctx()
		buf, err := t.cfg.Store.Get(ctx, key)
		cancel()
		return buf, err == nil, err
	}
	return nil, false, nil
}

// claimRemoteRestoreActivation conditionally creates the durable target
// receipt. A matching retry is accepted, while a different target identity is
// rejected even when it presents the same checkpoint and epoch.
func (t *fileS3Tier) claimRemoteRestoreActivation(operationID string, epoch uint64, targetID, checkpointKey string) (*s3TierRemoteRestoreActivation, error) {
	if !validS3TierRemoteRestoreOperationID(operationID) || epoch == 0 || targetID == "" || checkpointKey == "" {
		return nil, errors.New("invalid remote restore activation request")
	}
	key := t.remoteRestoreActivationKey(operationID)
	if buf, found, err := t.getImmutableIfPresent(key); err != nil {
		return nil, err
	} else if found {
		var activation s3TierRemoteRestoreActivation
		if err := json.Unmarshal(buf, &activation); err != nil || !validS3TierRemoteRestoreActivation(&activation) ||
			activation.OperationID != operationID || activation.Epoch != epoch || activation.CheckpointKey != checkpointKey {
			return nil, errors.New("remote restore activation conflicts with checkpoint")
		}
		if activation.TargetID != targetID {
			return nil, errors.New("remote restore checkpoint is already activated by another target")
		}
		return &activation, nil
	}
	activation := &s3TierRemoteRestoreActivation{
		Version: 1, OperationID: operationID, Epoch: epoch, TargetID: targetID, CheckpointKey: checkpointKey, Activated: time.Now().UTC(),
	}
	buf, err := json.Marshal(activation)
	if err != nil {
		return nil, err
	}
	if err := t.putImmutable(key, buf); err != nil {
		return nil, fmt.Errorf("S3 tier remote restore activation verification failed: %w", err)
	}
	return activation, nil
}

func validS3TierRemoteRestoreRetirement(retirement *s3TierRemoteRestoreRetirement) bool {
	return retirement != nil && retirement.Version == 1 && validS3TierRemoteRestoreOperationID(retirement.OperationID) &&
		retirement.Epoch > 0 && retirement.TargetID != "" && retirement.CheckpointKey != "" && !retirement.Retired.IsZero()
}

func (t *fileS3Tier) remoteRestoreRetirement(operationID string) (*s3TierRemoteRestoreRetirement, error) {
	buf, found, err := t.getImmutableIfPresent(t.remoteRestoreRetirementKey(operationID))
	if err != nil || !found {
		return nil, err
	}
	var retirement s3TierRemoteRestoreRetirement
	if err := json.Unmarshal(buf, &retirement); err != nil || !validS3TierRemoteRestoreRetirement(&retirement) || retirement.OperationID != operationID {
		return nil, errors.New("remote restore retirement is invalid")
	}
	return &retirement, nil
}

// retireRemoteRestoreSource first verifies a durable target activation, then
// persists an immutable retirement receipt. The receipt makes local cleanup
// retryable after a source crash without ever deleting remote recovery data.
func (t *fileS3Tier) retireRemoteRestoreSource(state *s3TierRemoteRestoreState) (*s3TierRemoteRestoreRetirement, error) {
	if state == nil || state.State != s3TierRemoteRestoreCheckpointed || state.CheckpointKey == "" {
		return nil, errors.New("remote source retirement requires a checkpointed operation")
	}
	if existing, err := t.remoteRestoreRetirement(state.OperationID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.Epoch != state.Epoch || existing.CheckpointKey != state.CheckpointKey {
			return nil, errors.New("remote restore retirement conflicts with checkpoint")
		}
		return existing, nil
	}
	buf, found, err := t.getImmutableIfPresent(t.remoteRestoreActivationKey(state.OperationID))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("remote source retirement requires target activation")
	}
	var activation s3TierRemoteRestoreActivation
	if err := json.Unmarshal(buf, &activation); err != nil || !validS3TierRemoteRestoreActivation(&activation) ||
		activation.OperationID != state.OperationID || activation.Epoch != state.Epoch || activation.CheckpointKey != state.CheckpointKey {
		return nil, errors.New("remote source retirement activation conflicts with checkpoint")
	}
	retirement := &s3TierRemoteRestoreRetirement{
		Version: 1, OperationID: state.OperationID, Epoch: state.Epoch, TargetID: activation.TargetID,
		CheckpointKey: state.CheckpointKey, Retired: time.Now().UTC(),
	}
	data, err := json.Marshal(retirement)
	if err != nil {
		return nil, err
	}
	if err := t.putImmutable(t.remoteRestoreRetirementKey(state.OperationID), data); err != nil {
		return nil, fmt.Errorf("S3 tier remote source retirement verification failed: %w", err)
	}
	return retirement, nil
}

func (t *fileS3Tier) ensureStreamRecord() (*s3TierStreamRecord, error) {
	t.mu.Lock()
	if t.record != nil {
		record := *t.record
		t.mu.Unlock()
		return &record, nil
	}
	t.mu.Unlock()
	digest, err := t.configSHA256()
	if err != nil {
		return nil, err
	}
	incarnation := make([]byte, 16)
	if _, err := rand.Read(incarnation); err != nil {
		return nil, err
	}
	record := &s3TierStreamRecord{Version: 1, Incarnation: hex.EncodeToString(incarnation), Prefix: strings.Trim(t.cfg.Prefix, "/"), StreamName: t.fs.cfg.Name, Created: t.fs.cfg.Created.UnixNano(), ConfigSHA256: digest}
	buf, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	ctx, cancel := t.ctx()
	created, err := t.cfg.Store.PutIfAbsent(ctx, t.streamRecordKey(), buf)
	cancel()
	if err != nil {
		return nil, err
	}
	if !created {
		ctx, cancel = t.ctx()
		buf, err = t.cfg.Store.Get(ctx, t.streamRecordKey())
		cancel()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(buf, record); err != nil || !t.validStreamRecord(record) {
			return nil, fmt.Errorf("S3 tier stream record conflicts with local stream")
		}
	}
	if err := writeAtomically(t.fs.dios, t.streamRecordFile(), buf, defaultFilePerms, true); err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.record == nil {
		t.record = record
	}
	result := *t.record
	t.mu.Unlock()
	return &result, nil
}

func (t *fileS3Tier) commitManifest(add *s3BlockDescriptor) error {
	var added []s3BlockDescriptor
	if add != nil {
		added = []s3BlockDescriptor{*add}
	}
	return t.commitManifestBatch(added)
}
func (t *fileS3Tier) commitManifestBatch(added []s3BlockDescriptor) error {
	record, err := t.ensureStreamRecord()
	if err != nil {
		return err
	}
	if err = t.finishIndexPending(); err != nil {
		return err
	}
	t.mu.Lock()
	generation, root := t.gen, t.indexRoot
	t.mu.Unlock()
	if len(added) == 0 && generation > 0 {
		return nil
	}
	t.indexStaging = make(map[string][]byte)
	defer func() { t.indexStaging = nil }()
	next := root
	var obsolete []s3IndexRef
	for _, add := range added {
		refs, err := t.insertIndex(next, add, &obsolete)
		if err != nil {
			return err
		}
		if len(refs) == 1 {
			next = &refs[0]
		} else {
			r, err := t.storeIndexPage(s3IndexPage{Version: 1, Incarnation: record.Incarnation, Level: refs[0].Level + 1, Children: refs})
			if err != nil {
				return err
			}
			next = &r
		}
	}
	if root != nil && next != nil && *next == *root {
		return nil
	}
	if err = t.publishStagedIndex(next); err != nil {
		return err
	}
	m := s3TierManifest{Version: 3, Generation: generation + 1, Prefix: strings.Trim(t.cfg.Prefix, "/"), Incarnation: record.Incarnation, Root: next}
	if next != nil {
		m.Count = next.Count
	}
	pending := s3IndexPending{Manifest: m, Added: added, Obsolete: obsolete}
	buf, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	if err = writeAtomically(t.fs.dios, t.indexPendingFile(), buf, defaultFilePerms, true); err != nil {
		return err
	}
	return t.finishIndexPending()
}

func (t *fileS3Tier) findCheckpoint(sequence uint64) (*s3TierCheckpoint, error) {
	ctx, cancel := t.ctx()
	keys, err := t.cfg.Store.List(ctx, t.checkpointKey(sequence))
	cancel()
	if err != nil {
		return nil, err
	}
	key := t.checkpointKey(sequence)
	found := false
	for _, candidate := range keys {
		if candidate == key {
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}
	ctx, cancel = t.ctx()
	buf, err := t.cfg.Store.Get(ctx, key)
	cancel()
	if err != nil {
		return nil, err
	}
	var checkpoint s3TierCheckpoint
	if err := json.Unmarshal(buf, &checkpoint); err != nil || !t.validCheckpoint(&checkpoint) || checkpoint.CoveredThrough != sequence {
		return nil, fmt.Errorf("invalid remote S3 tier checkpoint at sequence %d", sequence)
	}
	return &checkpoint, nil
}

func (t *fileS3Tier) commitCheckpoint(sequence uint64) error {
	t.mu.Lock()
	if t.checkpoint != nil && t.checkpoint.CoveredThrough == sequence {
		t.mu.Unlock()
		return nil
	}
	record := t.record
	generation, manifestSHA256 := t.gen, t.manifestSHA256
	t.mu.Unlock()
	if record == nil || generation == 0 || manifestSHA256 == "" {
		return errors.New("S3 tier checkpoint has no committed manifest")
	}
	checkpoint := &s3TierCheckpoint{Version: 1, Incarnation: record.Incarnation, ManifestGeneration: generation, ManifestKey: t.manifestKey(generation), ManifestSHA256: manifestSHA256, CoveredThrough: sequence, ConfigSHA256: record.ConfigSHA256}
	buf, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	if err := t.putImmutable(t.checkpointKey(sequence), buf); err != nil {
		return fmt.Errorf("S3 tier checkpoint verification failed at sequence %d: %w", sequence, err)
	}
	if err := writeAtomically(t.fs.dios, t.checkpointFile(), buf, defaultFilePerms, true); err != nil {
		return err
	}
	t.mu.Lock()
	t.checkpoint = checkpoint
	t.mu.Unlock()
	return nil
}

func (t *fileS3Tier) hasDescriptor(index uint32) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.desc[index]
	return ok
}

func (t *fileS3Tier) loop() {
	var ageC <-chan time.Time
	if t.cfg.RemoteFlushMaxAge > 0 {
		// Polling is bounded even for tiny configured ages. The due decision
		// always uses persisted message time, never time since this loop began.
		ticker := time.NewTicker(max(10*time.Millisecond, min(time.Second, t.cfg.RemoteFlushMaxAge/4)))
		defer ticker.Stop()
		ageC = ticker.C
	}
	var (
		delay     time.Duration
		timer     *time.Timer
		retryC    <-chan time.Time
		scanTimer *time.Timer
		scanC     <-chan time.Time
		nextScan  time.Time
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if scanTimer != nil {
			scanTimer.Stop()
		}
	}()
	for {
		ageTick := false
		wakeScan := false
		select {
		case <-t.wake:
			reclaimAt := t.cfg.LocalHighBytes - min(t.cfg.LocalHighBytes-t.cfg.LocalLowBytes, uint64(t.fs.fcfg.BlockSize))
			if !t.localPressure.Load() && t.residentSize() <= reclaimAt && time.Now().Before(nextScan) {
				// Coalesce normal publisher wakes, but keep a timer so the final
				// write is processed even when no later publisher arrives.
				if scanC == nil {
					scanTimer = time.NewTimer(time.Until(nextScan))
					scanC = scanTimer.C
				}
				continue
			}
			wakeScan = true
		case <-scanC:
			scanC = nil
			wakeScan = true
		case <-ageC:
			ageTick = true
			t.mu.Lock()
			backingOff := time.Now().Before(t.budgetRetryAt)
			t.mu.Unlock()
			if backingOff {
				continue
			}
		case <-retryC:
			retryC = nil
		case <-t.quit:
			return
		}
		if wakeScan {
			if timer != nil {
				timer.Stop()
			}
			retryC = nil
		}
		if !ageTick && scanC != nil {
			scanTimer.Stop()
			scanC = nil
		}
		var err error
		if t.localPressure.Swap(false) {
			err = t.reclaimDiskBudgetBlock()
			if err != nil {
				t.localPressure.Store(true)
			}
		}
		if err == nil {
			err = t.flushAgedBlocks()
		}
		// An age flush can consume the remaining local headroom. Do not make
		// reclamation wait for another publisher wake after that flush yields.
		reclaimAt := t.cfg.LocalHighBytes - min(t.cfg.LocalHighBytes-t.cfg.LocalLowBytes, uint64(t.fs.fcfg.BlockSize))
		if err == nil && (!ageTick || t.localPressure.Load() || t.residentSize() > reclaimAt) {
			err = t.evictToBudget()
			// Explicit drains and pressure work remain outside this normal-wake
			// rate limit. Neither age-flush ticks nor retry timers are delayed.
			nextScan = time.Now().Add(100 * time.Millisecond)
		}
		if err != nil {
			t.fs.warn("S3 tier eviction failed: %v", err)
			t.stats.retryAttempts.Add(1)
			if delay == 0 {
				delay = t.cfg.RetryMin
			} else if delay < t.cfg.RetryMax/2 {
				delay *= 2
			} else {
				delay = t.cfg.RetryMax
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(delay)
			retryC = timer.C
		} else {
			delay = 0
		}
	}
}

func (t *fileS3Tier) kick() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *fileS3Tier) stop() {
	t.once.Do(func() {
		close(t.quit)
		if t.fs != nil {
			t.fs.dios.budget.Load().unregister(t)
		}
	})
}

func checksumS3Block(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func (t *fileS3Tier) ensureHydrated(index uint32) error {
	t.hydrateMu.RLock()
	defer t.hydrateMu.RUnlock()
	if t.fs.isClosed() {
		return ErrStoreClosed
	}
	t.mu.Lock()
	d, ok := t.desc[index]
	t.mu.Unlock()
	if !ok {
		return nil
	}
	path := filepath.Join(t.fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, index))
	if info, err := os.Stat(path); err == nil {
		if info.Size() != int64(d.Size) {
			return fmt.Errorf("S3 tier block %d local size mismatch", index)
		}
		t.stats.cacheHits.Add(1)
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	t.mu.Lock()
	if fetch := t.fetch[index]; fetch != nil {
		t.mu.Unlock()
		<-fetch.done
		return fetch.err
	}
	fetch := &s3TierFetch{done: make(chan struct{})}
	t.fetch[index] = fetch
	t.mu.Unlock()

	t.stats.fetches.Add(1)
	err := t.fetchBlock(d, path)
	if err != nil {
		t.stats.fetchErrors.Add(1)
	}
	t.mu.Lock()
	fetch.err = err
	delete(t.fetch, index)
	close(fetch.done)
	t.mu.Unlock()
	return err
}

func (t *fileS3Tier) fetchBlock(d s3BlockDescriptor, path string) error {
	budget := t.fs.dios.budget.Load()
	if budget != nil {
		if _, err := budget.reserveGrowth(path+".tmp", uint64(d.Size), false); err != nil {
			return err
		}
		defer budget.completed(path+".tmp", path)
	}
	// An in-flight cold fetch will become resident. Publish admission must see
	// that claim before network I/O, even while no local payload exists yet.
	t.residentMu.Lock()
	previous := t.resident[d.Index]
	reserved := max(previous, uint64(d.Size))
	t.setResidentLocked(d.Index, reserved)
	t.residentMu.Unlock()
	installAttempted := false
	defer func() {
		if !installAttempted {
			t.residentMu.Lock()
			// Fetches are single-flight for an immutable block. Concurrent unlink
			// can clear this charge; rollback must not resurrect removed bytes.
			if reserved != 0 && t.resident[d.Index] == reserved {
				t.setResidentLocked(d.Index, previous)
			}
			t.residentMu.Unlock()
		}
	}()

	ctx, cancel := t.ctx()
	data, err := t.cfg.Store.Get(ctx, d.Key)
	cancel()
	if err != nil {
		// Closing the store cancels remote I/O intentionally. Preserve the
		// normal read/close contract rather than reporting a storage outage.
		if t.remoteCtx != nil && t.remoteCtx.Err() != nil {
			return ErrStoreClosed
		}
		return fmt.Errorf("%w: block %d unavailable: %v", errS3TierUnavailable, d.Index, err)
	}
	if len(data) != d.Size || checksumS3Block(data) != d.SHA256 {
		return fmt.Errorf("%w: block %d remote checksum mismatch", errS3TierUnavailable, d.Index)
	}
	// Serialize payload creation with unlink/accounting. A failed atomic write
	// may have reached rename, so retain its conservative charge until recovery.
	t.residentMu.Lock()
	installAttempted = true
	t.setResidentLocked(d.Index, max(t.resident[d.Index], uint64(len(data))))
	err = writeAtomically(t.fs.dios, path, data, defaultFilePerms, true)
	t.residentMu.Unlock()
	if err != nil {
		return err
	}
	// A cold read adds a local payload just like a publish does. Schedule the
	// same reclamation pass so repeated range scans cannot grow the cache.
	t.kick()
	return nil
}

// evictToBudget first gives old sealed blocks remote coverage, then separately
// reclaims local copies only when the node-local budget requires it. The two
// watermark pairs intentionally allow an online stream to overlap local and
// remote copies for normal replay performance.
func (t *fileS3Tier) evictToBudget() error {
	t.runMu.Lock()
	defer t.runMu.Unlock()
	if t.fs.isClosed() {
		return ErrStoreClosed
	}
	fs := t.fs
	// Begin reclamation one block before the hard publish fence. This gives
	// background I/O headroom at steady rates without increasing the budget.
	reclaimAt := t.cfg.LocalHighBytes - min(t.cfg.LocalHighBytes-t.cfg.LocalLowBytes, uint64(fs.fcfg.BlockSize))
	fs.mu.RLock()
	var used, uncovered uint64
	t.mu.Lock()
	remote := make(map[uint32]struct{}, len(t.desc))
	for index := range t.desc {
		remote[index] = struct{}{}
	}
	t.mu.Unlock()
	type candidate struct {
		mb     *msgBlock
		remote bool
		last   int64
	}
	var candidates []candidate
	for _, mb := range fs.blks {
		if info, err := os.Stat(mb.mfn); err == nil {
			used += uint64(info.Size())
			if mb != fs.lmb {
				mb.mu.RLock()
				last := mb.llts
				mb.mu.RUnlock()
				_, isRemote := remote[mb.index]
				candidates = append(candidates, candidate{mb: mb, remote: isRemote, last: last})
				if !isRemote {
					uncovered += uint64(info.Size())
				}
			}
		}
	}
	fs.mu.RUnlock()
	// Remote coverage is governed by its own threshold and does not evict a
	// local copy. This is how a live channel can remain fast after S3 upload.
	// Under local pressure, reclaim covered blocks first (or upload and
	// reclaim one block at a time below). Covering the entire backlog before
	// freeing any disk unnecessarily holds publishers at the capacity fence.
	if uncovered > t.cfg.RemoteHighBytes && used <= reclaimAt {
		var pending []*msgBlock
		for _, candidate := range candidates {
			if !candidate.remote {
				pending = append(pending, candidate.mb)
			}
		}
		for len(pending) > 0 && uncovered > t.cfg.RemoteLowBytes {
			// Publishers can consume headroom while a bounded remote batch is
			// in flight. Yield between batches so newly covered payloads can be
			// reclaimed before another batch extends the admission outage.
			if t.residentSize() > reclaimAt {
				break
			}
			n := min(s3TierUploadBatchSize, len(pending))
			sizes, err := t.coverBlocks(pending[:n])
			if err != nil {
				return err
			}
			for _, size := range sizes {
				uncovered -= min(uncovered, size)
			}
			pending = pending[n:]
		}
	}
	// Also handle pressure that arrived during the final coverage batch. The
	// physical scan excludes pending cold fetches from reclaimable disk bytes.
	if t.residentSize() > reclaimAt {
		used = t.localBytes()
	}
	if used <= reclaimAt {
		return nil
	}
	// Coverage may have changed above. Refresh the candidate view before
	// choosing local victims so newly uploaded blocks are preferred for
	// reclamation over blocks that still lack a durable remote copy.
	t.mu.Lock()
	for i := range candidates {
		_, candidates[i].remote = t.desc[candidates[i].mb.index]
	}
	t.mu.Unlock()
	// Reclaim hydrated remote blocks first, least recently used. This keeps the
	// local directory bounded as a cache before offloading additional hot data.
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].remote != candidates[j].remote {
			return candidates[i].remote
		}
		if candidates[i].remote {
			return candidates[i].last < candidates[j].last
		}
		return candidates[i].mb.index < candidates[j].mb.index
	})
	for i := 0; i < len(candidates) && used > t.cfg.LocalLowBytes; i++ {
		candidate := candidates[i]
		if !candidate.remote {
			// Pressure must not fall back to a root commit per block. Cover a
			// bounded batch, then reclaim it before doing more remote I/O.
			// Already-covered victims sorted above never wait for this batch.
			n := min(s3TierUploadBatchSize, len(candidates)-i)
			blocks := make([]*msgBlock, n)
			for j := range blocks {
				blocks[j] = candidates[i+j].mb
			}
			if _, err := t.coverBlocks(blocks); err != nil {
				return err
			}
			for j := 0; j < n; j++ {
				candidates[i+j].remote = true
			}
		}
		size, err := t.evictBlock(candidate.mb, true)
		if err != nil {
			return err
		}
		if size > used {
			used = 0
		} else {
			used -= size
		}
	}
	// Resume normal remote coverage after pressure reclamation even if the
	// publisher stopped and no new write arrives to wake the worker.
	if used <= reclaimAt && uncovered > t.cfg.RemoteHighBytes {
		t.kick()
	}
	return nil
}

// drain seals the active block before it is called, then ensures every local
// sealed block has a verified remote descriptor. It intentionally preserves
// local payloads; lifecycle code decides whether to reclaim them later.
// A checkpoint is only committed after this exact coverage boundary exists.
func (t *fileS3Tier) drain(coveredThrough uint64) (uint64, error) {
	t.runMu.Lock()
	defer t.runMu.Unlock()
	if t.fs.isClosed() {
		return 0, ErrStoreClosed
	}
	fs := t.fs
	fs.mu.RLock()
	candidates := make([]*msgBlock, 0, len(fs.blks))
	for _, mb := range fs.blks {
		if mb != fs.lmb {
			candidates = append(candidates, mb)
		}
	}
	fs.mu.RUnlock()
	if len(candidates) == 0 && coveredThrough != 0 {
		return 0, nil
	}
	if _, err := t.ensureStreamRecord(); err != nil {
		return 0, err
	}
	t.mu.Lock()
	localCheckpoint := t.checkpoint != nil && t.checkpoint.CoveredThrough == coveredThrough
	t.mu.Unlock()
	if localCheckpoint {
		return 0, nil
	}
	if checkpoint, err := t.findCheckpoint(coveredThrough); err != nil {
		return 0, err
	} else if checkpoint != nil {
		record, err := t.ensureStreamRecord()
		if err != nil || checkpoint.Incarnation != record.Incarnation {
			return 0, fmt.Errorf("S3 tier checkpoint conflicts with stream record")
		}
		buf, err := json.Marshal(checkpoint)
		if err != nil {
			return 0, err
		}
		if err := writeAtomically(t.fs.dios, t.checkpointFile(), buf, defaultFilePerms, true); err != nil {
			return 0, err
		}
		t.mu.Lock()
		t.checkpoint = checkpoint
		t.mu.Unlock()
		return 0, nil
	}
	var drained uint64
	var pending []*msgBlock
	for _, mb := range candidates {
		if !t.hasDescriptor(mb.index) {
			pending = append(pending, mb)
		}
	}
	for len(pending) > 0 {
		if fs.fcfg.srv != nil && fs.fcfg.srv.shutdown.Load() {
			return drained, ErrStoreClosed
		}
		n := min(s3TierUploadBatchSize, len(pending))
		if _, err := t.coverBlocks(pending[:n]); err != nil {
			return drained, err
		}
		drained += uint64(n)
		pending = pending[n:]
	}
	// An empty stream still needs a committed root. Nonempty checkpoints pin
	// the existing immutable root rather than rewriting its descriptor history.
	if err := t.commitManifest(nil); err != nil {
		return drained, err
	}
	if err := t.commitCheckpoint(coveredThrough); err != nil {
		return drained, err
	}
	return drained, nil
}

// ensureLocalCapacity rejects writes promptly while background offload makes
// room. Waiting for runMu here would put unbounded batches of remote I/O on the
// publish path, leaving clients unsure whether a timed-out publish was stored.
// This transient error must not poison fs.werr: retries can succeed after drain.
func (t *fileS3Tier) ensureLocalCapacity() error {
	if t.residentSize() <= t.cfg.LocalHighBytes {
		return nil
	}
	t.kick()
	// Allow a short background reclamation window for small local budgets,
	// without waiting for an upload batch or acquiring its serialization lock.
	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if t.residentSize() <= t.cfg.LocalHighBytes {
				return nil
			}
		case <-deadline.C:
			t.stats.capacityErrors.Add(1)
			return errS3TierLocalCapacity
		}
	}
}

func (t *fileS3Tier) residentSize() uint64 {
	t.residentMu.Lock()
	defer t.residentMu.Unlock()
	return t.residentBytes
}

func (t *fileS3Tier) setResidentLocked(index uint32, size uint64) {
	t.residentBytes -= t.resident[index]
	t.residentBytes = addSaturate(t.residentBytes, size)
	if size == 0 {
		delete(t.resident, index)
	} else {
		t.resident[index] = size
	}
}

// Called under the block lock before appending. Keep a conservative charge even
// if writing fails: a partial write must not grant admission to another writer.
func (t *fileS3Tier) accountResidentAppend(index uint32, size uint64) {
	t.residentMu.Lock()
	t.setResidentLocked(index, max(t.resident[index], size))
	t.residentMu.Unlock()
}

func (t *fileS3Tier) localBytes() uint64 {
	fs := t.fs
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var used uint64
	for _, mb := range fs.blks {
		if info, err := os.Stat(mb.mfn); err == nil {
			used += uint64(info.Size())
		}
	}
	return used
}

func (t *fileS3Tier) statsSnapshot() S3TierStats {
	t.mu.Lock()
	var remoteBytes uint64
	for _, d := range t.desc {
		remoteBytes += uint64(d.Size)
	}
	remoteBlocks := uint64(len(t.desc))
	var checkpointSeq uint64
	if t.checkpoint != nil {
		checkpointSeq = t.checkpoint.CoveredThrough
	}
	gcPending := uint64(len(t.gc))
	t.mu.Unlock()
	ioStats := make(map[string]S3TierIOStats, 4)
	for i, name := range []string{"payload", "descriptor", "manifest", "lifecycle"} {
		c := &t.stats.immutableIO[i]
		ioStats[name] = S3TierIOStats{PutCalls: c.puts.Load(), PutBytes: c.putBytes.Load(), PutSeconds: float64(c.putNanos.Load()) / float64(time.Second), VerifyCalls: c.gets.Load(), VerifyBytes: c.getBytes.Load(), VerifySeconds: float64(c.getNanos.Load()) / float64(time.Second), Errors: c.errors.Load(), SlowDown: c.slowDown.Load()}
	}
	backlogBytes, backlogBlocks := t.backlog()
	return S3TierStats{
		ImmutableIO: ioStats,
		LocalBytes:  t.localBytes(), RemoteBytes: remoteBytes, RemoteBlocks: remoteBlocks,
		Fetches: t.stats.fetches.Load(), FetchErrors: t.stats.fetchErrors.Load(), CacheHits: t.stats.cacheHits.Load(),
		Uploads: t.stats.uploads.Load(), UploadErrors: t.stats.uploadErrors.Load(), Evictions: t.stats.evictions.Load(),
		CapacityErrors: t.stats.capacityErrors.Load(), HighWatermark: t.cfg.LocalHighBytes, LowWatermark: t.cfg.LocalLowBytes,
		RemoteHigh: t.cfg.RemoteHighBytes, RemoteLow: t.cfg.RemoteLowBytes, CheckpointSeq: checkpointSeq,
		BacklogBytes: backlogBytes, BacklogBlocks: backlogBlocks, RetryAttempts: t.stats.retryAttempts.Load(),
		GCPending: gcPending, GCDeletes: t.stats.gcDeletes.Load(), GCErrors: t.stats.gcErrors.Load(),
		RestorePrepares: t.stats.restorePrepares.Load(), RestoreCheckpoints: t.stats.restoreCheckpoints.Load(),
		RestoreAborts: t.stats.restoreAborts.Load(), RestoreActivations: t.stats.restoreActivations.Load(),
		RestoreRetirements: t.stats.restoreRetirements.Load(), RestoreErrors: t.stats.restoreErrors.Load(),
	}
}

// backlog is the sealed local payload that has not reached the remote tier.
// It is physical transfer debt, separate from the stream's logical retention.
func (t *fileS3Tier) backlog() (uint64, uint64) {
	fs := t.fs
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	var bytes, blocks uint64
	for _, mb := range fs.blks {
		if mb == fs.lmb {
			continue
		}
		if _, covered := t.desc[mb.index]; covered {
			continue
		}
		if info, err := os.Stat(mb.mfn); err == nil {
			bytes += uint64(info.Size())
			blocks++
		}
	}
	return bytes, blocks
}

func (t *fileS3Tier) acquireUploadSlot() (func(), error) {
	ctx, cancel := t.ctx()
	defer cancel()
	select {
	case s3TierUploads <- struct{}{}:
		return func() { <-s3TierUploads }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("S3 tier upload capacity: %w", ctx.Err())
	}
}

// coverBlocks overlaps immutable payload and descriptor I/O, at most four
// blocks per batch. One immutable root commits the ordered descriptors
// before local coverage is installed. Journal speculative payloads before remote writes
// so a partial batch can be retried or garbage-collected after a crash.
// Callers hold runMu and pass distinct, sealed blocks.
func (t *fileS3Tier) coverBlocks(blocks []*msgBlock) (sizes []uint64, resultErr error) {
	if len(blocks) > s3TierUploadBatchSize {
		return nil, errors.New("S3 tier upload batch exceeds four blocks")
	}
	t.mu.Lock()
	backoff, retryErr := time.Now().Before(t.budgetRetryAt), t.budgetRetryErr
	t.mu.Unlock()
	if backoff {
		return nil, retryErr
	}
	defer func() { t.noteCoverageResult(resultErr) }()
	if _, err := t.ensureStreamRecord(); err != nil {
		t.stats.uploadErrors.Add(1)
		return nil, err
	}
	hashes := make([]string, len(blocks))
	errs := make([]error, len(blocks))
	var workers sync.WaitGroup
	for i, mb := range blocks {
		workers.Add(1)
		go func(i int, mb *msgBlock) {
			defer workers.Done()
			release, err := t.acquireUploadSlot()
			if err != nil {
				errs[i] = err
				return
			}
			defer release()
			fs := t.fs
			fs.mu.RLock()
			mb.mu.RLock()
			if fs.closing || fs.lmb == mb || mb.pendingWriteSizeLocked() != 0 {
				mb.mu.RUnlock()
				fs.mu.RUnlock()
				return
			}
			data, err := os.ReadFile(mb.mfn)
			mb.mu.RUnlock()
			fs.mu.RUnlock()
			if err != nil {
				errs[i] = err
				return
			}
			hash := checksumS3Block(data)
			key := fmt.Sprintf("%s/blocks/%010d-%s.blk", strings.Trim(t.cfg.Prefix, "/"), mb.index, hash)
			if err = t.recordGCCandidate(key); err != nil {
				errs[i] = err
				return
			}
			t.stats.uploads.Add(1)
			if err = t.putImmutable(key, data); err != nil {
				t.stats.uploadErrors.Add(1)
				errs[i] = err
				return
			}
			hashes[i] = hash
		}(i, mb)
	}
	workers.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	// Stage descriptors for one atomic bounded index update. No staged payload
	// is reclaimed, and no remote coverage is advertised before the root commits.
	var batch []s3TierCoverage
	t.coverageBatch = &batch
	defer func() { t.coverageBatch = nil }()
	sizes = make([]uint64, len(blocks))
	for i, mb := range blocks {
		var err error
		sizes[i], err = t.evictBlock(mb, false, hashes[i])
		if err != nil {
			return sizes, err
		}
	}
	t.coverageBatch = nil
	// Each descriptor is independent. Retain the global upload bound, and
	// verify every descriptor before any page/root can advertise the batch.
	errs = make([]error, len(batch))
	for i, item := range batch {
		workers.Add(1)
		go func(i int, d s3BlockDescriptor) {
			defer workers.Done()
			release, err := t.acquireUploadSlot()
			if err != nil {
				errs[i] = err
				return
			}
			defer release()
			buf, err := json.Marshal(d)
			if err == nil {
				err = t.putImmutable(t.descriptorKey(d.Index), buf)
			}
			if err != nil {
				t.stats.uploadErrors.Add(1)
				errs[i] = err
			}
		}(i, item.descriptor)
	}
	workers.Wait()
	if err := errors.Join(errs...); err != nil {
		return sizes, err
	}
	added := make([]s3BlockDescriptor, len(batch))
	for i, item := range batch {
		added[i] = item.descriptor
	}
	if len(added) > 0 {
		if err := t.commitManifestBatch(added); err != nil {
			return sizes, err
		}
		for _, item := range batch {
			if err := t.recordRemoteBlock(item.block, item.data, item.descriptor); err != nil {
				return sizes, err
			}
			t.mu.Lock()
			err := t.removeGCCandidateLocked(item.descriptor.Key)
			t.mu.Unlock()
			if err != nil {
				return sizes, err
			}
		}
	}
	return sizes, nil
}

// Background age batches and direct pressure work share one retry gate;
// publisher wakeups must not turn an object-store outage into a busy retry loop.
func (t *fileS3Tier) noteCoverageResult(err error) {
	if t.fs.dios.budget.Load() == nil && t.cfg.RemoteFlushMaxAge == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err == nil {
		t.budgetRetryAt, t.budgetRetryDelay, t.budgetRetryErr = time.Time{}, 0, nil
		return
	}
	if t.budgetRetryDelay == 0 {
		t.budgetRetryDelay = t.cfg.RetryMin
	} else if t.budgetRetryDelay < t.cfg.RetryMax/2 {
		t.budgetRetryDelay *= 2
	} else {
		t.budgetRetryDelay = t.cfg.RetryMax
	}
	t.budgetRetryAt = time.Now().Add(t.budgetRetryDelay)
	t.budgetRetryErr = err
}

// evictBlock uploads a sealed block when needed. reclaim determines whether
// the local copy is removed after remote coverage is durably recorded.
func (t *fileS3Tier) evictBlock(mb *msgBlock, reclaim bool, verified ...string) (size uint64, resultErr error) {
	fs := t.fs
	t.mu.Lock()
	_, alreadyRemote := t.desc[mb.index]
	t.mu.Unlock()
	if !alreadyRemote {
		release, err := t.acquireUploadSlot()
		if err != nil {
			return 0, err
		}
		defer release()
	}
	fs.mu.RLock()
	mb.mu.RLock()
	if fs.closing || fs.lmb == mb || mb.pendingWriteSizeLocked() != 0 {
		mb.mu.RUnlock()
		fs.mu.RUnlock()
		return 0, nil
	}
	data, err := os.ReadFile(mb.mfn)
	d := s3BlockDescriptor{Version: 2, Index: mb.index, FirstSeq: mb.first.seq, LastSeq: mb.last.seq, Size: len(data)}
	mb.mu.RUnlock()
	fs.mu.RUnlock()
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if alreadyRemote {
		if reclaim {
			return t.removeLocalBlock(mb, data, nil)
		}
		return uint64(len(data)), nil
	}
	// Aggregate pressure wakes more often than the upload retry interval. All
	// coverage callers share this gate so pressure and publisher kicks cannot
	// bypass the configured outage backoff. Covered local eviction remains free
	// to proceed without making a remote request.
	if fs.dios.budget.Load() != nil || t.cfg.RemoteFlushMaxAge > 0 {
		t.mu.Lock()
		if time.Now().Before(t.budgetRetryAt) {
			err := t.budgetRetryErr
			t.mu.Unlock()
			return 0, err
		}
		t.mu.Unlock()
		defer func() { t.noteCoverageResult(resultErr) }()
	}
	record, err := t.ensureStreamRecord()
	if err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	d.Incarnation = record.Incarnation
	d.SHA256 = checksumS3Block(data)
	d.Key = fmt.Sprintf("%s/blocks/%010d-%s.blk", strings.Trim(t.cfg.Prefix, "/"), d.Index, d.SHA256)
	// A batch may have already verified this exact content-addressed payload.
	// Re-read and hash the local block before trusting that result; metadata
	// publication and local reclamation retain their existing ordering.
	if len(verified) == 0 || verified[0] != d.SHA256 {
		t.stats.uploads.Add(1)
		if err := t.putImmutable(d.Key, data); err != nil {
			t.stats.uploadErrors.Add(1)
			return 0, err
		}
	}
	if t.coverageBatch != nil {
		*t.coverageBatch = append(*t.coverageBatch, s3TierCoverage{block: mb, data: data, descriptor: d})
		return uint64(len(data)), nil
	}
	buf, err := json.Marshal(d)
	if err != nil {
		return 0, err
	}
	if err := t.putImmutable(t.descriptorKey(d.Index), buf); err != nil {
		t.stats.uploadErrors.Add(1)
		if journalErr := t.recordGCCandidate(d.Key); journalErr != nil {
			return 0, fmt.Errorf("S3 tier descriptor write failed: %v; recording garbage candidate: %w", err, journalErr)
		}
		return 0, err
	}
	if err := t.commitManifest(&d); err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	if err := writeAtomically(fs.dios, t.descriptorFile(d.Index), buf, defaultFilePerms, true); err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	if reclaim {
		return t.removeLocalBlock(mb, data, &d)
	}
	if err := t.recordRemoteBlock(mb, data, d); err != nil {
		return 0, err
	}
	t.mu.Lock()
	err = t.removeGCCandidateLocked(d.Key)
	t.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return uint64(len(data)), nil
}

func (t *fileS3Tier) recordRemoteBlock(mb *msgBlock, expected []byte, d s3BlockDescriptor) error {
	fs := t.fs
	fs.mu.Lock()
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer fs.mu.Unlock()
	if fs.closing || fs.lmb == mb || mb.pendingWriteSizeLocked() != 0 {
		return nil
	}
	current, err := os.ReadFile(mb.mfn)
	if err != nil || !bytes.Equal(current, expected) {
		return fmt.Errorf("S3 tier block %d changed during upload: %w", mb.index, err)
	}
	t.mu.Lock()
	t.desc[d.Index] = d
	t.mu.Unlock()
	return nil
}

func (t *fileS3Tier) removeLocalBlock(mb *msgBlock, expected []byte, descriptor *s3BlockDescriptor) (uint64, error) {
	fs := t.fs
	fs.mu.Lock()
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer fs.mu.Unlock()
	if fs.closing || fs.lmb == mb {
		return 0, nil
	}
	current, err := os.ReadFile(mb.mfn)
	if err != nil || !bytes.Equal(current, expected) {
		return 0, fmt.Errorf("S3 tier block %d changed during upload: %w", mb.index, err)
	}
	if descriptor != nil {
		t.mu.Lock()
		t.desc[descriptor.Index] = *descriptor
		t.mu.Unlock()
	}
	t.mu.Lock()
	_, committed := t.desc[mb.index]
	t.mu.Unlock()
	if !committed {
		return 0, fmt.Errorf("S3 tier block %d has no durable descriptor", mb.index)
	}
	mb.closeFDsLockedNoCheck()
	t.residentMu.Lock()
	if err := os.Remove(mb.mfn); err != nil {
		t.residentMu.Unlock()
		return 0, err
	}
	// Only successful unlink releases residency; remote coverage and durable
	// descriptor publication have already completed above.
	t.setResidentLocked(mb.index, 0)
	t.residentMu.Unlock()
	fs.dios.budget.Load().completed(mb.mfn)
	if canFsyncDirectories {
		if err := syncDir(filepath.Dir(mb.mfn)); err != nil {
			return 0, err
		}
	}
	t.stats.evictions.Add(1)
	return uint64(len(expected)), nil
}

var errS3TierMutation = errors.New("experimental S3 tier does not support deletion or rewrite")
var errS3TierUnavailable = errors.New("S3 tier storage unavailable")
var errS3TierLocalCapacity = errors.New("S3 tier local capacity exceeded")
var errS3TierNotEnabled = errors.New("S3 tier is not enabled for this stream")
var errS3TierClusteredDrain = errors.New("S3 tier remote drain does not support clustered streams")
var errS3TierRemoteRestoreFenced = errors.New("S3 tier stream is fenced for remote restore")

// errS3TierBlockNotHydrated crosses the local read path without doing network
// I/O under file-store or block locks. Its caller hydrates precisely this block
// and retries the normal read path.
type errS3TierBlockNotHydrated struct{ index uint32 }

func (e *errS3TierBlockNotHydrated) Error() string {
	return fmt.Sprintf("S3 tier block %d is not hydrated", e.index)
}

func (fs *fileStore) hydrateS3TierMiss(err error) (bool, error) {
	var missing *errS3TierBlockNotHydrated
	if !errors.As(err, &missing) {
		return false, nil
	}
	if fs.tier == nil {
		return false, err
	}
	return true, fs.tier.ensureHydrated(missing.index)
}

// reclaimDiskBudgetBlock performs at most one block of work per stream/pass,
// independently of its normal residency and remote-coverage watermarks.
func (t *fileS3Tier) reclaimDiskBudgetBlock() error {
	t.runMu.Lock()
	defer t.runMu.Unlock()
	fs := t.fs
	fs.mu.Lock()
	if fs.closing || fs.isClosed() {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	var candidate *msgBlock
	t.mu.Lock()
	for _, mb := range fs.blks {
		if mb != fs.lmb {
			// Remote-only blocks cannot release local capacity. Skip them so
			// repeated pressure passes continue toward the resident tail.
			if _, err := os.Stat(mb.mfn); os.IsNotExist(err) {
				continue
			} else if err != nil {
				t.mu.Unlock()
				fs.mu.Unlock()
				return err
			}
			if candidate == nil {
				candidate = mb
			}
			if _, covered := t.desc[mb.index]; covered {
				candidate = mb
				break
			}
		}
	}
	t.mu.Unlock()
	if candidate == nil && fs.lmb != nil {
		fs.lmb.mu.RLock()
		nonempty := fs.lmb.msgs > 0
		fs.lmb.mu.RUnlock()
		if nonempty {
			candidate = fs.lmb
			if _, err := fs.newMsgBlockForWrite(); err != nil {
				fs.mu.Unlock()
				return err
			}
			fs.dirty++
		}
	}
	fs.mu.Unlock()
	if candidate == nil {
		return nil
	}
	_, err := t.evictBlock(candidate, true)
	return err
}

// Called with fs.mu held: concurrent publishers cannot both spend the same
// per-stream residency headroom. Reclamation remains asynchronous.
func (t *fileS3Tier) ensureProjectedLocalCapacity(additional uint64) error {
	b := t.fs.dios.budget.Load()
	if b == nil {
		// The waiting precheck runs outside fs.mu. Recheck current occupancy
		// here so concurrent publishers cannot all spend the same headroom.
		// Preserve the legacy one-record watermark overshoot; never wait while
		// holding the stream lock that reclamation needs.
		if t.residentSize() > t.cfg.LocalHighBytes {
			t.kick()
			t.stats.capacityErrors.Add(1)
			return errS3TierLocalCapacity
		}
		return nil
	}
	b.mu.Lock()
	var used uint64
	for _, mb := range t.fs.blks {
		e := b.files[mb.mfn]
		used = addSaturate(used, max(e.actual, e.planned))
	}
	b.mu.Unlock()
	additional = diskBudgetRound(additional)
	if used > t.cfg.LocalHighBytes || additional > t.cfg.LocalHighBytes-used {
		t.localPressure.Store(true)
		t.kick()
		t.stats.capacityErrors.Add(1)
		return errS3TierLocalCapacity
	}
	return nil
}

// flushAgedBlocks grants remote coverage without changing local residency.
// Message timestamps survive restart, so reopening cannot reset an old tail's age.
func (t *fileS3Tier) flushAgedBlocks() error {
	if t.cfg.RemoteFlushMaxAge == 0 {
		return nil
	}
	t.runMu.Lock()
	defer t.runMu.Unlock()
	fs := t.fs
	fs.mu.Lock()
	if fs.closing || fs.isClosed() {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	// Snapshot once: a long outage backlog must not rescan all historical
	// descriptors after every successful block upload.
	var candidates []*msgBlock
	cutoff := time.Now().Add(-t.cfg.RemoteFlushMaxAge).UnixNano()
	t.mu.Lock()
	for _, mb := range fs.blks {
		if _, covered := t.desc[mb.index]; covered {
			continue
		}
		mb.mu.RLock()
		due := mb.msgs > 0 && mb.first.ts > 0 && mb.first.ts <= cutoff
		mb.mu.RUnlock()
		if due {
			candidates = append(candidates, mb)
		}
	}
	t.mu.Unlock()
	if len(candidates) > 0 && candidates[len(candidates)-1] == fs.lmb {
		if _, err := fs.newMsgBlockForWrite(); err != nil {
			fs.mu.Unlock()
			return err
		}
		fs.dirty++
	}
	fs.mu.Unlock()
	reclaimAt := t.cfg.LocalHighBytes - min(t.cfg.LocalHighBytes-t.cfg.LocalLowBytes, uint64(fs.fcfg.BlockSize))
	for len(candidates) > 0 {
		// Age-based coverage is independent of residency. Yield as soon as
		// pressure arrives so already committed blocks can free headroom before
		// another remote write; otherwise one old backlog can fence publishers
		// for the entire flush even while safe local victims are available.
		if t.localPressure.Load() || t.residentSize() > reclaimAt {
			t.kick()
			return nil
		}
		n := min(s3TierUploadBatchSize, len(candidates))
		sizes, err := t.coverBlocks(candidates[:n])
		if err != nil {
			return err
		}
		for _, size := range sizes {
			if size == 0 {
				return nil
			}
		}
		candidates = candidates[n:]
	}
	return nil
}
