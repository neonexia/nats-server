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
	RemoteHighBytes uint64
	RemoteLowBytes  uint64
	Timeout         time.Duration
	RetryMin        time.Duration
	RetryMax        time.Duration
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
)

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

	BlockSize       uint64
	LocalHighBytes  uint64
	LocalLowBytes   uint64
	RemoteHighBytes uint64
	RemoteLowBytes  uint64
	Timeout         time.Duration
	RetryMin        time.Duration
	RetryMax        time.Duration

	store S3TierObjectStore
}

// S3TierStats reports physical tier activity separately from the stream's
// logical retention counters. Values are exposed in the JetStream monitor view.
type S3TierStats struct {
	LocalBytes     uint64 `json:"local_bytes"`
	RemoteBytes    uint64 `json:"remote_bytes"`
	RemoteBlocks   uint64 `json:"remote_blocks"`
	Fetches        uint64 `json:"fetches"`
	FetchErrors    uint64 `json:"fetch_errors"`
	CacheHits      uint64 `json:"cache_hits"`
	Uploads        uint64 `json:"uploads"`
	UploadErrors   uint64 `json:"upload_errors"`
	Evictions      uint64 `json:"evictions"`
	CapacityErrors uint64 `json:"capacity_errors"`
	HighWatermark  uint64 `json:"high_watermark"`
	LowWatermark   uint64 `json:"low_watermark"`
	RemoteHigh     uint64 `json:"remote_high_watermark"`
	RemoteLow      uint64 `json:"remote_low_watermark"`
	CheckpointSeq  uint64 `json:"checkpoint_sequence"`
	BacklogBytes   uint64 `json:"backlog_bytes"`
	BacklogBlocks  uint64 `json:"backlog_blocks"`
	RetryAttempts  uint64 `json:"retry_attempts"`
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

func newS3TierServerConfig() *S3TierServerConfig {
	return &S3TierServerConfig{
		BlockSize:       defaultS3TierBlockSize,
		LocalHighBytes:  defaultS3TierLocalHigh,
		LocalLowBytes:   defaultS3TierLocalLow,
		RemoteHighBytes: defaultS3TierRemoteHigh,
		RemoteLowBytes:  defaultS3TierRemoteLow,
		Timeout:         defaultS3TierTimeout,
		RetryMin:        defaultS3TierRetryMin,
		RetryMax:        defaultS3TierRetryMax,
	}
}

func (c *S3TierServerConfig) resolveStore() error {
	if c == nil || c.Endpoint == "" || c.Bucket == "" || strings.Trim(c.Prefix, "/") == "" {
		return errors.New("S3 tier requires endpoint, bucket, and opaque prefix")
	}
	if c.CredentialProvider != "env" && c.CredentialProvider != "environment" {
		return fmt.Errorf("S3 tier credential provider %q is not supported", c.CredentialProvider)
	}
	if c.AccessKeyEnv == "" || c.SecretKeyEnv == "" {
		return errors.New("S3 tier environment credential provider requires access_key_env and secret_key_env")
	}
	accessKey, ok := os.LookupEnv(c.AccessKeyEnv)
	if !ok || accessKey == "" {
		return fmt.Errorf("S3 tier access key environment variable %q is not set", c.AccessKeyEnv)
	}
	secretKey, ok := os.LookupEnv(c.SecretKeyEnv)
	if !ok || secretKey == "" {
		return fmt.Errorf("S3 tier secret key environment variable %q is not set", c.SecretKeyEnv)
	}
	var sessionToken string
	if c.SessionTokenEnv != "" {
		sessionToken = os.Getenv(c.SessionTokenEnv)
	}
	store, err := newS3TierMinIOStore(c.Endpoint, accessKey, secretKey, sessionToken, c.Region, c.Bucket, c.TLS)
	if err != nil {
		return err
	}
	c.store = store
	return nil
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
		RemoteHighBytes: c.RemoteHighBytes, RemoteLowBytes: c.RemoteLowBytes,
		Timeout: c.Timeout, RetryMin: c.RetryMin, RetryMax: c.RetryMax,
	}
}

func (c *S3TierServerConfig) equal(other *S3TierServerConfig) bool {
	if c == nil || other == nil {
		return c == other
	}
	return c.Endpoint == other.Endpoint && c.Bucket == other.Bucket && c.Region == other.Region && c.TLS == other.TLS && c.Prefix == other.Prefix &&
		c.CredentialProvider == other.CredentialProvider && c.AccessKeyEnv == other.AccessKeyEnv && c.SecretKeyEnv == other.SecretKeyEnv && c.SessionTokenEnv == other.SessionTokenEnv &&
		c.BlockSize == other.BlockSize && c.LocalHighBytes == other.LocalHighBytes && c.LocalLowBytes == other.LocalLowBytes && c.RemoteHighBytes == other.RemoteHighBytes && c.RemoteLowBytes == other.RemoteLowBytes &&
		c.Timeout == other.Timeout && c.RetryMin == other.RetryMin && c.RetryMax == other.RetryMax
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
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("S3 tier endpoint and bucket are required")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(accessKey, secretKey, sessionToken), Secure: secure, Region: region,
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

// s3TierManifest is an immutable snapshot of committed block descriptors.
// Readers can discover a stream's remote index without inspecting payloads;
// each new generation receives a new object key so a partial update can never
// replace an earlier complete view.
type s3TierManifest struct {
	Version     uint8               `json:"version"`
	Generation  uint64              `json:"generation"`
	Prefix      string              `json:"prefix"`
	Incarnation string              `json:"incarnation,omitempty"`
	Blocks      []s3BlockDescriptor `json:"blocks"`
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

type fileS3Tier struct {
	fs             *fileStore
	cfg            S3TierConfig
	mu             sync.Mutex // Protects descriptors and in-flight fetches.
	runMu          sync.Mutex // One eviction pass at a time.
	desc           map[uint32]s3BlockDescriptor
	fetch          map[uint32]*s3TierFetch
	gen            uint64
	manifestSHA256 string
	record         *s3TierStreamRecord
	checkpoint     *s3TierCheckpoint
	stats          s3TierCounters
	wake           chan struct{}
	quit           chan struct{}
	once           sync.Once
}

type s3TierCounters struct {
	fetches, fetchErrors, cacheHits, uploads, uploadErrors, evictions, capacityErrors, retryAttempts atomic.Uint64
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
	if cfg.RemoteHighBytes == 0 {
		cfg.RemoteHighBytes, cfg.RemoteLowBytes = cfg.LocalHighBytes, cfg.LocalLowBytes
	}
	return &fileS3Tier{fs: fs, cfg: cfg, desc: make(map[uint32]s3BlockDescriptor), fetch: make(map[uint32]*s3TierFetch), wake: make(chan struct{}, 1), quit: make(chan struct{})}
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
		t.RetryMin <= 0 || t.RetryMax < t.RetryMin {
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
	return context.WithTimeout(context.Background(), t.cfg.Timeout)
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
		if err := json.Unmarshal(buf, &manifest); err != nil || (manifest.Version != 1 && manifest.Version != 2) || manifest.Generation == 0 || manifest.Prefix != strings.Trim(t.cfg.Prefix, "/") {
			return fmt.Errorf("invalid local S3 tier manifest: %v", err)
		}
		t.gen = manifest.Generation
		t.manifestSHA256 = checksumS3Block(buf)
		manifestIncarnation = manifest.Incarnation
	} else if !os.IsNotExist(err) {
		return err
	}
	if buf, err := os.ReadFile(t.streamRecordFile()); err == nil {
		var record s3TierStreamRecord
		if err := json.Unmarshal(buf, &record); err != nil || !t.validStreamRecord(&record) {
			return fmt.Errorf("invalid local S3 tier stream record: %v", err)
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
		t.desc[d.Index] = d
		if local, err := os.ReadFile(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, d.Index))); err == nil {
			if len(local) != d.Size || checksumS3Block(local) != d.SHA256 {
				return fmt.Errorf("S3 tier block %d local checksum mismatch during recovery", d.Index)
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	go t.loop()
	return nil
}

func validS3BlockDescriptor(d s3BlockDescriptor) bool {
	return (d.Version == 1 || d.Version == 2) &&
		d.Index != 0 && d.FirstSeq != 0 && d.LastSeq >= d.FirstSeq && d.Size > 0 &&
		d.SHA256 != "" && d.Key != "" && (d.Version == 1 || d.Incarnation != "")
}

func (t *fileS3Tier) configSHA256() (string, error) {
	buf, err := json.Marshal(t.fs.cfg.StreamConfig)
	if err != nil {
		return "", err
	}
	return checksumS3Block(buf), nil
}

func (t *fileS3Tier) validStreamRecord(record *s3TierStreamRecord) bool {
	if record == nil || record.Version != 1 || record.Incarnation == "" || record.Prefix != strings.Trim(t.cfg.Prefix, "/") || record.StreamName != t.fs.cfg.Name {
		return false
	}
	digest, err := t.configSHA256()
	return err == nil && record.ConfigSHA256 == digest
}

func (t *fileS3Tier) validCheckpoint(checkpoint *s3TierCheckpoint) bool {
	if checkpoint == nil || checkpoint.Version != 1 || checkpoint.Incarnation == "" || checkpoint.ManifestGeneration == 0 || checkpoint.ManifestKey == "" || checkpoint.ManifestSHA256 == "" || checkpoint.CoveredThrough == 0 {
		return false
	}
	digest, err := t.configSHA256()
	return err == nil && checkpoint.ConfigSHA256 == digest &&
		checkpoint.ManifestKey == t.manifestKey(checkpoint.ManifestGeneration)
}

func (t *fileS3Tier) putImmutable(key string, data []byte) error {
	ctx, cancel := t.ctx()
	_, err := t.cfg.Store.PutIfAbsent(ctx, key, data)
	cancel()
	if err != nil {
		return err
	}
	ctx, cancel = t.ctx()
	committed, err := t.cfg.Store.Get(ctx, key)
	cancel()
	if err != nil || !bytes.Equal(committed, data) {
		return fmt.Errorf("S3 tier immutable object verification failed for %q: %w", key, err)
	}
	return nil
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
	record, err := t.ensureStreamRecord()
	if err != nil {
		return err
	}
	t.mu.Lock()
	blocks := make([]s3BlockDescriptor, 0, len(t.desc)+1)
	for _, current := range t.desc {
		blocks = append(blocks, current)
	}
	if add != nil {
		blocks = append(blocks, *add)
	}
	generation := t.gen + 1
	t.mu.Unlock()
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Index < blocks[j].Index })
	manifest := s3TierManifest{Version: 2, Generation: generation, Prefix: strings.Trim(t.cfg.Prefix, "/"), Incarnation: record.Incarnation, Blocks: blocks}
	buf, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	key := t.manifestKey(generation)
	if err := t.putImmutable(key, buf); err != nil {
		return fmt.Errorf("S3 tier manifest verification failed for generation %d: %w", generation, err)
	}
	if err := writeAtomically(t.fs.dios, t.manifestFile(), buf, defaultFilePerms, true); err != nil {
		return err
	}
	t.mu.Lock()
	t.gen = generation
	t.manifestSHA256 = checksumS3Block(buf)
	t.mu.Unlock()
	return nil
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
	var (
		delay  time.Duration
		timer  *time.Timer
		retryC <-chan time.Time
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-t.wake:
			if timer != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			retryC = nil
		case <-retryC:
			retryC = nil
		case <-t.quit:
			return
		}
		if err := t.evictToBudget(); err != nil {
			t.fs.warn("S3 tier eviction failed: %v", err)
			t.stats.retryAttempts.Add(1)
			if delay == 0 {
				delay = t.cfg.RetryMin
			} else if delay < t.cfg.RetryMax/2 {
				delay *= 2
			} else {
				delay = t.cfg.RetryMax
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

func (t *fileS3Tier) stop() { t.once.Do(func() { close(t.quit) }) }

func checksumS3Block(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func (t *fileS3Tier) ensureHydrated(index uint32) error {
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
	ctx, cancel := t.ctx()
	data, err := t.cfg.Store.Get(ctx, d.Key)
	cancel()
	if err != nil {
		return fmt.Errorf("%w: block %d unavailable: %v", errS3TierUnavailable, d.Index, err)
	}
	if len(data) != d.Size || checksumS3Block(data) != d.SHA256 {
		return fmt.Errorf("%w: block %d remote checksum mismatch", errS3TierUnavailable, d.Index)
	}
	if err := writeAtomically(t.fs.dios, path, data, defaultFilePerms, true); err != nil {
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
	fs := t.fs
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
	if uncovered > t.cfg.RemoteHighBytes {
		for _, candidate := range candidates {
			if uncovered <= t.cfg.RemoteLowBytes {
				break
			}
			if candidate.remote {
				continue
			}
			size, err := t.evictBlock(candidate.mb, false)
			if err != nil {
				return err
			}
			if size > uncovered {
				uncovered = 0
			} else {
				uncovered -= size
			}
		}
	}
	if used <= t.cfg.LocalHighBytes {
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
	for _, candidate := range candidates {
		if used <= t.cfg.LocalLowBytes {
			break
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
	return nil
}

// drain seals the active block before it is called, then ensures every local
// sealed block has a verified remote descriptor. It intentionally preserves
// local payloads; lifecycle code decides whether to reclaim them later.
// A checkpoint is only committed after this exact coverage boundary exists.
func (t *fileS3Tier) drain(coveredThrough uint64) (uint64, error) {
	t.runMu.Lock()
	defer t.runMu.Unlock()
	fs := t.fs
	fs.mu.RLock()
	candidates := make([]*msgBlock, 0, len(fs.blks))
	for _, mb := range fs.blks {
		if mb != fs.lmb {
			candidates = append(candidates, mb)
		}
	}
	fs.mu.RUnlock()
	if len(candidates) == 0 || coveredThrough == 0 {
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
	for _, mb := range candidates {
		t.mu.Lock()
		_, covered := t.desc[mb.index]
		t.mu.Unlock()
		if covered {
			continue
		}
		if _, err := t.evictBlock(mb, false); err != nil {
			return drained, err
		}
		drained++
	}
	// Always write a fresh v2 manifest before a new checkpoint. It binds every
	// descriptor, including any pre-foundation descriptors, to this record.
	if err := t.commitManifest(nil); err != nil {
		return drained, err
	}
	if err := t.commitCheckpoint(coveredThrough); err != nil {
		return drained, err
	}
	return drained, nil
}

// ensureLocalCapacity is invoked before accepting another publish after a
// previous asynchronous eviction could not reclaim enough local space. It
// deliberately returns a transient error instead of poisoning fs.werr: S3 can
// recover and a later publish may then make progress.
func (t *fileS3Tier) ensureLocalCapacity() error {
	if t.localBytes() <= t.cfg.LocalHighBytes {
		return nil
	}
	if err := t.evictToBudget(); err != nil {
		t.stats.capacityErrors.Add(1)
		return fmt.Errorf("%w: %v", errS3TierLocalCapacity, err)
	}
	if t.localBytes() > t.cfg.LocalHighBytes {
		t.stats.capacityErrors.Add(1)
		return errS3TierLocalCapacity
	}
	return nil
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
	t.mu.Unlock()
	backlogBytes, backlogBlocks := t.backlog()
	return S3TierStats{
		LocalBytes: t.localBytes(), RemoteBytes: remoteBytes, RemoteBlocks: remoteBlocks,
		Fetches: t.stats.fetches.Load(), FetchErrors: t.stats.fetchErrors.Load(), CacheHits: t.stats.cacheHits.Load(),
		Uploads: t.stats.uploads.Load(), UploadErrors: t.stats.uploadErrors.Load(), Evictions: t.stats.evictions.Load(),
		CapacityErrors: t.stats.capacityErrors.Load(), HighWatermark: t.cfg.LocalHighBytes, LowWatermark: t.cfg.LocalLowBytes,
		RemoteHigh: t.cfg.RemoteHighBytes, RemoteLow: t.cfg.RemoteLowBytes, CheckpointSeq: checkpointSeq,
		BacklogBytes: backlogBytes, BacklogBlocks: backlogBlocks, RetryAttempts: t.stats.retryAttempts.Load(),
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

// evictBlock uploads a sealed block when needed. reclaim determines whether
// the local copy is removed after remote coverage is durably recorded.
func (t *fileS3Tier) evictBlock(mb *msgBlock, reclaim bool) (uint64, error) {
	fs := t.fs
	t.mu.Lock()
	_, alreadyRemote := t.desc[mb.index]
	t.mu.Unlock()
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
	record, err := t.ensureStreamRecord()
	if err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	d.Incarnation = record.Incarnation
	d.SHA256 = checksumS3Block(data)
	d.Key = fmt.Sprintf("%s/blocks/%010d-%s.blk", strings.Trim(t.cfg.Prefix, "/"), d.Index, d.SHA256)
	t.stats.uploads.Add(1)
	if err := t.putImmutable(d.Key, data); err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	buf, err := json.Marshal(d)
	if err != nil {
		return 0, err
	}
	if err := t.putImmutable(t.descriptorKey(d.Index), buf); err != nil {
		t.stats.uploadErrors.Add(1)
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
	if err := os.Remove(mb.mfn); err != nil {
		return 0, err
	}
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
