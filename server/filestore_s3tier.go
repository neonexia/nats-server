// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	Timeout        time.Duration
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
}

// S3TierObjectStore makes the block protocol testable with fault injection.
// Keys are relative to a bucket and must be treated as immutable once written.
type S3TierObjectStore interface {
	Put(context.Context, string, []byte) error
	Get(context.Context, string) ([]byte, error)
	List(context.Context, string) ([]string, error)
}

// S3TierMinIOStore works with AWS S3 and S3-compatible endpoints such as MinIO.
type S3TierMinIOStore struct {
	client *minio.Client
	bucket string
}

func NewS3TierMinIOStore(endpoint, accessKey, secretKey, bucket string, secure bool) (*S3TierMinIOStore, error) {
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("S3 tier endpoint and bucket are required")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure,
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
	Version  uint8  `json:"version"`
	Index    uint32 `json:"index"`
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
	Key      string `json:"key"`
}

// s3TierManifest is an immutable snapshot of committed block descriptors.
// Readers can discover a stream's remote index without inspecting payloads;
// each new generation receives a new object key so a partial update can never
// replace an earlier complete view.
type s3TierManifest struct {
	Version    uint8               `json:"version"`
	Generation uint64              `json:"generation"`
	Prefix     string              `json:"prefix"`
	Blocks     []s3BlockDescriptor `json:"blocks"`
}

type fileS3Tier struct {
	fs    *fileStore
	cfg   S3TierConfig
	mu    sync.Mutex // Protects descriptors and in-flight fetches.
	runMu sync.Mutex // One eviction pass at a time.
	desc  map[uint32]s3BlockDescriptor
	fetch map[uint32]*s3TierFetch
	gen   uint64
	stats s3TierCounters
	wake  chan struct{}
	quit  chan struct{}
	once  sync.Once
}

type s3TierCounters struct {
	fetches, fetchErrors, cacheHits, uploads, uploadErrors, evictions, capacityErrors atomic.Uint64
}

type s3TierFetch struct {
	done chan struct{}
	err  error
}

func newFileS3Tier(fs *fileStore, cfg S3TierConfig) *fileS3Tier {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &fileS3Tier{fs: fs, cfg: cfg, desc: make(map[uint32]s3BlockDescriptor), fetch: make(map[uint32]*s3TierFetch), wake: make(chan struct{}, 1), quit: make(chan struct{})}
}

func validateS3Tier(fcfg FileStoreConfig, cfg StreamConfig) error {
	if fcfg.S3Tier == nil {
		return nil
	}
	t := fcfg.S3Tier
	if t.Store == nil || strings.Trim(t.Prefix, "/") == "" || t.LocalLowBytes == 0 || t.LocalHighBytes <= t.LocalLowBytes {
		return fmt.Errorf("S3 tier requires store, unique prefix, and high > low > 0 byte watermarks")
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
		if err := json.Unmarshal(buf, &manifest); err != nil || manifest.Version != 1 || manifest.Generation == 0 || manifest.Prefix != strings.Trim(t.cfg.Prefix, "/") {
			return fmt.Errorf("invalid local S3 tier manifest: %v", err)
		}
		t.gen = manifest.Generation
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
	return d.Version == 1 && d.Index != 0 && d.FirstSeq != 0 && d.LastSeq >= d.FirstSeq && d.Size > 0 && d.SHA256 != "" && d.Key != ""
}

func (t *fileS3Tier) commitManifest(d s3BlockDescriptor) error {
	t.mu.Lock()
	blocks := make([]s3BlockDescriptor, 0, len(t.desc)+1)
	for _, current := range t.desc {
		blocks = append(blocks, current)
	}
	blocks = append(blocks, d)
	generation := t.gen + 1
	t.mu.Unlock()
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Index < blocks[j].Index })
	manifest := s3TierManifest{Version: 1, Generation: generation, Prefix: strings.Trim(t.cfg.Prefix, "/"), Blocks: blocks}
	buf, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	key := t.manifestKey(generation)
	ctx, cancel := t.ctx()
	err = t.cfg.Store.Put(ctx, key, buf)
	cancel()
	if err != nil {
		return err
	}
	ctx, cancel = t.ctx()
	committed, err := t.cfg.Store.Get(ctx, key)
	cancel()
	if err != nil || !bytes.Equal(committed, buf) {
		return fmt.Errorf("S3 tier manifest verification failed for generation %d: %w", generation, err)
	}
	if err := writeAtomically(t.fs.dios, t.manifestFile(), buf, defaultFilePerms, true); err != nil {
		return err
	}
	t.mu.Lock()
	t.gen = generation
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
	for {
		select {
		case <-t.wake:
			if err := t.evictToBudget(); err != nil {
				t.fs.warn("S3 tier eviction failed: %v", err)
			}
		case <-t.quit:
			return
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

// evictToBudget copies sealed blocks, verifies the remote bytes, commits the
// descriptor remotely and locally, then unlinks local payloads under block lock.
func (t *fileS3Tier) evictToBudget() error {
	t.runMu.Lock()
	defer t.runMu.Unlock()
	fs := t.fs
	fs.mu.RLock()
	var used uint64
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
			}
		}
	}
	fs.mu.RUnlock()
	if used <= t.cfg.LocalHighBytes {
		return nil
	}
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
		size, err := t.evictBlock(candidate.mb)
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
	t.mu.Unlock()
	return S3TierStats{
		LocalBytes: t.localBytes(), RemoteBytes: remoteBytes, RemoteBlocks: remoteBlocks,
		Fetches: t.stats.fetches.Load(), FetchErrors: t.stats.fetchErrors.Load(), CacheHits: t.stats.cacheHits.Load(),
		Uploads: t.stats.uploads.Load(), UploadErrors: t.stats.uploadErrors.Load(), Evictions: t.stats.evictions.Load(),
		CapacityErrors: t.stats.capacityErrors.Load(), HighWatermark: t.cfg.LocalHighBytes, LowWatermark: t.cfg.LocalLowBytes,
	}
}

func (t *fileS3Tier) evictBlock(mb *msgBlock) (uint64, error) {
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
	d := s3BlockDescriptor{Version: 1, Index: mb.index, FirstSeq: mb.first.seq, LastSeq: mb.last.seq, Size: len(data)}
	mb.mu.RUnlock()
	fs.mu.RUnlock()
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if alreadyRemote {
		return t.removeLocalBlock(mb, data, nil)
	}
	d.SHA256 = checksumS3Block(data)
	d.Key = fmt.Sprintf("%s/blocks/%010d-%s.blk", strings.Trim(t.cfg.Prefix, "/"), d.Index, d.SHA256)
	t.stats.uploads.Add(1)
	ctx, cancel := t.ctx()
	err = t.cfg.Store.Put(ctx, d.Key, data)
	cancel()
	if err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	ctx, cancel = t.ctx()
	verified, err := t.cfg.Store.Get(ctx, d.Key)
	cancel()
	if err != nil || !bytes.Equal(verified, data) {
		t.stats.uploadErrors.Add(1)
		return 0, fmt.Errorf("S3 tier upload verification failed for block %d: %w", d.Index, err)
	}
	buf, err := json.Marshal(d)
	if err != nil {
		return 0, err
	}
	ctx, cancel = t.ctx()
	err = t.cfg.Store.Put(ctx, t.descriptorKey(d.Index), buf)
	cancel()
	if err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	ctx, cancel = t.ctx()
	committed, err := t.cfg.Store.Get(ctx, t.descriptorKey(d.Index))
	cancel()
	if err != nil || !bytes.Equal(committed, buf) {
		t.stats.uploadErrors.Add(1)
		return 0, fmt.Errorf("S3 tier descriptor verification failed for block %d: %w", d.Index, err)
	}
	if err := t.commitManifest(d); err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	if err := writeAtomically(fs.dios, t.descriptorFile(d.Index), buf, defaultFilePerms, true); err != nil {
		t.stats.uploadErrors.Add(1)
		return 0, err
	}
	return t.removeLocalBlock(mb, data, &d)
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
