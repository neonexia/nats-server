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
	"strings"
	"sync"
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

type fileS3Tier struct {
	fs    *fileStore
	cfg   S3TierConfig
	mu    sync.Mutex // Serializes cache hydration and local payload unlink.
	runMu sync.Mutex // One eviction pass at a time.
	desc  map[uint32]s3BlockDescriptor
	wake  chan struct{}
	quit  chan struct{}
	once  sync.Once
}

func newFileS3Tier(fs *fileStore, cfg S3TierConfig) *fileS3Tier {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &fileS3Tier{fs: fs, cfg: cfg, desc: make(map[uint32]s3BlockDescriptor), wake: make(chan struct{}, 1), quit: make(chan struct{})}
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

// prepareS3Tier runs before the normal file-store recovery. Hydrating every
// referenced block is deliberately conservative: existing recovery requires
// local block bytes and must never infer that a remote-only range was deleted.
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
	ctx, cancel := t.ctx()
	keys, err := t.cfg.Store.List(ctx, strings.Trim(t.cfg.Prefix, "/")+"/descriptors/")
	cancel()
	if err != nil {
		return fmt.Errorf("S3 tier descriptor listing: %w", err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".json") {
			continue
		}
		ctx, cancel = t.ctx()
		buf, getErr := t.cfg.Store.Get(ctx, key)
		cancel()
		if getErr != nil {
			return fmt.Errorf("S3 tier descriptor %q: %w", key, getErr)
		}
		var d s3BlockDescriptor
		if err := json.Unmarshal(buf, &d); err != nil || d.Version != 1 || d.Index == 0 || d.Key == "" || key != t.descriptorKey(d.Index) {
			return fmt.Errorf("invalid S3 tier descriptor %q: %v", key, err)
		}
		t.desc[d.Index] = d
		if err := writeAtomically(fs.dios, t.descriptorFile(d.Index), buf, defaultFilePerms, true); err != nil {
			return err
		}
		if local, err := os.ReadFile(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, d.Index))); err == nil {
			if len(local) != d.Size || checksumS3Block(local) != d.SHA256 {
				return fmt.Errorf("S3 tier block %d local checksum mismatch during recovery", d.Index)
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := t.ensureHydrated(d.Index); err != nil {
			return err
		}
	}
	localDescriptors, err := filepath.Glob(filepath.Join(fs.fcfg.StoreDir, msgDir, "*.tier.json"))
	if err != nil {
		return err
	}
	for _, path := range localDescriptors {
		var index uint32
		if _, err := fmt.Sscanf(filepath.Base(path), "%d.tier.json", &index); err != nil {
			return fmt.Errorf("invalid local S3 tier descriptor %q", path)
		}
		if _, ok := t.desc[index]; !ok {
			return fmt.Errorf("S3 tier descriptor %d missing remotely", index)
		}
	}
	go t.loop()
	return nil
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
	defer t.mu.Unlock()
	d, ok := t.desc[index]
	if !ok {
		return nil
	}
	path := filepath.Join(t.fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, index))
	if info, err := os.Stat(path); err == nil {
		if info.Size() != int64(d.Size) {
			return fmt.Errorf("S3 tier block %d local size mismatch", index)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	ctx, cancel := t.ctx()
	data, err := t.cfg.Store.Get(ctx, d.Key)
	cancel()
	if err != nil {
		return fmt.Errorf("%w: block %d unavailable: %v", errS3TierUnavailable, index, err)
	}
	if len(data) != d.Size || checksumS3Block(data) != d.SHA256 {
		return fmt.Errorf("%w: block %d remote checksum mismatch", errS3TierUnavailable, index)
	}
	return writeAtomically(t.fs.dios, path, data, defaultFilePerms, true)
}

// prefetchS3Block performs remote I/O before LoadNextMsg takes the broad fs
// read lock. The all-block scan is an intentionally simple spike tradeoff.
func (fs *fileStore) prefetchS3Block(start uint64, throughTail bool) error {
	if fs.tier == nil {
		return nil
	}
	fs.mu.RLock()
	var indices []uint32
	for _, mb := range fs.blks {
		if mb.last.seq >= start && mb != fs.lmb {
			if _, err := os.Stat(mb.mfn); os.IsNotExist(err) {
				indices = append(indices, mb.index)
			}
			if !throughTail {
				break
			}
		}
	}
	fs.mu.RUnlock()
	for _, index := range indices {
		if err := fs.tier.ensureHydrated(index); err != nil {
			return err
		}
	}
	return nil
}

func (fs *fileStore) prefetchS3Last(subject string) error {
	if fs.tier == nil {
		return nil
	}
	if subject == _EMPTY_ || subject == fwcs {
		return fs.prefetchS3Block(fs.lastSeq(), false)
	}
	fs.mu.RLock()
	var index uint32
	if subjectHasWildcard(subject) {
		fs.psim.Match(stringToBytes(subject), func(_ []byte, state *psi) {
			if state.lblk > index {
				index = state.lblk
			}
		})
	} else if state, ok := fs.psim.Find(stringToBytes(subject)); ok {
		index = state.lblk
	}
	mb := fs.bim[index]
	fs.mu.RUnlock()
	if mb == nil {
		return nil
	}
	if _, err := os.Stat(mb.mfn); os.IsNotExist(err) {
		return fs.tier.ensureHydrated(index)
	}
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
	var candidates []*msgBlock
	for _, mb := range fs.blks {
		if info, err := os.Stat(mb.mfn); err == nil {
			used += uint64(info.Size())
			if mb != fs.lmb {
				candidates = append(candidates, mb)
			}
		}
	}
	fs.mu.RUnlock()
	if used <= t.cfg.LocalHighBytes {
		return nil
	}
	for _, mb := range candidates {
		if used <= t.cfg.LocalLowBytes {
			break
		}
		size, err := t.evictBlock(mb)
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

func (t *fileS3Tier) evictBlock(mb *msgBlock) (uint64, error) {
	fs := t.fs
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
	d.SHA256 = checksumS3Block(data)
	d.Key = fmt.Sprintf("%s/blocks/%010d-%s.blk", strings.Trim(t.cfg.Prefix, "/"), d.Index, d.SHA256)
	ctx, cancel := t.ctx()
	err = t.cfg.Store.Put(ctx, d.Key, data)
	cancel()
	if err != nil {
		return 0, err
	}
	ctx, cancel = t.ctx()
	verified, err := t.cfg.Store.Get(ctx, d.Key)
	cancel()
	if err != nil || !bytes.Equal(verified, data) {
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
		return 0, err
	}
	ctx, cancel = t.ctx()
	committed, err := t.cfg.Store.Get(ctx, t.descriptorKey(d.Index))
	cancel()
	if err != nil || !bytes.Equal(committed, buf) {
		return 0, fmt.Errorf("S3 tier descriptor verification failed for block %d: %w", d.Index, err)
	}
	if err := writeAtomically(fs.dios, t.descriptorFile(d.Index), buf, defaultFilePerms, true); err != nil {
		return 0, err
	}
	fs.mu.Lock()
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer fs.mu.Unlock()
	if fs.closing || fs.lmb == mb {
		return 0, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	current, err := os.ReadFile(mb.mfn)
	if err != nil || !bytes.Equal(current, data) {
		return 0, fmt.Errorf("S3 tier block %d changed during upload: %w", d.Index, err)
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
	t.desc[d.Index] = d
	return uint64(len(data)), nil
}

var errS3TierMutation = errors.New("experimental S3 tier does not support deletion or rewrite")
var errS3TierUnavailable = errors.New("S3 tier storage unavailable")
