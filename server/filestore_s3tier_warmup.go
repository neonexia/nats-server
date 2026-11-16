// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"encoding/json"
	"errors"
	"strings"
)

// Live preparation pins only immutable remote history. It grants no ownership,
// captures no consumer positions, and never fences the source's live traffic.
func (t *fileS3Tier) snapshotRemoteWarmup(operationID string) error {
	if !validS3TierRemoteRestoreOperationID(operationID) {
		return errors.New("invalid warmup operation")
	}
	key := t.remoteRestorePrefix(operationID) + "/warmup.json"
	if _, found, err := t.getImmutableIfPresent(key); err != nil || found {
		return err
	}
	t.mu.Lock()
	manifest := s3TierManifest{Version: 3, Generation: t.gen, Prefix: strings.Trim(t.cfg.Prefix, "/")}
	if t.record != nil {
		manifest.Incarnation = t.record.Incarnation
	}
	if t.indexRoot != nil {
		root := *t.indexRoot
		manifest.Root = &root
		manifest.Count = root.Count
	}
	t.mu.Unlock()
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return t.putImmutable(key, data)
}

func (t *fileS3Tier) loadRemoteWarmup(operationID string) (*s3TierRemoteRestoreMaterial, error) {
	data, found, err := t.getImmutableIfPresent(t.remoteRestorePrefix(operationID) + "/warmup.json")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("source warmup snapshot missing")
	}
	var manifest s3TierManifest
	if json.Unmarshal(data, &manifest) != nil || manifest.Version != 3 || manifest.Prefix != strings.Trim(t.cfg.Prefix, "/") {
		return nil, errors.New("invalid warmup snapshot")
	}
	if manifest.Generation == 0 {
		if manifest.Root != nil || manifest.Count != 0 {
			return nil, errors.New("uncommitted warmup contains blocks")
		}
		return &s3TierRemoteRestoreMaterial{manifest: manifest, manifestData: data}, nil
	}
	blocks, pages, err := t.loadManifestIndex(manifest)
	if err != nil {
		return nil, err
	}
	for _, block := range blocks {
		if !validS3BlockDescriptor(block) || block.Incarnation != manifest.Incarnation {
			return nil, errors.New("invalid warmup block")
		}
	}
	manifest.Blocks = blocks
	return &s3TierRemoteRestoreMaterial{manifest: manifest, manifestData: data, indexPages: pages}, nil
}

// Roll the active block briefly, then upload the captured immutable blocks with
// no stream admission or consumer fence held. Later writes use the new tail.
func (fs *fileStore) warmRemoteHistory(limits *S3TierSourceLimits, operationIDs ...string) error {
	if limits == nil {
		return errors.New("live history preparation requires upload limits")
	}
	if err := limits.validate(); err != nil {
		return err
	}
	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	if fs.lmb != nil {
		fs.lmb.mu.RLock()
		hasMessages := fs.lmb.msgs > 0
		fs.lmb.mu.RUnlock()
		if hasMessages {
			if _, err := fs.newMsgBlockForWrite(); err != nil {
				fs.mu.Unlock()
				return err
			}
			fs.dirty++
		}
	}
	blocks := make([]*msgBlock, 0, len(fs.blks))
	for _, block := range fs.blks {
		if block != fs.lmb {
			blocks = append(blocks, block)
		}
	}
	fs.mu.Unlock()
	t := fs.tier
	t.runMu.Lock()
	defer t.runMu.Unlock()
	operationID := ""
	if len(operationIDs) > 0 {
		operationID = operationIDs[0]
	}
	if err := t.configureSourceBudget(&s3TierRemoteRestoreState{OperationID: operationID, State: s3TierRemoteRestorePrepared, SourceLimits: limits}); err != nil {
		return err
	}
	var pending []*msgBlock
	for _, block := range blocks {
		if !t.hasDescriptor(block.index) {
			pending = append(pending, block)
		}
	}
	for len(pending) > 0 {
		n := min(s3TierUploadBatchSize, len(pending))
		if _, err := t.coverBlocks(pending[:n]); err != nil {
			return err
		}
		pending = pending[n:]
	}
	return nil
}
