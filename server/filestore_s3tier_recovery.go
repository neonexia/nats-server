// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *Server) s3TierConfigForGeneration(acc *Account, stream, generation string) *S3TierConfig {
	if generation != "" && !validS3TierRemoteRestoreOperationID(generation) {
		return nil
	}
	cfg := s.s3TierConfigForRemoteRestore(acc, stream)
	if cfg != nil && generation != "" {
		cfg.Prefix = strings.TrimRight(cfg.Prefix, "/") + "/generations/" + generation
	}
	return cfg
}

// A restored backup can precede the failed owner's uploaded tail. Reusing its
// immutable object names would collide as soon as those sequences are written
// again. All payloads have been verified and downloaded into this private tree;
// detach remote descriptors so the replacement uploads under a fresh identity.
func rebaseRecoveryStore(dios *diskIOSemaphore, dir, prefix string) error {
	entries, err := os.ReadDir(filepath.Join(dir, msgDir))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".tier.json") || strings.HasPrefix(name, "s3-tier.") || name == "s3-index" {
			if err := removeAllWithRetry(dios, filepath.Join(dir, msgDir, name)); err != nil {
				return err
			}
		}
	}
	if err := writeFileWithSync(dios, filepath.Join(dir, "s3-tier.json"), []byte(strings.Trim(prefix, "/")), defaultFilePerms); err != nil {
		return err
	}
	return syncDir(filepath.Join(dir, msgDir))
}

// Recovery checkpoints deliberately live outside the migration checkpoint
// namespace. A backup is not a source fence and must never authorize RESTORE_REMOTE.
func (t *fileS3Tier) recoveryCheckpointKey(id string) string {
	return fmt.Sprintf("%s/recovery/%s/checkpoint.json", strings.Trim(t.cfg.Prefix, "/"), id)
}

// The caller holds isolateMu and remoteConsumerMu: messages, deliveries and ACKs
// share one checkpoint boundary. Normal serving resumes on success or failure;
// unlike a planned move this operation never changes durable ownership.
func (mset *stream) recoveryCheckpoint(req JSApiStreamRemoteRestoreRequest) (*s3TierRemoteRestoreCheckpoint, string, string, error) {
	if !validS3TierRemoteRestoreOperationID(req.OperationID) || req.SourceGeneration == 0 || req.SourceLimits == nil {
		return nil, "", "", errors.New("recovery checkpoint requires operation ID, source generation and upload limits")
	}
	if err := req.SourceLimits.validate(); err != nil {
		return nil, "", "", err
	}
	mset.mu.RLock()
	fenced := mset.remoteRestore.fenced()
	fs, ok := mset.store.(*fileStore)
	config := mset.cfg
	mset.mu.RUnlock()
	if fenced || !ok || fs.tier == nil {
		return nil, "", "", errors.New("recovery checkpoint requires an unfenced S3-backed stream")
	}
	t := fs.tier
	key := t.recoveryCheckpointKey(req.OperationID)
	// A lost response must return the original consumer and message boundary,
	// even when normal traffic has advanced since the first successful request.
	if data, found, err := t.getImmutableIfPresent(key); err != nil {
		return nil, "", "", err
	} else if found {
		var previous s3TierRemoteRestoreCheckpoint
		if json.Unmarshal(data, &previous) != nil || previous.Version != 1 || previous.OperationID != req.OperationID || previous.Epoch != req.SourceGeneration || previous.StreamConfig.Name != config.Name {
			return nil, "", "", errors.New("recovery checkpoint identity conflicts with request")
		}
		return &previous, key, checksumS3Block(data), nil
	}
	if err := t.configureSourceBudget(&s3TierRemoteRestoreState{OperationID: req.OperationID, State: s3TierRemoteRestorePrepared, SourceLimits: req.SourceLimits}); err != nil {
		return nil, "", "", err
	}
	defer t.clearSourceBudget()
	if _, err := fs.DrainS3Tier(); err != nil {
		return nil, "", "", err
	}
	t.mu.Lock()
	var messages s3TierCheckpoint
	if t.checkpoint != nil {
		messages = *t.checkpoint
	}
	t.mu.Unlock()
	var state StreamState
	fs.FastState(&state)
	if messages.Version != 1 || messages.CoveredThrough != state.LastSeq {
		return nil, "", "", errors.New("recovery checkpoint lacks complete message coverage")
	}
	consumers, err := remoteRestoreConsumers(mset)
	if err != nil {
		return nil, "", "", err
	}
	checkpoint := &s3TierRemoteRestoreCheckpoint{Version: 1, OperationID: req.OperationID, Epoch: req.SourceGeneration, Created: time.Now().UTC(), StreamConfig: config, StreamState: state, MessageCheckpoint: messages, Consumers: consumers}
	data, err := json.Marshal(checkpoint)
	if err == nil {
		err = t.putImmutable(key, data)
	}
	if err != nil {
		return nil, "", "", err
	}
	return checkpoint, key, checksumS3Block(data), nil
}
