// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// The target fence is separate from the source checkpoint fence: staging does
// not claim remote ownership, and a restarted target must not infer activation
// merely because its stream directory and consumers already exist.
type s3TierRemoteRestoreTargetStage struct {
	RecoveryCheckpointID     string `json:"recovery_checkpoint_id,omitempty"`
	RecoveryCheckpointSHA256 string `json:"recovery_checkpoint_sha256,omitempty"`
	Version                  uint8  `json:"version"`
	OperationID              string `json:"operation_id"`
	Epoch                    uint64 `json:"epoch"`
	TargetID                 string `json:"target_id"`
	CheckpointKey            string `json:"checkpoint_key"`
	Ready                    bool   `json:"ready"`
	Activated                bool   `json:"activated"`
}

func (s *s3TierRemoteRestoreTargetStage) valid() bool {
	return s != nil && s.Version == 1 && validS3TierRemoteRestoreOperationID(s.OperationID) && s.Epoch > 0 && s.TargetID != "" && s.CheckpointKey != "" && (!s.Activated || s.Ready)
}
func (t *fileS3Tier) remoteRestoreStageFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.staged-target.json")
}
func (t *fileS3Tier) remoteRestoreTargetStage() *s3TierRemoteRestoreTargetStage {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.targetStage == nil {
		return nil
	}
	copy := *t.targetStage
	return &copy
}
func (t *fileS3Tier) writeRemoteRestoreTargetStage(stage *s3TierRemoteRestoreTargetStage) error {
	if !stage.valid() {
		return errors.New("invalid staged restore target")
	}
	data, err := json.Marshal(stage)
	if err != nil {
		return err
	}
	if err = writeAtomically(t.fs.dios, t.remoteRestoreStageFile(), data, defaultFilePerms, true); err != nil {
		return err
	}
	t.mu.Lock()
	copy := *stage
	t.targetStage = &copy
	t.mu.Unlock()
	return nil
}

// The coordinator invokes this only after ownership cutover. Persist the remote
// receipt and local activation before releasing either ingress or consumer ACKs.
func (mset *stream) activateStagedRemoteRestore(stage *s3TierRemoteRestoreTargetStage) error {
	if !stage.valid() || !stage.Ready {
		return errors.New("staged target is not ready")
	}
	fs, ok := mset.store.(*fileStore)
	if !ok || fs.tier == nil {
		return errS3TierNotEnabled
	}
	if !stage.Activated {
		activation, err := fs.tier.claimRemoteRestoreActivation(stage.OperationID, stage.Epoch, stage.TargetID, stage.CheckpointKey)
		if err != nil {
			return err
		}
		if err = fs.tier.writeRemoteRestoreActivation(activation); err != nil {
			return err
		}
		stage.Activated = true
		if err = fs.tier.writeRemoteRestoreTargetStage(stage); err != nil {
			return err
		}
	}
	mset.mu.Lock()
	mset.remoteRestoreFence.Store(mset.remoteRestore.fenced())
	mset.mu.Unlock()
	if err := mset.completeRestore(); err != nil {
		return err
	}
	return mset.completeRemoteRestoreConsumers()
}

// syncRestoredState runs while consumer updates are fenced or excluded by the
// source handoff barrier. Advance the background writer generation and fsync the
// settled position before acknowledging prepare/staging. Normal traffic retains
// its existing batched write path.
func (o *consumerFileStore) syncRestoredState() error {
	deadline := time.Now().Add(5 * time.Second)
	o.mu.Lock()
	for o.writing && !o.closed {
		o.mu.Unlock()
		if time.Now().After(deadline) {
			return errors.New("consumer restore sync timed out")
		}
		time.Sleep(time.Millisecond)
		o.mu.Lock()
	}
	defer o.mu.Unlock()
	if o.closed {
		return ErrStoreClosed
	}
	o.restoreGeneration++
	data, err := o.encodeState()
	if err != nil {
		return err
	}
	if data, err = o.encryptState(data); err != nil {
		return err
	}
	if err = writeAtomically(o.fs.dios, o.ifn, data, defaultFilePerms, true); err != nil {
		return err
	}
	// State alone cannot recover a consumer. Its configuration, checksum and
	// optional encryption key must reach disk before the target is ready too.
	files := []string{JetStreamMetaFile, JetStreamMetaFileSum}
	if o.aek != nil {
		files = append(files, JetStreamMetaFileKey)
	}
	for _, name := range files {
		path := filepath.Join(o.odir, name)
		if _, err := os.Stat(path); err != nil {
			return err
		}
		if err := o.fs.syncFileAndDir(path); err != nil {
			return err
		}
	}
	// Persist the consumer directory entry in its parent as well.
	if canFsyncDirectories {
		o.fs.dios.acquire()
		err = syncDir(filepath.Dir(o.odir))
		o.fs.dios.release()
		if err != nil {
			return err
		}
	}
	o.dirty = false
	return nil
}

// Caller holds the shared account lock and stream restore lock, and has verified the shared durable abort decision.
// Never use generic stream deletion: rollback removes local copies only.
func (s *Server) discardRemoteRestoreTarget(acc *Account, jsa *jsAccount, name string, req JSApiStreamRestoreRemoteRequest) error {
	matches := func(stage *s3TierRemoteRestoreTargetStage) bool {
		return stage.valid() && !stage.Activated && stage.OperationID == req.OperationID && stage.Epoch == req.ExpectedEpoch && stage.TargetID == req.TargetID
	}
	streamDir := filepath.Join(jsa.storeDir, streamsDir, name)
	if existing, err := acc.lookupStream(name); err == nil {
		fs, ok := existing.store.(*fileStore)
		if !ok || fs.tier == nil || !matches(fs.tier.remoteRestoreTargetStage()) {
			return errors.New("discard target belongs to another operation or is active")
		}
		cleanupRemoteRestoreTarget(existing)
	} else if data, err := os.ReadFile(filepath.Join(streamDir, msgDir, "s3-tier.staged-target.json")); err == nil {
		var stage s3TierRemoteRestoreTargetStage
		if json.Unmarshal(data, &stage) != nil || !matches(&stage) {
			return errors.New("discard target metadata does not match")
		}
		if err := removeAllWithRetry(s.diskIOSemaphore(), streamDir); err != nil {
			return err
		}
	} else if _, statErr := os.Stat(streamDir); !os.IsNotExist(statErr) {
		return errors.New("discard cannot identify destination stream")
	}
	if _, err := os.Stat(streamDir); !os.IsNotExist(err) {
		return errors.New("discard destination cleanup incomplete")
	}
	stageDir := filepath.Join(jsa.storeDir, "__remote_restore__", name, req.OperationID)
	if err := removeAllWithRetry(s.diskIOSemaphore(), stageDir); err != nil {
		return err
	}
	// Persist removal before acknowledging cleanup and releasing reserved space.
	if canFsyncDirectories {
		for _, parent := range []string{filepath.Dir(streamDir), filepath.Dir(stageDir)} {
			if err := syncDir(parent); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}
