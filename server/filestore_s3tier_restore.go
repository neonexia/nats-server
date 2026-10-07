// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type s3TierRemoteRestoreMaterial struct {
	checkpoint   s3TierRemoteRestoreCheckpoint
	manifest     s3TierManifest
	manifestData []byte
	streamRecord s3TierStreamRecord
	streamData   []byte
	messageData  []byte
}

func s3TierStreamConfigSHA256(cfg StreamConfig) (string, error) {
	buf, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return checksumS3Block(buf), nil
}

func (t *fileS3Tier) loadRemoteRestoreMaterial(operationID string, expectedEpoch uint64) (*s3TierRemoteRestoreMaterial, error) {
	if !validS3TierRemoteRestoreOperationID(operationID) || expectedEpoch == 0 {
		return nil, errors.New("remote restore requires an operation ID and expected epoch")
	}
	checkpointKey := t.remoteRestoreCheckpointKey(operationID)
	checkpointData, found, err := t.getImmutableIfPresent(checkpointKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("remote restore checkpoint was not found")
	}
	var checkpoint s3TierRemoteRestoreCheckpoint
	if err := json.Unmarshal(checkpointData, &checkpoint); err != nil || checkpoint.Version != 1 || checkpoint.OperationID != operationID || checkpoint.Epoch != expectedEpoch || checkpoint.Created.IsZero() {
		return nil, errors.New("invalid remote restore checkpoint")
	}
	if checkpoint.StreamConfig.Name == "" || checkpoint.StreamState.LastSeq != checkpoint.MessageCheckpoint.CoveredThrough {
		return nil, errors.New("remote restore checkpoint has an invalid stream boundary")
	}
	configSHA256, err := s3TierStreamConfigSHA256(checkpoint.StreamConfig)
	if err != nil {
		return nil, err
	}
	messageCheckpoint := checkpoint.MessageCheckpoint
	if messageCheckpoint.Version != 1 || messageCheckpoint.Incarnation == "" || messageCheckpoint.CoveredThrough == 0 ||
		messageCheckpoint.ConfigSHA256 != configSHA256 || messageCheckpoint.ManifestKey != t.manifestKey(messageCheckpoint.ManifestGeneration) ||
		messageCheckpoint.ManifestSHA256 == "" {
		return nil, errors.New("remote restore checkpoint has an invalid message checkpoint")
	}

	ctx, cancel := t.ctx()
	manifestData, err := t.cfg.Store.Get(ctx, messageCheckpoint.ManifestKey)
	cancel()
	if err != nil || checksumS3Block(manifestData) != messageCheckpoint.ManifestSHA256 {
		return nil, fmt.Errorf("remote restore manifest verification failed: %w", err)
	}
	var manifest s3TierManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil || manifest.Version != 2 ||
		manifest.Generation != messageCheckpoint.ManifestGeneration || manifest.Prefix != strings.Trim(t.cfg.Prefix, "/") ||
		manifest.Incarnation != messageCheckpoint.Incarnation {
		return nil, errors.New("remote restore manifest is invalid")
	}
	if len(manifest.Blocks) == 0 {
		return nil, errors.New("remote restore manifest has no blocks")
	}
	for i, block := range manifest.Blocks {
		if !validS3BlockDescriptor(block) || block.Incarnation != messageCheckpoint.Incarnation || block.LastSeq > messageCheckpoint.CoveredThrough ||
			(i > 0 && manifest.Blocks[i-1].Index >= block.Index) {
			return nil, errors.New("remote restore manifest has invalid block descriptors")
		}
	}

	ctx, cancel = t.ctx()
	streamData, err := t.cfg.Store.Get(ctx, t.streamRecordKey())
	cancel()
	if err != nil {
		return nil, err
	}
	var streamRecord s3TierStreamRecord
	if err := json.Unmarshal(streamData, &streamRecord); err != nil || streamRecord.Version != 1 ||
		streamRecord.Incarnation != messageCheckpoint.Incarnation || streamRecord.Prefix != strings.Trim(t.cfg.Prefix, "/") ||
		streamRecord.StreamName != checkpoint.StreamConfig.Name || streamRecord.ConfigSHA256 != configSHA256 {
		return nil, errors.New("remote restore stream record is invalid")
	}

	messageKey := t.checkpointKey(messageCheckpoint.CoveredThrough)
	ctx, cancel = t.ctx()
	messageData, err := t.cfg.Store.Get(ctx, messageKey)
	cancel()
	if err != nil {
		return nil, err
	}
	var storedMessageCheckpoint s3TierCheckpoint
	if err := json.Unmarshal(messageData, &storedMessageCheckpoint); err != nil || storedMessageCheckpoint != messageCheckpoint {
		return nil, errors.New("remote restore message checkpoint is invalid")
	}
	return &s3TierRemoteRestoreMaterial{checkpoint, manifest, manifestData, streamRecord, streamData, messageData}, nil
}

func writeS3TierRestoreFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), defaultDirPerms); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFilePerms)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// stageRemoteRestore writes a complete private file-store directory. No stream
// points at it until the caller verifies the staged state and completes the
// activation receipt protocol.
func (t *fileS3Tier) stageRemoteRestore(dir string, material *s3TierRemoteRestoreMaterial) error {
	if material == nil {
		return errors.New("remote restore material is required")
	}
	if err := writeS3TierRestoreFile(filepath.Join(dir, "s3-tier.json"), []byte(strings.Trim(t.cfg.Prefix, "/"))); err != nil {
		return err
	}
	msgPath := filepath.Join(dir, msgDir)
	if err := writeS3TierRestoreFile(filepath.Join(msgPath, "s3-tier.manifest.json"), material.manifestData); err != nil {
		return err
	}
	if err := writeS3TierRestoreFile(filepath.Join(msgPath, "s3-tier.stream.json"), material.streamData); err != nil {
		return err
	}
	if err := writeS3TierRestoreFile(filepath.Join(msgPath, "s3-tier.checkpoint.json"), material.messageData); err != nil {
		return err
	}
	for _, block := range material.manifest.Blocks {
		ctx, cancel := t.ctx()
		data, err := t.cfg.Store.Get(ctx, block.Key)
		cancel()
		if err != nil || len(data) != block.Size || checksumS3Block(data) != block.SHA256 {
			return fmt.Errorf("remote restore block %d verification failed: %w", block.Index, err)
		}
		descriptorData, err := json.Marshal(block)
		if err != nil {
			return err
		}
		if err := writeS3TierRestoreFile(filepath.Join(msgPath, fmt.Sprintf(blkScan, block.Index)), data); err != nil {
			return err
		}
		if err := writeS3TierRestoreFile(filepath.Join(msgPath, fmt.Sprintf("%d.tier.json", block.Index)), descriptorData); err != nil {
			return err
		}
	}
	return syncDir(dir)
}
