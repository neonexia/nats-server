// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

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

// s3TierArchive preserves a final history boundary. It is not a consumer
// recovery checkpoint. The same receipt is retained locally outside the stream
// directory so an interrupted deletion can resume without reviving the stream.
type s3TierArchive struct {
	Version     int              `json:"version"`
	OperationID string           `json:"operation_id"`
	Account     string           `json:"account"`
	Stream      string           `json:"stream"`
	Prefix      string           `json:"prefix"`
	ArchiveKey  string           `json:"archive_key"`
	Archived    time.Time        `json:"archived"`
	Config      StreamConfig     `json:"config"`
	State       StreamState      `json:"state"`
	Checkpoint  s3TierCheckpoint `json:"checkpoint"`
}

// JSApiStreamArchiveRemoteRequest opts into resumable, non-blocking observation.
// The default remains the original synchronous archive contract.
type JSApiStreamArchiveRemoteRequest struct {
	OperationID string `json:"operation_id"`
	Async       bool   `json:"async,omitempty"`
	StatusOnly  bool   `json:"status_only,omitempty"`
}

type JSApiStreamArchiveRemoteResponse struct {
	ApiResponse
	Success        bool   `json:"success"`
	OperationID    string `json:"operation_id,omitempty"`
	State          string `json:"state,omitempty"`
	ArchiveKey     string `json:"archive_key,omitempty"`
	CoveredThrough uint64 `json:"covered_through"`
}

const JSApiStreamArchiveRemoteResponseType = "io.nats.jetstream.api.v1.stream_archive_remote_response"

// One active operation per account matches archiveMu serialization. A process-
// wide bound prevents API callers from creating an unbounded set of workers.
var s3TierArchiveAsyncGate = make(chan struct{}, 16)

type s3TierArchiveTask struct {
	stream, operation string
	running           bool
	err               error
}

func (s *Server) archiveStreamAsync(acc *Account, name string, req JSApiStreamArchiveRemoteRequest, resp *JSApiStreamArchiveRemoteResponse) error {
	resp.OperationID = req.OperationID
	if !isValidAssetName(name) || !validS3TierRemoteRestoreOperationID(req.OperationID) {
		return errors.New("archive requires a valid stream and operation_id")
	}
	_, jsa, err := acc.checkForJetStream()
	if err != nil {
		return err
	}
	jsa.archiveTaskMu.Lock()
	defer jsa.archiveTaskMu.Unlock()
	task := jsa.archiveTask
	if task != nil && task.stream == name && task.running {
		if task.operation != req.OperationID {
			return errors.New("archive operation_id conflicts with the current stream operation")
		}
		resp.State = "running"
		return nil
	}

	// A receipt fences the name before cleanup completes. After restart, only
	// absent stream files and no live stream establish completed local cleanup.
	marker := jsa.archiveTombstoneFile(name)
	if data, readErr := os.ReadFile(marker); readErr == nil {
		var archive s3TierArchive
		if json.Unmarshal(data, &archive) != nil || !validS3TierArchive(&archive, acc.Name, name, req.OperationID) {
			return errors.New("local archive receipt conflicts with stream identity or operation")
		}
		_, statErr := os.Stat(filepath.Join(jsa.storeDir, streamsDir, name))
		mset, _ := acc.lookupStream(name)
		if os.IsNotExist(statErr) && mset == nil {
			// Preserve the same durability check used by synchronous receipt recovery.
			if canFsyncDirectories {
				if err := syncDir(filepath.Dir(marker)); err != nil {
					return err
				}
				if err := syncDir(jsa.storeDir); err != nil {
					return err
				}
				if err := syncDir(filepath.Join(jsa.storeDir, streamsDir)); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			resp.Success, resp.State, resp.ArchiveKey, resp.CoveredThrough = true, "archived", archive.ArchiveKey, archive.Checkpoint.CoveredThrough
			return nil
		}
		if statErr != nil && !os.IsNotExist(statErr) {
			return statErr
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	// A synchronous retry may have completed an earlier failed async task.
	// Durable receipt recovery above takes precedence over transient failure state.
	if task != nil && task.stream == name {
		if task.operation != req.OperationID {
			return errors.New("archive operation_id conflicts with the current stream operation")
		}
		if req.StatusOnly && task.err != nil {
			resp.State = "failed"
			return task.err
		}
	}
	if req.StatusOnly {
		resp.State = "not_started"
		return nil
	}
	if task != nil && task.running {
		resp.State = "busy"
		return nil
	}
	if s.shutdown.Load() {
		return errors.New("server is shutting down")
	}
	select {
	case s3TierArchiveAsyncGate <- struct{}{}:
	default:
		resp.State = "busy"
		return nil
	}
	task = &s3TierArchiveTask{stream: name, operation: req.OperationID, running: true}
	jsa.archiveTask = task
	if !s.startGoRoutine(func() {
		defer s.grWG.Done()
		defer func() { <-s3TierArchiveAsyncGate }()
		archive, archiveErr := s.archiveStream(acc, name, req.OperationID)
		jsa.archiveTaskMu.Lock()
		task.err, task.running = archiveErr, false
		jsa.archiveTaskMu.Unlock()
		if archiveErr != nil {
			s.Warnf("S3 tier archive for stream %q failed: %v", name, archiveErr)
		} else {
			s.Noticef("S3 tier archive %s released local stream %q through sequence %d", req.OperationID, name, archive.Checkpoint.CoveredThrough)
		}
	}) {
		<-s3TierArchiveAsyncGate
		jsa.archiveTask = nil
		return errors.New("server is shutting down")
	}
	resp.State = "running"
	return nil
}

func (jsa *jsAccount) archiveTombstoneFile(stream string) string {
	return filepath.Join(jsa.storeDir, "__remote_archives__", stream+".json")
}

func validS3TierArchive(a *s3TierArchive, account, stream, operation string) bool {
	return a.Version == 1 && a.Account == account && a.Stream == stream &&
		a.OperationID == operation && validS3TierRemoteRestoreOperationID(operation) &&
		a.Config.Name == stream && a.Config.Sealed && !a.Archived.IsZero() &&
		a.Prefix != "" && a.ArchiveKey == a.Prefix+"/archive.json" &&
		a.Checkpoint.Incarnation != "" && a.Checkpoint.ManifestSHA256 != "" &&
		a.Checkpoint.CoveredThrough == a.State.LastSeq
}

func (t *fileS3Tier) archiveRemote(account, operation string, config StreamConfig) (*s3TierArchive, error) {
	if _, err := t.fs.DrainS3Tier(); err != nil {
		return nil, err
	}
	if t.fs.fcfg.srv != nil && t.fs.fcfg.srv.shutdown.Load() {
		return nil, ErrStoreClosed
	}
	t.mu.Lock()
	if t.checkpoint == nil {
		t.mu.Unlock()
		return nil, errors.New("remote archive has no message checkpoint")
	}
	checkpoint := *t.checkpoint
	t.mu.Unlock()
	var state StreamState
	t.fs.FastState(&state)
	if checkpoint.CoveredThrough != state.LastSeq {
		return nil, errors.New("remote archive checkpoint does not cover the stream tail")
	}
	prefix := strings.Trim(t.cfg.Prefix, "/")
	key := prefix + "/archive.json"
	if data, found, err := t.getImmutableIfPresent(key); err != nil {
		return nil, err
	} else if found {
		var archive s3TierArchive
		if err := json.Unmarshal(data, &archive); err != nil || !validS3TierArchive(&archive, account, config.Name, operation) || archive.Checkpoint != checkpoint {
			return nil, errors.New("remote archive conflicts with stream identity or operation")
		}
		return &archive, nil
	}
	// Verify the final manifest and each payload without populating the local
	// cache. Cleanup must not rely solely on an earlier successful upload.
	ctx, cancel := t.ctx()
	data, err := t.cfg.Store.Get(ctx, checkpoint.ManifestKey)
	cancel()
	if err != nil {
		return nil, err
	}
	var manifest s3TierManifest
	if checksumS3Block(data) != checkpoint.ManifestSHA256 || json.Unmarshal(data, &manifest) != nil || manifest.Incarnation != checkpoint.Incarnation || manifest.Prefix != prefix {
		return nil, errors.New("remote archive manifest verification failed")
	}
	ctx, cancel = t.ctx()
	data, err = t.cfg.Store.Get(ctx, t.checkpointKey(state.LastSeq))
	cancel()
	var remoteCheckpoint s3TierCheckpoint
	if err != nil {
		return nil, err
	}
	if json.Unmarshal(data, &remoteCheckpoint) != nil || remoteCheckpoint != checkpoint {
		return nil, errors.New("remote archive checkpoint verification failed")
	}
	for _, block := range manifest.Blocks {
		// Shutdown starts before quitCh closes. Stop between bounded object calls
		// so a background archive cannot hold shutdown through the entire history.
		if t.fs.fcfg.srv != nil && t.fs.fcfg.srv.shutdown.Load() {
			return nil, ErrStoreClosed
		}
		ctx, cancel := t.ctx()
		data, err := t.cfg.Store.Get(ctx, block.Key)
		cancel()
		if err != nil {
			return nil, err
		}
		if len(data) != block.Size || checksumS3Block(data) != block.SHA256 {
			return nil, fmt.Errorf("remote archive block %d verification failed", block.Index)
		}
	}
	archive := &s3TierArchive{Version: 1, OperationID: operation, Account: account, Stream: config.Name,
		Prefix: prefix, ArchiveKey: key, Archived: time.Now().UTC(), Config: config, State: state, Checkpoint: checkpoint}
	data, err = json.Marshal(archive)
	if err != nil {
		return nil, err
	}
	if err := t.putImmutable(key, data); err != nil {
		return nil, err
	}
	return archive, nil
}

// archiveStream commits remote history and a permanent local name fence before
// removing any files. The fence is also checked by startup and stream creation.
func (s *Server) archiveStream(acc *Account, name, operation string) (*s3TierArchive, error) {
	if !isValidAssetName(name) || !validS3TierRemoteRestoreOperationID(operation) {
		return nil, errors.New("archive requires a valid stream and operation_id")
	}
	_, jsa, err := acc.checkForJetStream()
	if err != nil {
		return nil, err
	}
	jsa.archiveMu.Lock()
	defer jsa.archiveMu.Unlock()
	if s.shutdown.Load() {
		return nil, ErrStoreClosed
	}
	marker := jsa.archiveTombstoneFile(name)
	var archive *s3TierArchive
	if data, err := os.ReadFile(marker); err == nil {
		archive = &s3TierArchive{}
		if json.Unmarshal(data, archive) != nil || !validS3TierArchive(archive, acc.Name, name, operation) {
			return nil, errors.New("local archive receipt conflicts with stream identity or operation")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if archive != nil && canFsyncDirectories {
		// A previous write may have renamed the receipt but failed its final
		// directory sync. Reestablish that durability before resuming removal.
		if err := syncDir(filepath.Dir(marker)); err != nil {
			return nil, err
		}
		if err := syncDir(jsa.storeDir); err != nil {
			return nil, err
		}
	}
	mset, lookupErr := acc.lookupStream(name)
	if archive == nil && lookupErr != nil {
		return nil, errors.New("stream missing without a committed archive receipt")
	}
	dios := defaultDiskIOSemaphore()
	if mset != nil {
		mset.isolateMu.Lock()
		defer mset.isolateMu.Unlock()
		mset.mu.RLock()
		fs, ok := mset.store.(*fileStore)
		config, restoring := mset.cfg, mset.remoteRestore.fenced() || mset.restoring
		mset.mu.RUnlock()
		if !ok || fs.tier == nil || !config.Sealed || restoring {
			return nil, errors.New("archive requires a sealed remote stream without an active move or restore")
		}
		dios = fs.dios
		if archive == nil {
			archive, err = fs.tier.archiveRemote(acc.Name, operation, config)
			if err != nil {
				return nil, err
			}
			if err = mkdirAllWithDiskBudget(dios, filepath.Dir(marker), defaultDirPerms); err != nil {
				return nil, err
			}
			if canFsyncDirectories {
				if err = syncDir(jsa.storeDir); err != nil {
					return nil, err
				}
			}
			data, err := json.Marshal(archive)
			if err != nil {
				return nil, err
			}
			if err = writeFileWithSync(dios, marker, data, defaultFilePerms); err != nil {
				return nil, err
			}
		}
		// Uploads and cold reads write sidecars/payloads outside the store
		// lock. Quiesce them before stopping so they cannot recreate files
		// after cleanup. Queued operations observe the closed store.
		fs.tier.runMu.Lock()
		defer fs.tier.runMu.Unlock()
		fs.tier.hydrateMu.Lock()
		defer fs.tier.hydrateMu.Unlock()
		if err = mset.stop(false, false); err != nil {
			return nil, err
		}
		// stop has already removed accounting for stored bytes. Release the
		// stream reservation only once, while the live instance still exists.
		s.getJetStream().releaseStreamResources(&config)
	}
	// Even an absent stream needs cleanup: a prior attempt may have stopped
	// successfully and then failed midway through removing its directory.
	dir := filepath.Join(jsa.storeDir, streamsDir, name)
	if err = removeAllWithRetry(dios, dir); err != nil {
		return nil, err
	}
	if canFsyncDirectories {
		if err = syncDir(filepath.Dir(dir)); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	return archive, nil
}

func (s *Server) jsStreamArchiveRemoteRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, hdr, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}
	resp := JSApiStreamArchiveRemoteResponse{ApiResponse: ApiResponse{Type: JSApiStreamArchiveRemoteResponseType}}
	if errorOnRequiredApiLevel(hdr) {
		resp.Error = NewJSRequiredApiLevelError()
	} else if hasJS, doErr := acc.checkJetStream(); !hasJS {
		if !doErr {
			return
		}
		resp.Error = NewJSNotEnabledForAccountError()
	} else if s.JetStreamIsClustered() {
		resp.Error = NewJSStreamGeneralError(errS3TierClusteredDrain, Unless(errS3TierClusteredDrain))
	} else {
		var req JSApiStreamArchiveRemoteRequest
		if err = json.Unmarshal(msg, &req); err == nil && req.StatusOnly && !req.Async {
			err = errors.New("status_only requires async")
		}
		if err == nil && req.Async {
			err = s.archiveStreamAsync(acc, streamNameFromSubject(subject), req, &resp)
		} else if err == nil {
			var archive *s3TierArchive
			archive, err = s.archiveStream(acc, streamNameFromSubject(subject), req.OperationID)
			if err == nil {
				resp.Success, resp.ArchiveKey, resp.CoveredThrough = true, archive.ArchiveKey, archive.Checkpoint.CoveredThrough
				s.Noticef("S3 tier archive %s released local stream %q through sequence %d", req.OperationID, archive.Stream, resp.CoveredThrough)
			}
		}
		if err != nil {
			s.Warnf("S3 tier archive for stream %q failed: %v", streamNameFromSubject(subject), err)
			resp.Error = NewJSStreamGeneralError(err, Unless(err))
		}
	}
	if resp.Error != nil {
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
}
