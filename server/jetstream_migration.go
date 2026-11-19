// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
)

// Whole shards can contain several namespaces. Keep both the stream count and
// durable reservation journal finite before any source is paused.
const maxMigrationStreams = 4096
const maxMigrationJournalBytes = 1 << 20

const (
	JSApiMigrationReserve = "$JS.API.MIGRATION.RESERVE"
	JSApiMigrationRelease = "$JS.API.MIGRATION.RELEASE"
)

type JSApiMigrationReservationRequest struct {
	OperationID          string   `json:"operation_id"`
	Streams              []string `json:"streams"`
	LocalBytes           uint64   `json:"local_bytes"`
	SiblingHeadroomBytes uint64   `json:"sibling_headroom_bytes"`
}

// Engine-wide admission is durable before the coordinator fences any source.
// Only management clients can reach these JetStream subjects; this does not
// provide a public namespace-move operator endpoint.
func (s *Server) jsMigrationReservationRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, hdr, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		return
	}
	resp := JSApiStreamRemoteRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRemoteRestoreResponseType}}
	defer func() {
		if resp.Error != nil {
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		} else {
			s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		}
	}()
	fail := func(err error) { resp.Error = NewJSStreamGeneralError(err, Unless(err)) }
	if errorOnRequiredApiLevel(hdr) {
		resp.Error = NewJSRequiredApiLevelError()
		return
	}
	if s.JetStreamIsClustered() {
		fail(errS3TierClusteredDrain)
		return
	}
	_, jsa, err := acc.checkForJetStream()
	if err != nil {
		fail(err)
		return
	}
	budget := s.diskIOSemaphore().budget.Load()
	if budget == nil {
		fail(errors.New("migration requires an enforced engine disk budget"))
		return
	}
	var req JSApiMigrationReservationRequest
	if err = json.Unmarshal(msg, &req); err != nil {
		fail(err)
		return
	}
	if !validS3TierRemoteRestoreOperationID(req.OperationID) || len(req.Streams) == 0 || len(req.Streams) > maxMigrationStreams {
		fail(errors.New("migration requires an operation and 1–4096 unique streams"))
		return
	}
	sort.Strings(req.Streams)
	for i, name := range req.Streams {
		if !isValidAssetName(name) || (i > 0 && req.Streams[i-1] == name) {
			fail(errors.New("invalid migration stream manifest"))
			return
		}
	}
	jsa.archiveMu.Lock()
	defer jsa.archiveMu.Unlock()
	roots := make([]string, 0, len(req.Streams)*2)
	for _, name := range req.Streams {
		for _, path := range []string{filepath.Join(jsa.storeDir, "__remote_restore__", name, req.OperationID), filepath.Join(jsa.storeDir, streamsDir, name)} {
			rel, err := filepath.Rel(budget.root, path)
			if err != nil {
				fail(err)
				return
			}
			roots = append(roots, rel)
		}
		existing, lookupErr := acc.lookupStream(name)
		if lookupErr == nil {
			fs, ok := existing.store.(*fileStore)
			if !ok || fs.tier == nil {
				fail(errors.New("destination stream already exists"))
				return
			}
			stage := fs.tier.remoteRestoreTargetStage()
			if stage == nil || stage.OperationID != req.OperationID {
				fail(errors.New("destination stream belongs to another operation"))
				return
			}
			if subject == JSApiMigrationRelease && (!stage.Ready || !stage.Activated) {
				fail(errors.New("cannot release an unactivated migration"))
				return
			}
		} else if subject == JSApiMigrationRelease {
			// Before any staging starts, cancellation may release an unused hold.
			// Once files exist, forward recovery must finish activation first.
			for _, path := range []string{filepath.Join(jsa.storeDir, "__remote_restore__", name, req.OperationID), filepath.Join(jsa.storeDir, streamsDir, name)} {
				if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
					fail(errors.New("cannot release incomplete migration material"))
					return
				}
			}
		}
	}
	if subject == JSApiMigrationReserve {
		err = budget.reserveMigration(diskMigrationReservation{Version: 1, OperationID: req.OperationID, Roots: roots, LimitBytes: req.LocalBytes}, req.SiblingHeadroomBytes)
		resp.State = "reserved"
	} else {
		// A shortened manifest must not release the hold for remaining streams.
		budget.mu.Lock()
		matches := budget.migration == nil || (budget.migration.OperationID == req.OperationID && len(budget.migration.Roots) == len(roots))
		if matches && budget.migration != nil {
			for i := range roots {
				if budget.migration.Roots[i] != roots[i] {
					matches = false
					break
				}
			}
		}
		budget.mu.Unlock()
		if !matches {
			fail(errors.New("release manifest does not match reservation"))
			return
		}
		err = budget.releaseMigration(req.OperationID)
		resp.State = "released"
	}
	if err != nil {
		fail(err)
		return
	}
	resp.Success = true
	resp.OperationID = req.OperationID
}
