// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/time/rate"
)

// S3TierRestoreLimits bounds payload downloads for one staged stream. This does
// not replace aggregate disk admission or the namespace's total byte reservation.
type S3TierRestoreLimits struct {
	operationID    string
	PayloadBytes   uint64 `json:"payload_bytes"`
	BytesPerSecond uint64 `json:"bytes_per_second"`
	BurstBytes     int    `json:"burst_bytes"`
	Concurrency    int    `json:"concurrency"`
}

func (l S3TierRestoreLimits) validate() error {
	if l.PayloadBytes == 0 || l.BytesPerSecond == 0 || l.BytesPerSecond > 1<<40 || l.BurstBytes < 4096 || l.BurstBytes > 64<<20 || l.Concurrency < 1 || l.Concurrency > 4 {
		return errors.New("restore limits require positive payload_bytes, bytes_per_second <= 1 TiB/s, burst_bytes between 4 KiB and 64 MiB, and concurrency between 1 and 4")
	}
	return nil
}

func (l S3TierRestoreLimits) admit(blocks []s3BlockDescriptor) error {
	if err := l.validate(); err != nil {
		return err
	}
	var bytes uint64
	for _, block := range blocks {
		if block.Size < 0 || uint64(block.Size) > l.PayloadBytes-bytes {
			return fmt.Errorf("restore payload exceeds reserved limit of %d bytes", l.PayloadBytes)
		}
		bytes += uint64(block.Size)
	}
	return nil
}

// Charge before issuing a request, sharing the limiter across workers. A block
// larger than the burst is admitted in bounded chunks; cancellation interrupts
// waiting and never launches an unbudgeted download.
func waitRestoreDownload(ctx context.Context, limiter *rate.Limiter, bytes int) error {
	if limiter == nil {
		return nil
	}
	for bytes > 0 {
		n := min(bytes, limiter.Burst())
		if err := limiter.WaitN(ctx, n); err != nil {
			return err
		}
		bytes -= n
	}
	return nil
}

// The reservation anchors the limiter until release, including gaps between
// channel commands. Concurrent staging must not multiply the transfer ceiling.
type s3MigrationDownloads struct {
	limits  S3TierRestoreLimits
	limiter *rate.Limiter
	slots   chan struct{}
}

func (b *fileDiskBudget) migrationDownloadBudget(limits S3TierRestoreLimits) (*s3MigrationDownloads, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.migration == nil || b.migration.OperationID != limits.operationID {
		return nil, errors.New("download budget has no matching reservation")
	}
	if b.migrationDownloads == nil {
		b.migrationDownloads = &s3MigrationDownloads{limits: limits, limiter: rate.NewLimiter(rate.Limit(limits.BytesPerSecond), limits.BurstBytes), slots: make(chan struct{}, limits.Concurrency)}
	}
	if b.migrationDownloads.limits != limits {
		return nil, errors.New("migration download limits disagree")
	}
	return b.migrationDownloads, nil
}
