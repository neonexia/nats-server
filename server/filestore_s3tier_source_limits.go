// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/time/rate"
)

// S3TierSourceLimits bounds checkpoint uploads, including metadata. Streams of
// one operation share a ceiling; siblings retain their own offload budgets.
// Durable stream intents restore the shared ceiling after restart.
type S3TierSourceLimits struct {
	BytesPerSecond uint64 `json:"bytes_per_second"`
	BurstBytes     int    `json:"burst_bytes"`
	Concurrency    int    `json:"concurrency,omitempty"`
}

func (l S3TierSourceLimits) validate() error {
	if l.BytesPerSecond == 0 || l.BytesPerSecond > 1<<40 || l.BurstBytes < 4096 || l.BurstBytes > 64<<20 || l.Concurrency < 0 || l.Concurrency > 4 {
		return errors.New("source limits require bytes_per_second in 1..1TiB/s and burst_bytes in 4KiB..64MiB")
	}
	return nil
}
func sameSourceLimits(a, b *S3TierSourceLimits) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

type s3SourceBudget struct {
	limits  S3TierSourceLimits
	limiter *rate.Limiter
	slots   chan struct{}
	owner   *fileDiskBudget
	key     string
	refs    int
}

// References belong to stream lifetimes, so finished operations cannot leave an
// ever-growing limiter registry. All streams of one move share the same tokens.
func (b *fileDiskBudget) acquireSourceBudget(key string, limits S3TierSourceLimits) (*s3SourceBudget, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sourceBudgets == nil {
		b.sourceBudgets = make(map[string]*s3SourceBudget)
	}
	budget := b.sourceBudgets[key]
	if budget != nil && budget.limits != limits {
		return nil, errors.New("source migration limits disagree")
	}
	if budget == nil {
		budget = &s3SourceBudget{limits: limits, limiter: rate.NewLimiter(rate.Limit(limits.BytesPerSecond), limits.BurstBytes), slots: make(chan struct{}, max(1, limits.Concurrency)), owner: b, key: key}
		b.sourceBudgets[key] = budget
	}
	budget.refs++
	return budget, nil
}
func (b *s3SourceBudget) release() {
	if b == nil || b.owner == nil {
		return
	}
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	b.refs--
	if b.refs == 0 {
		delete(b.owner.sourceBudgets, b.key)
	}
}
func (t *fileS3Tier) clearSourceBudget() { t.sourceBudget.Swap(nil).release() }

func (t *fileS3Tier) configureSourceBudget(state *s3TierRemoteRestoreState) error {
	if !state.fenced() || state.SourceLimits == nil {
		t.clearSourceBudget()
		return nil
	}
	if err := t.remoteCtx.Err(); err != nil {
		return err
	}
	limits := *state.SourceLimits
	limits.Concurrency = max(1, limits.Concurrency)
	if current := t.sourceBudget.Load(); current != nil && current.limits == limits && current.key == state.OperationID {
		return nil
	}
	var budget *s3SourceBudget
	var err error
	if t.fs != nil && t.fs.dios != nil && t.fs.dios.budget.Load() != nil && state.OperationID != "" {
		budget, err = t.fs.dios.budget.Load().acquireSourceBudget(state.OperationID, limits)
		if err != nil {
			return err
		}
	} else {
		if state.SourceLimits.Concurrency > 0 {
			return errors.New("shared source migration budget required")
		}
		budget = &s3SourceBudget{limits: limits, limiter: rate.NewLimiter(rate.Limit(limits.BytesPerSecond), limits.BurstBytes), slots: make(chan struct{}, limits.Concurrency), key: state.OperationID}
	}
	t.sourceBudget.Swap(budget).release()
	// Cancellation may race acquisition; never retain a registry reference after stop.
	if err := t.remoteCtx.Err(); err != nil {
		t.clearSourceBudget()
		return err
	}
	return nil
}
func (t *fileS3Tier) acquireSourceUpload(ctx context.Context) (func(), error) {
	budget := t.sourceBudget.Load()
	if budget == nil {
		return func() {}, nil
	}
	select {
	case budget.slots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-budget.slots }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *fileS3Tier) waitSourceUpload(bytes int) error {
	if budget := t.sourceBudget.Load(); budget != nil {
		ctx := t.remoteCtx
		if ctx == nil {
			ctx = context.Background()
		}
		return waitRestoreDownload(ctx, budget.limiter, bytes)
	}
	return nil
}
