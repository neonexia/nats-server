// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.
//go:build !skip_js_tests

package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreS3TierRemoteFlushAge(t *testing.T) {
	for _, mode := range []string{"quiet_partial", "restart", "disabled", "outage"} {
		t.Run(mode, func(t *testing.T) {
			remote := &testS3BlockStore{objects: make(map[string][]byte)}
			fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024 * 1024, S3Tier: &S3TierConfig{Store: remote, Prefix: "age/" + mode, LocalHighBytes: 4 * 1024 * 1024, LocalLowBytes: 2 * 1024 * 1024, RemoteHighBytes: 4 * 1024 * 1024, RemoteLowBytes: 2 * 1024 * 1024, RemoteFlushMaxAge: 100 * time.Millisecond, RetryMin: 300 * time.Millisecond, RetryMax: 600 * time.Millisecond}}
			cfg := StreamConfig{Name: "TEST", Subjects: []string{"events"}, Storage: FileStorage, Retention: LimitsPolicy, Discard: DiscardNew, DenyDelete: true, DenyPurge: true}
			if mode == "disabled" || mode == "restart" {
				fcfg.S3Tier.RemoteFlushMaxAge = 0
			}
			fs, err := newFileStore(fcfg, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { fs.Stop() }()
			if mode == "outage" {
				remote.mu.Lock()
				remote.failPut = true
				remote.mu.Unlock()
			}
			timestamp := time.Now().UnixNano()
			if mode == "restart" {
				timestamp = time.Now().Add(-time.Hour).UnixNano()
			}
			if err := fs.StoreRawMsg("events", nil, []byte("quiet message"), 1, timestamp, 0, false); err != nil {
				t.Fatal(err)
			}
			if mode == "restart" {
				if err := fs.Stop(); err != nil {
					t.Fatal(err)
				}
				fcfg.S3Tier.RemoteFlushMaxAge = time.Minute
				fs, err = newFileStore(fcfg, cfg)
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "disabled" {
				time.Sleep(250 * time.Millisecond)
				if fs.tier.hasDescriptor(1) {
					t.Fatal("disabled age policy uploaded quiet tail")
				}
				return
			}
			if mode == "outage" {
				checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
					if fs.tier.stats.uploadErrors.Load() == 0 {
						return errors.New("waiting for outage attempt")
					}
					return nil
				})
				deadline := time.Now().Add(150 * time.Millisecond)
				for time.Now().Before(deadline) {
					fs.tier.kick()
					time.Sleep(5 * time.Millisecond)
				}
				if got := fs.tier.stats.uploadErrors.Load(); got != 1 {
					t.Fatalf("age timer bypassed backoff: %d attempts", got)
				}
				remote.mu.Lock()
				remote.failPut = false
				remote.mu.Unlock()
			}
			checkFor(t, 3*time.Second, 5*time.Millisecond, func() error {
				if !fs.tier.hasDescriptor(1) {
					return errors.New("quiet tail has no remote descriptor")
				}
				return nil
			})
			if _, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, "1.blk")); err != nil {
				t.Fatalf("age coverage evicted local payload: %v", err)
			}
			if fs.tier.stats.evictions.Load() != 0 {
				t.Fatal("age coverage changed local residency")
			}
			if state := fs.State(); state.Msgs != 1 || state.LastSeq != 1 {
				t.Fatalf("coverage changed message state: %+v", state)
			}
		})
	}
}

func TestJetStreamS3TierRemoteFlushAgeConfiguration(t *testing.T) {
	t.Setenv("AGE_ACCESS", "access")
	t.Setenv("AGE_SECRET", "secret")
	for _, value := range []string{"0s", "-1s", "invalid"} {
		t.Run(value, func(t *testing.T) {
			path := createConfFile(t, []byte(fmt.Sprintf(`jetstream { s3_tier {
endpoint: "localhost:9000"
bucket: "test"
prefix: "age"
remote_flush_max_age: %q
credentials { provider: env, access_key_env: AGE_ACCESS, secret_key_env: AGE_SECRET }
} }`, value)))
			opts, err := ProcessConfigFile(path)
			if value != "0s" {
				if err == nil {
					t.Fatal("invalid age accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if opts.jetStreamS3Tier.RemoteFlushMaxAge != 0 {
				t.Fatal("explicit zero did not disable age policy")
			}
			changed := *opts.jetStreamS3Tier
			changed.RemoteFlushMaxAge = time.Second
			if opts.jetStreamS3Tier.equal(&changed) {
				t.Fatal("configuration comparison ignored age policy")
			}
		})
	}
}
