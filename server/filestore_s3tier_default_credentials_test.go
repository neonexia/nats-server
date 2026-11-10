// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//go:build !skip_js_tests

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func isolateS3DefaultCredentials(t *testing.T) {
	t.Helper()
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_STS"} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials"))
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
}

func TestJetStreamS3TierDefaultRefreshCompatibleEndpoint(t *testing.T) {
	endpoint := os.Getenv("TEST_S3_TIER_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TEST_S3_TIER_ENDPOINT to the disposable RustFS STS fixture")
	}
	// This test uses genuine RustFS STS keys for S3 authorization, but advertises
	// a shorter lifetime at the container endpoint to exercise refresh promptly.
	// It does not claim an AWS 15-minute role-expiration qualification.
	isolateS3DefaultCredentials(t)
	stsClient := sts.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: awscredentials.NewStaticCredentialsProvider("testaccess", "testsecret123", "")}, func(o *sts.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
	})
	var rotations atomic.Int64
	var unavailable atomic.Bool
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			http.Error(w, "identity unavailable", 503)
			return
		}
		result, err := stsClient.AssumeRole(r.Context(), &sts.AssumeRoleInput{RoleArn: aws.String("arn:aws:iam::123456789012:role/test"), RoleSessionName: aws.String("credential-test"), DurationSeconds: aws.Int32(900)})
		if err != nil {
			t.Errorf("RustFS STS: %v", err)
			http.Error(w, "STS failed", 503)
			return
		}
		rotations.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"AccessKeyId": result.Credentials.AccessKeyId, "SecretAccessKey": result.Credentials.SecretAccessKey, "Token": result.Credentials.SessionToken, "Expiration": time.Now().Add(35 * time.Second).UTC().Format(time.RFC3339)})
	}))
	defer identity.Close()
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", identity.URL)
	cfg := &S3TierServerConfig{Endpoint: endpoint, Bucket: "tier-test", Prefix: "credentials", Region: "us-east-1", CredentialProvider: "default", Timeout: 2 * time.Second}
	if err := cfg.resolveStore(); err != nil {
		t.Fatal(err)
	}
	fs, fcfg, stream := testTieredFileStore(t, cfg.store, t.TempDir(), fmt.Sprintf("credentials/%d", time.Now().UnixNano()))
	defer func() {
		if fs != nil {
			fs.Stop()
		}
	}()
	for n := 0; n < 3; n++ {
		seq, _, err := fs.StoreMsg("events", nil, []byte(fmt.Sprintf("message-%d", n)), 0)
		if err != nil || seq != uint64(n+1) {
			t.Fatalf("publish: sequence=%d error=%v", seq, err)
		}
		if _, err := fs.DrainS3Tier(); err != nil {
			t.Fatalf("drain during refresh: %v", err)
		}
		if n < 2 {
			time.Sleep(12 * time.Second)
		}
	}
	if rotations.Load() < 3 {
		t.Fatalf("expected three real STS credential generations, got %d", rotations.Load())
	}
	unavailable.Store(true)
	time.Sleep(12 * time.Second)
	if _, _, err := fs.StoreMsg("events", nil, []byte("after-outage"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.DrainS3Tier(); err == nil {
		t.Fatal("drain succeeded without refreshable credentials")
	}
	if _, err := fs.LoadMsg(4, nil); err != nil {
		t.Fatalf("credential outage lost unpreserved local data: %v", err)
	}
	unavailable.Store(false)
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := fs.DrainS3Tier()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("drain did not recover without restarting: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := fs.Stop(); err != nil {
		t.Fatal(err)
	}
	var err error
	fs, err = newFileStore(fcfg, stream)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 4; seq++ {
		message, err := fs.LoadMsg(seq, nil)
		want := fmt.Sprintf("message-%d", seq-1)
		if seq == 4 {
			want = "after-outage"
		}
		if err != nil || string(message.msg) != want {
			t.Fatalf("message %d after restart: error=%v", seq, err)
		}
	}
}

func TestJetStreamS3TierDefaultCredentialSources(t *testing.T) {
	for _, source := range []string{"env", "profile", "missing"} {
		t.Run(source, func(t *testing.T) {
			isolateS3DefaultCredentials(t)
			var calls atomic.Int64
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if !strings.Contains(r.Header.Get("Authorization"), "Credential="+source+"-key/") || r.Header.Get("X-Amz-Security-Token") != "temporary-token" {
					t.Error("wrong credential source or missing session token")
				}
				io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", `"etag"`)
			}))
			defer endpoint.Close()
			switch source {
			case "env":
				t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
				t.Setenv("AWS_SESSION_TOKEN", "temporary-token")
			case "profile":
				t.Setenv("AWS_PROFILE", "test")
				if err := os.WriteFile(os.Getenv("AWS_SHARED_CREDENTIALS_FILE"), []byte("[test]\naws_access_key_id=profile-key\naws_secret_access_key=secret\naws_session_token=temporary-token\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &S3TierServerConfig{Endpoint: strings.TrimPrefix(endpoint.URL, "http://"), Bucket: "history", Prefix: "instance", Region: "us-east-1", CredentialProvider: "default", Timeout: time.Second}
			if err := cfg.resolveStore(); err != nil {
				t.Fatal(err)
			}
			_, err := cfg.store.PutIfAbsent(context.Background(), "block", []byte("payload"))
			if source == "missing" {
				if err == nil || calls.Load() != 0 {
					t.Fatalf("missing identity must fail without an S3 request: err=%v calls=%d", err, calls.Load())
				}
			} else if err != nil || calls.Load() != 1 {
				t.Fatalf("publish failed: %v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestJetStreamS3TierDefaultCredentialTimeout(t *testing.T) {
	isolateS3DefaultCredentials(t)
	release := make(chan struct{})
	var objectRequests atomic.Int64
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/credentials" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			http.Error(w, "unavailable", 503)
			return
		}
		objectRequests.Add(1)
	}))
	defer endpoint.Close()
	defer close(release)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", endpoint.URL+"/credentials")
	cfg := &S3TierServerConfig{Endpoint: strings.TrimPrefix(endpoint.URL, "http://"), Bucket: "history", Prefix: "instance", Region: "us-east-1", CredentialProvider: "default", Timeout: 150 * time.Millisecond}
	if err := cfg.resolveStore(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err := cfg.store.PutIfAbsent(context.Background(), "block", []byte("payload"))
	if err == nil || time.Since(started) > time.Second || objectRequests.Load() != 0 {
		t.Fatalf("identity timeout did not fail closed promptly: error=%v objects=%d", err, objectRequests.Load())
	}
}

func TestJetStreamS3TierDefaultCredentialRefreshAndRecovery(t *testing.T) {
	isolateS3DefaultCredentials(t)
	var mu sync.Mutex
	var generation, requests int
	var expires time.Time
	var unavailable atomic.Bool
	var invalid atomic.Bool
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/credentials" {
			if unavailable.Load() {
				http.Error(w, "provider failure with secret material", 503)
				return
			}
			generation++
			expires = time.Now().Add(35 * time.Second)
			if invalid.Load() {
				expires = time.Now().Add(-time.Minute)
			}
			fmt.Fprintf(w, `{"AccessKeyId":"key-%d","SecretAccessKey":"secret","Token":"token-%d","Expiration":%q}`, generation, generation, expires.UTC().Format(time.RFC3339))
			return
		}
		requests++
		if !expires.After(time.Now()) || r.Header.Get("X-Amz-Security-Token") != fmt.Sprintf("token-%d", generation) {
			t.Error("S3 request used expired or stale credentials")
		}
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"etag"`)
	}))
	defer endpoint.Close()
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", endpoint.URL+"/credentials")
	cfg := &S3TierServerConfig{Endpoint: strings.TrimPrefix(endpoint.URL, "http://"), Bucket: "history", Prefix: "instance", Region: "us-east-1", CredentialProvider: "default", Timeout: time.Second}
	if err := cfg.resolveStore(); err != nil {
		t.Fatal(err)
	}
	// Cross real expiry/refresh boundaries with simultaneous signing operations.
	// A local identity endpoint keeps this deterministic and independent of AWS.
	var workers sync.WaitGroup
	end := time.Now().Add(37 * time.Second)
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for n := 0; time.Now().Before(end); n++ {
				if _, err := cfg.store.PutIfAbsent(context.Background(), fmt.Sprintf("block-%d-%d", worker, n), []byte("payload")); err != nil {
					t.Errorf("upload during refresh: %v", err)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(worker)
	}
	workers.Wait()
	mu.Lock()
	before, refreshes := requests, generation
	mu.Unlock()
	if refreshes < 2 || before < 100 {
		t.Fatalf("load did not exercise refresh: generations=%d requests=%d", refreshes, before)
	}
	unavailable.Store(true)
	time.Sleep(12 * time.Second) // Exceed the maximum 11-second effective cache lifetime.
	started := time.Now()
	_, err := cfg.store.PutIfAbsent(context.Background(), "provider-down", []byte("payload"))
	if err == nil || strings.Contains(err.Error(), "secret material") || time.Since(started) > 3*time.Second {
		t.Fatalf("expected bounded, sanitized credential failure: %v", err)
	}
	mu.Lock()
	if requests != before {
		t.Error("S3 request escaped after credential refresh failed")
	}
	mu.Unlock()
	unavailable.Store(false)
	invalid.Store(true)
	// The AWS cache shares a refresh that can outlive one caller's timeout.
	// Observe the expired credential result, not a still-pending provider outage.
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := cfg.store.PutIfAbsent(context.Background(), "expired", []byte("payload"))
		if err == nil {
			t.Fatal("accepted expired credentials")
		}
		if strings.Contains(err.Error(), "missing or expired") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired credentials were not exercised: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	invalid.Store(false)
	deadline = time.Now().Add(20 * time.Second)
	for {
		_, err := cfg.store.PutIfAbsent(context.Background(), "recovered", []byte("payload"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("credential recovery required restart: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
