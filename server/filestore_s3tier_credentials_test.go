// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJetStreamS3TierCredentialProviderSelection(t *testing.T) {
	isolateS3DefaultCredentials(t)
	for _, test := range []struct {
		name, config, wantError string
	}{
		{"iam", "provider: iam", ""},
		{"default", "provider: default", ""},
		{"mixed default", "provider: default, access_key_env: AWS_ACCESS_KEY_ID", "does not accept"},
		{"unsupported", "provider: automatic", "is not supported"},
		{"omitted defaults to chain", "", ""},
		{"mixed access key", "provider: iam, access_key_env: AWS_ACCESS_KEY_ID", "does not accept"},
		{"mixed secret key", "provider: iam, secret_key_env: AWS_SECRET_ACCESS_KEY", "does not accept"},
		{"mixed token", "provider: iam, session_token_env: AWS_SESSION_TOKEN", "does not accept"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := createConfFile(t, []byte(fmt.Sprintf(`jetstream { s3_tier {
    endpoint: "s3.us-east-2.amazonaws.com"
    bucket: "test-history"
    region: "us-east-2"
    tls: true
    prefix: "instances/test"
    credentials { %s }
   } }`, test.config)))
			_, err := ProcessConfigFile(config)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected error containing %q, got %v", test.wantError, err)
			}
		})
	}
}

func TestJetStreamS3TierIAMCredentialRefresh(t *testing.T) {
	// Exercise the IAM provider's HTTP container endpoint on loopback. The same
	// cached expiry handling wraps EC2 metadata retrieval; no cloud access occurs.
	for _, name := range []string{"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"} {
		t.Setenv(name, "")
	}
	var mu sync.Mutex
	var lookups int
	var signedTokens []string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/credentials" {
			lookups++
			expiration := time.Now().Add(time.Hour)
			if lookups == 1 {
				expiration = time.Now().Add(-time.Minute)
			}
			fmt.Fprintf(w, `{"AccessKeyId":"role-key-%d","SecretAccessKey":"role-secret","Token":"token-%d","Expiration":%q,"Code":"Success"}`, lookups, lookups, expiration.UTC().Format(time.RFC3339))
			return
		}
		if r.Method != http.MethodPut {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(400)
			return
		}
		signedTokens = append(signedTokens, r.Header.Get("X-Amz-Security-Token"))
		if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
			t.Error("request missing SigV4 authorization")
		}
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"etag"`)
	}))
	defer endpoint.Close()
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", endpoint.URL+"/credentials")
	// Explicit IAM selection must ignore unrelated static access-key variables.
	t.Setenv("AWS_ACCESS_KEY_ID", "unused-static-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "unused-static-secret")
	cfg := &S3TierServerConfig{Endpoint: strings.TrimPrefix(endpoint.URL, "http://"), Bucket: "test-history", Prefix: "instance", Region: "us-east-2", CredentialProvider: "iam"}
	if err := cfg.resolveStore(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cfg.store.PutIfAbsent(ctx, fmt.Sprintf("block-%d", i), []byte("payload"))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if lookups != 2 {
		t.Fatalf("credential fetch count = %d, want 2 (refresh then cache)", lookups)
	}
	if strings.Join(signedTokens, ",") != "token-1,token-2,token-2" {
		t.Fatalf("signed tokens = %v", signedTokens)
	}
}

func TestJetStreamS3TierIAMCredentialFailure(t *testing.T) {
	for _, name := range []string{"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"} {
		t.Setenv(name, "")
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/credentials" {
			t.Errorf("object request made without role credentials: %s", r.URL.Path)
		}
		http.Error(w, "role unavailable", http.StatusServiceUnavailable)
	}))
	defer endpoint.Close()
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", endpoint.URL+"/credentials")
	cfg := &S3TierServerConfig{Endpoint: strings.TrimPrefix(endpoint.URL, "http://"), Bucket: "test-history", Prefix: "instance", Region: "us-east-2", CredentialProvider: "iam"}
	if err := cfg.resolveStore(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cfg.store.PutIfAbsent(ctx, "block", []byte("payload")); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected role retrieval failure, got %v", err)
	}
}
