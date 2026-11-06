// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// The AWS cache owns expiry and concurrent refresh. Keeping a second expiry cache
// in the S3 client would hide refreshed credentials, so every signing operation
// consults the AWS cache (normally an in-memory lookup, not a network request).
type s3TierAWSCredentials struct {
	provider aws.CredentialsProvider
	timeout  time.Duration
}

func newS3TierAWSCredentials(region string, timeout time.Duration) (*credentials.Credentials, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region),
		awsconfig.WithCredentialsCacheOptions(func(options *aws.CredentialsCacheOptions) {
			options.ExpiryWindow = 30 * time.Second
			options.ExpiryWindowJitterFrac = 0.2
		}))
	if err != nil {
		// Shared profiles and credential-process errors may contain secret values.
		return nil, errors.New("S3 tier AWS credential chain configuration failed")
	}
	return credentials.New(&s3TierAWSCredentials{provider: cfg.Credentials, timeout: timeout}), nil
}

func (c *s3TierAWSCredentials) IsExpired() bool { return true }

func (c *s3TierAWSCredentials) Retrieve() (credentials.Value, error) {
	return c.RetrieveWithCredContext(nil)
}

func (c *s3TierAWSCredentials) RetrieveWithCredContext(_ *credentials.CredContext) (credentials.Value, error) {
	// MinIO's credential interface carries no request context. Bound credential
	// discovery separately so an unavailable identity endpoint cannot hang uploads.
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	value, err := c.provider.Retrieve(ctx)
	if err != nil {
		return credentials.Value{}, errors.New("S3 tier AWS credentials unavailable")
	}
	if value.AccessKeyID == "" || value.SecretAccessKey == "" || (value.CanExpire && !value.Expires.After(time.Now())) {
		return credentials.Value{}, errors.New("S3 tier AWS credentials missing or expired")
	}
	return credentials.Value{AccessKeyID: value.AccessKeyID, SecretAccessKey: value.SecretAccessKey,
		SessionToken: value.SessionToken, SignerType: credentials.SignatureV4}, nil
}
