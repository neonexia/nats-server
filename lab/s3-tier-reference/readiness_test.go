package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/nats-io/nats.go"
)

func TestBucketReadinessRequiresLocationAfterBucketPUTSucceeds(t *testing.T) {
	var locations atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			if locations.Add(1) == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`<Error><Code>InvalidRequest</Code><Message>location not ready</Message></Error>`))
				return
			}
			_, _ = w.Write([]byte(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		t.Errorf("unexpected readiness request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer endpoint.Close()
	client, err := minio.New(strings.TrimPrefix(endpoint.URL, "http://"), &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ensureBucket(ctx, client, "reference", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if locations.Load() < 2 {
		t.Fatal("bucket PUT incorrectly satisfied location readiness")
	}
}

func TestCapacityWaitNeverRetriesAmbiguousPublish(t *testing.T) {
	calls := 0
	_, rejected, err := publishWithCapacityWait(func() (*nats.PubAck, error) { calls++; return nil, nats.ErrTimeout }, time.Second)
	if err != nats.ErrTimeout || calls != 1 || rejected != 0 {
		t.Fatalf("calls=%d rejected=%d err=%v", calls, rejected, err)
	}
}

func TestCapacityWaitRetriesOnlyConfirmedRejection(t *testing.T) {
	calls := 0
	ack, rejected, err := publishWithCapacityWait(func() (*nats.PubAck, error) {
		calls++
		if calls == 1 {
			return nil, &nats.APIError{Code: 503, ErrorCode: 10077, Description: "local capacity exceeded"}
		}
		return &nats.PubAck{Sequence: 1}, nil
	}, time.Second)
	if err != nil || calls != 2 || rejected != 1 || ack.Sequence != 1 {
		t.Fatalf("calls=%d rejected=%d ack=%v err=%v", calls, rejected, ack, err)
	}
}

func TestDrainRecoveryWaitsForStorageWorkerAndDoesNotRetryTransportErrors(t *testing.T) {
	calls := 0
	response, err := drainAfterOutage(func() (restoreResponse, error) {
		calls++
		if calls == 1 {
			return restoreResponse{Error: &apiError{Code: 500, ErrCode: 10051, Description: "store unavailable"}}, nil
		}
		return restoreResponse{Success: true}, nil
	}, time.Second)
	if err != nil || !response.Success || calls != 2 {
		t.Fatalf("calls=%d response=%+v err=%v", calls, response, err)
	}
	calls = 0
	_, err = drainAfterOutage(func() (restoreResponse, error) { calls++; return restoreResponse{}, nats.ErrTimeout }, time.Second)
	if calls != 1 || err != nats.ErrTimeout {
		t.Fatal("unexpected transport retry")
	}
}
