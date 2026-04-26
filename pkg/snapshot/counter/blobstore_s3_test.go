//go:build integration

// Integration tests for ShardSnapshot Save/Load over S3BlobStore. Gated
// behind the `integration` build tag so the default `go test ./...` run
// stays hermetic.
//
// Usage:
//
//   docker compose up -d minio
//   MINIO_ENDPOINT=http://localhost:9000 go test -tags=integration ./pkg/snapshot/counter/...

package counter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/xargin/opentrade/pkg/snapshot"
)

// requireS3 returns an *s3.Client + a fresh bucket, or skips when
// MINIO_ENDPOINT is unset. Bucket is removed on test cleanup.
func requireS3(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("MINIO_ENDPOINT unset; skipping S3 integration test")
	}
	accessKey := getenvOr("MINIO_ACCESS_KEY_ID", "minioadmin")
	secretKey := getenvOr("MINIO_SECRET_ACCESS_KEY", "minioadmin")
	region := getenvOr("MINIO_REGION", "us-east-1")

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	bucket := fmt.Sprintf("opentrade-counter-test-%d", time.Now().UnixNano())
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
	t.Cleanup(func() { cleanupBucket(t, client, bucket) })
	return client, bucket
}

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func cleanupBucket(t *testing.T, client *s3.Client, bucket string) {
	ctx := context.Background()
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup list: %v", err)
			return
		}
		for _, obj := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: obj.Key,
			}); err != nil {
				t.Logf("cleanup delete object %s: %v", aws.ToString(obj.Key), err)
			}
		}
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("cleanup delete bucket %s: %v", bucket, err)
	}
}

// TestS3BlobStore_RoundTrip is the smoke test — a snapshot saved through
// the S3 backend must come back identical.
func TestS3BlobStore_RoundTrip(t *testing.T) {
	client, bucket := requireS3(t)
	store := snapshot.NewS3BlobStore(client, bucket, "counter/")
	ctx := context.Background()

	snap := &ShardSnapshot{
		Version:    Version,
		ShardID:    0,
		CounterSeq: 123,
		Accounts: []AccountSnapshot{{
			UserID:   "u1",
			Balances: []BalanceSnapshot{{Asset: "USDT", Available: "100", Frozen: "50"}},
		}},
	}
	if err := Save(ctx, store, "shard-0", snap, snapshot.FormatProto); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := Load(ctx, store, "shard-0")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.CounterSeq != 123 || len(got.Accounts) != 1 || got.Accounts[0].UserID != "u1" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

// TestS3BlobStore_GetMissingReturnsErrNotExist is the cold-start contract:
// a key that has never been written must surface as os.ErrNotExist so
// callers can branch on it.
func TestS3BlobStore_GetMissingReturnsErrNotExist(t *testing.T) {
	client, bucket := requireS3(t)
	store := snapshot.NewS3BlobStore(client, bucket, "counter/")
	ctx := context.Background()
	_, err := Load(ctx, store, "never-written")
	if err == nil {
		t.Fatal("want error on missing key")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want os.ErrNotExist, got %v", err)
	}
}

// TestS3BlobStore_ProtoPreferredOverJSON mirrors the FS test: when both
// formats exist on the store, Load picks the proto one (ADR-0049).
func TestS3BlobStore_ProtoPreferredOverJSON(t *testing.T) {
	client, bucket := requireS3(t)
	store := snapshot.NewS3BlobStore(client, bucket, "counter/")
	ctx := context.Background()
	protoSnap := &ShardSnapshot{Version: Version, ShardID: 0, CounterSeq: 7}
	jsonSnap := &ShardSnapshot{Version: Version, ShardID: 0, CounterSeq: 42}
	if err := Save(ctx, store, "shard-0", protoSnap, snapshot.FormatProto); err != nil {
		t.Fatal(err)
	}
	if err := Save(ctx, store, "shard-0", jsonSnap, snapshot.FormatJSON); err != nil {
		t.Fatal(err)
	}
	got, err := Load(ctx, store, "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	if got.CounterSeq != 7 {
		t.Fatalf("want proto (seq=7) to win, got seq=%d", got.CounterSeq)
	}
}

// TestS3BlobStore_EmptyPrefix confirms the common case of writing at the
// bucket root works (prefix == ""), which is how single-purpose snapshot
// buckets are typically laid out.
func TestS3BlobStore_EmptyPrefix(t *testing.T) {
	client, bucket := requireS3(t)
	store := snapshot.NewS3BlobStore(client, bucket, "")
	ctx := context.Background()

	snap := &ShardSnapshot{Version: Version, ShardID: 1, CounterSeq: 5}
	if err := Save(ctx, store, "shard-1", snap, snapshot.FormatProto); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := Load(ctx, store, "shard-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.CounterSeq != 5 {
		t.Fatalf("round trip: got seq=%d", got.CounterSeq)
	}
}
