//go:build integration

// Integration tests for S3BlobStore. Gated behind the `integration` build
// tag so the default `go test ./...` run stays hermetic.
//
// Usage:
//
//   docker compose up -d minio
//   MINIO_ENDPOINT=http://localhost:9000 go test -tags=integration ./pkg/snapshot/...
//
// Credentials default to the docker-compose MinIO root user
// (minioadmin/minioadmin) and can be overridden via MINIO_ACCESS_KEY_ID /
// MINIO_SECRET_ACCESS_KEY. Each test creates a unique bucket and cleans
// up after itself so tests are independent and idempotent.

package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	bucket := fmt.Sprintf("opentrade-blobstore-test-%d", time.Now().UnixNano())
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

// cleanupBucket best-effort empties and deletes the bucket. Failures here
// don't fail the test — they just leak a bucket that can be swept manually.
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

// TestS3BlobStore_ListAndDelete exercises the BlobLister impl used
// by the ADR-0064 M1d on-demand snapshot housekeeper. Seed a mix
// of periodic + on-demand keys, verify prefix filter, delete one
// on-demand key, verify it disappears from subsequent List.
func TestS3BlobStore_ListAndDelete(t *testing.T) {
	client, bucket := requireS3(t)
	store := NewS3BlobStore(client, bucket, "ondemand-test/")
	ctx := context.Background()

	// Seed both periodic and on-demand keys.
	seed := map[string][]byte{
		"vshard-001.pb":                        []byte("periodic-1"),
		"vshard-002.pb":                        []byte("periodic-2"),
		"vshard-001-ondemand-1700000000000.pb": []byte("ondemand-1a"),
		"vshard-001-ondemand-1700000000001.pb": []byte("ondemand-1b"),
		"vshard-002-ondemand-1700000000002.pb": []byte("ondemand-2"),
	}
	for k, v := range seed {
		if err := store.Put(ctx, k, v); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Prefix filter: on-demand for vshard-001.
	got, err := store.List(ctx, "vshard-001-ondemand-")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("prefix filter want 2, got %d: %+v", len(got), got)
	}
	for _, o := range got {
		if o.Size == 0 {
			t.Errorf("List entry %q zero size", o.Key)
		}
		if o.LastModified.IsZero() {
			t.Errorf("List entry %q zero LastModified", o.Key)
		}
		// Store-owned prefix must be stripped — caller works in
		// Put/Get key space.
		if len(o.Key) > 0 && o.Key[0] == '/' {
			t.Errorf("List key %q has leading slash; store prefix not stripped", o.Key)
		}
	}

	// Delete one on-demand key; subsequent List sees only the
	// remaining one.
	if err := store.Delete(ctx, "vshard-001-ondemand-1700000000000.pb"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = store.List(ctx, "vshard-001-ondemand-")
	if err != nil {
		t.Fatalf("List after Delete: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 after Delete, got %d: %+v", len(got), got)
	}
	if got[0].Key != "vshard-001-ondemand-1700000000001.pb" {
		t.Errorf("remaining key = %q, want vshard-001-ondemand-1700000000001.pb", got[0].Key)
	}

	// Delete idempotent: deleting an already-gone key is nil.
	if err := store.Delete(ctx, "vshard-001-ondemand-1700000000000.pb"); err != nil {
		t.Fatalf("Delete (already gone) = %v, want nil", err)
	}
}
