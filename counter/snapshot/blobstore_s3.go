// S3BlobStore stores snapshots in an S3-compatible object store (AWS S3,
// MinIO, …). Objects are addressed as `<prefix><key>`; prefix is the
// caller's responsibility to terminate (or not) with a slash.
//
// Atomicity: S3 PutObject replaces the whole object in a single request —
// concurrent readers see either the old or the new object, never a partial
// write. No tmp-and-rename dance is needed.
//
// Credentials / region come from the AWS SDK default config chain
// (environment, IAM role, shared profile). Callers build an *s3.Client and
// hand it in so tests can inject custom endpoints (MinIO) without this
// package pulling in SDK config surface.

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3BlobStore is a BlobStore backed by an S3 bucket.
type S3BlobStore struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3BlobStore wraps a prepared *s3.Client. bucket is required; prefix
// may be empty.
func NewS3BlobStore(client *s3.Client, bucket, prefix string) *S3BlobStore {
	return &S3BlobStore{client: client, bucket: bucket, prefix: prefix}
}

// Put uploads data via PutObject. Body is a bytes.Reader so the SDK can
// retry (needs io.Seeker) and compute Content-Length up front.
func (s *S3BlobStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s/%s%s: %w", s.bucket, s.prefix, key, err)
	}
	return nil
}

// Get fetches the object. A missing key surfaces as os.ErrNotExist so
// snapshot.Load can treat it as a cold-start signal.
func (s *S3BlobStore) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, os.ErrNotExist
		}
		// S3 returns NotFound on HeadObject-shaped 404s too; GetObject
		// normally raises NoSuchKey but some compat implementations emit
		// the generic variant.
		var nf *s3types.NotFound
		if errors.As(err, &nf) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("s3 get %s/%s%s: %w", s.bucket, s.prefix, key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 read body: %w", err)
	}
	return data, nil
}
