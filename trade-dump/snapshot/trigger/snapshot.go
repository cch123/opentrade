// Package trigger is the producer-owned wire format + I/O for the
// trigger snapshot per ADR-0067. trade-dump's trigger shadow pipeline
// writes here; trigger startup reads here once ADR-0067 M5 lands.
//
// The wire-format type itself lives in api/gen/snapshot
// (TriggerSnapshot proto); this package contributes only the
// encode/decode dispatch and the BlobStore I/O wrappers, mirroring
// trade-dump/snapshot/counter for ShardSnapshot.
package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"

	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	"github.com/xargin/opentrade/pkg/snapshot"
)

// Save encodes snap and writes it under `baseKey + format.Ext()`. The
// BlobStore is responsible for atomicity (FSBlobStore stages through
// .tmp + fsync + rename).
func Save(ctx context.Context, store snapshot.BlobStore, baseKey string, snap *snapshotpb.TriggerSnapshot, format snapshot.Format) error {
	data, err := Encode(snap, format)
	if err != nil {
		return fmt.Errorf("encode %s: %w", format, err)
	}
	key := baseKey + format.Ext()
	if err := store.Put(ctx, key, data); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// Load fetches a snapshot by probing proto then json under baseKey
// (ADR-0049). Returns os.ErrNotExist when neither variant is present
// — callers branch on it for cold start.
func Load(ctx context.Context, store snapshot.BlobStore, baseKey string) (*snapshotpb.TriggerSnapshot, error) {
	for _, format := range []snapshot.Format{snapshot.FormatProto, snapshot.FormatJSON} {
		key := baseKey + format.Ext()
		data, err := store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("get %s: %w", key, err)
		}
		var snap snapshotpb.TriggerSnapshot
		if err := Decode(data, format, &snap); err != nil {
			return nil, fmt.Errorf("decode %s: %w", key, err)
		}
		return &snap, nil
	}
	return nil, os.ErrNotExist
}

// Encode dispatches to the format-specific marshaler.
func Encode(snap *snapshotpb.TriggerSnapshot, format snapshot.Format) ([]byte, error) {
	switch format {
	case snapshot.FormatProto:
		return proto.Marshal(snap)
	case snapshot.FormatJSON:
		return json.MarshalIndent(snap, "", "  ")
	default:
		return nil, fmt.Errorf("trigger snapshot: unknown format %d", format)
	}
}

// Decode dispatches to the format-specific unmarshaler. dst must be
// non-nil; the function fills it in place.
func Decode(data []byte, format snapshot.Format, dst *snapshotpb.TriggerSnapshot) error {
	if dst == nil {
		return errors.New("trigger snapshot: nil destination")
	}
	switch format {
	case snapshot.FormatProto:
		return proto.Unmarshal(data, dst)
	case snapshot.FormatJSON:
		return json.Unmarshal(data, dst)
	default:
		return fmt.Errorf("trigger snapshot: unknown format %d", format)
	}
}
