// Package snapshot is the shared infrastructure consumers use to persist
// service-specific snapshots. It contributes two pieces:
//
//   - Format: the on-disk encoding token (proto / json) and its file
//     extension (ADR-0049).
//   - BlobStore: the byte-level Put / Get / List / Delete contract,
//     plus FSBlobStore (local fs, atomic tmp+rename) and S3BlobStore
//     (ADR-0058).
//
// Service-specific snapshot types live alongside their owners:
//
//   - pkg/snapshot/counter — Counter ShardSnapshot (also produced by
//     trade-dump's shadow engine via ADR-0061).
//   - pkg/snapshot/trigger — trigger engine Snapshot.
//
// Each subpackage owns its own Save/Load shape and proto mapping; this
// top-level package stays type-neutral so new consumers don't need to
// reach into another service's snapshot module.
package snapshot

import "fmt"

// Format names the on-disk encoding. ADR-0049.
type Format int

const (
	// FormatProto is the default binary encoding (file extension .pb).
	FormatProto Format = iota
	// FormatJSON is the debug-friendly text encoding (file extension .json).
	FormatJSON
)

// String returns the canonical CLI token ("proto" / "json").
func (f Format) String() string {
	switch f {
	case FormatProto:
		return "proto"
	case FormatJSON:
		return "json"
	default:
		return "unknown"
	}
}

// Ext returns the file extension (including the leading dot).
func (f Format) Ext() string {
	switch f {
	case FormatProto:
		return ".pb"
	case FormatJSON:
		return ".json"
	default:
		return ""
	}
}

// ParseFormat parses a CLI token back to Format. Empty / unrecognised input
// returns an error; the caller is expected to surface it as an invalid-flag
// error at startup.
func ParseFormat(s string) (Format, error) {
	switch s {
	case "proto", "pb", "protobuf":
		return FormatProto, nil
	case "json":
		return FormatJSON, nil
	default:
		return 0, fmt.Errorf("snapshot: unknown format %q (want proto|json)", s)
	}
}
