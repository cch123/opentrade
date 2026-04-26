// Package snapshot is the shared snapshot infrastructure. Per ADR-0066 §4
// it contributes only the genuinely service-agnostic pieces:
//
//   - Format: the on-disk encoding token (proto / json) and its file
//     extension (ADR-0049).
//   - BlobStore: the byte-level Put / Get / List / Delete contract,
//     plus FSBlobStore (local fs, atomic tmp+rename) and S3BlobStore
//     (ADR-0058).
//
// Service-specific snapshot types and the state ↔ snapshot conversion
// live with their producer (trade-dump, the projection platform per
// ADR-0066) — see trade-dump/snapshot/counter and, after ADR-0067,
// trade-dump/snapshot/trigger. Consumers (counter, trigger) keep a
// thin loadSnapshot wrapper inside the service.
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
