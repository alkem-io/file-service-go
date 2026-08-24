// Package model holds the domain types of the file-service core — documents,
// content metadata, MIME classification helpers, auth results — plus the
// sentinel errors adapters translate their backend failures into. It depends
// on nothing but the standard library and uuid, so every other layer can
// import it freely.
package model

import (
	"time"

	"github.com/google/uuid"
)

// ContentMetadata captures the typed view of the file.content_metadata
// JSONB column. Empty (Populated=false) means the row has no metadata
// recorded. Populated=true with DecodeFailed=true means the decoder ran
// and confirmed the bytes are unreadable — a permanent sentinel; the
// dimension sweep MUST short-circuit on this. Populated=true with
// ImageWidth/ImageHeight non-nil is the normal measured case. Populated
// can be true with all other fields zero/nil for non-image rows.
type ContentMetadata struct {
	ImageWidth   *int
	ImageHeight  *int
	DecodeFailed bool
	// Populated tracks whether the JSONB column was non-empty (i.e. not
	// `{}`). A read of an empty `{}` column produces Populated=false; a
	// read of any non-empty JSON object — including {"_decodeFailed":
	// true} — produces Populated=true. The dimension sweep skips when
	// Populated=true regardless of dim fields.
	Populated bool
}

// IsEmpty reports whether this metadata represents an empty content_metadata
// column (the legacy state). Synonym for !m.Populated.
func (m ContentMetadata) IsEmpty() bool { return !m.Populated }

// Document represents a full document record from Alkemio's document table.
type Document struct {
	ID                uuid.UUID
	ExternalID        string
	MimeType          string
	Size              int
	DisplayName       string
	CreatedBy         *uuid.UUID
	TemporaryLocation bool
	StorageBucketID   uuid.UUID
	AuthorizationID   uuid.UUID
	TagsetID          *uuid.UUID
	CreatedDate       time.Time
	UpdatedDate       time.Time
	Version           int

	// Reused is set to true when a dedup lookup returned an existing row
	// instead of inserting a new one. Response-only; not persisted.
	// When Reused is true, the caller-supplied AuthorizationID and TagsetID
	// were ignored — the existing row's values are authoritative.
	Reused bool

	// ContentMetadata is the typed view of the file.content_metadata JSONB
	// column. Single source of truth for the row's stored metadata; the
	// ImageWidth / ImageHeight mirrors below are convenience fields for
	// the wire shape and stay in sync with this value.
	ContentMetadata ContentMetadata

	// ImageWidth and ImageHeight are post-rotation pixel dimensions for
	// raster images, SVG (from viewBox), and GIF (canvas dims). Both nil
	// when the upload is not an image, or when the row's content_metadata
	// is empty/sentinel. Mirror of ContentMetadata.ImageWidth/ImageHeight;
	// kept on the struct so handlers don't have to walk into the typed
	// metadata for the common dim-rendering path.
	ImageWidth  *int
	ImageHeight *int
}

// CreateDocumentInput contains fields needed to create a new document.
type CreateDocumentInput struct {
	DisplayName       string
	CreatedBy         *uuid.UUID
	TemporaryLocation bool
	StorageBucketID   uuid.UUID
	// AuthorizationID is optional for internal create callers. uuid.Nil
	// persists as SQL NULL; a real UUID references an authorization policy.
	// CopyDocumentInput intentionally remains required.
	AuthorizationID uuid.UUID
	TagsetID        *uuid.UUID

	// SkipDedup, when true, bypasses the per-bucket content-hash dedup
	// lookup and forces a fresh row insert even if an existing row in the
	// same bucket has the same externalID. Use case: placeholder uploads
	// (e.g., empty buffers for Collabora documents) where two logical
	// documents must NOT share a backing row even though their content
	// hashes match. Default false preserves the dedup behavior.
	SkipDedup bool
}

// CopyDocumentInput contains fields needed to copy an existing document into
// another bucket. The new row references the same content (same externalID,
// mimeType, size, displayName) as the source; only ownership/placement
// changes. The source row is not modified.
type CopyDocumentInput struct {
	DestinationBucketID uuid.UUID
	AuthorizationID     uuid.UUID
	TagsetID            *uuid.UUID
	CreatedBy           *uuid.UUID

	// SkipDedup mirrors the same flag on CreateDocumentInput. Default false
	// runs the per-bucket dedup lookup; true forces a fresh row insert.
	SkipDedup bool
}

// StoredFile represents the result of a file storage operation.
type StoredFile struct {
	ExternalID string
	MimeType   string
	Size       int
	Created    bool // true if a new file was written; false if dedup matched an existing file

	// ImageWidth and ImageHeight are post-rotation pixel dimensions for
	// the just-stored bytes, populated by StoreAndLink from the result of
	// Process. Nil for non-image content. Used by the Replace handler to
	// surface dims on ReplaceContentResponse without re-reading the row.
	ImageWidth  *int
	ImageHeight *int

	// ReplaceOutcome reports how StoreAndLink reconciled the MIME type
	// (accepted | fallback_generic_sniff). The HTTP adapter counts it in
	// content_replace_outcomes_total; empty for non-replace flows.
	ReplaceOutcome string
}

// AuthResult represents the outcome of an authorization check.
type AuthResult struct {
	Allowed bool
	Reason  string
}

// DeletedDocument contains IDs the caller needs for cleanup after deletion.
type DeletedDocument struct {
	ExternalID      string
	AuthorizationID uuid.UUID
	TagsetID        *uuid.UUID
}
