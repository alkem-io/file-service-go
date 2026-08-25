package alkemiodb

import (
	"encoding/json"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-service/internal/adapter/outbound/alkemiodb/queries"
	"github.com/alkem-io/file-service/internal/domain/model"
)

// parseContentMetadata produces the typed view of a content_metadata JSONB
// blob. Empty / `{}` → Populated=false. Otherwise Populated=true with the
// extracted dim/sentinel fields. Unknown JSON keys are tolerated (FR-017) —
// json.Unmarshal into the targeted struct ignores extras. Malformed JSON
// surfaces as Populated=true with no dims; we don't crash on bad bytes.
func parseContentMetadata(raw []byte) model.ContentMetadata {
	meta := model.ContentMetadata{}
	if len(raw) == 0 || string(raw) == `{}` {
		return meta // Populated=false
	}
	meta.Populated = true
	var v struct {
		ImageWidth   *int `json:"imageWidth,omitempty"`
		ImageHeight  *int `json:"imageHeight,omitempty"`
		DecodeFailed bool `json:"_decodeFailed,omitempty"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		// Malformed JSON: surface as Populated but no dims; future
		// revisits can decide whether to retry. Don't crash.
		return meta
	}
	meta.ImageWidth = v.ImageWidth
	meta.ImageHeight = v.ImageHeight
	meta.DecodeFailed = v.DecodeFailed
	return meta
}

func uuidToPgx(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

func uuidToPgxNullable(id *uuid.UUID) pgtype.UUID {
	if id == nil {
		return pgtype.UUID{Valid: false}
	}
	return pgtype.UUID{Bytes: *id, Valid: true}
}

// uuidToPgxNullableNil maps uuid.Nil to SQL NULL and every real UUID to a
// valid pgx UUID. Create and CreateWithOutbox both call createDocumentParams,
// so this is the single nullable-authorization mapping for both write paths.
func uuidToPgxNullableNil(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{Valid: false}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func pgxToUUID(id pgtype.UUID) uuid.UUID {
	if !id.Valid {
		return uuid.Nil
	}
	return id.Bytes
}

func pgxToUUIDNullable(id pgtype.UUID) *uuid.UUID {
	if !id.Valid {
		return nil
	}
	u := uuid.UUID(id.Bytes)
	return &u
}

func timeToPgx(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func timeToPgxNow() pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: time.Now(), Valid: true}
}

func pgxToTime(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time
}

func safeInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	return int32(n)
}

// documentRow is the subset of fields shared by every sqlc query row that
// materializes a full document record. sqlc emits a distinct Go type per
// query; Go structural conversion lets us map any row type into this one
// as long as the fields match exactly. Field names must match sqlc's
// generated casing (StorageBucketId, not StorageBucketID) — hence the
// nolint directives below.
type documentRow struct {
	ID                pgtype.UUID
	ExternalID        string
	MimeType          string
	Size              int32
	DisplayName       string
	CreatedBy         pgtype.UUID
	TemporaryLocation bool
	StorageBucketId   pgtype.UUID //nolint:staticcheck // ST1003: sqlc-generated casing must match for structural conversion
	AuthorizationId   pgtype.UUID //nolint:staticcheck // ST1003: sqlc-generated casing must match for structural conversion
	TagsetId          pgtype.UUID //nolint:staticcheck // ST1003: sqlc-generated casing must match for structural conversion
	CreatedDate       pgtype.Timestamptz
	UpdatedDate       pgtype.Timestamptz
	Version           int32
	ContentMetadata   []byte
}

func documentFromRow(r documentRow) model.Document {
	doc := model.Document{
		ID:                pgxToUUID(r.ID),
		ExternalID:        r.ExternalID,
		MimeType:          r.MimeType,
		Size:              int(r.Size),
		DisplayName:       r.DisplayName,
		CreatedBy:         pgxToUUIDNullable(r.CreatedBy),
		TemporaryLocation: r.TemporaryLocation,
		StorageBucketID:   pgxToUUID(r.StorageBucketId),
		AuthorizationID:   pgxToUUID(r.AuthorizationId),
		TagsetID:          pgxToUUIDNullable(r.TagsetId),
		CreatedDate:       pgxToTime(r.CreatedDate),
		UpdatedDate:       pgxToTime(r.UpdatedDate),
		Version:           int(r.Version),
	}
	doc.ContentMetadata = parseContentMetadata(r.ContentMetadata)
	// Mirror the dim fields onto the Document struct for handlers that
	// consume the wire shape directly. Sentinel rows (DecodeFailed=true)
	// surface as nil dims even though Populated=true.
	if !doc.ContentMetadata.DecodeFailed {
		doc.ImageWidth = doc.ContentMetadata.ImageWidth
		doc.ImageHeight = doc.ContentMetadata.ImageHeight
	}
	return doc
}

func rowToDocument(row queries.GetDocumentByIDRow) model.Document {
	return documentFromRow(documentRow(row))
}

func findRowToDocument(row queries.FindDocumentByExternalIDAndBucketRow) model.Document {
	return documentFromRow(documentRow(row))
}
