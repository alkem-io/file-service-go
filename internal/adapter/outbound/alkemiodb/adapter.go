// Package alkemiodb implements port.DocumentRepo against Alkemio's shared
// Postgres database using pgx and sqlc-generated queries. It owns the mapping
// between domain models and the document table rows — including the JSONB
// content_metadata serialization — and converts Postgres failure modes
// (no rows, unique violations) into the domain's sentinel errors.
package alkemiodb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"

	"github.com/alkem-io/file-service/internal/adapter/outbound/alkemiodb/queries"
	"github.com/alkem-io/file-service/internal/domain/model"
)

// Pool is the DB handle the adapter needs: the sqlc DBTX surface (Exec/Query/QueryRow) plus
// Begin, for the transactional backup-outbox writes (a document row + its outbox row committed
// together — 008-continuous-file-backup FR-001). Both *pgxpool.Pool and pgxmock's pool satisfy
// it, so the transactional path stays unit-testable.
type Pool interface {
	queries.DBTX
	// Begin starts a transaction for the transactional outbox writes.
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Adapter implements port.DocumentRepo (and port.BackupOutboxRepo) using pgx/sqlc.
type Adapter struct {
	queries *queries.Queries
	pool    Pool        // for Begin (the transactional outbox writes); nil-safe for the non-outbox methods
	logger  *zap.Logger // surfaces best-effort backup-outbox NOTIFY failures; never nil
}

// New creates an Adapter from a Begin-capable DBTX connection (pgxpool.Pool, pgxmock, etc.).
// The logger is a no-op — use NewWithLogger to surface best-effort NOTIFY failures.
func New(db Pool) *Adapter {
	return &Adapter{
		queries: queries.New(db),
		pool:    db,
		logger:  zap.NewNop(),
	}
}

// NewWithLogger is New with a Zap logger for best-effort backup-outbox NOTIFY warnings.
// A nil logger falls back to the no-op logger.
func NewWithLogger(db Pool, logger *zap.Logger) *Adapter {
	a := New(db)
	if logger != nil {
		a.logger = logger
	}
	return a
}

// GetByID implements port.DocumentRepo: pgx.ErrNoRows is translated to
// model.ErrDocumentNotFound; other database errors pass through unwrapped.
func (a *Adapter) GetByID(ctx context.Context, id uuid.UUID) (model.Document, error) {
	row, err := a.queries.GetDocumentByID(ctx, uuidToPgx(id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Document{}, model.ErrDocumentNotFound
		}
		return model.Document{}, err
	}
	return rowToDocument(row), nil
}

// FindByExternalIDAndBucket implements port.DocumentRepo's dedup lookup.
// No row for the (externalID, storageBucketID) pair → model.ErrDocumentNotFound.
func (a *Adapter) FindByExternalIDAndBucket(ctx context.Context, externalID string, storageBucketID uuid.UUID) (model.Document, error) {
	row, err := a.queries.FindDocumentByExternalIDAndBucket(ctx, queries.FindDocumentByExternalIDAndBucketParams{
		ExternalID:      externalID,
		StorageBucketId: uuidToPgx(storageBucketID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Document{}, model.ErrDocumentNotFound
		}
		return model.Document{}, err
	}
	return findRowToDocument(row), nil
}

// Create inserts a new document row, serializing contentMetadata into the
// JSONB content_metadata column (see marshalContentMetadata for the shape
// rules). Any unique violation surfaces as model.ErrDuplicateKey; the service
// probes (externalID, bucket) once to distinguish a content winner from an
// authorizationId/tagsetId conflict.
func (a *Adapter) Create(ctx context.Context, doc model.Document, contentMetadata model.ContentMetadata) (uuid.UUID, error) {
	raw, err := marshalContentMetadata(contentMetadata)
	if err != nil {
		return uuid.Nil, err
	}
	id, err := a.queries.CreateDocument(ctx, createDocumentParams(doc, raw))
	if err != nil {
		if isUniqueViolation(err) {
			return uuid.Nil, model.ErrDuplicateKey
		}
		return uuid.Nil, err
	}
	return pgxToUUID(id), nil
}

// createDocumentParams maps a domain document (+ serialized content_metadata) to the sqlc
// insert params — the ONE owner of that mapping, shared by Create and the transactional
// CreateWithOutbox (so the two can't drift on the row shape).
func createDocumentParams(doc model.Document, raw []byte) queries.CreateDocumentParams {
	return queries.CreateDocumentParams{
		ID:                uuidToPgx(doc.ID),
		ExternalID:        doc.ExternalID,
		MimeType:          doc.MimeType,
		Size:              safeInt32(doc.Size),
		DisplayName:       doc.DisplayName,
		CreatedBy:         uuidToPgxNullable(doc.CreatedBy),
		TemporaryLocation: doc.TemporaryLocation,
		StorageBucketId:   uuidToPgx(doc.StorageBucketID),
		// uuid.Nil means the create caller intentionally omitted authorization.
		// Persist SQL NULL: the FK cannot reference the all-zero UUID, while the
		// UNIQUE column permits multiple NULL rows.
		AuthorizationId: uuidToPgxNullableNil(doc.AuthorizationID),
		TagsetId:        uuidToPgxNullable(doc.TagsetID),
		CreatedDate:     timeToPgx(doc.CreatedDate),
		UpdatedDate:     timeToPgx(doc.UpdatedDate),
		ContentMetadata: raw,
	}
}

// isUniqueViolation reports whether err is any Postgres unique-constraint violation.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

// UpdateFile rewrites the content fields (externalID, mimeType, size) plus
// content_metadata in one statement — the Replace flow. Returns
// model.ErrDuplicateKey when the new content hash collides with another row
// in the same bucket, and model.ErrDocumentNotFound when the row is gone or
// expectedExternalID/version no longer match (a concurrent content or metadata
// mutation won).
func (a *Adapter) UpdateFile(ctx context.Context, id uuid.UUID, expectedExternalID string, expectedVersion int, externalID, mimeType string, size int, contentMetadata model.ContentMetadata) error {
	raw, err := marshalContentMetadata(contentMetadata)
	if err != nil {
		return err
	}
	rows, err := a.queries.UpdateDocumentFile(ctx, updateFileParams(id, expectedExternalID, expectedVersion, externalID, mimeType, size, raw))
	if err != nil {
		if isUniqueViolation(err) {
			return model.ErrDuplicateKey
		}
		return err
	}
	if rows == 0 {
		return model.ErrDocumentNotFound
	}
	return nil
}

// updateFileParams maps the Replace content fields to the sqlc update params — the ONE owner,
// shared by UpdateFile and the transactional UpdateFileWithOutbox.
func updateFileParams(id uuid.UUID, expectedExternalID string, expectedVersion int, externalID, mimeType string, size int, raw []byte) queries.UpdateDocumentFileParams {
	return queries.UpdateDocumentFileParams{
		ID:              uuidToPgx(id),
		ExternalID:      externalID,
		MimeType:        mimeType,
		Size:            safeInt32(size),
		UpdatedDate:     timeToPgxNow(),
		ContentMetadata: raw,
		ExternalID_2:    expectedExternalID,
		Version:         safeInt32(expectedVersion),
	}
}

// UpdateMetadata mutates bucket/temporaryLocation/displayName under
// optimistic locking (WHERE version = $given, SET version = version + 1).
// 0 rows affected — row missing or version stale — returns
// model.ErrDocumentNotFound; the service maps that to its conflict error.
func (a *Adapter) UpdateMetadata(ctx context.Context, id uuid.UUID, storageBucketID uuid.UUID, temporaryLocation bool, displayName string, version int) error {
	rows, err := a.queries.UpdateDocumentMetadata(ctx, queries.UpdateDocumentMetadataParams{
		ID:                uuidToPgx(id),
		StorageBucketId:   uuidToPgx(storageBucketID),
		TemporaryLocation: temporaryLocation,
		DisplayName:       displayName,
		UpdatedDate:       timeToPgxNow(),
		Version:           safeInt32(version),
	})
	if err != nil {
		// Defensive: keeps PATCH consistent with Create/UpdateFile if a
		// uniqueness constraint is added later (e.g. (externalID,
		// storageBucketId)). No such constraint exists in this repo's
		// db/schema/document.sql today, but the production schema can
		// diverge, and a 409 beats a 500 if it fires.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return model.ErrDuplicateKey
		}
		return err
	}
	if rows == 0 {
		return model.ErrDocumentNotFound
	}
	return nil
}

// BackfillContentMetadata persists computed content_metadata on a row without
// bumping version. Compare-and-set: only writes when content_metadata is still
// empty AND the row's externalID matches expectedExternalID. A 0-rows-affected
// outcome means another writer (Replace, or another backfill) won the race; we
// report that separately from a database error so sweep accounting stays honest.
// FR-018.
func (a *Adapter) BackfillContentMetadata(ctx context.Context, id uuid.UUID, expectedExternalID string, contentMetadata model.ContentMetadata) (bool, error) {
	raw, err := marshalContentMetadata(contentMetadata)
	if err != nil {
		return false, err
	}
	// Defensive: writing "{}" would leave the row matching the sweep
	// predicate (content_metadata = '{}'::jsonb), re-arming the helper to
	// retry on every subsequent sweep. Service layer never reaches
	// here with an empty-equivalent value, but skip explicitly to keep the
	// backfill loop closed even if a future caller misuses this method.
	if string(raw) == `{}` {
		return false, nil
	}
	rowsAffected, err := a.queries.BackfillContentMetadata(ctx, queries.BackfillContentMetadataParams{
		ID:              uuidToPgx(id),
		ContentMetadata: raw,
		ExternalID:      expectedExternalID,
	})
	if err != nil {
		return false, err
	}
	return rowsAffected > 0, nil
}

// marshalContentMetadata serializes the typed value to the JSONB shape stored
// in the file.content_metadata column. Empty Populated → "{}". DecodeFailed
// → {"_decodeFailed": true}. Both dims set → {"imageWidth": N, "imageHeight":
// M}. Other shapes (e.g. non-image rows where Populated=true) default to "{}".
//
// Rejects half-populated dims (exactly one of ImageWidth / ImageHeight set):
// silently serializing those as "{}" would round-trip the row as
// "unmeasured" and trigger every later sweep — masking what is
// almost certainly a caller bug.
func marshalContentMetadata(meta model.ContentMetadata) ([]byte, error) {
	if !meta.Populated {
		return []byte(`{}`), nil
	}
	if meta.DecodeFailed {
		return []byte(`{"_decodeFailed":true}`), nil
	}
	if (meta.ImageWidth == nil) != (meta.ImageHeight == nil) {
		return nil, errors.New("content metadata: ImageWidth and ImageHeight must both be set or both nil")
	}
	if meta.ImageWidth != nil && meta.ImageHeight != nil {
		return json.Marshal(struct {
			ImageWidth  int `json:"imageWidth"`
			ImageHeight int `json:"imageHeight"`
		}{*meta.ImageWidth, *meta.ImageHeight})
	}
	return []byte(`{}`), nil
}

// Delete removes the row and returns the cleanup identifiers (content hash,
// authorization id, optional tagset id) from the DELETE ... RETURNING.
// A missing row returns model.ErrDocumentNotFound.
func (a *Adapter) Delete(ctx context.Context, id uuid.UUID) (model.DeletedDocument, error) {
	row, err := a.queries.DeleteDocument(ctx, uuidToPgx(id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.DeletedDocument{}, model.ErrDocumentNotFound
		}
		return model.DeletedDocument{}, err
	}
	return model.DeletedDocument{
		ExternalID:      row.ExternalID,
		AuthorizationID: pgxToUUID(row.AuthorizationId),
		TagsetID:        pgxToUUIDNullable(row.TagsetId),
	}, nil
}

// CountByExternalID counts rows referencing a content hash across all
// buckets — the refcount that decides whether the underlying blob may be
// physically deleted.
func (a *Adapter) CountByExternalID(ctx context.Context, externalID string) (int, error) {
	count, err := a.queries.CountDocumentsByExternalID(ctx, externalID)
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// ListByMimeTypes returns documents whose stored MIME type is one of the
// given values (spec 019 repair-job scan). Partial rows: only identity and
// content-summary fields are populated.
func (a *Adapter) ListByMimeTypes(ctx context.Context, mimeTypes []string) ([]model.Document, error) {
	rows, err := a.queries.ListDocumentsByMimeTypes(ctx, mimeTypes)
	if err != nil {
		return nil, err
	}
	docs := make([]model.Document, 0, len(rows))
	for _, r := range rows {
		docs = append(docs, model.Document{
			ID:          pgxToUUID(r.ID),
			ExternalID:  r.ExternalID,
			MimeType:    r.MimeType,
			Size:        int(r.Size),
			DisplayName: r.DisplayName,
		})
	}
	return docs, nil
}

// ListImagesNeedingDims implements port.DocumentRepo — a keyset-paged scan of image rows with
// unpopulated content_metadata, feeding the dims backfill sweep (the sweep-dims job).
func (a *Adapter) ListImagesNeedingDims(ctx context.Context, afterID uuid.UUID, limit int32) ([]model.Document, error) {
	rows, err := a.queries.ListImagesNeedingDims(ctx, queries.ListImagesNeedingDimsParams{
		ID:    uuidToPgx(afterID),
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	docs := make([]model.Document, 0, len(rows))
	for _, r := range rows {
		docs = append(docs, model.Document{
			ID:         pgxToUUID(r.ID),
			ExternalID: r.ExternalID,
			MimeType:   r.MimeType,
		})
	}
	return docs, nil
}

// UpdateMimeType corrects only the stored MIME type (spec 019 repair-job
// relabel). Compare-and-set on externalID: 0 rows affected means the row's
// content changed concurrently (or the row is gone) — reported as
// (false, nil) so the caller skips instead of overwriting a fresher value.
func (a *Adapter) UpdateMimeType(ctx context.Context, id uuid.UUID, expectedExternalID, mimeType string) (bool, error) {
	rows, err := a.queries.UpdateDocumentMimeType(ctx, queries.UpdateDocumentMimeTypeParams{
		ID:          uuidToPgx(id),
		MimeType:    mimeType,
		UpdatedDate: timeToPgxNow(),
		ExternalID:  expectedExternalID,
	})
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}
