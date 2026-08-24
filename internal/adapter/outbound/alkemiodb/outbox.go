package alkemiodb

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/file-service/internal/adapter/outbound/alkemiodb/queries"
	"github.com/alkem-io/file-service/internal/domain/model"
)

// CreateWithOutbox inserts a document row AND enqueues its backup-outbox row in ONE transaction
// (008-continuous-file-backup FR-001: no committed outbox entry without a file row, and every
// committed non-temporary file row carries its backup hint). After commit it emits a best-effort
// NOTIFY so the backup worker wakes immediately — the durable table + the worker's poll floor
// cover a lost NOTIFY. A unique violation on the document → model.ErrDuplicateKey with NO outbox
// row written; the service performs the same one-shot content-winner/conflict classification as
// on the non-outbox path.
func (a *Adapter) CreateWithOutbox(ctx context.Context, doc model.Document, contentMetadata model.ContentMetadata, priority int16) (uuid.UUID, error) {
	raw, err := marshalContentMetadata(contentMetadata)
	if err != nil {
		return uuid.Nil, err
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit
	q := a.queries.WithTx(tx)

	id, err := q.CreateDocument(ctx, createDocumentParams(doc, raw))
	if err != nil {
		if isUniqueViolation(err) {
			return uuid.Nil, model.ErrDuplicateKey
		}
		return uuid.Nil, err
	}
	if err := q.EnqueueBackupOutbox(ctx, queries.EnqueueBackupOutboxParams{
		FileId:      uuidToPgx(doc.ID),
		ExternalID:  doc.ExternalID,
		Priority:    priority,
		CreatedBy:   uuidToPgxNullable(doc.CreatedBy),
		CreatedDate: timeToPgx(doc.CreatedDate),
		Size:        int64(doc.Size),
	}); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	a.notifyBackup(ctx)
	return pgxToUUID(id), nil
}

// UpdateFileWithOutbox rewrites a document's content fields AND enqueues a backup-outbox row for
// the NEW externalID in ONE transaction (a replace = a new content hash = a new object to back
// up). The update compare-and-sets expectedExternalID, and
// model.ErrDuplicateKey / model.ErrDocumentNotFound are surfaced exactly as UpdateFile does.
// The outbox breadcrumb: createdBy is null (unknown on a replace) and createdDate is enqueue time
// (now) — the RPO-lag semantics the consumer's backlog gauge expects.
func (a *Adapter) UpdateFileWithOutbox(ctx context.Context, id uuid.UUID, expectedExternalID string, expectedVersion int, externalID, mimeType string, size int, contentMetadata model.ContentMetadata, priority int16) error {
	raw, err := marshalContentMetadata(contentMetadata)
	if err != nil {
		return err
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := a.queries.WithTx(tx)

	rows, err := q.UpdateDocumentFile(ctx, updateFileParams(id, expectedExternalID, expectedVersion, externalID, mimeType, size, raw))
	if err != nil {
		if isUniqueViolation(err) {
			return model.ErrDuplicateKey
		}
		return err
	}
	if rows == 0 {
		return model.ErrDocumentNotFound
	}
	if err := q.EnqueueBackupOutbox(ctx, queries.EnqueueBackupOutboxParams{
		FileId:      uuidToPgx(id),
		ExternalID:  externalID,
		Priority:    priority,
		CreatedBy:   uuidToPgxNullable(nil), // no actor breadcrumb on a content replace
		CreatedDate: timeToPgxNow(),
		Size:        int64(size),
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	a.notifyBackup(ctx)
	return nil
}

// PromoteWithOutbox makes an already-stored temporary document permanent and enqueues its current
// content in the same transaction. UpdateDocumentMetadata's version guard serializes promotion
// with content replacement; content replacement also bumps version, so whichever commits first
// forces the stale operation to retry with fresh temporary/content state.
func (a *Adapter) PromoteWithOutbox(ctx context.Context, current model.Document, storageBucketID uuid.UUID, displayName string, priority int16) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := a.queries.WithTx(tx)

	rows, err := q.UpdateDocumentMetadata(ctx, queries.UpdateDocumentMetadataParams{
		ID:                uuidToPgx(current.ID),
		StorageBucketId:   uuidToPgx(storageBucketID),
		TemporaryLocation: false,
		DisplayName:       displayName,
		UpdatedDate:       timeToPgxNow(),
		Version:           safeInt32(current.Version),
	})
	if err != nil {
		if isUniqueViolation(err) {
			return model.ErrDuplicateKey
		}
		return err
	}
	if rows == 0 {
		return model.ErrDocumentNotFound
	}
	if err := q.EnqueueBackupOutbox(ctx, queries.EnqueueBackupOutboxParams{
		FileId:      uuidToPgx(current.ID),
		ExternalID:  current.ExternalID,
		Priority:    priority,
		CreatedBy:   uuidToPgxNullable(current.CreatedBy),
		CreatedDate: timeToPgxNow(),
		Size:        int64(current.Size),
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	a.notifyBackup(ctx)
	return nil
}

// PruneBackupOutbox deletes `done` outbox rows older than the cutoff, keeping the shared outbox
// bounded (SC-008). file-service owns the outbox DML; the durable record lives in the ledger.
func (a *Adapter) PruneBackupOutbox(ctx context.Context, olderThan time.Time) (int64, error) {
	return a.queries.PruneBackupOutboxDone(ctx, timeToPgx(olderThan))
}

// DeletePendingByHash implements port.BackupOutboxRepo (orphan hygiene — see the query doc).
func (a *Adapter) DeletePendingByHash(ctx context.Context, externalID string) (int64, error) {
	return a.queries.DeleteBackupOutboxPendingByHash(ctx, externalID)
}

// notifyBackup emits NOTIFY on the backup channel after a committed enqueue. Best-effort — it
// must NOT fail a committed write, and the durable table + the consumer's poll floor guarantee
// progress if the notification is lost — so the error is not propagated. It IS logged at warn,
// though: a persistently failing NOTIFY (e.g. a permissions issue) should be visible rather than
// silently dropped.
func (a *Adapter) notifyBackup(ctx context.Context) {
	if err := a.queries.NotifyBackupOutbox(ctx); err != nil {
		a.logger.Warn("backup-outbox NOTIFY failed (best-effort; the consumer's poll floor still drains)",
			zap.Error(err))
	}
}
