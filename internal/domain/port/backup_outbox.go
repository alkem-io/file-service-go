package port

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/alkem-io/file-service/internal/domain/model"
)

// BackupOutboxRepo is the transactional producer for the continuous-backup outbox
// (008-continuous-file-backup). Its write methods commit a document row AND its backup-outbox
// row in ONE transaction (FR-001: no committed outbox entry without a file row), then emit
// NOTIFY. A nil BackupOutboxRepo on the FileService means the producer is OFF (the flag default)
// and the plain DocumentRepo path runs instead — so this port is a pure opt-in overlay on the
// existing write path, never a behaviour change when disabled.
type BackupOutboxRepo interface {
	// CreateWithOutbox inserts a new document and enqueues its backup hint atomically.
	// Returns model.ErrDuplicateKey on any document unique violation (no outbox row written),
	// exactly as DocumentRepo.Create does, so service classification is identical.
	CreateWithOutbox(ctx context.Context, doc model.Document, contentMetadata model.ContentMetadata, priority int16) (uuid.UUID, error)
	// UpdateFileWithOutbox replaces a document's content and enqueues a backup hint for the new
	// content hash atomically, compare-and-set against expectedExternalID and expectedVersion. Same
	// error semantics as DocumentRepo.UpdateFile.
	UpdateFileWithOutbox(ctx context.Context, id uuid.UUID, expectedExternalID string, expectedVersion int, externalID, mimeType string, size int, contentMetadata model.ContentMetadata, priority int16) error
	// PromoteWithOutbox atomically applies a temporary→permanent metadata update and enqueues the
	// already-stored content. Without this path, the normal two-phase upload flow would exclude the
	// temporary create and then make the object permanent without ever producing a backup hint.
	PromoteWithOutbox(ctx context.Context, current model.Document, storageBucketID uuid.UUID, displayName string, priority int16) error
	// PruneBackupOutbox drops `done` outbox rows older than the cutoff, keeping the shared
	// outbox bounded (SC-008); returns the number pruned.
	PruneBackupOutbox(ctx context.Context, olderThan time.Time) (int64, error)
	// DeletePendingByHash drops every still-pending hint for a blob file-service has deleted.
	// The implementation must guard the hash-wide delete with an atomic absence check against the
	// live file table, so a concurrent re-upload never loses its newly-enqueued hint. Returns the
	// number removed.
	DeletePendingByHash(ctx context.Context, externalID string) (int64, error)
}
