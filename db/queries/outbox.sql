-- file-service is the outbox DML owner (008-continuous-file-backup). The INSERT runs INSIDE
-- the same transaction as the `file` row (FR-001: write-then-record; no committed outbox
-- entry without a file row). The consumer (file-backup-service) reads/claims via a scoped role.

-- name: EnqueueBackupOutbox :exec
-- Record a content-hash backup hint for a newly stored/replaced object.
INSERT INTO file_backup_outbox ("fileId", "externalID", priority, "createdBy", "createdDate", size)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: NotifyBackupOutbox :exec
-- Best-effort post-commit wake for the backup worker after an outbox row is recorded. A lost
-- NOTIFY is covered by the durable table + the worker's poll floor, so callers ignore the error.
NOTIFY file_backup_outbox;

-- name: PruneBackupOutboxDone :execrows
-- Keep the outbox bounded (SC-008): drop rows the consumer already finished (status='done')
-- older than a retention cutoff. The ledger (file-backup-service's own DB) keeps the durable
-- record. Only 'done' is pruned — pending/in_progress/dead_letter/skipped are retained.
DELETE FROM file_backup_outbox
WHERE status = 'done' AND "createdDate" < $1;

-- name: DeleteBackupOutboxPendingByHash :execrows
-- Orphan hygiene: after file-service removes an unreferenced blob, ALL still-pending hints for
-- that hash point at bytes that no longer exist. Delete the whole orphan set, including hints for
-- sibling documents that shared the blob and were deleted earlier. The hash-wide delete is guarded
-- by the owner's live `file` table: if any document currently references the hash, no hint is
-- touched. This preserves a concurrent re-upload safely under PostgreSQL statement snapshots:
-- a re-upload committed before this statement is visible to NOT EXISTS and blocks the delete; one
-- committed after the snapshot has an outbox row invisible to this DELETE and therefore untouched.
-- in_progress rows are left to the consumer's own 404→skip backstop; done/skipped/dead_letter
-- are history, untouched.
DELETE FROM file_backup_outbox o
WHERE o."externalID" = $1 AND o.status = 'pending'
  AND NOT EXISTS (SELECT 1 FROM file f WHERE f."externalID" = $1);
