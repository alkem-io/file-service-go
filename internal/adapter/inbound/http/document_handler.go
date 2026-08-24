// Package http is the inbound HTTP adapter: chi routing, the internal
// document CRUD + streaming-ingest endpoints, the public file-serve endpoint,
// health/liveness probes, middleware (request ID, actor identity, logging),
// and the expvar resilience metrics. It translates HTTP requests into domain
// service calls and domain errors back into stable status codes and JSON
// bodies; no business rules live here.
package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/file-service/internal/domain/model"
	"github.com/alkem-io/file-service/internal/domain/port"
	"github.com/alkem-io/file-service/internal/domain/service"
)

// DocumentHandler handles all internal document endpoints.
type DocumentHandler struct {
	Service *service.FileService
	MaxAge  int
	Logger  *zap.Logger

	// Streaming-ingest guards (spec 020). Zero values fall back to the
	// historical defaults (32 MiB cap, 30 s idle).
	MaxUploadSize int64
	IdleTimeout   time.Duration
}

// GetMeta handles GET /internal/file/{id}/meta
func (h *DocumentHandler) GetMeta(w http.ResponseWriter, r *http.Request) {
	docID, err := parseDocID(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "invalid document ID")
		return
	}

	doc, err := h.Service.Repo.GetByID(r.Context(), docID)
	if err != nil {
		if errors.Is(err, model.ErrDocumentNotFound) {
			writeJSONError(w, http.StatusNotFound, "document not found")
			return
		}
		h.Logger.Error("failed to lookup document", zap.Error(err))
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}

	documentMetaResponse(doc).Render(w)
}

// GetContent handles GET /internal/file/{id}/content
func (h *DocumentHandler) GetContent(w http.ResponseWriter, r *http.Request) {
	docID, err := parseDocID(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "invalid document ID")
		return
	}

	doc, err := h.Service.Repo.GetByID(r.Context(), docID)
	if err != nil {
		if errors.Is(err, model.ErrDocumentNotFound) {
			writeJSONError(w, http.StatusNotFound, "document not found")
			return
		}
		h.Logger.Error("failed to lookup document", zap.Error(err))
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}

	rc, size, err := h.Service.Storage.ReadStream(doc.ExternalID)
	if err != nil {
		writeStorageReadError(w, h.Logger, err, "file not found on storage", "failed to read file from storage")
		return
	}

	w.Header().Set("Content-Type", doc.MimeType)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	// Streamed (constant memory) — a stored key, so a malformed one is a 500 (server data), never 400.
	streamBlob(w, h.Logger, rc, size, doc.ExternalID)
}

// GetBlobContent handles GET /internal/blob/{hash}/content — a content-addressed
// read. It returns the immutable blob stored under a SHA3-256 content hash,
// independent of any document row. The store is already content-addressed
// (Storage.Read keys on the hash); this exposes that read over the API.
//
// This is the read path the backup/replication worker uses (workspace#008,
// FR-008): the outbox records an object's content hash, and the blob under a
// hash never changes, so a fetch-by-hash is version-exact — it returns the
// EXACT bytes that were enqueued, never whichever version the (mutable)
// document currently points at, which is what GET /file/{id}/content returns.
// A hash with no blob (e.g. a version superseded and refcount-deleted) is a
// clean 404, distinguishable by the caller from a transport failure.
func (h *DocumentHandler) GetBlobContent(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")

	// Storage owns key validation (one definition — the deliberately legacy-CID-permissive
	// isValidExternalID), so the handler never drifts from what the store serves; the shared
	// writeStorageReadError maps the failure. file-service does NOT try to tell an absent blob
	// from a store outage (storage can't, so it doesn't claim to) — a non-ENOENT backend error
	// is a retryable 500; a 404 is just "no such blob" and the worker decides what that means.
	rc, size, err := h.Service.Storage.ReadStream(hash)
	if err != nil {
		// The key here is the client's URL {hash}, so a malformed one is a 400 (client error).
		// This is the ONLY read path where that's true — GetContent/ServeDocument read a stored
		// key — so the 400 lives here, not in the shared helper (keeps it off their API contract).
		if errors.Is(err, port.ErrInvalidKey) {
			writeJSONError(w, http.StatusBadRequest, "invalid content hash")
			return
		}
		writeStorageReadError(w, h.Logger, err, "blob not found", "failed to read blob from storage")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff") // raw bytes, never a document to render
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	// Streamed (constant memory) so a bulk parallel reader can't drive RSS to N×blobsize; streamBlob
	// classifies a mid-stream failure (backend fault / short read → abort; client disconnect → silent).
	streamBlob(w, h.Logger, rc, size, hash)
}

// readErrCapture records a non-EOF error from the wrapped reader so a caller can tell a source
// (backend) read failure from a destination (client) write failure after io.Copy returns — which
// io.Copy itself does not distinguish.
type readErrCapture struct {
	r   io.Reader
	err error
}

func (c *readErrCapture) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		c.err = err
	}
	return n, err
}

// createFields collects the multipart metadata fields by name (spec 020:
// the fields trail the file part — research R4). Typed struct rather than a
// map: direct field reads keep generated API docs clean (map-index string
// literals are misinferred as path parameters by the spec generator).
type createFields struct {
	displayName       string
	storageBucketID   string
	authorizationID   string
	tagsetID          string
	createdBy         string
	temporaryLocation string
	skipDedup         string
	allowedMimeTypes  string
	maxFileSize       string
}

func (f *createFields) set(name, value string) {
	switch name {
	case "displayName":
		f.displayName = value
	case "storageBucketId":
		f.storageBucketID = value
	case "authorizationId":
		f.authorizationID = value
	case "tagsetId":
		f.tagsetID = value
	case "createdBy":
		f.createdBy = value
	case "temporaryLocation":
		f.temporaryLocation = value
	case "skipDedup":
		f.skipDedup = value
	case "allowedMimeTypes":
		f.allowedMimeTypes = value
	case "maxFileSize":
		f.maxFileSize = value
	}
}

// parseOptionalUUID parses an optional UUID form field: empty means absent
// (nil, nil). The error names the field so it can serve directly as a 400
// body.
func parseOptionalUUID(value, field string) (*uuid.UUID, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", field)
	}
	return &parsed, nil
}

// parseCreateAuthorizationID accepts omission as uuid.Nil (the domain's
// explicit SQL-NULL signal) but rejects an explicitly supplied all-zero UUID.
func parseCreateAuthorizationID(value string) (uuid.UUID, error) {
	if value == "" {
		return uuid.Nil, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, fmt.Errorf("invalid authorizationId")
	}
	return parsed, nil
}

// parseOptionalBool parses an optional boolean form field: empty means
// false. The error names the field so it can serve directly as a 400 body.
func parseOptionalBool(value, field string) (bool, error) {
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid %s: must be true or false", field)
	}
	return parsed, nil
}

// buildCreateInput validates the collected metadata fields after the upload
// has been staged (research R4).
func buildCreateInput(fields createFields) (input model.CreateDocumentInput, allowedMimeTypes []string, maxFileSize int, err error) {
	if err := validateDisplayName(fields.displayName); err != nil {
		return input, nil, 0, err
	}

	storageBucketID, err := uuid.Parse(fields.storageBucketID)
	if err != nil {
		return input, nil, 0, fmt.Errorf("invalid storageBucketId")
	}

	authorizationID, err := parseCreateAuthorizationID(fields.authorizationID)
	if err != nil {
		return input, nil, 0, err
	}

	tagsetID, err := parseOptionalUUID(fields.tagsetID, "tagsetId")
	if err != nil {
		return input, nil, 0, err
	}

	createdBy, err := parseOptionalUUID(fields.createdBy, "createdBy")
	if err != nil {
		return input, nil, 0, err
	}

	temporaryLocation, err := parseOptionalBool(fields.temporaryLocation, "temporaryLocation")
	if err != nil {
		return input, nil, 0, err
	}

	skipDedup, err := parseOptionalBool(fields.skipDedup, "skipDedup")
	if err != nil {
		return input, nil, 0, err
	}

	if v := fields.allowedMimeTypes; v != "" {
		parts := strings.Split(v, ",")
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				allowedMimeTypes = append(allowedMimeTypes, trimmed)
			}
		}
	}

	if v := fields.maxFileSize; v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 0 {
			return input, nil, 0, fmt.Errorf("invalid maxFileSize: must be a non-negative integer")
		}
		maxFileSize = parsed
	}

	input = model.CreateDocumentInput{
		DisplayName:       fields.displayName,
		CreatedBy:         createdBy,
		TemporaryLocation: temporaryLocation,
		StorageBucketID:   storageBucketID,
		AuthorizationID:   authorizationID,
		TagsetID:          tagsetID,
		SkipDedup:         skipDedup,
	}
	return input, allowedMimeTypes, maxFileSize, nil
}

// writeIngestTransportError maps a streaming transport failure to its HTTP
// response and outcome counter (spec 020 FR-008).
func (h *DocumentHandler) writeIngestTransportError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrOverLimit):
		IngestOutcomes.Add("rejected_over_limit", 1)
		writeJSONError(w, http.StatusRequestEntityTooLarge, "upload exceeds the configured size limit")
	case errors.Is(err, service.ErrStalled):
		IngestOutcomes.Add("stalled", 1)
		writeJSONError(w, http.StatusRequestTimeout, "upload stalled: no bytes received within the idle timeout")
	case errors.As(err, new(*service.MimeMismatchError)), errors.Is(err, service.ErrEmptyContent):
		// replace-path semantics handled by the caller; not transport
		writeJSONError(w, http.StatusInternalServerError, "internal error")
	default:
		// Client went away (connection reset, unexpected EOF, multipart
		// truncation): nothing useful to send, but try.
		IngestOutcomes.Add("client_abort", 1)
		writeJSONError(w, http.StatusBadRequest, "upload aborted before completion")
	}
}

// Create handles POST /internal/file — streaming ingest (spec 020): the
// file part flows request → sniff → (transcode) → staged storage without
// whole-file buffering. Parts are processed in whatever order they arrive;
// metadata validation always happens after the loop, before the stage is
// published. (The known caller sends the file part first — research R4 —
// which is why bucket-level limits cannot gate the stream early; the
// handler itself does not depend on any ordering.)
func (h *DocumentHandler) Create(w http.ResponseWriter, r *http.Request) {
	capBytes := h.MaxUploadSize
	if capBytes <= 0 {
		capBytes = 32 << 20
	}
	idle := h.IdleTimeout
	if idle <= 0 {
		idle = 30 * time.Second
	}
	pr := newProgressReader(w, r.Body, capBytes, idle)
	defer func() { _ = pr.Close() }()
	r.Body = pr

	mr, err := r.MultipartReader()
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	var fields createFields
	var staged *service.StagedUpload
	defer func() {
		// Any non-success exit path discards the stage (FR-006); Discard
		// after CompleteUpload is a no-op.
		staged.Discard()
	}()

	for {
		part, perr := mr.NextPart()
		if errors.Is(perr, io.EOF) {
			break
		}
		if perr != nil {
			h.writeIngestTransportError(w, perr)
			return
		}

		if part.FormName() == "file" {
			if staged != nil {
				writeJSONError(w, http.StatusBadRequest, "duplicate file part")
				return
			}
			declaredMIME := part.Header.Get("Content-Type")
			staged, err = h.Service.StageUpload(r.Context(), part, declaredMIME)
			if err != nil {
				h.writeStageError(w, err)
				return
			}
		} else if !h.readMetadataField(w, &fields, part) {
			return
		}
		_ = part.Close()
	}

	if staged == nil {
		writeJSONError(w, http.StatusBadRequest, "missing file part")
		return
	}

	input, allowedMimeTypes, maxFileSize, err := buildCreateInput(fields)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	doc, err := h.Service.CompleteUpload(r.Context(), staged, input, allowedMimeTypes, maxFileSize)
	if err != nil {
		h.writeCompleteUploadError(w, err)
		return
	}

	IngestOutcomes.Add("accepted", 1)
	CreateDocumentResponse{
		ID:          doc.ID.String(),
		ExternalID:  doc.ExternalID,
		MimeType:    doc.MimeType,
		Size:        doc.Size,
		Reused:      doc.Reused,
		ImageWidth:  doc.ImageWidth,
		ImageHeight: doc.ImageHeight,
	}.Render(w)
}

// readMetadataField reads one trailing (non-file) multipart part into
// fields, enforcing the per-field size cap. 16 KiB is ample for every
// metadata field (the largest legit value, a long allowedMimeTypes list, is
// ~2.5 KiB) and bounds per-request abuse surface. It reads one byte past
// the cap so an oversized field is REJECTED rather than silently truncated
// into a different request. Reports false after writing the error response
// itself.
func (h *DocumentHandler) readMetadataField(w http.ResponseWriter, fields *createFields, part *multipart.Part) bool {
	const maxCreateFieldBytes = 16 << 10
	b, err := io.ReadAll(io.LimitReader(part, maxCreateFieldBytes+1))
	if err != nil {
		h.writeIngestTransportError(w, err)
		return false
	}
	if len(b) > maxCreateFieldBytes {
		writeJSONError(w, http.StatusBadRequest, part.FormName()+" exceeds the 16 KiB field limit")
		return false
	}
	fields.set(part.FormName(), string(b))
	return true
}

// writeCompleteUploadError maps a CompleteUpload failure to its HTTP
// response and outcome counter (spec 020 FR-008): bucket-policy rejections
// (size, MIME) are 413/415, uniqueness conflicts are 409, anything else is
// a logged 500.
func (h *DocumentHandler) writeCompleteUploadError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrPayloadTooLarge):
		IngestOutcomes.Add("rejected_bucket_policy", 1)
		writeJSONError(w, http.StatusRequestEntityTooLarge, "file too large")
	case errors.Is(err, service.ErrUnsupportedMediaType):
		IngestOutcomes.Add("rejected_bucket_policy", 1)
		writeJSONError(w, http.StatusUnsupportedMediaType, "unsupported media type")
	case errors.Is(err, service.ErrConflict):
		writeJSONError(w, http.StatusConflict, "document conflicts with an existing row")
	default:
		IngestOutcomes.Add("failed_mid_stream", 1)
		h.Logger.Error("failed to create document", zap.Error(err))
		writeJSONError(w, http.StatusInternalServerError, "internal error")
	}
}

// writeStageError maps a StageUpload failure to its HTTP response and
// outcome counter (spec 020 FR-008).
func (h *DocumentHandler) writeStageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrOverLimit), errors.Is(err, service.ErrStalled):
		h.writeIngestTransportError(w, err)
	case errors.Is(err, port.ErrPixelBudgetExceeded):
		IngestOutcomes.Add("rejected_pixel_budget", 1)
		RejectedContentResponse{Code: "PIXEL_BUDGET_EXCEEDED", Error: err.Error()}.Render(w)
	case errors.Is(err, service.ErrImageProcessing):
		IngestOutcomes.Add("failed_mid_stream", 1)
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		if isClientStreamError(err) {
			IngestOutcomes.Add("client_abort", 1)
			writeJSONError(w, http.StatusBadRequest, "upload aborted before completion")
		} else {
			IngestOutcomes.Add("failed_mid_stream", 1)
			h.Logger.Error("failed to stage upload", zap.Error(err))
			writeJSONError(w, http.StatusInternalServerError, "internal error")
		}
	}
}

// isClientStreamError reports request-stream failures attributable to the
// client (abort, reset, truncated multipart) rather than to the service.
func isClientStreamError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "client disconnected") ||
		strings.Contains(err.Error(), "multipart")
}

// Copy handles POST /internal/file/copy.
// Materializes a new file row in another bucket that references the same
// content as the source. No bytes traverse the wire — content is
// content-addressed, so the new row simply points at the existing blob.
//
// Per-bucket dedup applies by default; SkipDedup=true forces a fresh row.
// On dedup hit, caller-supplied authorizationId/tagsetId are ignored
// (existing row is authoritative), matching the createDocument contract.
func (h *DocumentHandler) Copy(w http.ResponseWriter, r *http.Request) {
	var body CopyDocumentRequest
	if err := decodeStrictJSON(r, &body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	sourceID, err := uuid.Parse(body.SourceID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid sourceId")
		return
	}
	destBucketID, err := uuid.Parse(body.DestinationBucketID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid destinationBucketId")
		return
	}
	authID, err := uuid.Parse(body.AuthorizationID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid authorizationId")
		return
	}

	var tagsetID *uuid.UUID
	if body.TagsetID != nil && *body.TagsetID != "" {
		parsed, err := uuid.Parse(*body.TagsetID)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid tagsetId")
			return
		}
		tagsetID = &parsed
	}

	var createdBy *uuid.UUID
	if body.CreatedBy != nil && *body.CreatedBy != "" {
		parsed, err := uuid.Parse(*body.CreatedBy)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid createdBy")
			return
		}
		createdBy = &parsed
	}

	input := model.CopyDocumentInput{
		DestinationBucketID: destBucketID,
		AuthorizationID:     authID,
		TagsetID:            tagsetID,
		CreatedBy:           createdBy,
		SkipDedup:           body.SkipDedup,
	}

	doc, err := h.Service.CopyDocument(r.Context(), sourceID, input)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidAuthorizationID):
			writeJSONError(w, http.StatusBadRequest, "invalid authorizationId")
		case errors.Is(err, model.ErrDocumentNotFound):
			writeJSONError(w, http.StatusNotFound, "source document not found")
		case errors.Is(err, service.ErrConflict):
			writeJSONError(w, http.StatusConflict, "copy conflicts with an existing row")
		default:
			h.Logger.Error("failed to copy document", zap.Error(err))
			writeJSONError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	CreateDocumentResponse{
		ID:          doc.ID.String(),
		ExternalID:  doc.ExternalID,
		MimeType:    doc.MimeType,
		Size:        doc.Size,
		Reused:      doc.Reused,
		ImageWidth:  doc.ImageWidth,
		ImageHeight: doc.ImageHeight,
	}.Render(w)
}

// Delete handles DELETE /internal/file/{id}
func (h *DocumentHandler) Delete(w http.ResponseWriter, r *http.Request) {
	docID, err := parseDocID(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "invalid document ID")
		return
	}

	deleted, err := h.Service.DeleteDocument(r.Context(), docID)
	if err != nil {
		if errors.Is(err, model.ErrDocumentNotFound) {
			writeJSONError(w, http.StatusNotFound, "document not found")
			return
		}
		h.Logger.Error("failed to delete document", zap.Error(err))
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := DeleteDocumentResponse{}
	if deleted.AuthorizationID != uuid.Nil {
		s := deleted.AuthorizationID.String()
		resp.AuthorizationID = &s
	}
	if deleted.TagsetID != nil {
		s := deleted.TagsetID.String()
		resp.TagsetID = &s
	}
	resp.Render(w)
}

// Update handles PATCH /internal/file/{id}.
// Mutates storageBucketId, temporaryLocation, and/or displayName. Each
// field is optional; at least one must be present. Omitted fields retain
// their current value.
//
// displayName notes:
//   - Validation rejects empty/whitespace-only, length > 512 (matches
//     the file."displayName" VARCHAR(512) column), path separators, and
//     control characters.
//   - mimeType is immutable on this endpoint, so callers renaming a file
//     are responsible for keeping the extension consistent with mimeType.
//     This service does not parse extensions or enforce mimeType matching.
//   - displayName is not part of any uniqueness/dedup index, so renames
//     never conflict at the DB level.
func (h *DocumentHandler) Update(w http.ResponseWriter, r *http.Request) {
	docID, err := parseDocID(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "invalid document ID")
		return
	}

	var body UpdateDocumentRequest
	if err := decodeStrictJSON(r, &body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if body.StorageBucketID == nil && body.TemporaryLocation == nil && body.DisplayName == nil {
		writeJSONError(w, http.StatusBadRequest, "no fields to update")
		return
	}

	if body.DisplayName != nil {
		if err := validateDisplayName(*body.DisplayName); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	doc, err := h.Service.Repo.GetByID(r.Context(), docID)
	if err != nil {
		if errors.Is(err, model.ErrDocumentNotFound) {
			writeJSONError(w, http.StatusNotFound, "document not found")
			return
		}
		h.Logger.Error("failed to lookup document", zap.Error(err))
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}

	bucketID, err := resolveBucketID(doc, body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	tempLoc, displayName := mergeUpdateFallbacks(doc, body)

	updated, err := h.Service.UpdateDocumentMetadata(r.Context(), doc, bucketID, tempLoc, displayName)
	if err != nil {
		if errors.Is(err, service.ErrConflict) {
			writeJSONError(w, http.StatusConflict, "document was modified concurrently, retry with fresh version")
			return
		}
		if errors.Is(err, model.ErrDuplicateKey) {
			writeJSONError(w, http.StatusConflict, "destination bucket already contains a document with this content")
			return
		}
		h.Logger.Error("failed to update document", zap.Error(err))
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// A legacy image row may still have empty content_metadata; its dims are populated by the
	// sweep-dims job (RunDimsBackfill), not lazily here (a libvips decode has no place on a request
	// path — spec 019/020). Until the sweep reaches that row the response simply omits dims (FR-020).
	UpdateDocumentResponse{
		ID:                updated.ID.String(),
		StorageBucketID:   updated.StorageBucketID.String(),
		TemporaryLocation: updated.TemporaryLocation,
		DisplayName:       updated.DisplayName,
		ImageWidth:        updated.ImageWidth,
		ImageHeight:       updated.ImageHeight,
	}.Render(w)
}

// validateDisplayName rejects renames that would corrupt the row or
// produce a filename consumers can't safely use. Shared by POST (create)
// and PATCH (update) so both paths apply the same rules.
//
// The 512-byte cap is intentionally tighter than file."displayName"
// VARCHAR(512), which is character-based in Postgres: capping at bytes
// guarantees any accepted value fits regardless of UTF-8 expansion.
func validateDisplayName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("displayName must not be empty or whitespace-only")
	}
	if len(name) > 512 {
		return fmt.Errorf("displayName exceeds maximum length of 512 bytes")
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("displayName must not contain path separators ('/' or '\\')")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("displayName must not contain control characters")
		}
	}
	return nil
}

// ReplaceContent handles PUT /internal/file/{id}/content (store-and-link)
func (h *DocumentHandler) ReplaceContent(w http.ResponseWriter, r *http.Request) {
	docID, err := parseDocID(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "invalid document ID")
		return
	}

	capBytes := h.MaxUploadSize
	if capBytes <= 0 {
		capBytes = 32 << 20
	}
	idle := h.IdleTimeout
	if idle <= 0 {
		idle = 30 * time.Second
	}
	pr := newProgressReader(w, r.Body, capBytes, idle)
	defer func() { _ = pr.Close() }()

	// ReplaceContent note: if the new content's hash matches another file row in
	// the same bucket, the unique(externalID, storageBucketID) index is violated.
	// The service returns ErrConflict → 409 in that case. UpdateFile (PATCH) does
	// not touch externalID, so it cannot trigger this.
	result, err := h.Service.StoreAndLinkStream(r.Context(), docID, pr)
	if err != nil {
		var mismatch *service.MimeMismatchError
		switch {
		case errors.Is(err, service.ErrOverLimit), errors.Is(err, service.ErrStalled):
			h.writeIngestTransportError(w, err)
		case errors.Is(err, port.ErrPixelBudgetExceeded):
			IngestOutcomes.Add("rejected_pixel_budget", 1)
			RejectedContentResponse{Code: "PIXEL_BUDGET_EXCEEDED", Error: err.Error()}.Render(w)
		case errors.Is(err, model.ErrDocumentNotFound):
			writeJSONError(w, http.StatusNotFound, "document not found")
		case errors.Is(err, service.ErrEmptyContent):
			ReplaceOutcomes.Add(service.ReplaceOutcomeRejectedEmpty, 1)
			RejectedContentResponse{Code: "EMPTY_CONTENT", Error: err.Error()}.Render(w)
		case errors.As(err, &mismatch):
			ReplaceOutcomes.Add(service.ReplaceOutcomeRejectedMismatch, 1)
			RejectedContentResponse{
				Code:  "MIME_MISMATCH",
				Error: err.Error(),
				Detail: &MimeMismatchDetail{
					KnownMime:    mismatch.Known,
					DetectedMime: mismatch.Detected,
				},
			}.Render(w)
		case errors.Is(err, service.ErrImageProcessing):
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, service.ErrConflict):
			writeJSONError(w, http.StatusConflict, "document content changed concurrently or conflicts with another document in this bucket")
		case isClientStreamError(err):
			IngestOutcomes.Add("client_abort", 1)
			writeJSONError(w, http.StatusBadRequest, "upload aborted before completion")
		default:
			h.Logger.Error("failed to replace content", zap.Error(err))
			writeJSONError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	if result.ReplaceOutcome != "" {
		ReplaceOutcomes.Add(result.ReplaceOutcome, 1)
	}

	ReplaceContentResponse{
		ExternalID:  result.ExternalID,
		MimeType:    result.MimeType,
		Size:        result.Size,
		ImageWidth:  result.ImageWidth,
		ImageHeight: result.ImageHeight,
	}.Render(w)
}

func parseDocID(r *http.Request) (uuid.UUID, error) {
	return uuid.Parse(chi.URLParam(r, "id"))
}

// decodeStrictJSON decodes the request body into dst, rejecting unknown
// fields and any trailing data after the first JSON object.
//
// DisallowUnknownFields is load-bearing: immutable fields (e.g. mimeType on
// PATCH) must surface as a 400 rather than silently no-op.
func decodeStrictJSON[T any](r *http.Request, dst *T) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after first object")
	}
	return nil
}

// resolveBucketID resolves the effective storage bucket for a PATCH: the
// validated UUID from body.storageBucketId when present, otherwise the
// document's current bucket. A malformed storageBucketId yields an error
// whose message is safe to return verbatim as a 400 body.
func resolveBucketID(doc model.Document, body UpdateDocumentRequest) (uuid.UUID, error) {
	if body.StorageBucketID == nil {
		return doc.StorageBucketID, nil
	}
	parsed, err := uuid.Parse(*body.StorageBucketID)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("invalid storageBucketId")
	}
	return parsed, nil
}

// mergeUpdateFallbacks merges the validation-free PATCH fields over the
// row's current values: fields present in the body win, omitted fields keep
// what the document already has. storageBucketId is resolved separately by
// resolveBucketID because it needs UUID validation first.
func mergeUpdateFallbacks(doc model.Document, body UpdateDocumentRequest) (tempLoc bool, displayName string) {
	tempLoc = doc.TemporaryLocation
	if body.TemporaryLocation != nil {
		tempLoc = *body.TemporaryLocation
	}
	displayName = doc.DisplayName
	if body.DisplayName != nil {
		displayName = *body.DisplayName
	}
	return tempLoc, displayName
}

func documentMetaResponse(doc model.Document) DocumentMetaResponse {
	resp := DocumentMetaResponse{
		ID:                doc.ID.String(),
		ExternalID:        doc.ExternalID,
		MimeType:          doc.MimeType,
		Size:              doc.Size,
		DisplayName:       doc.DisplayName,
		TemporaryLocation: doc.TemporaryLocation,
		StorageBucketID:   doc.StorageBucketID.String(),
		AuthorizationID:   doc.AuthorizationID.String(),
		CreatedDate:       doc.CreatedDate,
		UpdatedDate:       doc.UpdatedDate,
	}
	if doc.CreatedBy != nil {
		s := doc.CreatedBy.String()
		resp.CreatedBy = &s
	}
	if doc.TagsetID != nil {
		s := doc.TagsetID.String()
		resp.TagsetID = &s
	}
	return resp
}
