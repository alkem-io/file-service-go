package http

import (
	"encoding/base64"
	"errors"
	"net/http"
	"os"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/file-service/internal/domain/model"
	"github.com/alkem-io/file-service/internal/domain/service"
)

const (
	// 256 covers the derived-text resolver's page fan-in while bounding DB and
	// storage work; callers with more ids page the request.
	maxContentBatchSize = 256
	// A canonical 256-UUID request is under 10 KiB. Leave headroom for JSON
	// whitespace while rejecting oversized bodies before the decoder allocates.
	maxContentBatchRequestBytes int64 = 16 << 10
	// Let one file at the historical upload ceiling fit while preventing a
	// 256-item request from accumulating gigabytes of raw and base64 data.
	maxContentBatchResponseBytes int64 = 32 << 20
)

// ContentBatch handles POST /internal/file/content-batch. Results preserve
// request order and report malformed ids and missing content per item.
func (h *DocumentHandler) ContentBatch(w http.ResponseWriter, r *http.Request) {
	var body ContentBatchRequest
	if !decodeContentBatchRequest(w, r, &body) {
		return
	}

	if len(body.Ids) == 0 {
		writeJSONError(w, http.StatusBadRequest, "ids must not be empty")
		return
	}
	if len(body.Ids) > maxContentBatchSize {
		writeJSONError(w, http.StatusBadRequest, "too many ids in one batch")
		return
	}

	type parsedID struct {
		raw    string
		parsed *uuid.UUID
	}
	parsed := make([]parsedID, len(body.Ids))
	validIDs := make([]uuid.UUID, 0, len(body.Ids))
	for i, raw := range body.Ids {
		id, err := uuid.Parse(raw)
		if err != nil {
			parsed[i] = parsedID{raw: raw}
			continue
		}
		parsed[i] = parsedID{raw: raw, parsed: &id}
		validIDs = append(validIDs, id)
	}

	results := h.Service.ReadContentBatch(r.Context(), validIDs, maxContentBatchResponseBytes)

	items := make([]ContentBatchItem, len(parsed))
	resultIdx := 0
	for i, p := range parsed {
		if p.parsed == nil {
			items[i] = ContentBatchItem{ID: p.raw, Found: false, Error: "invalid id"}
			continue
		}
		items[i] = h.batchItem(p.raw, results[resultIdx])
		resultIdx++
	}

	ContentBatchResponse{Items: items}.Render(w)
}

func decodeContentBatchRequest(w http.ResponseWriter, r *http.Request, dst *ContentBatchRequest) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxContentBatchRequestBytes)
	err := decodeStrictJSON(r, dst)
	if err == nil {
		return true
	}
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return false
	}
	writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
	return false
}

func (h *DocumentHandler) batchItem(rawID string, res service.BatchContentResult) ContentBatchItem {
	if res.Found {
		return ContentBatchItem{
			ID:            rawID,
			Found:         true,
			MimeType:      res.MimeType,
			ContentBase64: base64.StdEncoding.EncodeToString(res.Content),
		}
	}
	if res.Err != nil &&
		!errors.Is(res.Err, model.ErrDocumentNotFound) &&
		!errors.Is(res.Err, service.ErrBatchContentLimit) &&
		h.Logger != nil {
		h.Logger.Warn("content-batch: content read failed for id",
			zap.String("id", rawID), zap.Error(res.Err))
	}
	return ContentBatchItem{ID: rawID, Found: false, Error: batchMissReason(res.Err)}
}

func batchMissReason(err error) string {
	if errors.Is(err, model.ErrDocumentNotFound) {
		return "document not found"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "content not found"
	}
	if errors.Is(err, service.ErrBatchContentLimit) {
		return "batch content limit exceeded"
	}
	return "backend unavailable"
}
