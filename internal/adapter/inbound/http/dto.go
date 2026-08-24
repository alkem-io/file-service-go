package http

import (
	"encoding/json"
	"net/http"
	"time"
)

// CreateDocumentResponse is returned by POST /internal/file.
// Always uses HTTP 201 so strict POST clients and code generators treat
// every success uniformly. The Reused field distinguishes outcomes:
//   - Reused=false: a new file row was inserted
//   - Reused=true:  an existing row matched (externalID, storageBucketID)
//     and was returned as-is; the caller-supplied authorizationId/tagsetId
//     were ignored and should be cleaned up by the caller.
type CreateDocumentResponse struct {
	ID         string `json:"id"`
	ExternalID string `json:"externalID"`
	MimeType   string `json:"mimeType"`
	Size       int    `json:"size"`
	Reused     bool   `json:"reused"`
	// ImageWidth/ImageHeight are post-rotation pixel dimensions sourced from
	// content_metadata for image rows. Both nil for non-images and for
	// image rows whose metadata is empty/sentinel.
	ImageWidth  *int `json:"imageWidth,omitempty"`
	ImageHeight *int `json:"imageHeight,omitempty"`
}

// Render writes the response as JSON with HTTP 201 (always — see the type
// comment for why dedup reuse is not a 200).
func (r CreateDocumentResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(r)
}

// DeleteDocumentResponse is returned by DELETE /internal/document/:id.
type DeleteDocumentResponse struct {
	AuthorizationID *string `json:"authorizationId,omitempty"`
	TagsetID        *string `json:"tagsetId,omitempty"`
}

// Render writes the response as JSON with HTTP 200.
func (r DeleteDocumentResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// UpdateDocumentResponse is returned by PATCH /internal/file/:id.
type UpdateDocumentResponse struct {
	ID                string `json:"id"`
	StorageBucketID   string `json:"storageBucketId"`
	TemporaryLocation bool   `json:"temporaryLocation"`
	DisplayName       string `json:"displayName"`
	// ImageWidth/ImageHeight are post-rotation pixel dimensions sourced from
	// content_metadata. Both nil for non-image rows and for image rows
	// whose metadata is empty/sentinel.
	ImageWidth  *int `json:"imageWidth,omitempty"`
	ImageHeight *int `json:"imageHeight,omitempty"`
}

// Render writes the response as JSON with HTTP 200.
func (r UpdateDocumentResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// ReplaceContentResponse is returned by PUT /internal/document/:id/content.
type ReplaceContentResponse struct {
	ExternalID string `json:"externalID"`
	MimeType   string `json:"mimeType"`
	Size       int    `json:"size"`
	// ImageWidth/ImageHeight are post-rotation pixel dimensions for the
	// just-replaced bytes, populated from the ProcessResult inside
	// StoreAndLink. Both nil for non-image content.
	ImageWidth  *int `json:"imageWidth,omitempty"`
	ImageHeight *int `json:"imageHeight,omitempty"`
}

// Render writes the response as JSON with HTTP 200.
func (r ReplaceContentResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// MimeMismatchDetail carries the MIME pair behind a MIME_MISMATCH rejection.
type MimeMismatchDetail struct {
	KnownMime    string `json:"knownMime"`
	DetectedMime string `json:"detectedMime"`
}

// RejectedContentResponse is the 422 body for content-replace rejections
// (spec 019). Code is machine-readable and stable: EMPTY_CONTENT or
// MIME_MISMATCH. Detail is present only for MIME_MISMATCH.
type RejectedContentResponse struct {
	Code   string              `json:"code"`
	Error  string              `json:"error"`
	Detail *MimeMismatchDetail `json:"detail,omitempty"`
}

// Render writes the rejection as JSON with HTTP 422 Unprocessable Entity —
// the request was well-formed; the content itself was refused.
func (r RejectedContentResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	_ = json.NewEncoder(w).Encode(r)
}

// DocumentMetaResponse is returned by GET /internal/document/:id/meta.
type DocumentMetaResponse struct {
	ID                string    `json:"id"`
	ExternalID        string    `json:"externalID"`
	MimeType          string    `json:"mimeType"`
	Size              int       `json:"size"`
	DisplayName       string    `json:"displayName"`
	CreatedBy         *string   `json:"createdBy,omitempty"`
	TemporaryLocation bool      `json:"temporaryLocation"`
	StorageBucketID   string    `json:"storageBucketId"`
	AuthorizationID   string    `json:"authorizationId"`
	TagsetID          *string   `json:"tagsetId,omitempty"`
	CreatedDate       time.Time `json:"createdDate"`
	UpdatedDate       time.Time `json:"updatedDate"`
}

// Render writes the response as JSON with HTTP 200.
func (r DocumentMetaResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// UpdateDocumentRequest is the body for PATCH /internal/file/:id.
// All fields are optional; at least one must be present. Omitted fields
// retain their current value. mimeType, externalID, and size are immutable
// through this endpoint — see PUT /internal/file/{id}/content for content
// replacement (which also updates mimeType and size).
type UpdateDocumentRequest struct {
	StorageBucketID   *string `json:"storageBucketId,omitempty"`
	TemporaryLocation *bool   `json:"temporaryLocation,omitempty"`
	DisplayName       *string `json:"displayName,omitempty"`
}

// ContentBatchIDs is the bounded, ordered id list accepted by ContentBatch.
type ContentBatchIDs []string

// ContentBatchRequest preserves id order and duplicates.
type ContentBatchRequest struct {
	Ids ContentBatchIDs `json:"ids"`
}

// ContentBatchItem is the positional outcome for one requested id.
type ContentBatchItem struct {
	ID            string `json:"id"`
	Found         bool   `json:"found"`
	MimeType      string `json:"mimeType,omitempty"`
	ContentBase64 string `json:"contentBase64,omitempty"`
	Error         string `json:"error,omitempty"`
}

// ContentBatchResponse preserves request order, including duplicate ids.
type ContentBatchResponse struct {
	Items []ContentBatchItem `json:"items"`
}

// Render writes the response as JSON with HTTP 200.
func (r ContentBatchResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// CopyDocumentRequest is the JSON body for POST /internal/file/copy.
// Reuses CreateDocumentResponse for the response shape.
type CopyDocumentRequest struct {
	SourceID            string  `json:"sourceId"`
	DestinationBucketID string  `json:"destinationBucketId"`
	AuthorizationID     string  `json:"authorizationId"`
	TagsetID            *string `json:"tagsetId,omitempty"`
	CreatedBy           *string `json:"createdBy,omitempty"`
	SkipDedup           bool    `json:"skipDedup,omitempty"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status  string            `json:"status"`
	Details map[string]string `json:"details"`
}

// Render writes the response as JSON with the caller-chosen status code —
// 200 when healthy, 503 when a dependency check failed.
func (r HealthResponse) Render(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(r)
}
