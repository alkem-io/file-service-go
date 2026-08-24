package model

import "errors"

// ErrDocumentNotFound is returned when a document cannot be found in the repository.
var ErrDocumentNotFound = errors.New("document not found")

// ErrDuplicateKey is returned when an insert would violate any file-table
// unique constraint. The service disambiguates a dedup winner from an
// authorizationId/tagsetId collision by probing (externalID, bucket).
var ErrDuplicateKey = errors.New("duplicate key")
