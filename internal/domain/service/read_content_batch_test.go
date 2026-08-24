package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"

	"github.com/alkem-io/file-service/internal/domain/model"
)

type batchReadCloser struct {
	io.Reader
	closeErr error
}

func (r *batchReadCloser) Close() error { return r.closeErr }

func newBatchService(repo *mockRepo, storage *mockStorage) *FileService {
	return &FileService{
		Repo:    repo,
		Storage: storage,
		Logger:  nopLogger,
	}
}

func TestReadContentBatch_ReturnsEachBlobInOrder(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		a: {ID: a, ExternalID: "ext-a", MimeType: "text/plain"},
		b: {ID: b, ExternalID: "ext-b", MimeType: "application/octet-stream"},
	}}
	storage := &mockStorage{dataByID: map[string][]byte{
		"ext-a": []byte("AAA"),
		"ext-b": []byte("BBB"),
	}}
	svc := newBatchService(repo, storage)

	results := svc.ReadContentBatch(context.Background(), []uuid.UUID{a, b}, 1024)

	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].ID != a || !results[0].Found || string(results[0].Content) != "AAA" || results[0].MimeType != "text/plain" {
		t.Errorf("results[0] = %+v, want a/found/AAA/text-plain", results[0])
	}
	if results[1].ID != b || !results[1].Found || string(results[1].Content) != "BBB" || results[1].MimeType != "application/octet-stream" {
		t.Errorf("results[1] = %+v, want b/found/BBB/octet-stream", results[1])
	}
}

func TestReadContentBatch_UnknownDocumentNonFatal(t *testing.T) {
	present, missing := uuid.New(), uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		present: {ID: present, ExternalID: "ext", MimeType: "text/plain"},
		// missing intentionally absent.
	}}
	storage := &mockStorage{dataByID: map[string][]byte{"ext": []byte("ok")}}
	svc := newBatchService(repo, storage)

	results := svc.ReadContentBatch(context.Background(), []uuid.UUID{present, missing}, 1024)

	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if !results[0].Found {
		t.Errorf("results[0] (present) Found = false, want true")
	}
	if results[1].Found {
		t.Errorf("results[1] (missing) Found = true, want false")
	}
	if results[1].Err == nil {
		t.Errorf("results[1] (missing) Err = nil, want non-fatal not-found error")
	}
	if !errors.Is(results[1].Err, model.ErrDocumentNotFound) {
		t.Errorf("results[1].Err = %v, want wraps ErrDocumentNotFound", results[1].Err)
	}
}

func TestReadContentBatch_BlobReadErrorNonFatal(t *testing.T) {
	id := uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		id: {ID: id, ExternalID: "ext-gone", MimeType: "text/plain"},
	}}
	// dataByID is non-nil but doesn't contain "ext-gone" → Read returns readErr.
	storage := &mockStorage{
		dataByID: map[string][]byte{"other": []byte("x")},
		readErr:  errors.New("blob not on storage"),
	}
	svc := newBatchService(repo, storage)

	results := svc.ReadContentBatch(context.Background(), []uuid.UUID{id}, 1024)

	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if results[0].Found {
		t.Errorf("Found = true, want false on storage read error")
	}
	if results[0].Err == nil {
		t.Errorf("Err = nil, want the storage read error recorded")
	}
}

func TestReadContentBatch_EmptyInput(t *testing.T) {
	svc := newBatchService(&mockRepo{}, &mockStorage{})
	results := svc.ReadContentBatch(context.Background(), nil, 1024)
	if len(results) != 0 {
		t.Fatalf("len(results) = %d, want 0", len(results))
	}
}

func TestReadContentBatch_DuplicatesPreserved(t *testing.T) {
	id := uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		id: {ID: id, ExternalID: "ext", MimeType: "text/plain"},
	}}
	storage := &mockStorage{dataByID: map[string][]byte{"ext": []byte("dup")}}
	svc := newBatchService(repo, storage)

	results := svc.ReadContentBatch(context.Background(), []uuid.UUID{id, id, id}, 1024)

	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	for i, r := range results {
		if !r.Found || string(r.Content) != "dup" {
			t.Errorf("results[%d] = %+v, want found/dup", i, r)
		}
	}
}

func TestReadContentBatch_EnforcesAggregateByteLimit(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		first:  {ID: first, ExternalID: "first", MimeType: "text/plain"},
		second: {ID: second, ExternalID: "second", MimeType: "text/plain"},
	}}
	storage := &mockStorage{dataByID: map[string][]byte{
		"first":  []byte("1234"),
		"second": []byte("5678"),
	}}

	results := newBatchService(repo, storage).ReadContentBatch(
		context.Background(), []uuid.UUID{first, second}, 5,
	)

	if len(results) != 2 || !results[0].Found {
		t.Fatalf("results = %+v, want first item found", results)
	}
	if results[1].Found || !errors.Is(results[1].Err, ErrBatchContentLimit) {
		t.Fatalf("second result = %+v, want non-fatal byte-limit miss", results[1])
	}
}

func TestReadContentBatch_StreamFailuresArePerItem(t *testing.T) {
	id := uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		id: {ID: id, ExternalID: "blob", MimeType: "text/plain"},
	}}

	tests := []struct {
		name    string
		storage *mockStorage
	}{
		{
			name:    "mid-stream read error",
			storage: &mockStorage{streamBody: &faultyReadCloser{}, streamSize: 1},
		},
		{
			name: "close error",
			storage: &mockStorage{
				streamBody: &batchReadCloser{
					Reader:   bytes.NewReader([]byte("x")),
					closeErr: errors.New("close failed"),
				},
				streamSize: 1,
			},
		},
		{
			name: "short read",
			storage: &mockStorage{
				streamBody: io.NopCloser(bytes.NewReader([]byte("abc"))),
				streamSize: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := newBatchService(repo, tt.storage).ReadContentBatch(
				context.Background(), []uuid.UUID{id}, 10,
			)[0]
			if result.Found || result.Err == nil || len(result.Content) != 0 {
				t.Fatalf("result = %+v, want non-fatal stream failure", result)
			}
		})
	}
}

func TestReadContentBatch_RejectsStreamThatExceedsReportedBudget(t *testing.T) {
	id := uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		id: {ID: id, ExternalID: "blob", MimeType: "text/plain"},
	}}
	storage := &mockStorage{
		streamBody: io.NopCloser(bytes.NewReader([]byte("1234"))),
		streamSize: 1,
	}

	result := newBatchService(repo, storage).ReadContentBatch(
		context.Background(), []uuid.UUID{id}, 3,
	)[0]
	if result.Found || !errors.Is(result.Err, ErrBatchContentLimit) {
		t.Fatalf("result = %+v, want byte-limit miss", result)
	}
}

func TestReadContentBatch_ZeroRemainingSkipsNextBlob(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		first:  {ID: first, ExternalID: "first", MimeType: "text/plain"},
		second: {ID: second, ExternalID: "second", MimeType: "text/plain"},
	}}
	storage := &mockStorage{dataByID: map[string][]byte{
		"first":  []byte("1234"),
		"second": []byte("not read"),
	}}

	results := newBatchService(repo, storage).ReadContentBatch(
		context.Background(), []uuid.UUID{first, second}, 4,
	)
	if !results[0].Found || results[1].Found || !errors.Is(results[1].Err, ErrBatchContentLimit) {
		t.Fatalf("results = %+v, want first found and second limited", results)
	}
}

func TestReadContentBatch_ExhaustedBudgetStillReportsMissingDocument(t *testing.T) {
	first, missing := uuid.New(), uuid.New()
	repo := &mockRepo{docsByID: map[uuid.UUID]model.Document{
		first: {ID: first, ExternalID: "first", MimeType: "text/plain"},
		// missing intentionally absent: positional miss semantics still apply
		// after an earlier item consumes the byte budget.
	}}
	storage := &mockStorage{dataByID: map[string][]byte{"first": []byte("full")}}

	results := newBatchService(repo, storage).ReadContentBatch(
		context.Background(), []uuid.UUID{first, missing}, 4,
	)
	if !results[0].Found || !errors.Is(results[1].Err, model.ErrDocumentNotFound) {
		t.Fatalf("results = %+v, want first found and second document-not-found", results)
	}
}
