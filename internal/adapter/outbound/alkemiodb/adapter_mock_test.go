package alkemiodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v5"

	"github.com/alkem-io/file-service/internal/domain/model"
)

func columns() []string {
	return []string{"id", "externalID", "mimeType", "size", "displayName", "createdBy",
		"temporaryLocation", "storageBucketId", "authorizationId", "tagsetId",
		"createdDate", "updatedDate", "version", "content_metadata"}
}

func TestMock_GetByID_Found(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	docID := uuid.New()
	authID := uuid.New()
	bucketID := uuid.New()
	now := time.Now()

	mock.ExpectQuery("SELECT .+ FROM file WHERE id").
		WithArgs(pgtype.UUID{Bytes: docID, Valid: true}).
		WillReturnRows(mock.NewRows(columns()).AddRow(
			pgtype.UUID{Bytes: docID, Valid: true},
			"abc123",
			"text/plain",
			int32(42),
			"test.txt",
			pgtype.UUID{Valid: false},
			false,
			pgtype.UUID{Bytes: bucketID, Valid: true},
			pgtype.UUID{Bytes: authID, Valid: true},
			pgtype.UUID{Valid: false},
			pgtype.Timestamptz{Time: now, Valid: true},
			pgtype.Timestamptz{Time: now, Valid: true},
			int32(1),
			[]byte("{}"),
		))

	a := New(mock)
	doc, err := a.GetByID(context.Background(), docID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if doc.ID != docID {
		t.Errorf("ID = %v, want %v", doc.ID, docID)
	}
	if doc.ExternalID != "abc123" {
		t.Errorf("ExternalID = %q", doc.ExternalID)
	}
	if doc.MimeType != "text/plain" {
		t.Errorf("MimeType = %q", doc.MimeType)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMock_GetByID_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT .+ FROM file WHERE id").
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(mock.NewRows(columns()))

	a := New(mock)
	_, err = a.GetByID(context.Background(), uuid.New())
	if !errors.Is(err, model.ErrDocumentNotFound) {
		t.Errorf("expected ErrDocumentNotFound, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMock_Create_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	docID := uuid.New()

	mock.ExpectQuery("INSERT INTO file").
		WithArgs(
			pgxmock.AnyArg(), // id
			pgxmock.AnyArg(), // externalID
			pgxmock.AnyArg(), // mimeType
			pgxmock.AnyArg(), // size
			pgxmock.AnyArg(), // displayName
			pgxmock.AnyArg(), // createdBy
			pgxmock.AnyArg(), // temporaryLocation
			pgxmock.AnyArg(), // storageBucketId
			pgxmock.AnyArg(), // authorizationId
			pgxmock.AnyArg(), // tagsetId
			pgxmock.AnyArg(), // createdDate
			pgxmock.AnyArg(), // updatedDate
			[]byte(`{}`),     // content_metadata: empty Populated=false → "{}"
		).
		WillReturnRows(mock.NewRows([]string{"id"}).AddRow(pgtype.UUID{Bytes: docID, Valid: true}))

	a := New(mock)
	now := time.Now()
	id, err := a.Create(context.Background(), model.Document{
		ID:              docID,
		ExternalID:      "hash",
		MimeType:        "text/plain",
		Size:            10,
		DisplayName:     "test.txt",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
		CreatedDate:     now,
		UpdatedDate:     now,
	}, model.ContentMetadata{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id != docID {
		t.Errorf("ID = %v, want %v", id, docID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreateDocumentParams_OmittedAuthorizationIsNull(t *testing.T) {
	params := createDocumentParams(model.Document{AuthorizationID: uuid.Nil}, []byte(`{}`))
	if params.AuthorizationId.Valid {
		t.Fatalf("omitted authorization mapping = %+v, want SQL NULL", params.AuthorizationId)
	}

	authID := uuid.New()
	params = createDocumentParams(model.Document{AuthorizationID: authID}, []byte(`{}`))
	if !params.AuthorizationId.Valid || params.AuthorizationId.Bytes != authID {
		t.Fatalf("real authorization mapping = %+v, want valid %s", params.AuthorizationId, authID)
	}
}

func TestMock_UpdateFile_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	// Param order: id, new externalID, MIME, size, updatedDate, metadata,
	// expected old externalID, expected version.
	// Content metadata is the empty-Populated case → marshals to "{}".
	mock.ExpectExec("UPDATE file SET").
		WithArgs(pgxmock.AnyArg(), "newhash", "image/jpeg", pgxmock.AnyArg(), pgxmock.AnyArg(), []byte(`{}`), "oldhash", int32(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	a := New(mock)
	err = a.UpdateFile(context.Background(), uuid.New(), "oldhash", 1, "newhash", "image/jpeg", 999, model.ContentMetadata{})
	if err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMock_UpdateFile_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("UPDATE file SET").
		WithArgs(pgxmock.AnyArg(), "hash", "text/plain", pgxmock.AnyArg(), pgxmock.AnyArg(), []byte(`{}`), "oldhash", int32(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))

	a := New(mock)
	err = a.UpdateFile(context.Background(), uuid.New(), "oldhash", 1, "hash", "text/plain", 1, model.ContentMetadata{})
	if !errors.Is(err, model.ErrDocumentNotFound) {
		t.Errorf("expected ErrDocumentNotFound, got %v", err)
	}
}

func TestMock_Delete_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	authID := uuid.New()
	tagsetID := uuid.New()

	mock.ExpectQuery("DELETE FROM file WHERE id").
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(mock.NewRows([]string{"externalID", "authorizationId", "tagsetId"}).
			AddRow("abc123", pgtype.UUID{Bytes: authID, Valid: true}, pgtype.UUID{Bytes: tagsetID, Valid: true}))

	a := New(mock)
	deleted, err := a.Delete(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.AuthorizationID != authID {
		t.Errorf("AuthorizationID = %v, want %v", deleted.AuthorizationID, authID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMock_Delete_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("DELETE FROM file WHERE id").
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(mock.NewRows([]string{"externalID", "authorizationId", "tagsetId"}))

	a := New(mock)
	_, err = a.Delete(context.Background(), uuid.New())
	if !errors.Is(err, model.ErrDocumentNotFound) {
		t.Errorf("expected ErrDocumentNotFound, got %v", err)
	}
}

func TestMock_CountByExternalID(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("somehash").
		WillReturnRows(mock.NewRows([]string{"count"}).AddRow(int64(3)))

	a := New(mock)
	count, err := a.CountByExternalID(context.Background(), "somehash")
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

func TestMock_UpdateMetadata_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	// Param order in UpdateDocumentMetadata: $1=id, $2=storageBucketId,
	// $3=temporaryLocation, $4=displayName, $5=updatedDate, $6=version.
	// Pin everything except updatedDate (timestamp computed in adapter).
	docID := uuid.New()
	bucketID := uuid.New()
	mock.ExpectExec("UPDATE file SET").
		WithArgs(uuidToPgx(docID), uuidToPgx(bucketID), false, "name.txt", pgxmock.AnyArg(), int32(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	a := New(mock)
	err = a.UpdateMetadata(context.Background(), docID, bucketID, false, "name.txt", 1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMock_UpdateMetadata_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("UPDATE file SET").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))

	a := New(mock)
	err = a.UpdateMetadata(context.Background(), uuid.New(), uuid.New(), false, "name.txt", 1)
	if !errors.Is(err, model.ErrDocumentNotFound) {
		t.Errorf("expected ErrDocumentNotFound, got %v", err)
	}
}

func TestMock_GetByID_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT .+ FROM file WHERE id").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	a := New(mock)
	_, err = a.GetByID(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, model.ErrDocumentNotFound) {
		t.Error("should not be ErrDocumentNotFound for connection error")
	}
}

func TestMock_UpdateFile_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("UPDATE file SET").
		WithArgs(pgxmock.AnyArg(), "h", "t", pgxmock.AnyArg(), pgxmock.AnyArg(), []byte(`{}`), "old", int32(1)).
		WillReturnError(errors.New("connection reset"))

	a := New(mock)
	err = a.UpdateFile(context.Background(), uuid.New(), "old", 1, "h", "t", 1, model.ContentMetadata{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMock_UpdateMetadata_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("UPDATE file SET").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	a := New(mock)
	err = a.UpdateMetadata(context.Background(), uuid.New(), uuid.New(), false, "name.txt", 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMock_Delete_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("DELETE FROM file WHERE id").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	a := New(mock)
	_, err = a.Delete(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, model.ErrDocumentNotFound) {
		t.Error("should not be ErrDocumentNotFound for connection error")
	}
}

func TestMock_CountByExternalID_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("hash").
		WillReturnError(errors.New("connection reset"))

	a := New(mock)
	_, err = a.CountByExternalID(context.Background(), "hash")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMock_Create_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO file").
		WithArgs(
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			[]byte(`{}`), // content_metadata: empty Populated=false → "{}"
		).
		WillReturnError(errors.New("FK constraint violation"))

	a := New(mock)
	_, err = a.Create(context.Background(), model.Document{
		ID:              uuid.New(),
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
		CreatedDate:     time.Now(),
		UpdatedDate:     time.Now(),
	}, model.ContentMetadata{})
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestMock_ListImagesNeedingDims_PredicateGuardsSentinels: the "never re-decode a row we already
// decided about" invariant lives ONLY in this query's WHERE clause — the domain sweep uses a mock
// repo that bypasses it. So assert the predicate itself: image rows, content_metadata still EMPTY
// ('{}'), keyset-paged by id. Broadening it (e.g. to rows merely missing imageWidth) would re-decode
// every {_decodeFailed:true} sentinel on every boot — a CPU/IO storm this test exists to prevent.
func TestMock_ListImagesNeedingDims_PredicateGuardsSentinels(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()
	after := uuid.New()

	mock.ExpectQuery(`(?s)FROM file\s+WHERE "mimeType" LIKE 'image/%'\s+AND content_metadata = '\{\}'::jsonb\s+AND id > \$1\s+ORDER BY id\s+LIMIT \$2`).
		WithArgs(pgtype.UUID{Bytes: after, Valid: true}, int32(500)).
		WillReturnRows(mock.NewRows([]string{"id", "externalID", "mimeType"}))

	if _, err := New(mock).ListImagesNeedingDims(context.Background(), after, 500); err != nil {
		t.Fatalf("ListImagesNeedingDims: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
