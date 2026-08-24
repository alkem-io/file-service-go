package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/file-service/internal/domain/model"
	"github.com/alkem-io/file-service/internal/domain/port"
)

var nopLogger = zap.NewNop()

// faultyReadCloser fails on the first Read — a storage transport fault mid-header (EIO / NFS blip),
// as opposed to bytes that are simply undecodable.
type faultyReadCloser struct{}

func (f *faultyReadCloser) Read([]byte) (int, error) { return 0, errors.New("EIO: volume read fault") }
func (f *faultyReadCloser) Close() error             { return nil }

// --- Mocks ---

type mockRepo struct {
	doc    model.Document
	getErr error

	// docsByID, when non-nil, makes GetByID id-aware: it returns the mapped
	// document for a known id and model.ErrDocumentNotFound for an unknown
	// one. Used by the batched-read tests, which need several distinct rows in
	// one call. When nil, GetByID falls back to the doc/getErr pair so existing
	// single-document tests are unaffected.
	docsByID map[uuid.UUID]model.Document

	findDoc       *model.Document // nil means "not found"
	findErr       error           // if set, overrides findDoc
	findCalls     int
	lastFindExt   string
	lastFindBkt   uuid.UUID
	createErr     error
	createErrOnce error // returned only on the first Create call
	createCalls   int
	updateErr     error
	deleteResult  model.DeletedDocument
	deleteErr     error
	count         int
	countErr      error

	// Captured args from Create / UpdateFile content_metadata params.
	lastCreateDoc                 model.Document
	lastCreateContentMetadata     model.ContentMetadata
	lastUpdateFileContentMetadata model.ContentMetadata

	// Replace-path capture (spec 019): persisted MIME and call count.
	updateFileCalls    int
	lastUpdateFileMime string
	lastUpdateExpected string
	lastUpdateExternal string

	// MIME-repair capture (spec 019).
	suspects           []model.Document
	listErr            error
	lastListMimeTypes  []string
	imagesNeedingDims  []model.Document // ListImagesNeedingDims yields this once (cursor==Nil), then empty
	listImagesErr      error
	relabeled          map[uuid.UUID]string
	relabelExternalIDs map[uuid.UUID]string
	updateMimeErr      error
	updateMimeLostRace bool

	// Dimension-backfill capture.
	backfillCalls          int
	lastBackfillID         uuid.UUID
	lastBackfillExternalID string
	lastBackfillPayload    model.ContentMetadata
	backfillErr            error
	backfillLostRace       bool
}

// Ensure mockRepo implements the full interface at compile time.
var _ port.DocumentRepo = (*mockRepo)(nil)

func (m *mockRepo) GetByID(_ context.Context, id uuid.UUID) (model.Document, error) {
	if m.docsByID != nil {
		if doc, ok := m.docsByID[id]; ok {
			return doc, nil
		}
		return model.Document{}, model.ErrDocumentNotFound
	}
	return m.doc, m.getErr
}
func (m *mockRepo) FindByExternalIDAndBucket(_ context.Context, externalID string, storageBucketID uuid.UUID) (model.Document, error) {
	m.findCalls++
	m.lastFindExt = externalID
	m.lastFindBkt = storageBucketID
	if m.findErr != nil {
		return model.Document{}, m.findErr
	}
	if m.findDoc == nil {
		return model.Document{}, model.ErrDocumentNotFound
	}
	return *m.findDoc, nil
}
func (m *mockRepo) Create(_ context.Context, doc model.Document, contentMetadata model.ContentMetadata) (uuid.UUID, error) {
	m.createCalls++
	m.lastCreateDoc = doc
	m.lastCreateContentMetadata = contentMetadata
	if m.createErrOnce != nil && m.createCalls == 1 {
		return uuid.Nil, m.createErrOnce
	}
	return doc.ID, m.createErr
}
func (m *mockRepo) UpdateFile(_ context.Context, _ uuid.UUID, expectedExternalID string, _ int, externalID, mimeType string, _ int, contentMetadata model.ContentMetadata) error {
	m.updateFileCalls++
	m.lastUpdateFileMime = mimeType
	m.lastUpdateExpected = expectedExternalID
	m.lastUpdateExternal = externalID
	m.lastUpdateFileContentMetadata = contentMetadata
	return m.updateErr
}
func (m *mockRepo) BackfillContentMetadata(_ context.Context, id uuid.UUID, expectedExternalID string, metadata model.ContentMetadata) (bool, error) {
	m.backfillCalls++
	m.lastBackfillID = id
	m.lastBackfillExternalID = expectedExternalID
	m.lastBackfillPayload = metadata
	if m.backfillErr != nil {
		return false, m.backfillErr
	}
	return !m.backfillLostRace, nil
}
func (m *mockRepo) UpdateMetadata(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ bool, _ string, _ int) error {
	return m.updateErr
}
func (m *mockRepo) Delete(_ context.Context, _ uuid.UUID) (model.DeletedDocument, error) {
	return m.deleteResult, m.deleteErr
}
func (m *mockRepo) CountByExternalID(_ context.Context, _ string) (int, error) {
	return m.count, m.countErr
}
func (m *mockRepo) ListByMimeTypes(_ context.Context, mimeTypes []string) ([]model.Document, error) {
	m.lastListMimeTypes = mimeTypes
	return m.suspects, m.listErr
}

func (m *mockRepo) ListImagesNeedingDims(_ context.Context, after uuid.UUID, _ int32) ([]model.Document, error) {
	if m.listImagesErr != nil {
		return nil, m.listImagesErr
	}
	if after == uuid.Nil {
		return m.imagesNeedingDims, nil // first page
	}
	return nil, nil // subsequent pages empty → paging terminates
}
func (m *mockRepo) UpdateMimeType(_ context.Context, id uuid.UUID, expectedExternalID, mimeType string) (bool, error) {
	if m.updateMimeErr != nil {
		return false, m.updateMimeErr
	}
	if m.relabelExternalIDs == nil {
		m.relabelExternalIDs = map[uuid.UUID]string{}
	}
	m.relabelExternalIDs[id] = expectedExternalID
	if m.updateMimeLostRace {
		return false, nil
	}
	if m.relabeled == nil {
		m.relabeled = map[uuid.UUID]string{}
	}
	m.relabeled[id] = mimeType
	return true, nil
}

type mockStorage struct {
	data      []byte
	saved     []byte
	deleted   bool
	saveErr   error
	readErr   error
	deleteErr error

	// dataByID serves per-externalID content (MIME-repair tests). When nil,
	// Read falls back to the flat data/readErr pair.
	dataByID map[string][]byte

	// streamBody, when set, is what ReadStream hands back — used to inject a mid-stream read fault.
	streamBody io.ReadCloser
	streamSize int64

	// Streaming-stage controls (spec 020).
	openStageErr    error
	stageWriteErr   error
	stageCommitErr  error
	stageReaderErr  error // StagedReaderAt failure (infra read error, e.g. FS sync / S3 ranged read)
	stageReaderSize int64 // when >0, overrides the buffered size (short-read fault tests)
	stageDedupHit   bool
	stageDiscard    bool // count writes without buffering (allocation tests)
	stages          []*mockStage
}

func (m *mockStorage) Save(content []byte) (model.StoredFile, error) {
	m.saved = content
	if m.saveErr != nil {
		return model.StoredFile{}, m.saveErr
	}
	hash := ComputeHash(content)
	return model.StoredFile{ExternalID: hash, Size: len(content), Created: true}, nil
}
func (m *mockStorage) Read(externalID string) ([]byte, error) {
	if m.dataByID != nil {
		if d, ok := m.dataByID[externalID]; ok {
			return d, nil
		}
		return nil, m.readErr
	}
	return m.data, m.readErr
}
func (m *mockStorage) ReadStream(externalID string) (io.ReadCloser, int64, error) {
	if m.streamBody != nil {
		return m.streamBody, m.streamSize, nil
	}
	b, err := m.Read(externalID)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}
func (m *mockStorage) Delete(_ string) error         { m.deleted = true; return m.deleteErr }
func (m *mockStorage) Exists(_ string) (bool, error) { return m.data != nil, nil }

type mockProcessor struct {
	processErr error

	// Per-test overrides for Process result fields. Defaults: Process returns
	// {Content, MimeType, nil, nil, true} so the marshaling-failed-decode
	// branch in service.marshalContentMetadata is exercised by image-MIME
	// inputs unless dims are populated explicitly.
	processDimsW *int
	processDimsH *int

	// MeasureDims override — used by the dims-backfill sweep tests. Defaults:
	// returns (nil, nil, nil), the no-decoder-available signal, so the sweep
	// skips the persist and leaves the row for a future run.
	measureDimsW     *int
	measureDimsH     *int
	measureDimsErr   error
	measurePanic     bool   // MeasureDims panics for every row
	panicOnContent   []byte // MeasureDims panics only for the row whose bytes match (poison-row tests)
	measureDimsHook  func()
	measureDimsCalls int

	// detectMIME override — when non-empty, replaces the default
	// application/octet-stream (replace-path reconciliation tests).
	detectMIME string

	// Streaming-transcode overrides (spec 020).
	transcodeErr   error
	transcodeMIME  string
	transcodeCalls int
}

func (m *mockProcessor) DetectMIME(_ []byte) string {
	if m.detectMIME != "" {
		return m.detectMIME
	}
	return "application/octet-stream"
}
func (m *mockProcessor) Process(content []byte, mimeType string) (port.ProcessResult, error) {
	if m.processErr != nil {
		return port.ProcessResult{}, m.processErr
	}
	return port.ProcessResult{
		Content:     content,
		MimeType:    mimeType,
		ImageWidth:  m.processDimsW,
		ImageHeight: m.processDimsH,
		Measured:    true,
	}, nil
}
func (m *mockProcessor) MeasureDims(r io.Reader, _ string) (*int, *int, error) {
	m.measureDimsCalls++
	// Drain the reader as a real decoder does — the sweep wraps it to tell a storage read fault
	// apart from a decode failure, and that wrapper only sees a fault if the bytes are pulled.
	b, _ := io.ReadAll(r)
	if m.measureDimsHook != nil {
		m.measureDimsHook()
	}
	if m.measurePanic || (len(m.panicOnContent) > 0 && bytes.Equal(b, m.panicOnContent)) {
		panic("simulated decode panic on a malformed blob")
	}
	return m.measureDimsW, m.measureDimsH, m.measureDimsErr
}

// --- Tests ---

func TestCreateDocument_Happy(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{
		DisplayName:     "test.txt",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
	}

	doc, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.ExternalID == "" {
		t.Error("expected non-empty externalID")
	}
	if doc.ID == uuid.Nil {
		t.Error("expected non-nil document ID")
	}
	if doc.Reused {
		t.Error("expected Reused=false for new content")
	}
}

func TestCreateDocument_OmittedAuthorizationPersistsNil(t *testing.T) {
	repo := &mockRepo{}
	svc := &FileService{
		Logger:    nopLogger,
		Repo:      repo,
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	doc, err := svc.CreateDocument(context.Background(), model.CreateDocumentInput{
		DisplayName:     "snapshot.ybin",
		StorageBucketID: uuid.New(),
		// AuthorizationID intentionally omitted: internal snapshot rows are
		// stored with SQL NULL authorization.
	}, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	if doc.AuthorizationID != uuid.Nil || repo.lastCreateDoc.AuthorizationID != uuid.Nil {
		t.Fatalf("authorization = response %s / repo %s, want uuid.Nil", doc.AuthorizationID, repo.lastCreateDoc.AuthorizationID)
	}
}

// Dedup: same content in same bucket → existing row returned, Reused=true,
// caller-supplied auth/tagset ignored.
func TestCreateDocument_Dedup_SameContentSameBucket_ReturnsExisting(t *testing.T) {
	existingID := uuid.New()
	existingAuth := uuid.New()
	existingTagset := uuid.New()
	bucket := uuid.New()

	repo := &mockRepo{
		findDoc: &model.Document{
			ID:              existingID,
			ExternalID:      "sha3-of-content",
			MimeType:        "text/plain",
			Size:            7,
			StorageBucketID: bucket,
			AuthorizationID: existingAuth,
			TagsetID:        &existingTagset,
		},
	}
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: storage, Processor: &mockProcessor{}}

	callerAuth := uuid.New()
	callerTagset := uuid.New()
	input := model.CreateDocumentInput{
		DisplayName:     "test.txt",
		StorageBucketID: bucket,
		AuthorizationID: callerAuth,
		TagsetID:        &callerTagset,
	}

	doc, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !doc.Reused {
		t.Error("expected Reused=true for dedup match")
	}
	if doc.ID != existingID {
		t.Errorf("ID = %v, want existing %v", doc.ID, existingID)
	}
	if doc.AuthorizationID != existingAuth {
		t.Errorf("AuthorizationID = %v, want existing %v (caller-supplied must be ignored)", doc.AuthorizationID, existingAuth)
	}
	if doc.TagsetID == nil || *doc.TagsetID != existingTagset {
		t.Errorf("TagsetID = %v, want existing %v", doc.TagsetID, existingTagset)
	}
	if repo.createCalls != 0 {
		t.Errorf("expected 0 Create calls on dedup hit, got %d", repo.createCalls)
	}
	if repo.lastFindBkt != bucket {
		t.Errorf("lookup bucket = %v, want %v", repo.lastFindBkt, bucket)
	}
}

// Dedup is per-bucket: same content, different bucket → new row inserted.
func TestCreateDocument_Dedup_SameContentDifferentBucket_InsertsNew(t *testing.T) {
	// Default mockRepo findDoc=nil → returns ErrDocumentNotFound (bucket mismatch).
	repo := &mockRepo{}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	input := model.CreateDocumentInput{
		DisplayName:     "test.txt",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
	}
	doc, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Reused {
		t.Error("expected Reused=false for different-bucket content")
	}
	if repo.createCalls != 1 {
		t.Errorf("expected 1 Create call, got %d", repo.createCalls)
	}
}

// Defensive content-winner classification: a schema variant reports
// ErrDuplicateKey and the follow-up (externalID, bucket) probe finds the
// concurrent row. The service returns that row with Reused=true.
func TestCreateDocument_Dedup_CreateRace_ReturnsWinnerAsReused(t *testing.T) {
	winnerID := uuid.New()
	winnerAuth := uuid.New()
	bucket := uuid.New()

	callCount := 0
	repo := &mockRepoRace{
		find: func() (model.Document, error) {
			callCount++
			if callCount == 1 {
				// First lookup: no existing row → fall through to Create.
				return model.Document{}, model.ErrDocumentNotFound
			}
			// After Create race: the winner is now visible.
			return model.Document{
				ID:              winnerID,
				ExternalID:      "sha3-of-content",
				StorageBucketID: bucket,
				AuthorizationID: winnerAuth,
			}, nil
		},
		createErr: model.ErrDuplicateKey,
	}
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: storage, Processor: &mockProcessor{}}

	input := model.CreateDocumentInput{
		DisplayName:     "test.txt",
		StorageBucketID: bucket,
		AuthorizationID: uuid.New(),
	}
	doc, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !doc.Reused {
		t.Error("expected Reused=true after Create race")
	}
	if doc.ID != winnerID {
		t.Errorf("ID = %v, want winner %v", doc.ID, winnerID)
	}
	if doc.AuthorizationID != winnerAuth {
		t.Errorf("AuthorizationID = %v, want winner %v", doc.AuthorizationID, winnerAuth)
	}
}

func TestCreateDocument_DuplicateKeyWithoutContentWinnerReturnsConflict(t *testing.T) {
	repo := &mockRepoRace{
		find: func() (model.Document, error) {
			return model.Document{}, model.ErrDocumentNotFound
		},
		createErr: model.ErrDuplicateKey,
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	_, err := svc.CreateDocument(context.Background(), model.CreateDocumentInput{
		DisplayName:     "snapshot.ybin",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
	}, []byte("content"), "", nil, 0)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("error = %v, want ErrConflict for authorizationId/tagsetId collision", err)
	}
}

func TestCreateDocument_DuplicateKeyWinnerLookupFailurePropagates(t *testing.T) {
	dbErr := errors.New("database unavailable")
	call := 0
	repo := &mockRepoRace{
		find: func() (model.Document, error) {
			call++
			if call == 1 {
				return model.Document{}, model.ErrDocumentNotFound
			}
			return model.Document{}, dbErr
		},
		createErr: model.ErrDuplicateKey,
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	_, err := svc.CreateDocument(context.Background(), model.CreateDocumentInput{
		DisplayName:     "snapshot.ybin",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
	}, []byte("content"), "", nil, 0)
	if !errors.Is(err, dbErr) {
		t.Fatalf("error = %v, want wrapped lookup failure", err)
	}
	if errors.Is(err, ErrConflict) {
		t.Fatalf("transport failure must not be classified as a client conflict: %v", err)
	}
}

// SkipDedup: true → fresh row inserted even when an existing row matches.
// Lookup must NOT be called; existing row must NOT be returned/mutated.
func TestCreateDocument_SkipDedup_InsertsFreshRowWhenContentExists(t *testing.T) {
	bucket := uuid.New()
	repo := &mockRepo{
		// findDoc set deliberately — proves we never even call FindByExternalIDAndBucket.
		findDoc: &model.Document{
			ID:              uuid.New(),
			ExternalID:      "sha3-of-content",
			StorageBucketID: bucket,
			AuthorizationID: uuid.New(),
		},
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	callerAuth := uuid.New()
	callerTagset := uuid.New()
	input := model.CreateDocumentInput{
		DisplayName:     "placeholder.docx",
		StorageBucketID: bucket,
		AuthorizationID: callerAuth,
		TagsetID:        &callerTagset,
		SkipDedup:       true,
	}

	doc, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Reused {
		t.Error("expected Reused=false when SkipDedup=true")
	}
	if repo.findCalls != 0 {
		t.Errorf("expected 0 FindByExternalIDAndBucket calls when SkipDedup=true, got %d", repo.findCalls)
	}
	if repo.createCalls != 1 {
		t.Errorf("expected 1 Create call, got %d", repo.createCalls)
	}
	if doc.AuthorizationID != callerAuth {
		t.Errorf("AuthorizationID = %v, want caller-supplied %v (existing row's auth must not leak in)", doc.AuthorizationID, callerAuth)
	}
	if doc.TagsetID == nil || *doc.TagsetID != callerTagset {
		t.Errorf("TagsetID = %v, want caller-supplied %v", doc.TagsetID, callerTagset)
	}
}

// SkipDedup: true with empty content → still gets a fresh row.
// This is the Collabora placeholder path: 0-byte create that must not
// merge with another empty-content row in the same bucket.
func TestCreateDocument_SkipDedup_EmptyContent_GetsFreshRow(t *testing.T) {
	bucket := uuid.New()
	repo := &mockRepo{
		findDoc: &model.Document{
			ID:              uuid.New(),
			ExternalID:      "sha3-of-empty",
			StorageBucketID: bucket,
			AuthorizationID: uuid.New(),
		},
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	input := model.CreateDocumentInput{
		DisplayName:     "doc.docx",
		StorageBucketID: bucket,
		AuthorizationID: uuid.New(),
		SkipDedup:       true,
	}
	doc, err := svc.CreateDocument(context.Background(), input, []byte{}, "application/vnd.openxmlformats-officedocument.wordprocessingml.document", []string{"application/vnd.openxmlformats-officedocument.wordprocessingml.document"}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Reused {
		t.Error("expected Reused=false for skipDedup placeholder")
	}
	if repo.findCalls != 0 {
		t.Errorf("expected 0 FindByExternalIDAndBucket calls, got %d", repo.findCalls)
	}
}

// SkipDedup default false → backward-compatible: existing dedup behavior preserved.
// This is the regression guard against accidentally turning dedup off for everyone.
func TestCreateDocument_SkipDedupOmitted_DedupStillApplies(t *testing.T) {
	existingID := uuid.New()
	bucket := uuid.New()
	repo := &mockRepo{
		findDoc: &model.Document{
			ID:              existingID,
			ExternalID:      "sha3-of-content",
			StorageBucketID: bucket,
			AuthorizationID: uuid.New(),
		},
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	input := model.CreateDocumentInput{
		DisplayName:     "test.txt",
		StorageBucketID: bucket,
		AuthorizationID: uuid.New(),
		// SkipDedup omitted → defaults to false
	}
	doc, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !doc.Reused {
		t.Error("expected Reused=true (default dedup path)")
	}
	if doc.ID != existingID {
		t.Errorf("expected existing row ID %v, got %v", existingID, doc.ID)
	}
}

// SkipDedup hits the unique index: surface as ErrConflict, not Reused=true.
// Server-side may add unique(externalID, storageBucketID); if present, a
// SkipDedup placeholder collision must error rather than silently coalesce.
func TestCreateDocument_SkipDedup_DuplicateKey_ReturnsConflict(t *testing.T) {
	repo := &mockRepoRace{
		find: func() (model.Document, error) {
			t.Fatal("FindByExternalIDAndBucket must not be called when SkipDedup=true")
			return model.Document{}, nil
		},
		createErr: model.ErrDuplicateKey,
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	input := model.CreateDocumentInput{
		DisplayName:     "test.docx",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
		SkipDedup:       true,
	}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict on SkipDedup duplicate-key, got %v", err)
	}
}

// mockRepoRace is a minimal repo mock supporting a scripted FindByExternalIDAndBucket
// and a fixed Create error — used for the concurrent race test.
type mockRepoRace struct {
	find      func() (model.Document, error)
	createErr error
}

var _ port.DocumentRepo = (*mockRepoRace)(nil)

func (m *mockRepoRace) GetByID(_ context.Context, _ uuid.UUID) (model.Document, error) {
	return model.Document{}, nil
}
func (m *mockRepoRace) FindByExternalIDAndBucket(_ context.Context, _ string, _ uuid.UUID) (model.Document, error) {
	return m.find()
}
func (m *mockRepoRace) ListImagesNeedingDims(_ context.Context, _ uuid.UUID, _ int32) ([]model.Document, error) {
	return nil, nil
}
func (m *mockRepoRace) Create(_ context.Context, _ model.Document, _ model.ContentMetadata) (uuid.UUID, error) {
	return uuid.Nil, m.createErr
}
func (m *mockRepoRace) UpdateFile(_ context.Context, _ uuid.UUID, _ string, _ int, _, _ string, _ int, _ model.ContentMetadata) error {
	return nil
}
func (m *mockRepoRace) UpdateMetadata(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ bool, _ string, _ int) error {
	return nil
}
func (m *mockRepoRace) BackfillContentMetadata(_ context.Context, _ uuid.UUID, _ string, _ model.ContentMetadata) (bool, error) {
	return true, nil
}
func (m *mockRepoRace) Delete(_ context.Context, _ uuid.UUID) (model.DeletedDocument, error) {
	return model.DeletedDocument{}, nil
}
func (m *mockRepoRace) CountByExternalID(_ context.Context, _ string) (int, error) {
	return 0, nil
}
func (m *mockRepoRace) ListByMimeTypes(_ context.Context, _ []string) ([]model.Document, error) {
	return nil, nil
}
func (m *mockRepoRace) UpdateMimeType(_ context.Context, _ uuid.UUID, _, _ string) (bool, error) {
	return true, nil
}

func TestCreateDocument_TooLarge(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "big.bin", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 3)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestCreateDocument_UnsupportedMIME(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "test.bin", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", []string{"image/jpeg"}, 0)
	if !errors.Is(err, ErrUnsupportedMediaType) {
		t.Errorf("expected ErrUnsupportedMediaType, got %v", err)
	}
}

func TestCreateDocument_EmptyContent_UsesDeclaredMIME(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "new.docx", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	doc, err := svc.CreateDocument(context.Background(), input, []byte{}, "application/vnd.openxmlformats-officedocument.wordprocessingml.document", []string{"application/vnd.openxmlformats-officedocument.wordprocessingml.document"}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.MimeType != "application/vnd.openxmlformats-officedocument.wordprocessingml.document" {
		t.Errorf("MimeType = %q, want docx MIME", doc.MimeType)
	}
}

func TestCreateDocument_EmptyContent_RejectsDisallowedMIME(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "bad.exe", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte{}, "application/x-executable", []string{"image/png", "image/jpeg"}, 0)
	if !errors.Is(err, ErrUnsupportedMediaType) {
		t.Errorf("expected ErrUnsupportedMediaType, got %v", err)
	}
}

// Post-Save cleanup policy: once a blob is stored, we do NOT delete it on
// subsequent DB failures. A concurrent cross-bucket writer may have already
// linked the blob to its own row (storage dedup is global, table dedup is
// per-bucket). Orphaning a row is worse than orphaning a blob; orphan blobs
// are handled by a periodic GC sweep.
func TestCreateDocument_DBFails_DoesNotDeleteBlob(t *testing.T) {
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{createErr: errors.New("db error")},
		Storage:   storage,
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "test.txt", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if storage.deleted {
		t.Error("blob was deleted on DB failure; cross-bucket linker could be orphaned")
	}
}

// CopyDocument: happy path. Source exists in bucket A; copy to bucket B
// produces a fresh row with same content metadata, caller's auth/tagset.
func TestCopyDocument_RequiresAuthorization(t *testing.T) {
	repo := &mockRepo{}
	svc := &FileService{Logger: nopLogger, Repo: repo}

	_, err := svc.CopyDocument(context.Background(), uuid.New(), model.CopyDocumentInput{
		DestinationBucketID: uuid.New(),
		AuthorizationID:     uuid.Nil,
	})
	if !errors.Is(err, ErrInvalidAuthorizationID) {
		t.Fatalf("error = %v, want ErrInvalidAuthorizationID", err)
	}
	if repo.createCalls != 0 {
		t.Fatalf("copy with no authorization must not create a row; calls=%d", repo.createCalls)
	}
}

func TestCopyDocument_Happy(t *testing.T) {
	sourceID := uuid.New()
	bucketA := uuid.New()
	bucketB := uuid.New()
	source := model.Document{
		ID:              sourceID,
		ExternalID:      "sha3-of-content",
		MimeType:        "image/png",
		Size:            42,
		DisplayName:     "banner.png",
		StorageBucketID: bucketA,
		AuthorizationID: uuid.New(),
	}
	repo := &mockRepo{doc: source}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	callerAuth := uuid.New()
	callerTagset := uuid.New()
	doc, err := svc.CopyDocument(context.Background(), sourceID, model.CopyDocumentInput{
		DestinationBucketID: bucketB,
		AuthorizationID:     callerAuth,
		TagsetID:            &callerTagset,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Reused {
		t.Error("expected Reused=false for fresh copy")
	}
	if doc.ID == sourceID {
		t.Error("copy must have a fresh ID, got source ID")
	}
	if doc.ExternalID != source.ExternalID {
		t.Errorf("ExternalID = %q, want %q (content preserved)", doc.ExternalID, source.ExternalID)
	}
	if doc.MimeType != source.MimeType {
		t.Errorf("MimeType = %q, want %q", doc.MimeType, source.MimeType)
	}
	if doc.Size != source.Size {
		t.Errorf("Size = %d, want %d", doc.Size, source.Size)
	}
	if doc.DisplayName != source.DisplayName {
		t.Errorf("DisplayName = %q, want %q", doc.DisplayName, source.DisplayName)
	}
	if doc.StorageBucketID != bucketB {
		t.Errorf("StorageBucketID = %v, want destination %v", doc.StorageBucketID, bucketB)
	}
	if doc.AuthorizationID != callerAuth {
		t.Errorf("AuthorizationID = %v, want caller's %v", doc.AuthorizationID, callerAuth)
	}
	if doc.TagsetID == nil || *doc.TagsetID != callerTagset {
		t.Errorf("TagsetID = %v, want caller's %v", doc.TagsetID, callerTagset)
	}
	if doc.TemporaryLocation {
		t.Error("copies are deliberate placements; TemporaryLocation must be false")
	}
}

// CopyDocument: dedup hit. Destination bucket already has a row with
// this content → returns existing row with Reused=true; caller-supplied
// auth/tagset are NOT applied (regression guard for the reused contract).
func TestCopyDocument_DedupHit_ReturnsExistingReused(t *testing.T) {
	sourceID := uuid.New()
	bucketB := uuid.New()
	existingID := uuid.New()
	existingAuth := uuid.New()
	repo := &mockRepo{
		doc: model.Document{
			ID:         sourceID,
			ExternalID: "sha3-of-content",
			MimeType:   "image/png",
			Size:       42,
		},
		findDoc: &model.Document{
			ID:              existingID,
			ExternalID:      "sha3-of-content",
			StorageBucketID: bucketB,
			AuthorizationID: existingAuth,
		},
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	callerAuth := uuid.New() // should be ignored on dedup hit
	doc, err := svc.CopyDocument(context.Background(), sourceID, model.CopyDocumentInput{
		DestinationBucketID: bucketB,
		AuthorizationID:     callerAuth,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !doc.Reused {
		t.Error("expected Reused=true on dedup hit")
	}
	if doc.ID != existingID {
		t.Errorf("ID = %v, want existing %v", doc.ID, existingID)
	}
	if doc.AuthorizationID != existingAuth {
		t.Errorf("AuthorizationID = %v, want existing %v (caller's must be ignored)", doc.AuthorizationID, existingAuth)
	}
	if repo.createCalls != 0 {
		t.Errorf("expected 0 Create calls on dedup hit, got %d", repo.createCalls)
	}
}

// CopyDocument with SkipDedup=true forces a fresh row even when an
// existing row matches. Dedup lookup must NOT be called.
func TestCopyDocument_SkipDedup_BypassesDedup(t *testing.T) {
	sourceID := uuid.New()
	bucketB := uuid.New()
	repo := &mockRepo{
		doc: model.Document{
			ID:         sourceID,
			ExternalID: "sha3-of-content",
			MimeType:   "image/png",
			Size:       42,
		},
		// findDoc set to prove we never call FindByExternalIDAndBucket.
		findDoc: &model.Document{
			ID:              uuid.New(),
			StorageBucketID: bucketB,
			AuthorizationID: uuid.New(),
		},
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	doc, err := svc.CopyDocument(context.Background(), sourceID, model.CopyDocumentInput{
		DestinationBucketID: bucketB,
		AuthorizationID:     uuid.New(),
		SkipDedup:           true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Reused {
		t.Error("expected Reused=false when SkipDedup=true")
	}
	if repo.findCalls != 0 {
		t.Errorf("expected 0 FindByExternalIDAndBucket calls, got %d", repo.findCalls)
	}
	if repo.createCalls != 1 {
		t.Errorf("expected 1 Create call, got %d", repo.createCalls)
	}
}

// CR-3 regression: Copy from a source whose content_metadata is the
// _decodeFailed sentinel must propagate that sentinel verbatim to the new
// row, not rebuild from (nil, nil) dims into an empty {}. Otherwise the
// destination row's first metadata-returning read re-runs the doomed
// decode work the sentinel exists to short-circuit.
func TestCopyDocument_PropagatesDecodeFailedSentinel(t *testing.T) {
	sourceID := uuid.New()
	bucketB := uuid.New()
	source := model.Document{
		ID:              sourceID,
		ExternalID:      "sha3-of-corrupt",
		MimeType:        "image/jpeg",
		Size:            42,
		DisplayName:     "broken.jpg",
		StorageBucketID: uuid.New(),
		AuthorizationID: uuid.New(),
		// Sentinel: decoder ran and confirmed bytes unreadable.
		ContentMetadata: model.ContentMetadata{Populated: true, DecodeFailed: true},
	}
	repo := &mockRepo{doc: source}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	_, err := svc.CopyDocument(context.Background(), sourceID, model.CopyDocumentInput{
		DestinationBucketID: bucketB,
		AuthorizationID:     uuid.New(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := repo.lastCreateContentMetadata
	if !got.Populated || !got.DecodeFailed {
		t.Errorf("Create content_metadata = %+v, want Populated=true with DecodeFailed=true (sentinel must survive Copy)", got)
	}
}

// CopyDocument when source doesn't exist surfaces ErrDocumentNotFound
// (handler maps to 404).
func TestCopyDocument_SourceNotFound(t *testing.T) {
	repo := &mockRepo{getErr: model.ErrDocumentNotFound}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	_, err := svc.CopyDocument(context.Background(), uuid.New(), model.CopyDocumentInput{
		DestinationBucketID: uuid.New(),
		AuthorizationID:     uuid.New(),
	})
	if !errors.Is(err, model.ErrDocumentNotFound) {
		t.Errorf("expected ErrDocumentNotFound, got %v", err)
	}
}

// CopyDocument with SkipDedup=true and a unique-key violation surfaces
// ErrConflict (handler maps to 409). Mirrors CreateDocument's behavior.
func TestCopyDocument_SkipDedup_DuplicateKey_ReturnsConflict(t *testing.T) {
	sourceID := uuid.New()
	source := model.Document{ID: sourceID, ExternalID: "sha3-of-content", MimeType: "image/png", Size: 42}

	// Use mockRepoRace to script the create behavior (ErrDuplicateKey).
	// GetByID returns the source; find must not be called when SkipDedup=true.
	repo := &copyRaceRepo{source: source, createErr: model.ErrDuplicateKey}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: &mockStorage{}, Processor: &mockProcessor{}}

	_, err := svc.CopyDocument(context.Background(), sourceID, model.CopyDocumentInput{
		DestinationBucketID: uuid.New(),
		AuthorizationID:     uuid.New(),
		SkipDedup:           true,
	})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict on SkipDedup duplicate-key, got %v", err)
	}
}

// copyRaceRepo: minimal mock for CopyDocument duplicate-key tests.
// Returns source on GetByID, errors on Find (must not be called when
// SkipDedup=true), and a fixed Create error.
type copyRaceRepo struct {
	source    model.Document
	createErr error
}

var _ port.DocumentRepo = (*copyRaceRepo)(nil)

func (m *copyRaceRepo) GetByID(_ context.Context, _ uuid.UUID) (model.Document, error) {
	return m.source, nil
}
func (m *copyRaceRepo) FindByExternalIDAndBucket(_ context.Context, _ string, _ uuid.UUID) (model.Document, error) {
	return model.Document{}, model.ErrDocumentNotFound
}
func (m *copyRaceRepo) ListImagesNeedingDims(_ context.Context, _ uuid.UUID, _ int32) ([]model.Document, error) {
	return nil, nil
}
func (m *copyRaceRepo) Create(_ context.Context, _ model.Document, _ model.ContentMetadata) (uuid.UUID, error) {
	return uuid.Nil, m.createErr
}
func (m *copyRaceRepo) UpdateFile(_ context.Context, _ uuid.UUID, _ string, _ int, _, _ string, _ int, _ model.ContentMetadata) error {
	return nil
}
func (m *copyRaceRepo) UpdateMetadata(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ bool, _ string, _ int) error {
	return nil
}
func (m *copyRaceRepo) BackfillContentMetadata(_ context.Context, _ uuid.UUID, _ string, _ model.ContentMetadata) (bool, error) {
	return true, nil
}
func (m *copyRaceRepo) Delete(_ context.Context, _ uuid.UUID) (model.DeletedDocument, error) {
	return model.DeletedDocument{}, nil
}
func (m *copyRaceRepo) CountByExternalID(_ context.Context, _ string) (int, error) {
	return 0, nil
}
func (m *copyRaceRepo) ListByMimeTypes(_ context.Context, _ []string) ([]model.Document, error) {
	return nil, nil
}
func (m *copyRaceRepo) UpdateMimeType(_ context.Context, _ uuid.UUID, _, _ string) (bool, error) {
	return true, nil
}

func TestDeleteDocument_UniqueFile_DeletesFromStorage(t *testing.T) {
	authID := uuid.New()
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:          model.Document{ExternalID: "abc"},
			count:        0, // 0 remaining AFTER delete = last reference
			deleteResult: model.DeletedDocument{ExternalID: "abc", AuthorizationID: authID},
		},
		Storage: storage,
	}

	deleted, err := svc.DeleteDocument(context.Background(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if !storage.deleted {
		t.Error("expected file deletion for unique externalID")
	}
	if deleted.AuthorizationID != authID {
		t.Error("wrong authorizationID returned")
	}
}

func TestDeleteDocument_SharedFile_KeepsFile(t *testing.T) {
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:          model.Document{ExternalID: "shared"},
			count:        1, // 1 remaining AFTER delete = other doc still references it
			deleteResult: model.DeletedDocument{ExternalID: "shared", AuthorizationID: uuid.New()},
		},
		Storage: storage,
	}

	_, err := svc.DeleteDocument(context.Background(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if storage.deleted {
		t.Error("should not delete shared file")
	}
}

func TestStoreAndLink_Happy(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{doc: model.Document{ID: uuid.New()}},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{},
	}

	result, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("new content"))
	if err != nil {
		t.Fatal(err)
	}
	if result.ExternalID == "" {
		t.Error("expected non-empty externalID")
	}
}

// Same post-Save cleanup policy: UpdateFile failures don't delete the new blob,
// because another cross-bucket writer may have linked it in the meantime.
func TestStoreAndLink_DBFails_DoesNotDeleteBlob(t *testing.T) {
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{doc: model.Document{ID: uuid.New()}, updateErr: errors.New("db error")},
		Storage:   storage,
		Processor: &mockProcessor{},
	}

	_, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("content"))
	if err == nil {
		t.Fatal("expected error")
	}
	if storage.deleted {
		t.Error("blob was deleted on UpdateFile failure; cross-bucket linker could be orphaned")
	}
}

func TestStoreAndLink_ConcurrentReplaceLosesCASAndReturnsConflict(t *testing.T) {
	storage := &mockStorage{}
	repo := &mockRepo{
		doc:       model.Document{ID: uuid.New(), ExternalID: "old-hash", MimeType: "application/pdf"},
		updateErr: model.ErrDocumentNotFound, // zero-row expected-old-hash CAS
	}
	svc := &FileService{Logger: nopLogger, Repo: repo, Storage: storage, Processor: &mockProcessor{}}

	_, err := svc.StoreAndLink(context.Background(), repo.doc.ID, []byte("new content"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict for a stale concurrent replace", err)
	}
	if repo.lastUpdateExpected != "old-hash" {
		t.Fatalf("expected-old hash = %q, want old-hash", repo.lastUpdateExpected)
	}
	if storage.deleted {
		t.Fatal("a CAS-losing replace must not run stale old-blob cleanup")
	}
}

func TestUpdateDocumentMetadata_Happy(t *testing.T) {
	docID := uuid.New()
	newBucket := uuid.New()
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{doc: model.Document{
			ID:                docID,
			StorageBucketID:   uuid.New(),
			TemporaryLocation: true,
		}},
	}

	updated, err := svc.UpdateDocumentMetadata(context.Background(), model.Document{ID: docID, Version: 1}, newBucket, false, "renamed.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updated == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestUpdateDocumentMetadata_NotFound(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{updateErr: errors.New("not found")},
	}

	_, err := svc.UpdateDocumentMetadata(context.Background(), model.Document{ID: uuid.New(), Version: 1}, uuid.New(), false, "name.txt")
	if err == nil {
		t.Fatal("expected error for not found")
	}
}

func TestUpdateDocumentMetadata_UpdateFails(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:       model.Document{ID: uuid.New()},
			updateErr: errors.New("db error"),
		},
	}

	_, err := svc.UpdateDocumentMetadata(context.Background(), model.Document{ID: uuid.New(), Version: 1}, uuid.New(), false, "name.txt")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteDocument_NotFound(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{deleteErr: errors.New("not found")},
	}

	_, err := svc.DeleteDocument(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error for missing doc")
	}
}

func TestCreateDocument_StorageFails(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{},
		Storage:   &mockStorage{saveErr: errors.New("disk full")},
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "test.txt", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err == nil {
		t.Fatal("expected error for storage failure")
	}
}

func TestStoreAndLink_DocNotFound(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{getErr: errors.New("not found")},
	}

	_, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("content"))
	if err == nil {
		t.Fatal("expected error for missing doc")
	}
}

func TestStoreAndLink_StorageFails(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{doc: model.Document{ID: uuid.New()}},
		Storage:   &mockStorage{saveErr: errors.New("disk full")},
		Processor: &mockProcessor{},
	}

	_, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("content"))
	if err == nil {
		t.Fatal("expected error for storage failure")
	}
}

func TestCreateDocument_ProcessorFails(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo:    &mockRepo{},
		Storage: &mockStorage{},
		// spec 020: only transcodable images touch the processor, so the
		// failure test must sniff as an image to exercise the route.
		Processor: &mockProcessor{processErr: errors.New("corrupt image"), detectMIME: "image/png"},
	}

	input := model.CreateDocumentInput{DisplayName: "bad.jpg", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err == nil {
		t.Fatal("expected error for processor failure")
	}
}

func TestStoreAndLink_ProcessorFails(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		// spec 020: only transcodable images route through the processor on
		// replace; the stored type must be an image so the failure path runs.
		Repo:      &mockRepo{doc: model.Document{ID: uuid.New(), MimeType: "image/jpeg"}},
		Storage:   &mockStorage{},
		Processor: &mockProcessor{processErr: errors.New("corrupt image"), detectMIME: "image/jpeg"},
	}

	_, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("content"))
	if err == nil {
		t.Fatal("expected error for processor failure")
	}
}

func TestDeleteDocument_CountFails_StillSucceeds(t *testing.T) {
	// Post-delete count failure is non-fatal — row is already deleted
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			deleteResult: model.DeletedDocument{ExternalID: "abc", AuthorizationID: uuid.New()},
			countErr:     errors.New("db error"),
		},
		Storage: storage,
	}

	_, err := svc.DeleteDocument(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("count failure should not fail the delete: %v", err)
	}
	// File should NOT be deleted since count failed (we don't know if it's safe)
	if storage.deleted {
		t.Error("should not delete file when count fails")
	}
}

func TestDeleteDocument_DeleteRepoFails(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:       model.Document{ExternalID: "abc"},
			count:     1,
			deleteErr: errors.New("db error"),
		},
		Storage: &mockStorage{},
	}

	_, err := svc.DeleteDocument(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error for delete failure")
	}
}

func TestStoreAndLink_CleansUpOldBlob(t *testing.T) {
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:   model.Document{ID: uuid.New(), ExternalID: "old-hash"},
			count: 0, // old hash has no remaining references
		},
		Storage:   storage,
		Processor: &mockProcessor{},
	}

	result, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("new content"))
	if err != nil {
		t.Fatal(err)
	}
	// New content produces different hash than "old-hash"
	if result.ExternalID == "old-hash" {
		t.Skip("same hash — no cleanup needed")
	}
	if !storage.deleted {
		t.Error("expected old blob cleanup when no remaining references")
	}
}

func TestStoreAndLink_KeepsOldBlobWhenShared(t *testing.T) {
	storage := &mockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:   model.Document{ID: uuid.New(), ExternalID: "shared-hash"},
			count: 2, // other docs still reference old hash
		},
		Storage:   storage,
		Processor: &mockProcessor{},
	}

	_, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("new content"))
	if err != nil {
		t.Fatal(err)
	}
	if storage.deleted {
		t.Error("should NOT delete old blob when other docs reference it")
	}
}

func TestUpdateDocumentMetadata_VersionConflict(t *testing.T) {
	svc := &FileService{Logger: nopLogger,
		Repo: &mockRepo{
			doc:       model.Document{ID: uuid.New(), Version: 5},
			updateErr: model.ErrDocumentNotFound, // 0 rows = version mismatch
		},
	}

	_, err := svc.UpdateDocumentMetadata(context.Background(), model.Document{ID: uuid.New(), Version: 3}, uuid.New(), false, "name.txt")
	if err == nil {
		t.Fatal("expected error for version conflict")
	}
}

func TestCreateDocument_DBFails_DedupDoesNotDeleteSharedBlob(t *testing.T) {
	storage := &mockStorage{}
	// Override Save to return Created=false (dedup matched existing file)
	dedupStorage := &dedupMockStorage{inner: storage}
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{createErr: errors.New("db error")},
		Storage:   dedupStorage,
		Processor: &mockProcessor{},
	}

	input := model.CreateDocumentInput{DisplayName: "test.txt", StorageBucketID: uuid.New(), AuthorizationID: uuid.New()}
	_, err := svc.CreateDocument(context.Background(), input, []byte("content"), "", nil, 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if dedupStorage.deleted {
		t.Error("should NOT delete file on rollback when dedup matched (Created=false)")
	}
}

// dedupMockStorage simulates dedup: Save returns Created=false
type dedupMockStorage struct {
	inner   *mockStorage
	deleted bool
}

func (m *dedupMockStorage) Save(content []byte) (model.StoredFile, error) {
	hash := ComputeHash(content)
	return model.StoredFile{ExternalID: hash, Size: len(content), Created: false}, nil
}
func (m *dedupMockStorage) Read(id string) ([]byte, error) { return m.inner.Read(id) }
func (m *dedupMockStorage) ReadStream(id string) (io.ReadCloser, int64, error) {
	return m.inner.ReadStream(id)
}
func (m *dedupMockStorage) Delete(_ string) error          { m.deleted = true; return nil }
func (m *dedupMockStorage) Exists(id string) (bool, error) { return m.inner.Exists(id) }

func TestStoreAndLink_DBFails_DedupDoesNotDeleteSharedBlob(t *testing.T) {
	dedupStorage := &dedupMockStorage{}
	svc := &FileService{Logger: nopLogger,
		Repo:      &mockRepo{doc: model.Document{ID: uuid.New(), ExternalID: "old"}, updateErr: errors.New("db error")},
		Storage:   dedupStorage,
		Processor: &mockProcessor{},
	}

	_, err := svc.StoreAndLink(context.Background(), uuid.New(), []byte("content"))
	if err == nil {
		t.Fatal("expected error")
	}
	if dedupStorage.deleted {
		t.Error("should NOT delete file on rollback when dedup matched (Created=false)")
	}
}

// --- Content-metadata mapping and dedup healing ---

func intpService(v int) *int { return &v }

// TestProcessResultToContentMetadata_AllBranches locks in the four-case
// decision tree that turns a ProcessResult into the typed ContentMetadata
// the adapter persists (Decision 9 / T024). Direct unit test because the
// four cases would be tedious to thread through every CreateDocument-driven
// integration scenario.
func TestProcessResultToContentMetadata_AllBranches(t *testing.T) {
	cases := []struct {
		name string
		in   port.ProcessResult
		mime string
		want model.ContentMetadata
	}{
		{
			name: "dims present → Populated with dims",
			in:   port.ProcessResult{MimeType: "image/jpeg", ImageWidth: intpService(800), ImageHeight: intpService(600), Measured: true},
			mime: "image/jpeg",
			want: model.ContentMetadata{Populated: true, ImageWidth: intpService(800), ImageHeight: intpService(600)},
		},
		{
			name: "non-image MIME → empty (Populated=false → '{}' on persist)",
			in:   port.ProcessResult{MimeType: "application/pdf", Measured: false},
			mime: "application/pdf",
			want: model.ContentMetadata{},
		},
		{
			name: "image MIME + Measured=false → empty (no decoder, retry later)",
			in:   port.ProcessResult{MimeType: "image/png", Measured: false},
			mime: "image/png",
			want: model.ContentMetadata{},
		},
		{
			name: "image MIME + Measured=true + nil dims → _decodeFailed sentinel",
			in:   port.ProcessResult{MimeType: "image/jpeg", Measured: true},
			mime: "image/jpeg",
			want: model.ContentMetadata{Populated: true, DecodeFailed: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := processResultToContentMetadata(tc.in, tc.mime)
			if got.Populated != tc.want.Populated || got.DecodeFailed != tc.want.DecodeFailed {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
			if (got.ImageWidth == nil) != (tc.want.ImageWidth == nil) {
				t.Errorf("ImageWidth nil-ness mismatch: got %v, want %v", got.ImageWidth, tc.want.ImageWidth)
			}
			if got.ImageWidth != nil && tc.want.ImageWidth != nil && *got.ImageWidth != *tc.want.ImageWidth {
				t.Errorf("ImageWidth = %d, want %d", *got.ImageWidth, *tc.want.ImageWidth)
			}
			if (got.ImageHeight == nil) != (tc.want.ImageHeight == nil) {
				t.Errorf("ImageHeight nil-ness mismatch: got %v, want %v", got.ImageHeight, tc.want.ImageHeight)
			}
			if got.ImageHeight != nil && tc.want.ImageHeight != nil && *got.ImageHeight != *tc.want.ImageHeight {
				t.Errorf("ImageHeight = %d, want %d", *got.ImageHeight, *tc.want.ImageHeight)
			}
		})
	}
}

// --- Spec 020: streaming stage mock ---

type mockStage struct {
	parent    *mockStorage
	buf       bytes.Buffer
	n         int64
	writeErr  error
	commitErr error
	committed bool
	aborted   bool
}

func (s *mockStage) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	s.n += int64(len(p))
	if s.parent.stageDiscard {
		return len(p), nil
	}
	return s.buf.Write(p)
}

func (s *mockStage) Commit() (model.StoredFile, error) {
	if s.commitErr != nil {
		return model.StoredFile{}, s.commitErr
	}
	s.committed = true
	content := s.buf.Bytes()
	s.parent.saved = content
	return model.StoredFile{
		ExternalID: ComputeHash(content),
		Size:       len(content),
		Created:    !s.parent.stageDedupHit,
	}, nil
}

func (s *mockStage) Abort() error {
	s.aborted = true
	return nil
}

// StagedReaderAt exposes the staged bytes for pre-Commit inspection (the zip
// central-directory recovery path). Backed by a bytes.Reader over the buffer,
// which is an io.ReaderAt.
func (s *mockStage) StagedReaderAt() (io.ReaderAt, int64, error) {
	if s.committed || s.aborted {
		return nil, 0, errors.New("mockStage: reader after commit/abort")
	}
	if s.parent.stageReaderErr != nil {
		return nil, 0, s.parent.stageReaderErr
	}
	b := s.buf.Bytes()
	size := int64(len(b))
	if s.parent.stageReaderSize > 0 {
		size = s.parent.stageReaderSize
	}
	return bytes.NewReader(b), size, nil
}

func (m *mockStorage) OpenStage(_ context.Context) (port.StageWriter, error) {
	if m.openStageErr != nil {
		return nil, m.openStageErr
	}
	commitErr := m.stageCommitErr
	if commitErr == nil {
		commitErr = m.saveErr
	}
	st := &mockStage{parent: m, writeErr: m.stageWriteErr, commitErr: commitErr}
	m.stages = append(m.stages, st)
	return st, nil
}

// TranscodeStream (mock): pass-through copy; honors transcodeErr and reports
// transcodeMIME/dims overrides for ingest-path tests.
func (m *mockProcessor) TranscodeStream(r io.Reader, w io.Writer, mimeType string) (port.TranscodeResult, error) {
	m.transcodeCalls++
	if m.transcodeErr != nil {
		return port.TranscodeResult{}, m.transcodeErr
	}
	if m.processErr != nil {
		return port.TranscodeResult{}, m.processErr
	}
	if _, err := io.Copy(w, r); err != nil {
		return port.TranscodeResult{}, err
	}
	out := mimeType
	if m.transcodeMIME != "" {
		out = m.transcodeMIME
	}
	return port.TranscodeResult{MimeType: out, ImageWidth: m.processDimsW, ImageHeight: m.processDimsH, Measured: m.processDimsW != nil}, nil
}

func (m *dedupMockStorage) OpenStage(_ context.Context) (port.StageWriter, error) {
	return nil, errors.New("dedupMockStorage: OpenStage not used in these tests")
}
