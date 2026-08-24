package alkemiodb

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestUuidToPgx(t *testing.T) {
	id := uuid.New()
	pgxID := uuidToPgx(id)
	if !pgxID.Valid {
		t.Error("expected Valid=true")
	}
	if pgxID.Bytes != id {
		t.Error("bytes mismatch")
	}
}

func TestUuidToPgxNullable_Nil(t *testing.T) {
	pgxID := uuidToPgxNullable(nil)
	if pgxID.Valid {
		t.Error("expected Valid=false for nil")
	}
}

func TestUuidToPgxNullable_NonNil(t *testing.T) {
	id := uuid.New()
	pgxID := uuidToPgxNullable(&id)
	if !pgxID.Valid {
		t.Error("expected Valid=true")
	}
}

func TestUuidToPgxNullableNil_NilUUID(t *testing.T) {
	pgxID := uuidToPgxNullableNil(uuid.Nil)
	if pgxID.Valid {
		t.Error("uuid.Nil must map to SQL NULL")
	}
}

func TestUuidToPgxNullableNil_RealUUID(t *testing.T) {
	id := uuid.New()
	pgxID := uuidToPgxNullableNil(id)
	if !pgxID.Valid || pgxID.Bytes != id {
		t.Fatalf("real UUID mapping = %+v, want valid %s", pgxID, id)
	}
}

func TestPgxToUUID_Valid(t *testing.T) {
	id := uuid.New()
	pgxID := pgtype.UUID{Bytes: id, Valid: true}
	got := pgxToUUID(pgxID)
	if got != id {
		t.Errorf("got %v, want %v", got, id)
	}
}

func TestPgxToUUID_Invalid(t *testing.T) {
	pgxID := pgtype.UUID{Valid: false}
	got := pgxToUUID(pgxID)
	if got != uuid.Nil {
		t.Errorf("got %v, want Nil", got)
	}
}

func TestPgxToUUIDNullable_Valid(t *testing.T) {
	id := uuid.New()
	pgxID := pgtype.UUID{Bytes: id, Valid: true}
	got := pgxToUUIDNullable(pgxID)
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if *got != id {
		t.Errorf("got %v, want %v", *got, id)
	}
}

func TestPgxToUUIDNullable_Invalid(t *testing.T) {
	pgxID := pgtype.UUID{Valid: false}
	got := pgxToUUIDNullable(pgxID)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestTimeToPgx(t *testing.T) {
	now := time.Now()
	pgxT := timeToPgx(now)
	if !pgxT.Valid {
		t.Error("expected Valid=true")
	}
	if !pgxT.Time.Equal(now) {
		t.Error("time mismatch")
	}
}

func TestTimeToPgxNow(t *testing.T) {
	before := time.Now()
	pgxT := timeToPgxNow()
	after := time.Now()

	if !pgxT.Valid {
		t.Error("expected Valid=true")
	}
	if pgxT.Time.Before(before) || pgxT.Time.After(after) {
		t.Error("time not in expected range")
	}
}

func TestPgxToTime_Valid(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	pgxT := pgtype.Timestamptz{Time: now, Valid: true}
	got := pgxToTime(pgxT)
	if !got.Equal(now) {
		t.Errorf("got %v, want %v", got, now)
	}
}

func TestPgxToTime_Invalid(t *testing.T) {
	pgxT := pgtype.Timestamptz{Valid: false}
	got := pgxToTime(pgxT)
	if !got.IsZero() {
		t.Errorf("expected zero time, got %v", got)
	}
}

func TestSafeInt32_Normal(t *testing.T) {
	if got := safeInt32(42); got != 42 {
		t.Errorf("got %d", got)
	}
}

func TestSafeInt32_Overflow(t *testing.T) {
	if got := safeInt32(math.MaxInt32 + 1); got != math.MaxInt32 {
		t.Errorf("got %d, want MaxInt32", got)
	}
}

func TestSafeInt32_Underflow(t *testing.T) {
	if got := safeInt32(math.MinInt32 - 1); got != math.MinInt32 {
		t.Errorf("got %d, want MinInt32", got)
	}
}

func TestSafeInt32_Zero(t *testing.T) {
	if got := safeInt32(0); got != 0 {
		t.Errorf("got %d", got)
	}
}
