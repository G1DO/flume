package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewSegment(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Verify log file was created
	logPath := filepath.Join(dir, "00000000000000000000.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("log file was not created")
	}

	// Verify index file was created
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("index file was not created")
	}
}

func TestSegmentAppend(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Append first record
	rec1 := NewRecord([]byte("hello"))
	off1, err := seg.Append(rec1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if off1 != 0 {
		t.Errorf("expected offset 0, got %d", off1)
	}

	// Append second record
	rec2 := NewRecord([]byte("world"))
	off2, err := seg.Append(rec2)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if off2 != 1 {
		t.Errorf("expected offset 1, got %d", off2)
	}
}

func TestSegmentRead(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Append records
	messages := []string{"first", "second", "third", "fourth", "fifth"}
	for _, msg := range messages {
		rec := NewRecord([]byte(msg))
		_, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Read each record
	for i, expected := range messages {
		rec, err := seg.Read(int64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if string(rec.Payload) != expected {
			t.Errorf("offset %d: expected %q, got %q", i, expected, string(rec.Payload))
		}
	}
}

func TestSegmentReadNotFound(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Append one record
	rec := NewRecord([]byte("only one"))
	seg.Append(rec)

	// Try to read non-existent offset
	_, err = seg.Read(5)
	if err == nil {
		t.Error("expected error reading non-existent offset")
	}
}

func TestSegmentReadBelowBase(t *testing.T) {
	dir := t.TempDir()

	// Segment starting at offset 100
	seg, err := NewSegment(dir, 100, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	rec := NewRecord([]byte("test"))
	seg.Append(rec)

	// Try to read offset below base
	_, err = seg.Read(50)
	if err == nil {
		t.Error("expected error reading offset below base")
	}
}

func TestSegmentRecover(t *testing.T) {
	dir := t.TempDir()

	// Create segment and write records
	seg, err := NewSegment(dir, 0, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	messages := []string{"one", "two", "three"}
	for _, msg := range messages {
		rec := NewRecord([]byte(msg))
		seg.Append(rec)
	}
	seg.Close()

	// Reopen segment
	seg2, err := NewSegment(dir, 0, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment (reopen) failed: %v", err)
	}
	defer seg2.Close()

	// Recover state
	if err := seg2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Verify nextOffset is correct (should be 3)
	rec := NewRecord([]byte("four"))
	off, err := seg2.Append(rec)
	if err != nil {
		t.Fatalf("Append after recovery failed: %v", err)
	}
	if off != 3 {
		t.Errorf("expected offset 3 after recovery, got %d", off)
	}

	// Verify all records readable
	for i, expected := range append(messages, "four") {
		rec, err := seg2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if string(rec.Payload) != expected {
			t.Errorf("offset %d: expected %q, got %q", i, expected, string(rec.Payload))
		}
	}
}

func TestSegmentWithIndex(t *testing.T) {
	dir := t.TempDir()

	// Small index interval to force index entries
	config := ConfigSegment{IndexInterval: 50}
	seg, err := NewSegment(dir, 0, config)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Write many records to create multiple index entries
	for i := 0; i < 100; i++ {
		rec := NewRecord([]byte("test message with some content"))
		_, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Verify index has entries
	if seg.index.Len() < 2 {
		t.Errorf("expected multiple index entries, got %d", seg.index.Len())
	}

	// Read records from middle (should use index)
	rec, err := seg.Read(50)
	if err != nil {
		t.Fatalf("Read offset 50 failed: %v", err)
	}
	if string(rec.Payload) != "test message with some content" {
		t.Errorf("unexpected payload: %q", string(rec.Payload))
	}
}

func TestSegmentFsync(t *testing.T) {
	dir := t.TempDir()

	// Sync after every write
	config := ConfigSegment{SyncWrites: 1}
	seg, err := NewSegment(dir, 0, config)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Should not panic or error with sync enabled
	rec := NewRecord([]byte("synced"))
	_, err = seg.Append(rec)
	if err != nil {
		t.Fatalf("Append with sync failed: %v", err)
	}
}

func TestSegmentNonZeroBaseOffset(t *testing.T) {
	dir := t.TempDir()

	// Segment starting at offset 1000
	seg, err := NewSegment(dir, 1000, ConfigSegment{})
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Verify filename includes base offset
	logPath := filepath.Join(dir, "00000000000000001000.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("log file with correct base offset name was not created")
	}

	// First append should get offset 1000
	rec := NewRecord([]byte("test"))
	off, err := seg.Append(rec)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if off != 1000 {
		t.Errorf("expected offset 1000, got %d", off)
	}

	// Read should work
	rec2, err := seg.Read(1000)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(rec2.Payload) != "test" {
		t.Errorf("unexpected payload: %q", string(rec2.Payload))
	}
}
