package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewLog(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, LogConfig{})
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Should have one segment
	if len(log.segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(log.segments))
	}

	// First segment file should exist
	logPath := filepath.Join(dir, "00000000000000000000.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("first segment file was not created")
	}
}

func TestLogAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, LogConfig{})
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Append records
	messages := []string{"first", "second", "third", "fourth", "fifth"}
	for i, msg := range messages {
		rec := NewRecord([]byte(msg))
		off, err := log.Append(rec)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if off != int64(i) {
			t.Errorf("expected offset %d, got %d", i, off)
		}
	}

	// Read back each record
	for i, expected := range messages {
		rec, err := log.Read(int64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if string(rec.Payload) != expected {
			t.Errorf("offset %d: expected %q, got %q", i, expected, string(rec.Payload))
		}
	}
}

func TestLogRollover(t *testing.T) {
	dir := t.TempDir()

	// Very small max segment size to force rollover
	config := LogConfig{
		MaxSegmentBytes: 100, // 100 bytes triggers rollover quickly
	}
	log, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Append enough records to trigger rollover
	// Each record ~40+ bytes (16 header + payload)
	for i := 0; i < 10; i++ {
		rec := NewRecord([]byte("message that is reasonably long"))
		_, err := log.Append(rec)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Should have multiple segments now
	if len(log.segments) < 2 {
		t.Errorf("expected multiple segments after rollover, got %d", len(log.segments))
	}

	// All records should still be readable
	for i := 0; i < 10; i++ {
		rec, err := log.Read(int64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if string(rec.Payload) != "message that is reasonably long" {
			t.Errorf("offset %d: unexpected payload", i)
		}
	}
}

func TestLogRecover(t *testing.T) {
	dir := t.TempDir()

	// Create log and write records
	log, err := NewLog(dir, LogConfig{})
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	messages := []string{"alpha", "beta", "gamma"}
	for _, msg := range messages {
		rec := NewRecord([]byte(msg))
		log.Append(rec)
	}
	log.Close()

	// Reopen log
	log2, err := NewLog(dir, LogConfig{})
	if err != nil {
		t.Fatalf("NewLog (reopen) failed: %v", err)
	}
	defer log2.Close()

	// Recover
	if err := log2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Verify nextOffset is correct
	rec := NewRecord([]byte("delta"))
	off, err := log2.Append(rec)
	if err != nil {
		t.Fatalf("Append after recovery failed: %v", err)
	}
	if off != 3 {
		t.Errorf("expected offset 3 after recovery, got %d", off)
	}

	// Verify all records readable
	allMessages := append(messages, "delta")
	for i, expected := range allMessages {
		rec, err := log2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if string(rec.Payload) != expected {
			t.Errorf("offset %d: expected %q, got %q", i, expected, string(rec.Payload))
		}
	}
}

func TestLogRecoverMultipleSegments(t *testing.T) {
	dir := t.TempDir()

	// Small segments to create multiple
	config := LogConfig{MaxSegmentBytes: 100}
	log, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	// Write enough to create multiple segments
	for i := 0; i < 20; i++ {
		rec := NewRecord([]byte("segment test message"))
		log.Append(rec)
	}

	numSegments := len(log.segments)
	if numSegments < 2 {
		t.Fatalf("expected multiple segments, got %d", numSegments)
	}
	log.Close()

	// Reopen and recover
	log2, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("NewLog (reopen) failed: %v", err)
	}
	defer log2.Close()

	if err := log2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Should have same number of segments
	if len(log2.segments) != numSegments {
		t.Errorf("expected %d segments after recovery, got %d", numSegments, len(log2.segments))
	}

	// All records should be readable
	for i := 0; i < 20; i++ {
		rec, err := log2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if string(rec.Payload) != "segment test message" {
			t.Errorf("offset %d: unexpected payload", i)
		}
	}
}

func TestLogOldestNewestOffset(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, LogConfig{})
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Empty log
	if log.OldestOffset() != 0 {
		t.Errorf("expected oldest 0, got %d", log.OldestOffset())
	}
	if log.NewestOffset() != 0 {
		t.Errorf("expected newest 0, got %d", log.NewestOffset())
	}

	// After appending
	for i := 0; i < 5; i++ {
		rec := NewRecord([]byte("test"))
		log.Append(rec)
	}

	if log.OldestOffset() != 0 {
		t.Errorf("expected oldest 0, got %d", log.OldestOffset())
	}
	if log.NewestOffset() != 5 {
		t.Errorf("expected newest 5, got %d", log.NewestOffset())
	}
}

func TestLogReadNotFound(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, LogConfig{})
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Append a few records
	for i := 0; i < 3; i++ {
		rec := NewRecord([]byte("test"))
		log.Append(rec)
	}

	// Try to read non-existent offset
	_, err = log.Read(100)
	if err == nil {
		t.Error("expected error reading non-existent offset")
	}
}

func TestLogFindSegment(t *testing.T) {
	dir := t.TempDir()

	// Small segments
	config := LogConfig{MaxSegmentBytes: 50}
	log, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	// Create enough data to have multiple segments
	for i := 0; i < 10; i++ {
		rec := NewRecord([]byte("find segment test"))
		log.Append(rec)
	}

	// Make sure we have multiple segments
	if len(log.segments) < 2 {
		t.Skip("need multiple segments for this test")
	}

	// findSegment should return correct segment for various offsets
	for i := 0; i < 10; i++ {
		seg := log.findSegment(int64(i))
		if seg == nil {
			t.Errorf("findSegment returned nil for offset %d", i)
			continue
		}
		// Verify offset is within segment's range
		if int64(i) < seg.baseOffset {
			t.Errorf("offset %d below segment base %d", i, seg.baseOffset)
		}
	}
}
