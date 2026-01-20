package storage

import (
	"testing"
)

func TestNewRecord(t *testing.T) {
	payload := []byte("hello")
	record := NewRecord(payload)

	if record.Size != int32(len(payload)) {
		t.Errorf("expected size %d, got %d", len(payload), record.Size)
	}

	if record.Offset != 0 {
		t.Errorf("expected offset 0, got %d", record.Offset)
	}

	if record.CRC == 0 {
		t.Error("expected CRC to be computed, got 0")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	payload := []byte("hello world")
	original := NewRecord(payload)
	original.Offset = 42 // simulate log assigning offset

	// Encode to bytes
	encoded := original.Encode()

	// Decode back
	decoded, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify all fields match
	if decoded.Offset != original.Offset {
		t.Errorf("offset: expected %d, got %d", original.Offset, decoded.Offset)
	}
	if decoded.Size != original.Size {
		t.Errorf("size: expected %d, got %d", original.Size, decoded.Size)
	}
	if decoded.CRC != original.CRC {
		t.Errorf("CRC: expected %d, got %d", original.CRC, decoded.CRC)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Errorf("payload: expected %s, got %s", original.Payload, decoded.Payload)
	}
}

func TestValidateCRC(t *testing.T) {
	record := NewRecord([]byte("test data"))

	if !record.ValidateCRC() {
		t.Error("CRC should be valid for uncorrupted record")
	}

	// Corrupt the payload
	record.Payload[0] = 'X'

	if record.ValidateCRC() {
		t.Error("CRC should be invalid after corruption")
	}
}

func TestDecodeInvalidData(t *testing.T) {
	// Too short - less than header size
	_, err := DecodeRecord([]byte{1, 2, 3})
	if err != ErrInvalidRecord {
		t.Errorf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestTotalSize(t *testing.T) {
	payload := []byte("hello")
	record := NewRecord(payload)

	expected := HeaderSize + len(payload) // 16 + 5 = 21
	if record.TotalSize() != expected {
		t.Errorf("expected total size %d, got %d", expected, record.TotalSize())
	}
}
