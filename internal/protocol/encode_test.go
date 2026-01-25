package protocol

import (
	"bytes"
	"testing"
)

func TestRequestHeaderRoundTrip(t *testing.T) {
	original := &RequestHeader{
		Size:      100,
		APIKey:    APIKeyProduce,
		RequestID: 42,
	}

	// Encode
	encoded := EncodeRequestHeader(original)
	if len(encoded) != RequestHeaderSize {
		t.Errorf("expected %d bytes, got %d", RequestHeaderSize, len(encoded))
	}

	// Decode
	decoded, err := DecodeRequestHeader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Size != original.Size {
		t.Errorf("Size: expected %d, got %d", original.Size, decoded.Size)
	}
	if decoded.APIKey != original.APIKey {
		t.Errorf("APIKey: expected %d, got %d", original.APIKey, decoded.APIKey)
	}
	if decoded.RequestID != original.RequestID {
		t.Errorf("RequestID: expected %d, got %d", original.RequestID, decoded.RequestID)
	}
}

func TestResponseHeaderRoundTrip(t *testing.T) {
	original := &ResponseHeader{
		Size:      50,
		RequestID: 123,
	}

	encoded := EncodeResponseHeader(original)
	if len(encoded) != ResponseHeaderSize {
		t.Errorf("expected %d bytes, got %d", ResponseHeaderSize, len(encoded))
	}

	decoded, err := DecodeResponseHeader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Size != original.Size {
		t.Errorf("Size: expected %d, got %d", original.Size, decoded.Size)
	}
	if decoded.RequestID != original.RequestID {
		t.Errorf("RequestID: expected %d, got %d", original.RequestID, decoded.RequestID)
	}
}

func TestProduceRequestRoundTrip(t *testing.T) {
	original := &ProduceRequest{
		Topic:   "orders",
		Payload: []byte(`{"user": 123, "item": "book"}`),
	}

	encoded := EncodeProduceRequest(original)
	decoded, err := DecodeProduceRequest(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Errorf("Topic: expected %q, got %q", original.Topic, decoded.Topic)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("Payload: expected %q, got %q", original.Payload, decoded.Payload)
	}
}

func TestProduceRequestEmptyPayload(t *testing.T) {
	original := &ProduceRequest{
		Topic:   "events",
		Payload: []byte{},
	}

	encoded := EncodeProduceRequest(original)
	decoded, err := DecodeProduceRequest(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Errorf("Topic: expected %q, got %q", original.Topic, decoded.Topic)
	}
	if len(decoded.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(decoded.Payload))
	}
}

func TestProduceResponseRoundTrip(t *testing.T) {
	original := &ProduceResponse{
		Offset:    12345,
		ErrorCode: ErrNone,
	}

	encoded := EncodeProduceResponse(original)
	decoded, err := DecodeProduceResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Offset != original.Offset {
		t.Errorf("Offset: expected %d, got %d", original.Offset, decoded.Offset)
	}
	if decoded.ErrorCode != original.ErrorCode {
		t.Errorf("ErrorCode: expected %d, got %d", original.ErrorCode, decoded.ErrorCode)
	}
}

func TestProduceResponseError(t *testing.T) {
	original := &ProduceResponse{
		Offset:    -1,
		ErrorCode: ErrTopicNotFound,
	}

	encoded := EncodeProduceResponse(original)
	decoded, err := DecodeProduceResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Offset != -1 {
		t.Errorf("expected offset -1, got %d", decoded.Offset)
	}
	if decoded.ErrorCode != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %d", decoded.ErrorCode)
	}
}

func TestFetchRequestRoundTrip(t *testing.T) {
	original := &FetchRequest{
		Topic:    "logs",
		Offset:   1000,
		MaxBytes: 65536,
	}

	encoded := EncodeFetchRequest(original)
	decoded, err := DecodeFetchRequest(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Errorf("Topic: expected %q, got %q", original.Topic, decoded.Topic)
	}
	if decoded.Offset != original.Offset {
		t.Errorf("Offset: expected %d, got %d", original.Offset, decoded.Offset)
	}
	if decoded.MaxBytes != original.MaxBytes {
		t.Errorf("MaxBytes: expected %d, got %d", original.MaxBytes, decoded.MaxBytes)
	}
}

func TestFetchResponseRoundTrip(t *testing.T) {
	original := &FetchResponse{
		ErrorCode: ErrNone,
		Records: []FetchRecord{
			{Offset: 100, Payload: []byte("first message")},
			{Offset: 101, Payload: []byte("second message")},
			{Offset: 102, Payload: []byte("third message")},
		},
	}

	encoded := EncodeFetchResponse(original)
	decoded, err := DecodeFetchResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ErrorCode != original.ErrorCode {
		t.Errorf("ErrorCode: expected %d, got %d", original.ErrorCode, decoded.ErrorCode)
	}
	if len(decoded.Records) != len(original.Records) {
		t.Fatalf("Records count: expected %d, got %d", len(original.Records), len(decoded.Records))
	}

	for i, rec := range decoded.Records {
		if rec.Offset != original.Records[i].Offset {
			t.Errorf("Record %d Offset: expected %d, got %d", i, original.Records[i].Offset, rec.Offset)
		}
		if !bytes.Equal(rec.Payload, original.Records[i].Payload) {
			t.Errorf("Record %d Payload: expected %q, got %q", i, original.Records[i].Payload, rec.Payload)
		}
	}
}

func TestFetchResponseEmpty(t *testing.T) {
	original := &FetchResponse{
		ErrorCode: ErrNone,
		Records:   []FetchRecord{},
	}

	encoded := EncodeFetchResponse(original)
	decoded, err := DecodeFetchResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ErrorCode != ErrNone {
		t.Errorf("expected ErrNone, got %d", decoded.ErrorCode)
	}
	if len(decoded.Records) != 0 {
		t.Errorf("expected 0 records, got %d", len(decoded.Records))
	}
}

func TestFetchResponseError(t *testing.T) {
	original := &FetchResponse{
		ErrorCode: ErrOffsetOutOfRange,
		Records:   nil,
	}

	encoded := EncodeFetchResponse(original)
	decoded, err := DecodeFetchResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ErrorCode != ErrOffsetOutOfRange {
		t.Errorf("expected ErrOffsetOutOfRange, got %d", decoded.ErrorCode)
	}
}

func TestDecodeProduceRequestTruncated(t *testing.T) {
	// Too short
	_, err := DecodeProduceRequest([]byte{0, 1})
	if err == nil {
		t.Error("expected error for truncated request")
	}
}

func TestDecodeFetchRequestTruncated(t *testing.T) {
	_, err := DecodeFetchRequest([]byte{0, 1, 2})
	if err == nil {
		t.Error("expected error for truncated request")
	}
}

func TestDecodeFetchResponseTruncated(t *testing.T) {
	_, err := DecodeFetchResponse([]byte{0, 1})
	if err == nil {
		t.Error("expected error for truncated response")
	}
}
