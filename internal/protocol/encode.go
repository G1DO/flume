package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// --- Request Header ---

// EncodeRequestHeader writes a request header to bytes.
func EncodeRequestHeader(h *RequestHeader) []byte {
	buf := make([]byte, RequestHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(h.Size))
	binary.BigEndian.PutUint16(buf[4:6], uint16(h.APIKey))
	binary.BigEndian.PutUint32(buf[6:10], uint32(h.RequestID))
	return buf
}

// DecodeRequestHeader reads a request header from a reader.
func DecodeRequestHeader(r io.Reader) (*RequestHeader, error) {
	buf := make([]byte, RequestHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return &RequestHeader{
		Size:      int32(binary.BigEndian.Uint32(buf[0:4])),
		APIKey:    int16(binary.BigEndian.Uint16(buf[4:6])),
		RequestID: int32(binary.BigEndian.Uint32(buf[6:10])),
	}, nil
}

// --- Response Header ---

// EncodeResponseHeader writes a response header to bytes.
func EncodeResponseHeader(h *ResponseHeader) []byte {
	buf := make([]byte, ResponseHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(h.Size))
	binary.BigEndian.PutUint32(buf[4:8], uint32(h.RequestID))
	return buf
}

// DecodeResponseHeader reads a response header from a reader.
func DecodeResponseHeader(r io.Reader) (*ResponseHeader, error) {
	buf := make([]byte, ResponseHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return &ResponseHeader{
		Size:      int32(binary.BigEndian.Uint32(buf[0:4])),
		RequestID: int32(binary.BigEndian.Uint32(buf[4:8])),
	}, nil
}

// --- Produce Request ---

// EncodeProduceRequest encodes a produce request to bytes.
// Format: [TopicLen:2][Topic:var][Partition:4][KeyLen:4][Key:var][PayloadLen:4][Payload:var]
func EncodeProduceRequest(req *ProduceRequest) []byte {
	topicBytes := []byte(req.Topic)
	size := 2 + len(topicBytes) + 4 + 4 + len(req.Key) + 4 + len(req.Payload)
	buf := make([]byte, size)

	offset := 0
	// Topic length (2 bytes)
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(topicBytes)))
	offset += 2
	// Topic
	copy(buf[offset:], topicBytes)
	offset += len(topicBytes)
	// Partition (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(req.Partition))
	offset += 4
	// Key length (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(req.Key)))
	offset += 4
	// Key
	copy(buf[offset:], req.Key)
	offset += len(req.Key)
	// Payload length (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(req.Payload)))
	offset += 4
	// Payload
	copy(buf[offset:], req.Payload)

	return buf
}

// DecodeProduceRequest decodes a produce request from bytes.
func DecodeProduceRequest(data []byte) (*ProduceRequest, error) {
	if len(data) < 14 { // minimum: 2 (topic len) + 0 (topic) + 4 (partition) + 4 (key len) + 4 (payload len)
		return nil, fmt.Errorf("produce request too short")
	}

	offset := 0
	// Topic length
	topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if len(data) < offset+topicLen+12 {
		return nil, fmt.Errorf("produce request truncated")
	}

	// Topic
	topic := string(data[offset : offset+topicLen])
	offset += topicLen

	// Partition
	partition := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Key length
	keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if len(data) < offset+keyLen+4 {
		return nil, fmt.Errorf("produce request key truncated")
	}

	// Key
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+keyLen])
	offset += keyLen

	// Payload length
	payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if len(data) < offset+payloadLen {
		return nil, fmt.Errorf("produce request payload truncated")
	}

	// Payload
	payload := make([]byte, payloadLen)
	copy(payload, data[offset:offset+payloadLen])

	return &ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Payload:   payload,
	}, nil
}

// --- Produce Response ---

// EncodeProduceResponse encodes a produce response to bytes.
// Format: [Partition:4][Offset:8][ErrorCode:2]
func EncodeProduceResponse(resp *ProduceResponse) []byte {
	buf := make([]byte, 14)
	binary.BigEndian.PutUint32(buf[0:4], uint32(resp.Partition))
	binary.BigEndian.PutUint64(buf[4:12], uint64(resp.Offset))
	binary.BigEndian.PutUint16(buf[12:14], uint16(resp.ErrorCode))
	return buf
}

// DecodeProduceResponse decodes a produce response from bytes.
func DecodeProduceResponse(data []byte) (*ProduceResponse, error) {
	if len(data) < 14 {
		return nil, fmt.Errorf("produce response too short")
	}
	return &ProduceResponse{
		Partition: int32(binary.BigEndian.Uint32(data[0:4])),
		Offset:    int64(binary.BigEndian.Uint64(data[4:12])),
		ErrorCode: int16(binary.BigEndian.Uint16(data[12:14])),
	}, nil
}

// --- Fetch Request ---

// EncodeFetchRequest encodes a fetch request to bytes.
// Format: [TopicLen:2][Topic:var][Partition:4][Offset:8][MaxBytes:4]
func EncodeFetchRequest(req *FetchRequest) []byte {
	topicBytes := []byte(req.Topic)
	size := 2 + len(topicBytes) + 4 + 8 + 4
	buf := make([]byte, size)

	offset := 0
	// Topic length
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(topicBytes)))
	offset += 2
	// Topic
	copy(buf[offset:], topicBytes)
	offset += len(topicBytes)
	// Partition (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(req.Partition))
	offset += 4
	// Offset (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(req.Offset))
	offset += 8
	// MaxBytes (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(req.MaxBytes))

	return buf
}

// DecodeFetchRequest decodes a fetch request from bytes.
func DecodeFetchRequest(data []byte) (*FetchRequest, error) {
	if len(data) < 18 { // 2 + 0 + 4 + 8 + 4
		return nil, fmt.Errorf("fetch request too short")
	}

	offset := 0
	// Topic length
	topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if len(data) < offset+topicLen+16 {
		return nil, fmt.Errorf("fetch request truncated")
	}

	// Topic
	topic := string(data[offset : offset+topicLen])
	offset += topicLen

	// Partition
	partition := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Offset
	fetchOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// MaxBytes
	maxBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))

	return &FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    fetchOffset,
		MaxBytes:  maxBytes,
	}, nil
}

// --- Fetch Response ---

// EncodeFetchResponse encodes a fetch response to bytes.
// Format: [ErrorCode:2][RecordCount:4][Records:var]
// Each record: [Offset:8][PayloadLen:4][Payload:var]
func EncodeFetchResponse(resp *FetchResponse) []byte {
	// Calculate total size
	size := 2 + 4 // ErrorCode + RecordCount
	for _, rec := range resp.Records {
		size += 8 + 4 + len(rec.Payload) // Offset + PayloadLen + Payload
	}

	buf := make([]byte, size)
	offset := 0

	// ErrorCode
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(resp.ErrorCode))
	offset += 2

	// RecordCount
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(resp.Records)))
	offset += 4

	// Records
	for _, rec := range resp.Records {
		// Offset
		binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(rec.Offset))
		offset += 8
		// PayloadLen
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(rec.Payload)))
		offset += 4
		// Payload
		copy(buf[offset:], rec.Payload)
		offset += len(rec.Payload)
	}

	return buf
}

// DecodeFetchResponse decodes a fetch response from bytes.
func DecodeFetchResponse(data []byte) (*FetchResponse, error) {
	if len(data) < 6 { // ErrorCode(2) + RecordCount(4)
		return nil, fmt.Errorf("fetch response too short")
	}

	offset := 0

	// ErrorCode
	errorCode := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	// RecordCount
	recordCount := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Records
	records := make([]FetchRecord, 0, recordCount)
	for i := 0; i < recordCount; i++ {
		if len(data) < offset+12 { // Offset(8) + PayloadLen(4)
			return nil, fmt.Errorf("fetch response record %d truncated", i)
		}

		// Offset
		recOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// PayloadLen
		payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if len(data) < offset+payloadLen {
			return nil, fmt.Errorf("fetch response record %d payload truncated", i)
		}

		// Payload
		payload := make([]byte, payloadLen)
		copy(payload, data[offset:offset+payloadLen])
		offset += payloadLen

		records = append(records, FetchRecord{
			Offset:  recOffset,
			Payload: payload,
		})
	}

	return &FetchResponse{
		ErrorCode: errorCode,
		Records:   records,
	}, nil
}
