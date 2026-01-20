package storage

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	OffsetSize  = 8 // int64
	SizeSize    = 4 // int32
	CRCSize     = 4 // uint32
	HeaderSize  = OffsetSize + SizeSize + CRCSize // 16 bytes
)

type Record struct {
	Offset  int64
	Size    int32
	CRC     uint32
	Payload []byte
}

// NewRecord creates a record with the given payload.
// Offset is assigned later by the log. Size and CRC are computed automatically.
func NewRecord(payload []byte) *Record {
	return &Record{
		Offset:  0, // assigned when appended to log
		Size:    int32(len(payload)),
		CRC:     crc32.ChecksumIEEE(payload),
		Payload: payload,
	}
}

// Encode serializes the record to bytes for writing to disk.
// Format: [Offset:8][Size:4][CRC:4][Payload:Size]
func (r *Record) Encode() []byte {
	buf := make([]byte, HeaderSize+len(r.Payload))

	binary.BigEndian.PutUint64(buf[0:8], uint64(r.Offset))
	binary.BigEndian.PutUint32(buf[8:12], uint32(r.Size))
	binary.BigEndian.PutUint32(buf[12:16], r.CRC)
	copy(buf[16:], r.Payload)

	return buf
}

// DecodeRecord reads a record from bytes.
// Returns the record and any error encountered.
func DecodeRecord(data []byte) (*Record, error) {
	if len(data) < HeaderSize {
		return nil, ErrInvalidRecord
	}

	offset := int64(binary.BigEndian.Uint64(data[0:8]))
	size := int32(binary.BigEndian.Uint32(data[8:12]))
	crc := binary.BigEndian.Uint32(data[12:16])

	if len(data) < HeaderSize+int(size) {
		return nil, ErrInvalidRecord
	}

	payload := make([]byte, size)
	copy(payload, data[16:16+size])

	return &Record{
		Offset:  offset,
		Size:    size,
		CRC:     crc,
		Payload: payload,
	}, nil
}

// ValidateCRC checks if the payload matches the stored CRC.
func (r *Record) ValidateCRC() bool {
	return crc32.ChecksumIEEE(r.Payload) == r.CRC
}

// TotalSize returns the total bytes this record takes on disk.
func (r *Record) TotalSize() int {
	return HeaderSize + int(r.Size)
}
