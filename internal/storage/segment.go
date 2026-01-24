package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)
type ConfigSegment struct {
	SyncWrites    int   // fsync after N writes (0 = never)
	IndexInterval int64 // add index entry every N bytes (default 4096)
}

// Segment represents a single log segment file.
// Records are appended sequentially and read by offset.
type Segment struct {
	file            *os.File
	baseOffset      int64  // first offset in this segment
	nextOffset      int64  // next offset to assign
	path            string // file path for reopening
	config          ConfigSegment
	writesSinceSync int    // counter (resets after sync)
	index           *Index // sparse index for fast lookups
	currentPos      int64  // current write position in file
}

// NewSegment creates or opens a segment file.
// baseOffset is the starting offset for records in this segment.
func NewSegment(dir string, baseOffset int64, config ConfigSegment) (*Segment, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Segment filename based on base offset (e.g., 00000000000000000000.log)
	filename := formatOffset(baseOffset) + ".log"
	path := filepath.Join(dir, filename)

	// Open file for read/write, create if doesn't exist
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Create index with matching base offset
	indexConfig := IndexConfig{ByteInterval: config.IndexInterval}
	index, err := NewIndex(dir, baseOffset, indexConfig)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Segment{
		file:       file,
		baseOffset: baseOffset,
		nextOffset: baseOffset, // will be updated during recovery
		path:       path,
		config:     config,
		index:      index,
		currentPos: 0, // will be updated during recovery
	}, nil
}

// Append writes a record to the segment and returns the assigned offset.
func (s *Segment) Append(record *Record) (int64, error) {
	// Assign offset to record
	record.Offset = s.nextOffset

	// Remember position before write (for index)
	position := s.currentPos

	// Encode and write
	data := record.Encode()
	_, err := s.file.Write(data)
	if err != nil {
		return 0, err
	}

	// Update position tracker
	recordSize := len(data)
	s.currentPos += int64(recordSize)

	// Maybe add index entry
	if _, err := s.index.MaybeAdd(record.Offset, position, recordSize); err != nil {
		return 0, fmt.Errorf("index update failed: %w", err)
	}

	// Increment offset for next record
	offset := s.nextOffset
	s.nextOffset++

	// Fsync based on config
	s.writesSinceSync++
	if s.config.SyncWrites > 0 && s.writesSinceSync >= s.config.SyncWrites {
		if err := s.file.Sync(); err != nil {
			return 0, fmt.Errorf("sync failed: %w", err)
		}
		s.writesSinceSync = 0
	}

	return offset, nil
}

// Close closes the segment and its index.
func (s *Segment) Close() error {
	// Close index first
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.file.Close()
}

// formatOffset formats an offset as a 20-digit zero-padded string.
// Example: 0 → "00000000000000000000"
func formatOffset(offset int64) string {
	return fmt.Sprintf("%020d", offset)
}


func (s *Segment) Recover() error {
	// Recover index entries from disk
	if err := s.index.Recover(); err != nil {
		return fmt.Errorf("index recovery failed: %w", err)
	}

	// Seek to start of log file
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	// Scan all records to find last offset and current position
	lastOffset := s.baseOffset - 1
	var totalBytes int64 = 0

	for {
		record, err := ReadRecord(s.file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("decode failed: %w", err)
		}

		lastOffset = record.Offset
		totalBytes += int64(record.TotalSize())
	}

	s.nextOffset = lastOffset + 1
	s.currentPos = totalBytes
	return nil
}

func (s *Segment) Read(offset int64) (*Record, error) {
	// Check if offset could be in this segment
	if offset < s.baseOffset {
		return nil, fmt.Errorf("offset %d is below segment base %d", offset, s.baseOffset)
	}

	// Use index to find starting position (instead of scanning from 0)
	startPos := int64(0)
	if pos, found := s.index.Find(offset); found {
		startPos = pos
	}

	// Seek to the position from index
	_, err := s.file.Seek(startPos, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek failed: %w", err)
	}

	// Scan forward until we find the target offset
	for {
		record, err := ReadRecord(s.file)
		if err == io.EOF {
			return nil, fmt.Errorf("offset %d not found", offset)
		}
		if err != nil {
			return nil, fmt.Errorf("read failed: %w", err)
		}
		if record.Offset == offset {
			return record, nil
		}
	}
}
