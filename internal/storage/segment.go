package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)
type ConfigSegment struct{
	SyncWrites int
}

// Segment represents a single log segment file.
// Records are appended sequentially and read by offset.
type Segment struct {
	file            *os.File
	baseOffset      int64  // first offset in this segment
	nextOffset      int64  // next offset to assign
	path            string // file path for reopening
	config          ConfigSegment
	writesSinceSync int // counter (resets after sync)
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

	return &Segment{
		file:       file,
		baseOffset: baseOffset,
		nextOffset: baseOffset, // will be updated during recovery
		path:       path,
		config:     config,
	}, nil
}

// Append writes a record to the segment and returns the assigned offset.
func (s *Segment) Append(record *Record) (int64, error) {
	// Assign offset to record
	record.Offset = s.nextOffset

	// Encode and write
	data := record.Encode()
	_, err := s.file.Write(data)
	if err != nil {
		return 0, err
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

// Close closes the segment file.
func (s *Segment) Close() error {
	return s.file.Close()
}

// formatOffset formats an offset as a 20-digit zero-padded string.
// Example: 0 → "00000000000000000000"
func formatOffset(offset int64) string {
	return fmt.Sprintf("%020d", offset)
}


func (s *Segment) Recover() error {
    // 1. Seek to start of file
    	_, err := s.file.Seek(0, io.SeekStart)
if err != nil {
    return fmt.Errorf("seek failed: %w", err)
}
    // 2. Loop: decode records until EOF
    //    - track the last offset seen
	lastOffset :=s.baseOffset-1
    for {
		record, err := ReadRecord(s.file)
if err == io.EOF {
    break
}
if err != nil {
 return fmt.Errorf("decode failed: %w", err)

    
}
    // 3. Update s.nextOffset
    //    - if empty: stays at baseOffset
    //    - if records found: lastOffset + 1
         lastOffset = record.Offset
   
 


}
s.nextOffset = lastOffset + 1
		 return nil
    

}

func (s *Segment) Read(offset int64) (*Record, error) {
	// Check if offset could be in this segment
	if offset < s.baseOffset {
		return nil, fmt.Errorf("offset %d is below segment base %d", offset, s.baseOffset)
	}
//point on first of the file
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek failed: %w", err)
	}

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
