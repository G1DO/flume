package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Index entry size: 8 bytes offset + 8 bytes position = 16 bytes
const IndexEntrySize = 16

// IndexEntry maps a record offset to its physical position in the log file.
// This allows O(1) lookup instead of scanning from the start.
type IndexEntry struct {
	Offset   int64 // logical record number (0, 1, 2, ...)
	Position int64 // byte position in the .log file
}

// IndexConfig controls when new index entries are created.
type IndexConfig struct {
	ByteInterval int64 // add index entry every N bytes written (e.g., 4096)
}

// Index provides fast offset-to-position lookups using a sparse index.
// Instead of indexing every record, it indexes every ~N bytes of data.
// Lookups: binary search to find nearest entry, then small linear scan.
type Index struct {
	entries      []IndexEntry // sorted by offset, kept in memory for binary search
	file         *os.File     // .index file for persistence
	path         string       // file path (for reopening/debugging)
	config       IndexConfig  // controls indexing frequency
	bytesWritten int64        // bytes written since last index entry
}

// NewIndex creates or opens an index file.
// baseOffset is used to name the file (matches the segment file naming).
func NewIndex(dir string, baseOffset int64, config IndexConfig) (*Index, error) {
	// Create directory if needed
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Index file: 00000000000000000000.index
	filename := formatOffset(baseOffset) + ".index"
	path := filepath.Join(dir, filename)

	// Open for read/write, create if doesn't exist
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Default byte interval if not set
	if config.ByteInterval <= 0 {
		config.ByteInterval = 4096 // 4KB default
	}

	return &Index{
		entries:      make([]IndexEntry, 0),
		file:         file,
		path:         path,
		config:       config,
		bytesWritten: 0,
	}, nil
}

// MaybeAdd checks if we should add an index entry based on bytes written.
// Call this after every record append with the record's size and position.
// Returns true if an entry was added.
func (idx *Index) MaybeAdd(offset int64, position int64, recordSize int) (bool, error) {
	idx.bytesWritten += int64(recordSize)

	// Check if we've crossed the byte threshold
	if idx.bytesWritten >= idx.config.ByteInterval || len(idx.entries) == 0 {
		// Add entry
		entry := IndexEntry{Offset: offset, Position: position}
		idx.entries = append(idx.entries, entry)

		// Persist to disk
		if err := idx.writeEntry(entry); err != nil {
			return false, err
		}

		// Reset counter
		idx.bytesWritten = 0
		return true, nil
	}

	return false, nil
}

// writeEntry appends a single entry to the index file.
func (idx *Index) writeEntry(entry IndexEntry) error {
	buf := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(entry.Position))

	_, err := idx.file.Write(buf)
	return err
}

// Find returns the file position to start scanning for the given offset.
// Uses binary search to find the largest indexed offset <= target.
// Returns (position, true) if found, or (0, false) if index is empty.
func (idx *Index) Find(targetOffset int64) (int64, bool) {
	if len(idx.entries) == 0 {
		return 0, false
	}

	// Binary search: find largest offset <= targetOffset
	// sort.Search returns the smallest i where f(i) is true
	// We want the largest offset <= target, so we search for first offset > target, then go back one
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Offset > targetOffset
	})

	// i is now the first entry with offset > target (or len if none)
	// We want the entry before that
	if i == 0 {
		// All entries have offset > target, but if first entry offset == target, use it
		if idx.entries[0].Offset == targetOffset {
			return idx.entries[0].Position, true
		}
		// Target is before our first indexed offset
		// Return position 0 (start of file)
		return 0, true
	}

	// Entry at i-1 has offset <= target
	return idx.entries[i-1].Position, true
}

// Recover loads index entries from disk into memory.
// Call this on startup to restore the index.
func (idx *Index) Recover() error {
	// Seek to start of file
	_, err := idx.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	// Clear existing entries
	idx.entries = make([]IndexEntry, 0)

	// Read entries until EOF
	buf := make([]byte, IndexEntrySize)
	for {
		_, err := io.ReadFull(idx.file, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read failed: %w", err)
		}

		entry := IndexEntry{
			Offset:   int64(binary.BigEndian.Uint64(buf[0:8])),
			Position: int64(binary.BigEndian.Uint64(buf[8:16])),
		}
		idx.entries = append(idx.entries, entry)
	}

	return nil
}

// Close closes the index file.
func (idx *Index) Close() error {
	return idx.file.Close()
}

// Len returns the number of index entries.
func (idx *Index) Len() int {
	return len(idx.entries)
}
