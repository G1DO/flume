package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// LogConfig controls log behavior.
type LogConfig struct {
	MaxSegmentBytes int64 // rollover to new segment after this size (e.g., 1GB)
	SegmentConfig   ConfigSegment
}

// Log manages multiple segments, providing a single append-only log abstraction.
// Handles automatic rollover when segments get too large.
type Log struct {
	dir           string     // directory containing all segment files
	segments      []*Segment // all segments, sorted by baseOffset
	activeSegment *Segment   // current segment for writes
	config        LogConfig
}

// NewLog opens or creates a log in the given directory.
// Loads existing segments or creates the first one if empty.
func NewLog(dir string, config LogConfig) (*Log, error) {
	// Create directory if needed
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Set default max segment size (1GB)
	if config.MaxSegmentBytes <= 0 {
		config.MaxSegmentBytes = 1024 * 1024 * 1024
	}

	log := &Log{
		dir:      dir,
		segments: make([]*Segment, 0),
		config:   config,
	}

	// Load existing segments
	if err := log.loadSegments(); err != nil {
		return nil, err
	}

	// If no segments exist, create the first one (baseOffset = 0)
	if len(log.segments) == 0 {
		segment, err := NewSegment(dir, 0, config.SegmentConfig)
		if err != nil {
			return nil, err
		}
		log.segments = append(log.segments, segment)
	}

	// Active segment is always the last one
	log.activeSegment = log.segments[len(log.segments)-1]

	return log, nil
}

// loadSegments finds and opens all existing segment files in the directory.
func (l *Log) loadSegments() error {
	// Find all .log files
	pattern := filepath.Join(l.dir, "*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Parse base offsets from filenames and sort
	var baseOffsets []int64
	for _, file := range files {
		base := filepath.Base(file)
		offsetStr := strings.TrimSuffix(base, ".log")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			continue // skip invalid files
		}
		baseOffsets = append(baseOffsets, offset)
	}

	// Sort by offset (oldest first)
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// Open each segment
	for _, baseOffset := range baseOffsets {
		segment, err := NewSegment(l.dir, baseOffset, l.config.SegmentConfig)
		if err != nil {
			return fmt.Errorf("failed to open segment %d: %w", baseOffset, err)
		}
		l.segments = append(l.segments, segment)
	}

	return nil
}

// Append writes a record to the log and returns the assigned offset.
// Automatically rolls over to a new segment if needed.
func (l *Log) Append(record *Record) (int64, error) {
	// Check if we need to roll over to a new segment
	if l.activeSegment.currentPos >= l.config.MaxSegmentBytes {
		if err := l.rollover(); err != nil {
			return 0, fmt.Errorf("rollover failed: %w", err)
		}
	}

	// Append to active segment
	return l.activeSegment.Append(record)
}

// rollover closes current segment and creates a new one.
func (l *Log) rollover() error {
	// New segment starts at the next offset
	newBaseOffset := l.activeSegment.nextOffset

	// Create new segment
	segment, err := NewSegment(l.dir, newBaseOffset, l.config.SegmentConfig)
	if err != nil {
		return err
	}

	// Add to segments list and make it active
	l.segments = append(l.segments, segment)
	l.activeSegment = segment

	return nil
}

// Read retrieves a record by offset.
// Finds the correct segment and reads from it.
func (l *Log) Read(offset int64) (*Record, error) {
	// Find the segment containing this offset
	segment := l.findSegment(offset)
	if segment == nil {
		return nil, fmt.Errorf("offset %d not found in any segment", offset)
	}

	return segment.Read(offset)
}

// findSegment returns the segment that should contain the given offset.
// Uses binary search to find the segment with largest baseOffset <= offset.
func (l *Log) findSegment(offset int64) *Segment {
	if len(l.segments) == 0 {
		return nil
	}

	// Binary search: find first segment with baseOffset > offset
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	})

	// We want the segment before that (largest baseOffset <= offset)
	if i == 0 {
		// All segments have baseOffset > offset
		return nil
	}

	return l.segments[i-1]
}

// Recover restores log state from disk after a restart.
// Recovers all segments.
func (l *Log) Recover() error {
	for _, segment := range l.segments {
		if err := segment.Recover(); err != nil {
			return fmt.Errorf("segment %d recovery failed: %w", segment.baseOffset, err)
		}
	}
	return nil
}

// Close closes all segments.
func (l *Log) Close() error {
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// OldestOffset returns the first available offset in the log.
func (l *Log) OldestOffset() int64 {
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

// NewestOffset returns the next offset that will be assigned.
func (l *Log) NewestOffset() int64 {
	if l.activeSegment == nil {
		return 0
	}
	return l.activeSegment.nextOffset
}
