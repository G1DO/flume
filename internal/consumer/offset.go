package consumer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// OffsetKey uniquely identifies an offset: group + topic + partition.
type OffsetKey struct {
	Group     string
	Topic     string
	Partition int32
}

// OffsetStore persists committed offsets for consumer groups.
// Offsets are stored in memory and periodically flushed to disk.
type OffsetStore struct {
	dir     string
	offsets map[OffsetKey]int64
	mu      sync.RWMutex
}

// NewOffsetStore creates an offset store that persists to the given directory.
func NewOffsetStore(dir string) (*OffsetStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create offset dir: %w", err)
	}
	store := &OffsetStore{
		dir:     dir,
		offsets: make(map[OffsetKey]int64),
	}
	if err := store.load(); err != nil {
		return nil, fmt.Errorf("load offsets: %w", err)
	}
	return store, nil
}

// Commit stores the offset for a group/topic/partition.
// The offset is persisted to disk immediately.
func (s *OffsetStore) Commit(group, topic string, partition int32, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := OffsetKey{Group: group, Topic: topic, Partition: partition}
	s.offsets[key] = offset

	// Persist to disk
	return s.saveGroup(group)
}

// Fetch returns the committed offset for a group/topic/partition.
// Returns -1 if no offset has been committed.
func (s *OffsetStore) Fetch(group, topic string, partition int32) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := OffsetKey{Group: group, Topic: topic, Partition: partition}
	if offset, ok := s.offsets[key]; ok {
		return offset
	}
	return -1 // No committed offset
}

// groupPath returns the file path for a group's offsets.
func (s *OffsetStore) groupPath(group string) string {
	return filepath.Join(s.dir, group+".offsets")
}

// saveGroup writes all offsets for a group to disk.
// File format: [EntryCount:4][Entries...]
// Each entry: [TopicLen:2][Topic:var][Partition:4][Offset:8]
func (s *OffsetStore) saveGroup(group string) error {
	// Collect all entries for this group
	var entries []struct {
		topic     string
		partition int32
		offset    int64
	}
	for key, offset := range s.offsets {
		if key.Group == group {
			entries = append(entries, struct {
				topic     string
				partition int32
				offset    int64
			}{key.Topic, key.Partition, offset})
		}
	}

	// Calculate buffer size
	size := 4 // entry count
	for _, e := range entries {
		size += 2 + len(e.topic) + 4 + 8 // TopicLen + Topic + Partition + Offset
	}

	buf := make([]byte, size)
	pos := 0

	// Entry count
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(entries)))
	pos += 4

	// Each entry
	for _, e := range entries {
		// Topic length
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(e.topic)))
		pos += 2
		// Topic
		copy(buf[pos:], e.topic)
		pos += len(e.topic)
		// Partition
		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(e.partition))
		pos += 4
		// Offset
		binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(e.offset))
		pos += 8
	}

	// Write atomically (write to temp, then rename)
	path := s.groupPath(group)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

// load reads all offset files from disk.
func (s *OffsetStore) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) < 9 || name[len(name)-8:] != ".offsets" {
			continue
		}
		group := name[:len(name)-8]
		if err := s.loadGroup(group); err != nil {
			return fmt.Errorf("load group %s: %w", group, err)
		}
	}
	return nil
}

// loadGroup reads offsets for a single group from disk.
func (s *OffsetStore) loadGroup(group string) error {
	data, err := os.ReadFile(s.groupPath(group))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(data) < 4 {
		return fmt.Errorf("offset file too short")
	}

	pos := 0
	entryCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	for i := 0; i < entryCount; i++ {
		if len(data) < pos+2 {
			return fmt.Errorf("truncated entry %d", i)
		}
		topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2

		if len(data) < pos+topicLen+12 {
			return fmt.Errorf("truncated entry %d", i)
		}
		topic := string(data[pos : pos+topicLen])
		pos += topicLen

		partition := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4

		offset := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8

		key := OffsetKey{Group: group, Topic: topic, Partition: partition}
		s.offsets[key] = offset
	}

	return nil
}

// Close cleans up resources. Currently a no-op but here for interface consistency.
func (s *OffsetStore) Close() error {
	return nil
}
