package topic

import (
	"fmt"
	"path/filepath"

	"github.com/G1DO/flume/internal/storage"
)

// Partition wraps a Log with a partition ID.
// Each partition is an independent, ordered sequence of records.
type Partition struct {
	ID  int
	log *storage.Log
}

// NewPartition creates a new partition.
// dir should be the topic directory; partition subdirectory is created automatically.
func NewPartition(topicDir string, id int, config storage.LogConfig) (*Partition, error) {
	// Create partition directory: topicDir/partition-0, partition-1, etc.
	partitionDir := filepath.Join(topicDir, fmt.Sprintf("partition-%d", id))

	log, err := storage.NewLog(partitionDir, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	return &Partition{
		ID:  id,
		log: log,
	}, nil
}

// Append writes a record to the partition.
// Returns the assigned offset within this partition.
func (p *Partition) Append(record *storage.Record) (int64, error) {
	return p.log.Append(record)
}

// Read retrieves a record by offset from this partition.
func (p *Partition) Read(offset int64) (*storage.Record, error) {
	return p.log.Read(offset)
}

// Recover restores partition state from disk.
func (p *Partition) Recover() error {
	return p.log.Recover()
}

// Close closes the partition.
func (p *Partition) Close() error {
	return p.log.Close()
}

// OldestOffset returns the first available offset.
func (p *Partition) OldestOffset() int64 {
	return p.log.OldestOffset()
}

// NewestOffset returns the next offset to be assigned.
func (p *Partition) NewestOffset() int64 {
	return p.log.NewestOffset()
}
