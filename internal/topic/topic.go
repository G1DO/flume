package topic

import (
	"fmt"
	"path/filepath"

	"github.com/G1DO/flume/internal/storage"
)

// Topic manages multiple partitions.
type Topic struct {
	Name       string
	partitions []*Partition
	dir        string
}

// TopicConfig holds topic configuration.
type TopicConfig struct {
	NumPartitions int
	LogConfig     storage.LogConfig
}

// NewTopic creates a new topic with the specified number of partitions.
func NewTopic(dataDir, name string, config TopicConfig) (*Topic, error) {
	if config.NumPartitions <= 0 {
		config.NumPartitions = 1
	}

	topicDir := filepath.Join(dataDir, name)

	t := &Topic{
		Name:       name,
		partitions: make([]*Partition, config.NumPartitions),
		dir:        topicDir,
	}

	// Create all partitions
	for i := 0; i < config.NumPartitions; i++ {
		p, err := NewPartition(topicDir, i, config.LogConfig)
		if err != nil {
			// Close already created partitions
			for j := 0; j < i; j++ {
				t.partitions[j].Close()
			}
			return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		t.partitions[i] = p
	}

	return t, nil
}

// Partition returns the partition at the given index.
func (t *Topic) Partition(id int) (*Partition, error) {
	if id < 0 || id >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range [0, %d)", id, len(t.partitions))
	}
	return t.partitions[id], nil
}

// NumPartitions returns the number of partitions.
func (t *Topic) NumPartitions() int {
	return len(t.partitions)
}

// Append writes a record to the specified partition.
func (t *Topic) Append(partitionID int, record *storage.Record) (int64, error) {
	p, err := t.Partition(partitionID)
	if err != nil {
		return 0, err
	}
	return p.Append(record)
}

// Read retrieves a record from the specified partition.
func (t *Topic) Read(partitionID int, offset int64) (*storage.Record, error) {
	p, err := t.Partition(partitionID)
	if err != nil {
		return nil, err
	}
	return p.Read(offset)
}

// Recover restores all partitions from disk.
func (t *Topic) Recover() error {
	for i, p := range t.partitions {
		if err := p.Recover(); err != nil {
			return fmt.Errorf("partition %d recovery failed: %w", i, err)
		}
	}
	return nil
}

// Close closes all partitions.
func (t *Topic) Close() error {
	for _, p := range t.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}
