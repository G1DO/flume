package topic

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Manager is a registry of all topics.
type Manager struct {
	dataDir string
	topics  map[string]*Topic
	mu      sync.RWMutex
	config  TopicConfig // default config for new topics
}

// NewManager creates a new topic manager.
func NewManager(dataDir string, defaultConfig TopicConfig) *Manager {
	return &Manager{
		dataDir: dataDir,
		topics:  make(map[string]*Topic),
		config:  defaultConfig,
	}
}

// GetOrCreate returns an existing topic or creates a new one.
func (m *Manager) GetOrCreate(name string) (*Topic, error) {
	// Fast path: read lock
	m.mu.RLock()
	t, exists := m.topics[name]
	m.mu.RUnlock()
	if exists {
		return t, nil
	}

	// Slow path: write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check
	if t, exists := m.topics[name]; exists {
		return t, nil
	}

	// Create new topic
	t, err := NewTopic(m.dataDir, name, m.config)
	if err != nil {
		return nil, err
	}

	// Recover existing data
	if err := t.Recover(); err != nil {
		t.Close()
		return nil, err
	}

	m.topics[name] = t
	return t, nil
}

// Get returns an existing topic or nil if not found.
func (m *Manager) Get(name string) *Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.topics[name]
}

// LoadExisting loads all existing topics from disk.
func (m *Manager) LoadExisting() error {
	entries, err := os.ReadDir(m.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no data dir yet
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		topicDir := filepath.Join(m.dataDir, topicName)

		// Count partitions by looking for partition-N directories
		numPartitions := m.countPartitions(topicDir)
		if numPartitions == 0 {
			continue // not a valid topic directory
		}

		// Create topic with discovered partition count
		config := m.config
		config.NumPartitions = numPartitions

		t, err := NewTopic(m.dataDir, topicName, config)
		if err != nil {
			return fmt.Errorf("failed to load topic %s: %w", topicName, err)
		}

		if err := t.Recover(); err != nil {
			t.Close()
			return fmt.Errorf("failed to recover topic %s: %w", topicName, err)
		}

		m.topics[topicName] = t
	}

	return nil
}

// countPartitions counts partition-N directories in a topic directory.
func (m *Manager) countPartitions(topicDir string) int {
	count := 0
	for i := 0; ; i++ {
		partitionDir := filepath.Join(topicDir, fmt.Sprintf("partition-%d", i))
		if _, err := os.Stat(partitionDir); os.IsNotExist(err) {
			break
		}
		count++
	}
	return count
}

// Close closes all topics.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range m.topics {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Topics returns a list of all topic names.
func (m *Manager) Topics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.topics))
	for name := range m.topics {
		names = append(names, name)
	}
	return names
}
