package topic

import (
	"fmt"
	"path/filepath"

	"github.com/G1DO/flume/internal/storage"
)

// DefaultBufferSize is the default number of write requests a partition can buffer.
const DefaultBufferSize = 1024

// writeRequest represents a pending write to a partition.
// The caller sends a record and waits for the result on the response channel.
type writeRequest struct {
	record *storage.Record
	result chan writeResult
}

// writeResult holds the outcome of a write operation.
type writeResult struct {
	offset int64
	err    error
}

// Partition wraps a Log with a partition ID and a bounded write buffer.
// The write buffer provides backpressure: when the buffer is full,
// producers block until space is available.
type Partition struct {
	ID         int
	log        *storage.Log
	writeChan  chan writeRequest
	quit       chan struct{}
	done       chan struct{} // signals background goroutine has exited
	bufferSize int
}

// NewPartition creates a new partition with a bounded write buffer.
// dir should be the topic directory; partition subdirectory is created automatically.
func NewPartition(topicDir string, id int, config storage.LogConfig) (*Partition, error) {
	return NewPartitionWithBuffer(topicDir, id, config, DefaultBufferSize)
}

// NewPartitionWithBuffer creates a new partition with a specified buffer size.
func NewPartitionWithBuffer(topicDir string, id int, config storage.LogConfig, bufferSize int) (*Partition, error) {
	partitionDir := filepath.Join(topicDir, fmt.Sprintf("partition-%d", id))

	log, err := storage.NewLog(partitionDir, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	p := &Partition{
		ID:         id,
		log:        log,
		writeChan:  make(chan writeRequest, bufferSize),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
		bufferSize: bufferSize,
	}

	// Start background goroutine that drains the write channel
	go p.writeLoop()

	return p, nil
}

// writeLoop is the background goroutine that reads from the write channel
// and appends records to the log sequentially.
func (p *Partition) writeLoop() {
	defer close(p.done)
	for {
		select {
		case req, ok := <-p.writeChan:
			if !ok {
				// Channel closed, drain remaining requests
				return
			}
			offset, err := p.log.Append(req.record)
			req.result <- writeResult{offset: offset, err: err}
		case <-p.quit:
			// Drain remaining writes before exiting
			p.drain()
			return
		}
	}
}

// drain processes all remaining write requests in the channel.
func (p *Partition) drain() {
	for {
		select {
		case req, ok := <-p.writeChan:
			if !ok {
				return
			}
			offset, err := p.log.Append(req.record)
			req.result <- writeResult{offset: offset, err: err}
		default:
			return
		}
	}
}

// Append writes a record to the partition through the bounded buffer.
// Blocks if the buffer is full, providing backpressure to the producer.
// Returns the assigned offset within this partition.
func (p *Partition) Append(record *storage.Record) (int64, error) {
	req := writeRequest{
		record: record,
		result: make(chan writeResult, 1),
	}

	select {
	case p.writeChan <- req:
		// Request accepted into buffer
	case <-p.quit:
		return 0, fmt.Errorf("partition %d is shutting down", p.ID)
	}

	// Wait for the background goroutine to process the write
	select {
	case res := <-req.result:
		return res.offset, res.err
	case <-p.quit:
		return 0, fmt.Errorf("partition %d is shutting down", p.ID)
	}
}

// Read retrieves a record by offset from this partition.
func (p *Partition) Read(offset int64) (*storage.Record, error) {
	return p.log.Read(offset)
}

// Recover restores partition state from disk.
func (p *Partition) Recover() error {
	return p.log.Recover()
}

// Close closes the partition, draining buffered writes first.
func (p *Partition) Close() error {
	close(p.quit)
	<-p.done // wait for background goroutine to finish
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

// BufferLen returns the current number of pending writes in the buffer.
func (p *Partition) BufferLen() int {
	return len(p.writeChan)
}

// BufferCap returns the capacity of the write buffer.
func (p *Partition) BufferCap() int {
	return cap(p.writeChan)
}
