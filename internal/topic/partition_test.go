package topic

import (
	"sync"
	"testing"
	"time"

	"github.com/G1DO/flume/internal/storage"
)

func tempPartition(t *testing.T, bufferSize int) *Partition {
	t.Helper()
	dir := t.TempDir()
	p, err := NewPartitionWithBuffer(dir, 0, storage.LogConfig{}, bufferSize)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestPartitionAppendAndRead(t *testing.T) {
	p := tempPartition(t, 10)

	record := storage.NewRecord([]byte("hello"))
	offset, err := p.Append(record)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	got, err := p.Read(0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(got.Payload) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(got.Payload))
	}
}

func TestPartitionConcurrentAppends(t *testing.T) {
	p := tempPartition(t, 100)

	var wg sync.WaitGroup
	numWriters := 10
	writesPerWriter := 50
	errs := make(chan error, numWriters*writesPerWriter)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				record := storage.NewRecord([]byte("data"))
				_, err := p.Append(record)
				if err != nil {
					errs <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent append failed: %v", err)
	}

	expected := int64(numWriters * writesPerWriter)
	if p.NewestOffset() != expected {
		t.Fatalf("expected %d records, got newest offset %d", expected, p.NewestOffset())
	}
}

func TestPartitionBackpressure(t *testing.T) {
	// Buffer of 1: only one write request can be buffered at a time.
	// This makes backpressure observable.
	p := tempPartition(t, 1)

	// Fill the buffer: send a write that the background goroutine will pick up,
	// then another that fills the buffer channel.
	// We verify that all writes eventually complete (none lost).
	total := 20
	var wg sync.WaitGroup
	offsets := make(chan int64, total)

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			record := storage.NewRecord([]byte("backpressure-test"))
			offset, err := p.Append(record)
			if err != nil {
				t.Errorf("append %d failed: %v", n, err)
				return
			}
			offsets <- offset
		}(i)
	}

	wg.Wait()
	close(offsets)

	seen := make(map[int64]bool)
	for offset := range offsets {
		if seen[offset] {
			t.Fatalf("duplicate offset %d", offset)
		}
		seen[offset] = true
	}

	if len(seen) != total {
		t.Fatalf("expected %d unique offsets, got %d", total, len(seen))
	}
}

func TestPartitionBufferLenCap(t *testing.T) {
	p := tempPartition(t, 64)

	if p.BufferCap() != 64 {
		t.Fatalf("expected buffer cap 64, got %d", p.BufferCap())
	}

	// Buffer should be empty initially (or close to it)
	if p.BufferLen() > 1 {
		t.Fatalf("expected near-empty buffer, got %d", p.BufferLen())
	}
}

func TestPartitionCloseGraceful(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithBuffer(dir, 0, storage.LogConfig{}, 10)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	// Write some records
	for i := 0; i < 5; i++ {
		record := storage.NewRecord([]byte("before-close"))
		_, err := p.Append(record)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Close should drain and complete without hanging
	done := make(chan struct{})
	go func() {
		p.Close()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Close() timed out — possible deadlock")
	}
}

func TestPartitionAppendAfterClose(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithBuffer(dir, 0, storage.LogConfig{}, 10)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	p.Close()

	record := storage.NewRecord([]byte("after-close"))
	_, err = p.Append(record)
	if err == nil {
		t.Fatal("expected error on append after close, got nil")
	}
}
