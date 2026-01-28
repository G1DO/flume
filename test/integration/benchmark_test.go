package integration

import (
	"fmt"
	"sync"
	"testing"

	"github.com/G1DO/flume/internal/broker"
	"github.com/G1DO/flume/pkg/client"
)

// startBenchBroker creates a broker for benchmarks.
func startBenchBroker(b *testing.B) (*broker.Broker, string) {
	b.Helper()
	dir := b.TempDir()
	br, err := broker.NewBroker(broker.BrokerConfig{
		Port:          0,
		DataDir:       dir,
		NumPartitions: 3,
	})
	if err != nil {
		b.Fatalf("NewBroker: %v", err)
	}
	if err := br.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	b.Cleanup(func() { br.Stop() })
	return br, br.Addr().String()
}

func BenchmarkProduce(b *testing.B) {
	_, addr := startBenchBroker(b)

	producer, err := client.NewProducer(addr)
	if err != nil {
		b.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	payload := make([]byte, 100) // 100-byte message
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		_, err := producer.ProduceToPartition("bench-produce", 0, nil, payload)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
}

func BenchmarkProduceWithKey(b *testing.B) {
	_, addr := startBenchBroker(b)

	producer, err := client.NewProducer(addr)
	if err != nil {
		b.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	payload := make([]byte, 100)
	key := []byte("user-123")

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		_, err := producer.ProduceWithKey("bench-keyed", key, payload)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
}

func BenchmarkProduceLargeMessage(b *testing.B) {
	_, addr := startBenchBroker(b)

	producer, err := client.NewProducer(addr)
	if err != nil {
		b.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	payload := make([]byte, 10*1024) // 10KB message
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		_, err := producer.ProduceToPartition("bench-large", 0, nil, payload)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
}

func BenchmarkFetch(b *testing.B) {
	_, addr := startBenchBroker(b)

	// Pre-populate with messages
	producer, err := client.NewProducer(addr)
	if err != nil {
		b.Fatalf("NewProducer: %v", err)
	}

	numMessages := 10000
	payload := make([]byte, 100)
	for i := 0; i < numMessages; i++ {
		_, err := producer.ProduceToPartition("bench-fetch", 0, nil, payload)
		if err != nil {
			b.Fatalf("Produce %d: %v", i, err)
		}
	}
	producer.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		consumer, err := client.NewConsumer(addr, "bench-fetch", 0, 0)
		if err != nil {
			b.Fatalf("NewConsumer: %v", err)
		}
		messages, err := consumer.Fetch(1024 * 1024) // 1MB
		if err != nil {
			b.Fatalf("Fetch: %v", err)
		}
		if len(messages) == 0 {
			b.Fatal("expected messages")
		}
		consumer.Close()
	}
}

func BenchmarkConcurrentProducers(b *testing.B) {
	_, addr := startBenchBroker(b)

	numProducers := 4
	payload := make([]byte, 100)

	b.ResetTimer()
	b.SetBytes(int64(numProducers * len(payload)))

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for p := 0; p < numProducers; p++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				prod, err := client.NewProducer(addr)
				if err != nil {
					return
				}
				defer prod.Close()
				prod.ProduceToPartition(
					fmt.Sprintf("bench-concurrent-%d", id%3),
					int32(id%3), nil, payload,
				)
			}(p)
		}
		wg.Wait()
	}
}

func BenchmarkProduceAndFetchRoundTrip(b *testing.B) {
	_, addr := startBenchBroker(b)

	payload := make([]byte, 100)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Produce
		producer, err := client.NewProducer(addr)
		if err != nil {
			b.Fatalf("NewProducer: %v", err)
		}
		_, err = producer.ProduceToPartition("bench-roundtrip", 0, nil, payload)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
		producer.Close()

		// Fetch
		consumer, err := client.NewConsumer(addr, "bench-roundtrip", 0, int64(i))
		if err != nil {
			b.Fatalf("NewConsumer: %v", err)
		}
		messages, err := consumer.Fetch(65536)
		if err != nil {
			b.Fatalf("Fetch: %v", err)
		}
		if len(messages) == 0 {
			b.Fatal("expected message")
		}
		consumer.Close()
	}
}
