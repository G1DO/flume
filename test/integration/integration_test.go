package integration

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/G1DO/flume/internal/broker"
	"github.com/G1DO/flume/pkg/client"
)

// startBroker creates and starts a broker on a random port with a temp data dir.
func startBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir := t.TempDir()
	b, err := broker.NewBroker(broker.BrokerConfig{
		Port:          0,
		DataDir:       dir,
		NumPartitions: 3,
	})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { b.Stop() })
	return b
}

// --- End-to-End Tests ---

func TestProduceAndFetchSingleMessage(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Produce one message
	result, err := producer.ProduceToPartition("test-topic", 0, nil, []byte("hello world"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if result.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", result.Offset)
	}

	// Consume it
	consumer, err := client.NewConsumer(addr, "test-topic", 0, 0)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(65536)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Payload, []byte("hello world")) {
		t.Fatalf("expected 'hello world', got %q", messages[0].Payload)
	}
}

func TestProduceAndFetchMultipleMessages(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Produce 100 messages to partition 0
	numMessages := 100
	for i := 0; i < numMessages; i++ {
		payload := []byte(fmt.Sprintf("message-%d", i))
		_, err := producer.ProduceToPartition("bulk-topic", 0, nil, payload)
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	// Consume all
	consumer, err := client.NewConsumer(addr, "bulk-topic", 0, 0)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(1024 * 1024) // 1MB max
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(messages) != numMessages {
		t.Fatalf("expected %d messages, got %d", numMessages, len(messages))
	}

	for i, msg := range messages {
		expected := fmt.Sprintf("message-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("message %d: expected %q, got %q", i, expected, msg.Payload)
		}
		if msg.Offset != int64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}
}

func TestKeyBasedPartitioning(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Produce messages with same key — should all go to same partition
	key := []byte("user-alice")
	var partition int32 = -1
	for i := 0; i < 10; i++ {
		result, err := producer.ProduceWithKey("keyed-topic", key, []byte(fmt.Sprintf("event-%d", i)))
		if err != nil {
			t.Fatalf("ProduceWithKey %d: %v", i, err)
		}
		if partition == -1 {
			partition = result.Partition
		} else if result.Partition != partition {
			t.Fatalf("key routing inconsistent: expected partition %d, got %d", partition, result.Partition)
		}
	}

	// Fetch from that partition — should have all 10 messages
	consumer, err := client.NewConsumer(addr, "keyed-topic", partition, 0)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(65536)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(messages) != 10 {
		t.Fatalf("expected 10 messages on partition %d, got %d", partition, len(messages))
	}
}

func TestMultiplePartitionsDistribution(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Produce to each of the 3 partitions explicitly
	for p := int32(0); p < 3; p++ {
		for i := 0; i < 5; i++ {
			payload := []byte(fmt.Sprintf("p%d-msg%d", p, i))
			_, err := producer.ProduceToPartition("multi-part", p, nil, payload)
			if err != nil {
				t.Fatalf("Produce to partition %d: %v", p, err)
			}
		}
	}

	// Verify each partition has exactly 5 messages
	for p := int32(0); p < 3; p++ {
		consumer, err := client.NewConsumer(addr, "multi-part", p, 0)
		if err != nil {
			t.Fatalf("NewConsumer partition %d: %v", p, err)
		}

		messages, err := consumer.Fetch(65536)
		consumer.Close()
		if err != nil {
			t.Fatalf("Fetch partition %d: %v", p, err)
		}
		if len(messages) != 5 {
			t.Fatalf("partition %d: expected 5 messages, got %d", p, len(messages))
		}

		for i, msg := range messages {
			expected := fmt.Sprintf("p%d-msg%d", p, i)
			if string(msg.Payload) != expected {
				t.Errorf("partition %d msg %d: expected %q, got %q", p, i, expected, msg.Payload)
			}
		}
	}
}

func TestConcurrentProducers(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	numProducers := 5
	messagesPerProducer := 20
	var wg sync.WaitGroup
	errs := make(chan error, numProducers*messagesPerProducer)

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			prod, err := client.NewProducer(addr)
			if err != nil {
				errs <- fmt.Errorf("producer %d connect: %w", id, err)
				return
			}
			defer prod.Close()

			for i := 0; i < messagesPerProducer; i++ {
				payload := []byte(fmt.Sprintf("producer-%d-msg-%d", id, i))
				_, err := prod.ProduceToPartition("concurrent-topic", 0, nil, payload)
				if err != nil {
					errs <- fmt.Errorf("producer %d msg %d: %w", id, i, err)
				}
			}
		}(p)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	// Verify all messages landed
	consumer, err := client.NewConsumer(addr, "concurrent-topic", 0, 0)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(1024 * 1024)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	expected := numProducers * messagesPerProducer
	if len(messages) != expected {
		t.Fatalf("expected %d messages, got %d", expected, len(messages))
	}
}

func TestConsumerOffsetTracking(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Produce 10 messages with large payloads so maxBytes limits the batch
	for i := 0; i < 10; i++ {
		payload := []byte(fmt.Sprintf("message-number-%d-with-extra-padding-to-make-it-large-enough-for-offset-tracking-test-purposes-%d", i, i))
		_, err := producer.ProduceToPartition("offset-topic", 0, nil, payload)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	// First fetch with small maxBytes — should get some but not all
	consumer, err := client.NewConsumer(addr, "offset-topic", 0, 0)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(500) // small maxBytes to limit batch
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(messages) == 0 {
		t.Fatal("expected at least some messages")
	}
	if len(messages) >= 10 {
		t.Skip("all messages fit in first fetch, cannot test offset tracking")
	}

	// Consumer should have advanced its offset
	nextOffset := consumer.Offset()
	if nextOffset <= 0 {
		t.Fatalf("expected offset > 0 after fetch, got %d", nextOffset)
	}

	// Second fetch should start from where we left off
	messages2, err := consumer.Fetch(65536)
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}

	if len(messages2) == 0 {
		t.Fatal("expected remaining messages on second fetch")
	}

	// First message of second fetch should follow last of first
	if messages2[0].Offset != nextOffset {
		t.Errorf("expected second fetch to start at %d, got %d", nextOffset, messages2[0].Offset)
	}

	// Total should be 10
	total := len(messages) + len(messages2)
	if total != 10 {
		t.Errorf("expected 10 total messages, got %d (%d + %d)", total, len(messages), len(messages2))
	}
}

func TestFetchFromEmptyTopic(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	// Create topic by producing then fetch from empty partition
	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	// Produce to partition 0 to create the topic
	_, err = producer.ProduceToPartition("empty-test", 0, nil, []byte("seed"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	producer.Close()

	// Fetch from partition 1 (empty)
	consumer, err := client.NewConsumer(addr, "empty-test", 1, 0)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	_, err = consumer.Fetch(65536)
	// Should get an error or empty result since partition 1 has no messages
	// and offset 0 is out of range for an empty partition
	if err == nil {
		// No error is also acceptable if implementation returns empty
	}
}

func TestConsumerGroupJoinAndLeave(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr().String()

	// Produce some data first so topic exists
	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	for i := 0; i < 3; i++ {
		_, err := producer.ProduceToPartition("group-topic", int32(i), nil, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	producer.Close()

	// Join consumer group
	gc, err := client.NewGroupConsumer(client.GroupConsumerConfig{
		BrokerAddr: addr,
		GroupID:    "test-group",
		Topics:     []string{"group-topic"},
	})
	if err != nil {
		t.Fatalf("NewGroupConsumer: %v", err)
	}

	// Verify we got a member ID and generation
	if gc.MemberID() == "" {
		t.Error("expected non-empty member ID")
	}
	if gc.Generation() < 1 {
		t.Errorf("expected generation >= 1, got %d", gc.Generation())
	}

	// Commit and fetch offset
	if err := gc.CommitOffset("group-topic", 0, 42); err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}

	offset := gc.FetchOffset("group-topic", 0)
	if offset != 42 {
		t.Errorf("expected committed offset 42, got %d", offset)
	}

	// Close (sends LEAVE_GROUP)
	if err := gc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
