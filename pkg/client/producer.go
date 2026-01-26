package client

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/G1DO/flume/internal/protocol"
)

// ProduceResult contains the result of a produce operation.
type ProduceResult struct {
	Partition int32
	Offset    int64
}

// Producer sends messages to a broker.
type Producer struct {
	conn        net.Conn
	requestID   int32
	partitioner Partitioner
}

// NewProducer connects to a broker and returns a producer.
func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return &Producer{
		conn:        conn,
		partitioner: DefaultPartitioner,
	}, nil
}

// Produce sends a message to the specified topic.
// Uses round-robin partitioning (no key).
func (p *Producer) Produce(topic string, payload []byte) (*ProduceResult, error) {
	return p.ProduceWithKey(topic, nil, payload)
}

// ProduceWithKey sends a message with a key.
// Same key always goes to the same partition.
func (p *Producer) ProduceWithKey(topic string, key, payload []byte) (*ProduceResult, error) {
	return p.ProduceToPartition(topic, -1, key, payload)
}

// ProduceToPartition sends a message to a specific partition.
// If partition is -1, the broker uses key hash to determine partition.
func (p *Producer) ProduceToPartition(topic string, partition int32, key, payload []byte) (*ProduceResult, error) {
	reqID := atomic.AddInt32(&p.requestID, 1)

	// Build request
	req := &protocol.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Payload:   payload,
	}
	reqBytes := protocol.EncodeProduceRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)), // APIKey(2) + RequestID(4) + payload
		APIKey:    protocol.APIKeyProduce,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	// Send request
	if _, err := p.conn.Write(headerBytes); err != nil {
		return nil, fmt.Errorf("write header failed: %w", err)
	}
	if _, err := p.conn.Write(reqBytes); err != nil {
		return nil, fmt.Errorf("write payload failed: %w", err)
	}

	// Read response header
	respHeader, err := protocol.DecodeResponseHeader(p.conn)
	if err != nil {
		return nil, fmt.Errorf("read response header failed: %w", err)
	}

	// Read response payload
	payloadSize := respHeader.Size - 4 // Size - RequestID(4)
	respPayload := make([]byte, payloadSize)
	if _, err := io.ReadFull(p.conn, respPayload); err != nil {
		return nil, fmt.Errorf("read response payload failed: %w", err)
	}

	// Decode response
	resp, err := protocol.DecodeProduceResponse(respPayload)
	if err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		return nil, fmt.Errorf("broker error: %d", resp.ErrorCode)
	}

	return &ProduceResult{
		Partition: resp.Partition,
		Offset:    resp.Offset,
	}, nil
}

// Close closes the connection to the broker.
func (p *Producer) Close() error {
	return p.conn.Close()
}
