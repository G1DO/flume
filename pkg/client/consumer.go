package client

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/G1DO/flume/internal/protocol"
)

// Message represents a consumed message.
type Message struct {
	Offset  int64
	Payload []byte
}

// Consumer fetches messages from a broker partition.
type Consumer struct {
	conn      net.Conn
	topic     string
	partition int32
	offset    int64
	requestID int32
}

// NewConsumer connects to a broker and returns a consumer for the topic partition.
func NewConsumer(addr, topic string, partition int32, startOffset int64) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return &Consumer{
		conn:      conn,
		topic:     topic,
		partition: partition,
		offset:    startOffset,
	}, nil
}

// Fetch retrieves messages starting from the current offset.
// Returns up to maxBytes worth of messages.
func (c *Consumer) Fetch(maxBytes int32) ([]Message, error) {
	reqID := atomic.AddInt32(&c.requestID, 1)

	// Build request
	req := &protocol.FetchRequest{
		Topic:     c.topic,
		Partition: c.partition,
		Offset:    c.offset,
		MaxBytes:  maxBytes,
	}
	reqBytes := protocol.EncodeFetchRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyFetch,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	// Send request
	if _, err := c.conn.Write(headerBytes); err != nil {
		return nil, fmt.Errorf("write header failed: %w", err)
	}
	if _, err := c.conn.Write(reqBytes); err != nil {
		return nil, fmt.Errorf("write payload failed: %w", err)
	}

	// Read response header
	respHeader, err := protocol.DecodeResponseHeader(c.conn)
	if err != nil {
		return nil, fmt.Errorf("read response header failed: %w", err)
	}

	// Read response payload
	payloadSize := respHeader.Size - 4
	respPayload := make([]byte, payloadSize)
	if _, err := io.ReadFull(c.conn, respPayload); err != nil {
		return nil, fmt.Errorf("read response payload failed: %w", err)
	}

	// Decode response
	resp, err := protocol.DecodeFetchResponse(respPayload)
	if err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		return nil, fmt.Errorf("broker error: %d", resp.ErrorCode)
	}

	// Convert to messages and update offset
	messages := make([]Message, len(resp.Records))
	for i, rec := range resp.Records {
		messages[i] = Message{
			Offset:  rec.Offset,
			Payload: rec.Payload,
		}
		// Update offset to next message
		if rec.Offset >= c.offset {
			c.offset = rec.Offset + 1
		}
	}

	return messages, nil
}

// Offset returns the current offset.
func (c *Consumer) Offset() int64 {
	return c.offset
}

// Partition returns the partition being consumed.
func (c *Consumer) Partition() int32 {
	return c.partition
}

// Close closes the connection to the broker.
func (c *Consumer) Close() error {
	return c.conn.Close()
}
