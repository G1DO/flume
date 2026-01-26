package client

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/G1DO/flume/internal/protocol"
)

// Producer sends messages to a broker.
type Producer struct {
	conn      net.Conn
	requestID int32
}

// NewProducer connects to a broker and returns a producer.
func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return &Producer{conn: conn}, nil
}

// Produce sends a message to the specified topic.
// Returns the assigned offset or an error.
func (p *Producer) Produce(topic string, payload []byte) (int64, error) {
	reqID := atomic.AddInt32(&p.requestID, 1)

	// Build request
	req := &protocol.ProduceRequest{
		Topic:   topic,
		Payload: payload,
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
		return 0, fmt.Errorf("write header failed: %w", err)
	}
	if _, err := p.conn.Write(reqBytes); err != nil {
		return 0, fmt.Errorf("write payload failed: %w", err)
	}

	// Read response header
	respHeader, err := protocol.DecodeResponseHeader(p.conn)
	if err != nil {
		return 0, fmt.Errorf("read response header failed: %w", err)
	}

	// Read response payload
	payloadSize := respHeader.Size - 4 // Size - RequestID(4)
	respPayload := make([]byte, payloadSize)
	if _, err := io.ReadFull(p.conn, respPayload); err != nil {
		return 0, fmt.Errorf("read response payload failed: %w", err)
	}

	// Decode response
	resp, err := protocol.DecodeProduceResponse(respPayload)
	if err != nil {
		return 0, fmt.Errorf("decode response failed: %w", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		return 0, fmt.Errorf("broker error: %d", resp.ErrorCode)
	}

	return resp.Offset, nil
}

// Close closes the connection to the broker.
func (p *Producer) Close() error {
	return p.conn.Close()
}
