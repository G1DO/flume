package broker

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/G1DO/flume/internal/protocol"
)

func TestBrokerStartStop(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker(BrokerConfig{Port: 0, DataDir: dir})

	if err := b.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if b.Addr() == nil {
		t.Error("expected address after start")
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestBrokerProduce(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker(BrokerConfig{Port: 0, DataDir: dir})
	if err := b.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer b.Stop()

	// Connect to broker
	conn, err := net.Dial("tcp", b.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Send PRODUCE request
	req := &protocol.ProduceRequest{
		Topic:   "test-topic",
		Payload: []byte("hello world"),
	}
	reqBytes := protocol.EncodeProduceRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)), // APIKey(2) + RequestID(4) + payload
		APIKey:    protocol.APIKeyProduce,
		RequestID: 1,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	// Write request
	conn.Write(headerBytes)
	conn.Write(reqBytes)

	// Read response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respHeader, err := protocol.DecodeResponseHeader(conn)
	if err != nil {
		t.Fatalf("DecodeResponseHeader failed: %v", err)
	}

	if respHeader.RequestID != 1 {
		t.Errorf("expected RequestID 1, got %d", respHeader.RequestID)
	}

	// Read response payload
	respPayload := make([]byte, respHeader.Size-4) // Size - RequestID(4)
	if _, err := conn.Read(respPayload); err != nil {
		t.Fatalf("Read response payload failed: %v", err)
	}

	resp, err := protocol.DecodeProduceResponse(respPayload)
	if err != nil {
		t.Fatalf("DecodeProduceResponse failed: %v", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		t.Errorf("expected ErrNone, got %d", resp.ErrorCode)
	}
	if resp.Offset != 0 {
		t.Errorf("expected offset 0, got %d", resp.Offset)
	}
}

func TestBrokerProduceThenFetch(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker(BrokerConfig{Port: 0, DataDir: dir})
	if err := b.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer b.Stop()

	conn, err := net.Dial("tcp", b.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Produce 3 messages
	messages := []string{"first", "second", "third"}
	for i, msg := range messages {
		sendProduceRequest(t, conn, "orders", []byte(msg), int32(i))
	}

	// Fetch messages
	fetchReq := &protocol.FetchRequest{
		Topic:    "orders",
		Offset:   0,
		MaxBytes: 1024,
	}
	fetchReqBytes := protocol.EncodeFetchRequest(fetchReq)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(fetchReqBytes)),
		APIKey:    protocol.APIKeyFetch,
		RequestID: 100,
	}
	conn.Write(protocol.EncodeRequestHeader(header))
	conn.Write(fetchReqBytes)

	// Read fetch response
	respHeader, _ := protocol.DecodeResponseHeader(conn)
	respPayload := make([]byte, respHeader.Size-4)
	conn.Read(respPayload)

	resp, err := protocol.DecodeFetchResponse(respPayload)
	if err != nil {
		t.Fatalf("DecodeFetchResponse failed: %v", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		t.Errorf("expected ErrNone, got %d", resp.ErrorCode)
	}
	if len(resp.Records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(resp.Records))
	}

	for i, rec := range resp.Records {
		if rec.Offset != int64(i) {
			t.Errorf("record %d: expected offset %d, got %d", i, i, rec.Offset)
		}
		if !bytes.Equal(rec.Payload, []byte(messages[i])) {
			t.Errorf("record %d: expected %q, got %q", i, messages[i], rec.Payload)
		}
	}
}

func TestBrokerFetchTopicNotFound(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker(BrokerConfig{Port: 0, DataDir: dir})
	if err := b.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer b.Stop()

	conn, err := net.Dial("tcp", b.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Fetch from non-existent topic
	fetchReq := &protocol.FetchRequest{
		Topic:    "nonexistent",
		Offset:   0,
		MaxBytes: 1024,
	}
	fetchReqBytes := protocol.EncodeFetchRequest(fetchReq)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(fetchReqBytes)),
		APIKey:    protocol.APIKeyFetch,
		RequestID: 1,
	}
	conn.Write(protocol.EncodeRequestHeader(header))
	conn.Write(fetchReqBytes)

	respHeader, _ := protocol.DecodeResponseHeader(conn)
	respPayload := make([]byte, respHeader.Size-4)
	conn.Read(respPayload)

	resp, _ := protocol.DecodeFetchResponse(respPayload)
	if resp.ErrorCode != protocol.ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %d", resp.ErrorCode)
	}
}

func sendProduceRequest(t *testing.T, conn net.Conn, topic string, payload []byte, reqID int32) {
	req := &protocol.ProduceRequest{Topic: topic, Payload: payload}
	reqBytes := protocol.EncodeProduceRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyProduce,
		RequestID: reqID,
	}
	conn.Write(protocol.EncodeRequestHeader(header))
	conn.Write(reqBytes)

	// Read response (discard)
	respHeader, _ := protocol.DecodeResponseHeader(conn)
	respPayload := make([]byte, respHeader.Size-4)
	conn.Read(respPayload)
}
