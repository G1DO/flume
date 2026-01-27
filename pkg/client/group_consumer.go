package client

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/G1DO/flume/internal/protocol"
)

// GroupConsumer consumes from topics as part of a consumer group.
// Partitions are automatically assigned by the broker.
type GroupConsumer struct {
	conn      net.Conn
	groupID   string
	memberID  string
	generation int32
	topics    []string
	requestID int32

	assignments []Assignment
	mu          sync.RWMutex

	// Heartbeat management
	heartbeatInterval time.Duration
	sessionTimeout    time.Duration
	quit              chan struct{}
	wg                sync.WaitGroup
}

// Assignment represents a topic-partition assigned to this consumer.
type Assignment struct {
	Topic     string
	Partition int32
}

// GroupConsumerConfig configures a group consumer.
type GroupConsumerConfig struct {
	BrokerAddr        string
	GroupID           string
	Topics            []string
	SessionTimeout    time.Duration // Default: 10s
	HeartbeatInterval time.Duration // Default: 3s
}

// NewGroupConsumer creates a consumer that joins a consumer group.
func NewGroupConsumer(config GroupConsumerConfig) (*GroupConsumer, error) {
	if config.SessionTimeout == 0 {
		config.SessionTimeout = 10 * time.Second
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 3 * time.Second
	}

	conn, err := net.Dial("tcp", config.BrokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	gc := &GroupConsumer{
		conn:              conn,
		groupID:           config.GroupID,
		topics:            config.Topics,
		heartbeatInterval: config.HeartbeatInterval,
		sessionTimeout:    config.SessionTimeout,
		quit:              make(chan struct{}),
	}

	// Join the group
	if err := gc.joinGroup(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to join group: %w", err)
	}

	// Start heartbeat loop
	gc.wg.Add(1)
	go gc.heartbeatLoop()

	return gc, nil
}

// joinGroup sends a JOIN_GROUP request and updates membership state.
func (gc *GroupConsumer) joinGroup() error {
	reqID := atomic.AddInt32(&gc.requestID, 1)

	req := &protocol.JoinGroupRequest{
		GroupID:        gc.groupID,
		MemberID:       gc.memberID, // Empty on first join
		SessionTimeout: int32(gc.sessionTimeout.Milliseconds()),
		Topics:         gc.topics,
	}
	reqBytes := protocol.EncodeJoinGroupRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyJoinGroup,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	if _, err := gc.conn.Write(headerBytes); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := gc.conn.Write(reqBytes); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	// Read response
	respHeader, err := protocol.DecodeResponseHeader(gc.conn)
	if err != nil {
		return fmt.Errorf("read response header: %w", err)
	}

	respPayload := make([]byte, respHeader.Size-4)
	if _, err := io.ReadFull(gc.conn, respPayload); err != nil {
		return fmt.Errorf("read response payload: %w", err)
	}

	resp, err := protocol.DecodeJoinGroupResponse(respPayload)
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		return fmt.Errorf("join group error: %d", resp.ErrorCode)
	}

	gc.mu.Lock()
	gc.memberID = resp.MemberID
	gc.generation = resp.Generation
	gc.mu.Unlock()

	return nil
}

// heartbeatLoop sends periodic heartbeats to keep membership alive.
func (gc *GroupConsumer) heartbeatLoop() {
	defer gc.wg.Done()
	ticker := time.NewTicker(gc.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gc.quit:
			return
		case <-ticker.C:
			if err := gc.sendHeartbeat(); err != nil {
				// Heartbeat failed, may need to rejoin
				// For now, just log and continue
				// In production, would trigger rejoin
			}
		}
	}
}

// sendHeartbeat sends a single heartbeat.
func (gc *GroupConsumer) sendHeartbeat() error {
	gc.mu.RLock()
	memberID := gc.memberID
	generation := gc.generation
	gc.mu.RUnlock()

	reqID := atomic.AddInt32(&gc.requestID, 1)

	req := &protocol.HeartbeatRequest{
		GroupID:    gc.groupID,
		Generation: generation,
		MemberID:   memberID,
	}
	reqBytes := protocol.EncodeHeartbeatRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyHeartbeat,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	if _, err := gc.conn.Write(headerBytes); err != nil {
		return err
	}
	if _, err := gc.conn.Write(reqBytes); err != nil {
		return err
	}

	// Read response
	respHeader, err := protocol.DecodeResponseHeader(gc.conn)
	if err != nil {
		return err
	}

	respPayload := make([]byte, respHeader.Size-4)
	if _, err := io.ReadFull(gc.conn, respPayload); err != nil {
		return err
	}

	resp, err := protocol.DecodeHeartbeatResponse(respPayload)
	if err != nil {
		return err
	}

	if resp.ErrorCode != protocol.ErrNone {
		return fmt.Errorf("heartbeat error: %d", resp.ErrorCode)
	}

	return nil
}

// Fetch retrieves messages from the assigned partitions.
// For simplicity, fetches from the first assigned partition.
// A full implementation would round-robin or parallel fetch all partitions.
func (gc *GroupConsumer) Fetch(topic string, partition int32, maxBytes int32) ([]Message, error) {
	reqID := atomic.AddInt32(&gc.requestID, 1)

	// Get committed offset for this partition
	offset := gc.FetchOffset(topic, partition)
	if offset < 0 {
		offset = 0 // Start from beginning
	}

	req := &protocol.FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	}
	reqBytes := protocol.EncodeFetchRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyFetch,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	if _, err := gc.conn.Write(headerBytes); err != nil {
		return nil, err
	}
	if _, err := gc.conn.Write(reqBytes); err != nil {
		return nil, err
	}

	// Read response
	respHeader, err := protocol.DecodeResponseHeader(gc.conn)
	if err != nil {
		return nil, err
	}

	respPayload := make([]byte, respHeader.Size-4)
	if _, err := io.ReadFull(gc.conn, respPayload); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeFetchResponse(respPayload)
	if err != nil {
		return nil, err
	}

	if resp.ErrorCode != protocol.ErrNone {
		return nil, fmt.Errorf("fetch error: %d", resp.ErrorCode)
	}

	messages := make([]Message, len(resp.Records))
	for i, rec := range resp.Records {
		messages[i] = Message{
			Offset:  rec.Offset,
			Payload: rec.Payload,
		}
	}

	return messages, nil
}

// CommitOffset commits the offset for a topic-partition.
func (gc *GroupConsumer) CommitOffset(topic string, partition int32, offset int64) error {
	reqID := atomic.AddInt32(&gc.requestID, 1)

	req := &protocol.OffsetCommitRequest{
		GroupID:   gc.groupID,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}
	reqBytes := protocol.EncodeOffsetCommitRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyOffsetCommit,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	if _, err := gc.conn.Write(headerBytes); err != nil {
		return err
	}
	if _, err := gc.conn.Write(reqBytes); err != nil {
		return err
	}

	// Read response
	respHeader, err := protocol.DecodeResponseHeader(gc.conn)
	if err != nil {
		return err
	}

	respPayload := make([]byte, respHeader.Size-4)
	if _, err := io.ReadFull(gc.conn, respPayload); err != nil {
		return err
	}

	resp, err := protocol.DecodeOffsetCommitResponse(respPayload)
	if err != nil {
		return err
	}

	if resp.ErrorCode != protocol.ErrNone {
		return fmt.Errorf("commit error: %d", resp.ErrorCode)
	}

	return nil
}

// FetchOffset gets the committed offset for a topic-partition.
func (gc *GroupConsumer) FetchOffset(topic string, partition int32) int64 {
	reqID := atomic.AddInt32(&gc.requestID, 1)

	req := &protocol.OffsetFetchRequest{
		GroupID:   gc.groupID,
		Topic:     topic,
		Partition: partition,
	}
	reqBytes := protocol.EncodeOffsetFetchRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyOffsetFetch,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	if _, err := gc.conn.Write(headerBytes); err != nil {
		return -1
	}
	if _, err := gc.conn.Write(reqBytes); err != nil {
		return -1
	}

	// Read response
	respHeader, err := protocol.DecodeResponseHeader(gc.conn)
	if err != nil {
		return -1
	}

	respPayload := make([]byte, respHeader.Size-4)
	if _, err := io.ReadFull(gc.conn, respPayload); err != nil {
		return -1
	}

	resp, err := protocol.DecodeOffsetFetchResponse(respPayload)
	if err != nil {
		return -1
	}

	if resp.ErrorCode != protocol.ErrNone {
		return -1
	}

	return resp.Offset
}

// MemberID returns the assigned member ID.
func (gc *GroupConsumer) MemberID() string {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.memberID
}

// Generation returns the current group generation.
func (gc *GroupConsumer) Generation() int32 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.generation
}

// Close leaves the group and closes the connection.
func (gc *GroupConsumer) Close() error {
	close(gc.quit)
	gc.wg.Wait()

	// Send LEAVE_GROUP
	gc.mu.RLock()
	memberID := gc.memberID
	gc.mu.RUnlock()

	reqID := atomic.AddInt32(&gc.requestID, 1)
	req := &protocol.LeaveGroupRequest{
		GroupID:  gc.groupID,
		MemberID: memberID,
	}
	reqBytes := protocol.EncodeLeaveGroupRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyLeaveGroup,
		RequestID: reqID,
	}
	headerBytes := protocol.EncodeRequestHeader(header)

	gc.conn.Write(headerBytes)
	gc.conn.Write(reqBytes)

	// Don't wait for response, just close
	return gc.conn.Close()
}
