package broker

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/G1DO/flume/internal/protocol"
)

func TestBrokerJoinGroup(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(BrokerConfig{Port: 0, DataDir: dir})
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
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

	// Send JOIN_GROUP request
	req := &protocol.JoinGroupRequest{
		GroupID:        "test-group",
		MemberID:       "",
		SessionTimeout: 10000,
		Topics:         []string{"orders"},
	}
	reqBytes := protocol.EncodeJoinGroupRequest(req)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(reqBytes)),
		APIKey:    protocol.APIKeyJoinGroup,
		RequestID: 1,
	}
	conn.Write(protocol.EncodeRequestHeader(header))
	conn.Write(reqBytes)

	// Read response
	respHeader, err := protocol.DecodeResponseHeader(conn)
	if err != nil {
		t.Fatalf("DecodeResponseHeader failed: %v", err)
	}

	respPayload := make([]byte, respHeader.Size-4)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		t.Fatalf("Read response failed: %v", err)
	}

	resp, err := protocol.DecodeJoinGroupResponse(respPayload)
	if err != nil {
		t.Fatalf("DecodeJoinGroupResponse failed: %v", err)
	}

	if resp.ErrorCode != protocol.ErrNone {
		t.Errorf("expected ErrNone, got %d", resp.ErrorCode)
	}
	if resp.MemberID == "" {
		t.Error("expected non-empty member ID")
	}
	if resp.Generation != 1 {
		t.Errorf("expected generation 1, got %d", resp.Generation)
	}
	if resp.LeaderID != resp.MemberID {
		t.Error("first member should be leader")
	}
}

func TestBrokerOffsetCommitFetch(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(BrokerConfig{Port: 0, DataDir: dir})
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
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

	// Commit offset
	commitReq := &protocol.OffsetCommitRequest{
		GroupID:   "test-group",
		Topic:     "orders",
		Partition: 0,
		Offset:    42,
	}
	commitBytes := protocol.EncodeOffsetCommitRequest(commitReq)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(commitBytes)),
		APIKey:    protocol.APIKeyOffsetCommit,
		RequestID: 1,
	}
	conn.Write(protocol.EncodeRequestHeader(header))
	conn.Write(commitBytes)

	// Read commit response
	respHeader, _ := protocol.DecodeResponseHeader(conn)
	commitRespPayload := make([]byte, respHeader.Size-4)
	io.ReadFull(conn, commitRespPayload)

	commitResp, _ := protocol.DecodeOffsetCommitResponse(commitRespPayload)
	if commitResp.ErrorCode != protocol.ErrNone {
		t.Errorf("commit: expected ErrNone, got %d", commitResp.ErrorCode)
	}

	// Fetch offset
	fetchReq := &protocol.OffsetFetchRequest{
		GroupID:   "test-group",
		Topic:     "orders",
		Partition: 0,
	}
	fetchBytes := protocol.EncodeOffsetFetchRequest(fetchReq)

	header2 := &protocol.RequestHeader{
		Size:      int32(6 + len(fetchBytes)),
		APIKey:    protocol.APIKeyOffsetFetch,
		RequestID: 2,
	}
	conn.Write(protocol.EncodeRequestHeader(header2))
	conn.Write(fetchBytes)

	// Read fetch response
	respHeader2, _ := protocol.DecodeResponseHeader(conn)
	fetchRespPayload := make([]byte, respHeader2.Size-4)
	io.ReadFull(conn, fetchRespPayload)

	fetchResp, _ := protocol.DecodeOffsetFetchResponse(fetchRespPayload)
	if fetchResp.ErrorCode != protocol.ErrNone {
		t.Errorf("fetch: expected ErrNone, got %d", fetchResp.ErrorCode)
	}
	if fetchResp.Offset != 42 {
		t.Errorf("expected offset 42, got %d", fetchResp.Offset)
	}
}

func TestBrokerHeartbeat(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(BrokerConfig{Port: 0, DataDir: dir})
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
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

	// First join
	joinReq := &protocol.JoinGroupRequest{
		GroupID:        "test-group",
		MemberID:       "",
		SessionTimeout: 10000,
		Topics:         []string{"orders"},
	}
	joinBytes := protocol.EncodeJoinGroupRequest(joinReq)

	header := &protocol.RequestHeader{
		Size:      int32(6 + len(joinBytes)),
		APIKey:    protocol.APIKeyJoinGroup,
		RequestID: 1,
	}
	conn.Write(protocol.EncodeRequestHeader(header))
	conn.Write(joinBytes)

	respHeader, _ := protocol.DecodeResponseHeader(conn)
	joinRespPayload := make([]byte, respHeader.Size-4)
	io.ReadFull(conn, joinRespPayload)

	joinResp, _ := protocol.DecodeJoinGroupResponse(joinRespPayload)

	// Send heartbeat
	heartbeatReq := &protocol.HeartbeatRequest{
		GroupID:    "test-group",
		Generation: joinResp.Generation,
		MemberID:   joinResp.MemberID,
	}
	heartbeatBytes := protocol.EncodeHeartbeatRequest(heartbeatReq)

	header2 := &protocol.RequestHeader{
		Size:      int32(6 + len(heartbeatBytes)),
		APIKey:    protocol.APIKeyHeartbeat,
		RequestID: 2,
	}
	conn.Write(protocol.EncodeRequestHeader(header2))
	conn.Write(heartbeatBytes)

	respHeader2, _ := protocol.DecodeResponseHeader(conn)
	heartbeatRespPayload := make([]byte, respHeader2.Size-4)
	io.ReadFull(conn, heartbeatRespPayload)

	heartbeatResp, _ := protocol.DecodeHeartbeatResponse(heartbeatRespPayload)
	if heartbeatResp.ErrorCode != protocol.ErrNone {
		t.Errorf("heartbeat: expected ErrNone, got %d", heartbeatResp.ErrorCode)
	}
}
