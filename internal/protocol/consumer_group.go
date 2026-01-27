package protocol

import (
	"encoding/binary"
	"fmt"
)

// --- JoinGroup ---

// EncodeJoinGroupRequest encodes a join group request.
func EncodeJoinGroupRequest(req *JoinGroupRequest) []byte {
	// Calculate size
	size := 2 + len(req.GroupID) + 2 + len(req.MemberID) + 4 + 2
	for _, topic := range req.Topics {
		size += 2 + len(topic)
	}

	buf := make([]byte, size)
	pos := 0

	// GroupID
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.GroupID)))
	pos += 2
	copy(buf[pos:], req.GroupID)
	pos += len(req.GroupID)

	// MemberID
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.MemberID)))
	pos += 2
	copy(buf[pos:], req.MemberID)
	pos += len(req.MemberID)

	// SessionTimeout
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(req.SessionTimeout))
	pos += 4

	// TopicCount
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.Topics)))
	pos += 2

	// Topics
	for _, topic := range req.Topics {
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(topic)))
		pos += 2
		copy(buf[pos:], topic)
		pos += len(topic)
	}

	return buf
}

// DecodeJoinGroupRequest decodes a join group request.
func DecodeJoinGroupRequest(data []byte) (*JoinGroupRequest, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("join group request too short")
	}

	pos := 0

	// GroupID
	groupLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+groupLen+8 {
		return nil, fmt.Errorf("join group request truncated")
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	// MemberID
	memberLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+memberLen+6 {
		return nil, fmt.Errorf("join group request truncated")
	}
	memberID := string(data[pos : pos+memberLen])
	pos += memberLen

	// SessionTimeout
	sessionTimeout := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	// TopicCount
	topicCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	// Topics
	topics := make([]string, topicCount)
	for i := 0; i < topicCount; i++ {
		if len(data) < pos+2 {
			return nil, fmt.Errorf("join group request topic %d truncated", i)
		}
		topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if len(data) < pos+topicLen {
			return nil, fmt.Errorf("join group request topic %d truncated", i)
		}
		topics[i] = string(data[pos : pos+topicLen])
		pos += topicLen
	}

	return &JoinGroupRequest{
		GroupID:        groupID,
		MemberID:       memberID,
		SessionTimeout: sessionTimeout,
		Topics:         topics,
	}, nil
}

// EncodeJoinGroupResponse encodes a join group response.
func EncodeJoinGroupResponse(resp *JoinGroupResponse) []byte {
	// Calculate size
	size := 2 + 4 + 2 + len(resp.LeaderID) + 2 + len(resp.MemberID) + 2
	for _, member := range resp.Members {
		size += 2 + len(member)
	}

	buf := make([]byte, size)
	pos := 0

	// ErrorCode
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(resp.ErrorCode))
	pos += 2

	// Generation
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(resp.Generation))
	pos += 4

	// LeaderID
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(resp.LeaderID)))
	pos += 2
	copy(buf[pos:], resp.LeaderID)
	pos += len(resp.LeaderID)

	// MemberID
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(resp.MemberID)))
	pos += 2
	copy(buf[pos:], resp.MemberID)
	pos += len(resp.MemberID)

	// MemberCount
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(resp.Members)))
	pos += 2

	// Members
	for _, member := range resp.Members {
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(member)))
		pos += 2
		copy(buf[pos:], member)
		pos += len(member)
	}

	return buf
}

// DecodeJoinGroupResponse decodes a join group response.
func DecodeJoinGroupResponse(data []byte) (*JoinGroupResponse, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("join group response too short")
	}

	pos := 0

	// ErrorCode
	errorCode := int16(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	// Generation
	generation := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	// LeaderID
	leaderLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+leaderLen+4 {
		return nil, fmt.Errorf("join group response truncated")
	}
	leaderID := string(data[pos : pos+leaderLen])
	pos += leaderLen

	// MemberID
	memberLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+memberLen+2 {
		return nil, fmt.Errorf("join group response truncated")
	}
	memberID := string(data[pos : pos+memberLen])
	pos += memberLen

	// MemberCount
	memberCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	// Members
	members := make([]string, memberCount)
	for i := 0; i < memberCount; i++ {
		if len(data) < pos+2 {
			return nil, fmt.Errorf("join group response member %d truncated", i)
		}
		mLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if len(data) < pos+mLen {
			return nil, fmt.Errorf("join group response member %d truncated", i)
		}
		members[i] = string(data[pos : pos+mLen])
		pos += mLen
	}

	return &JoinGroupResponse{
		ErrorCode:  errorCode,
		Generation: generation,
		LeaderID:   leaderID,
		MemberID:   memberID,
		Members:    members,
	}, nil
}

// --- LeaveGroup ---

// EncodeLeaveGroupRequest encodes a leave group request.
func EncodeLeaveGroupRequest(req *LeaveGroupRequest) []byte {
	size := 2 + len(req.GroupID) + 2 + len(req.MemberID)
	buf := make([]byte, size)
	pos := 0

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.GroupID)))
	pos += 2
	copy(buf[pos:], req.GroupID)
	pos += len(req.GroupID)

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.MemberID)))
	pos += 2
	copy(buf[pos:], req.MemberID)

	return buf
}

// DecodeLeaveGroupRequest decodes a leave group request.
func DecodeLeaveGroupRequest(data []byte) (*LeaveGroupRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("leave group request too short")
	}

	pos := 0

	groupLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+groupLen+2 {
		return nil, fmt.Errorf("leave group request truncated")
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	memberLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+memberLen {
		return nil, fmt.Errorf("leave group request truncated")
	}
	memberID := string(data[pos : pos+memberLen])

	return &LeaveGroupRequest{
		GroupID:  groupID,
		MemberID: memberID,
	}, nil
}

// EncodeLeaveGroupResponse encodes a leave group response.
func EncodeLeaveGroupResponse(resp *LeaveGroupResponse) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(resp.ErrorCode))
	return buf
}

// DecodeLeaveGroupResponse decodes a leave group response.
func DecodeLeaveGroupResponse(data []byte) (*LeaveGroupResponse, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("leave group response too short")
	}
	return &LeaveGroupResponse{
		ErrorCode: int16(binary.BigEndian.Uint16(data[0:2])),
	}, nil
}

// --- Heartbeat ---

// EncodeHeartbeatRequest encodes a heartbeat request.
func EncodeHeartbeatRequest(req *HeartbeatRequest) []byte {
	size := 2 + len(req.GroupID) + 4 + 2 + len(req.MemberID)
	buf := make([]byte, size)
	pos := 0

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.GroupID)))
	pos += 2
	copy(buf[pos:], req.GroupID)
	pos += len(req.GroupID)

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(req.Generation))
	pos += 4

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.MemberID)))
	pos += 2
	copy(buf[pos:], req.MemberID)

	return buf
}

// DecodeHeartbeatRequest decodes a heartbeat request.
func DecodeHeartbeatRequest(data []byte) (*HeartbeatRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("heartbeat request too short")
	}

	pos := 0

	groupLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+groupLen+6 {
		return nil, fmt.Errorf("heartbeat request truncated")
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	generation := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	memberLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+memberLen {
		return nil, fmt.Errorf("heartbeat request truncated")
	}
	memberID := string(data[pos : pos+memberLen])

	return &HeartbeatRequest{
		GroupID:    groupID,
		Generation: generation,
		MemberID:   memberID,
	}, nil
}

// EncodeHeartbeatResponse encodes a heartbeat response.
func EncodeHeartbeatResponse(resp *HeartbeatResponse) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(resp.ErrorCode))
	return buf
}

// DecodeHeartbeatResponse decodes a heartbeat response.
func DecodeHeartbeatResponse(data []byte) (*HeartbeatResponse, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("heartbeat response too short")
	}
	return &HeartbeatResponse{
		ErrorCode: int16(binary.BigEndian.Uint16(data[0:2])),
	}, nil
}

// --- OffsetCommit ---

// EncodeOffsetCommitRequest encodes an offset commit request.
func EncodeOffsetCommitRequest(req *OffsetCommitRequest) []byte {
	size := 2 + len(req.GroupID) + 2 + len(req.Topic) + 4 + 8
	buf := make([]byte, size)
	pos := 0

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.GroupID)))
	pos += 2
	copy(buf[pos:], req.GroupID)
	pos += len(req.GroupID)

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.Topic)))
	pos += 2
	copy(buf[pos:], req.Topic)
	pos += len(req.Topic)

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(req.Partition))
	pos += 4

	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(req.Offset))

	return buf
}

// DecodeOffsetCommitRequest decodes an offset commit request.
func DecodeOffsetCommitRequest(data []byte) (*OffsetCommitRequest, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("offset commit request too short")
	}

	pos := 0

	groupLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+groupLen+14 {
		return nil, fmt.Errorf("offset commit request truncated")
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+topicLen+12 {
		return nil, fmt.Errorf("offset commit request truncated")
	}
	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	partition := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	offset := int64(binary.BigEndian.Uint64(data[pos : pos+8]))

	return &OffsetCommitRequest{
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}, nil
}

// EncodeOffsetCommitResponse encodes an offset commit response.
func EncodeOffsetCommitResponse(resp *OffsetCommitResponse) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(resp.ErrorCode))
	return buf
}

// DecodeOffsetCommitResponse decodes an offset commit response.
func DecodeOffsetCommitResponse(data []byte) (*OffsetCommitResponse, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("offset commit response too short")
	}
	return &OffsetCommitResponse{
		ErrorCode: int16(binary.BigEndian.Uint16(data[0:2])),
	}, nil
}

// --- OffsetFetch ---

// EncodeOffsetFetchRequest encodes an offset fetch request.
func EncodeOffsetFetchRequest(req *OffsetFetchRequest) []byte {
	size := 2 + len(req.GroupID) + 2 + len(req.Topic) + 4
	buf := make([]byte, size)
	pos := 0

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.GroupID)))
	pos += 2
	copy(buf[pos:], req.GroupID)
	pos += len(req.GroupID)

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(req.Topic)))
	pos += 2
	copy(buf[pos:], req.Topic)
	pos += len(req.Topic)

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(req.Partition))

	return buf
}

// DecodeOffsetFetchRequest decodes an offset fetch request.
func DecodeOffsetFetchRequest(data []byte) (*OffsetFetchRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("offset fetch request too short")
	}

	pos := 0

	groupLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+groupLen+6 {
		return nil, fmt.Errorf("offset fetch request truncated")
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+topicLen+4 {
		return nil, fmt.Errorf("offset fetch request truncated")
	}
	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	partition := int32(binary.BigEndian.Uint32(data[pos : pos+4]))

	return &OffsetFetchRequest{
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
	}, nil
}

// EncodeOffsetFetchResponse encodes an offset fetch response.
func EncodeOffsetFetchResponse(resp *OffsetFetchResponse) []byte {
	buf := make([]byte, 10)
	binary.BigEndian.PutUint16(buf[0:2], uint16(resp.ErrorCode))
	binary.BigEndian.PutUint64(buf[2:10], uint64(resp.Offset))
	return buf
}

// DecodeOffsetFetchResponse decodes an offset fetch response.
func DecodeOffsetFetchResponse(data []byte) (*OffsetFetchResponse, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("offset fetch response too short")
	}
	return &OffsetFetchResponse{
		ErrorCode: int16(binary.BigEndian.Uint16(data[0:2])),
		Offset:    int64(binary.BigEndian.Uint64(data[2:10])),
	}, nil
}
