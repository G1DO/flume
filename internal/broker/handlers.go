package broker

import (
	"hash/fnv"

	"github.com/G1DO/flume/internal/protocol"
	"github.com/G1DO/flume/internal/storage"
)

// handleProduce processes a PRODUCE request.
// Stores the message and returns the assigned offset.
func (b *Broker) handleProduce(requestID int32, payload []byte) []byte {
	// Decode request
	req, err := protocol.DecodeProduceRequest(payload)
	if err != nil {
		return b.produceResponse(requestID, -1, -1, protocol.ErrUnknown)
	}

	// Get or create topic
	t, err := b.topics.GetOrCreate(req.Topic)
	if err != nil {
		return b.produceResponse(requestID, -1, -1, protocol.ErrUnknown)
	}

	// Determine partition
	partition := req.Partition
	if partition < 0 {
		// Use key hash to determine partition
		partition = hashPartition(req.Key, t.NumPartitions())
	}

	// Validate partition
	if int(partition) >= t.NumPartitions() {
		return b.produceResponse(requestID, partition, -1, protocol.ErrUnknown)
	}

	// Create record and append to partition
	record := storage.NewRecord(req.Payload)
	offset, err := t.Append(int(partition), record)
	if err != nil {
		return b.produceResponse(requestID, partition, -1, protocol.ErrUnknown)
	}

	return b.produceResponse(requestID, partition, offset, protocol.ErrNone)
}

// hashPartition computes partition from key using FNV-1a hash.
func hashPartition(key []byte, numPartitions int) int32 {
	if len(key) == 0 || numPartitions <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(numPartitions))
}

// produceResponse builds a PRODUCE response.
func (b *Broker) produceResponse(requestID int32, partition int32, offset int64, errorCode int16) []byte {
	resp := protocol.EncodeProduceResponse(&protocol.ProduceResponse{
		Partition: partition,
		Offset:    offset,
		ErrorCode: errorCode,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)), // RequestID(4) + payload
		RequestID: requestID,
	})
	return append(header, resp...)
}

// handleFetch processes a FETCH request.
// Returns messages starting from the requested offset.
func (b *Broker) handleFetch(requestID int32, payload []byte) []byte {
	// Decode request
	req, err := protocol.DecodeFetchRequest(payload)
	if err != nil {
		return b.fetchResponse(requestID, protocol.ErrUnknown, nil)
	}

	// Get topic
	t := b.topics.Get(req.Topic)
	if t == nil {
		return b.fetchResponse(requestID, protocol.ErrTopicNotFound, nil)
	}

	// Get partition
	p, err := t.Partition(int(req.Partition))
	if err != nil {
		return b.fetchResponse(requestID, protocol.ErrUnknown, nil)
	}

	// Read records starting from offset
	var records []protocol.FetchRecord
	var bytesRead int32 = 0
	offset := req.Offset

	for {
		// Check max bytes limit
		if req.MaxBytes > 0 && bytesRead >= req.MaxBytes {
			break
		}

		// Try to read next record
		record, err := p.Read(offset)
		if err != nil {
			break // No more records or error
		}

		// Add to response
		records = append(records, protocol.FetchRecord{
			Offset:  record.Offset,
			Payload: record.Payload,
		})

		bytesRead += int32(8 + 4 + len(record.Payload)) // Offset + PayloadLen + Payload
		offset++
	}

	// If no records and offset is out of range, return error
	if len(records) == 0 && req.Offset >= p.NewestOffset() {
		return b.fetchResponse(requestID, protocol.ErrOffsetOutOfRange, nil)
	}

	return b.fetchResponse(requestID, protocol.ErrNone, records)
}

// fetchResponse builds a FETCH response.
func (b *Broker) fetchResponse(requestID int32, errorCode int16, records []protocol.FetchRecord) []byte {
	resp := protocol.EncodeFetchResponse(&protocol.FetchResponse{
		ErrorCode: errorCode,
		Records:   records,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)), // RequestID(4) + payload
		RequestID: requestID,
	})
	return append(header, resp...)
}

// --- Consumer Group Handlers ---

// handleJoinGroup processes a JOIN_GROUP request.
func (b *Broker) handleJoinGroup(requestID int32, payload []byte) []byte {
	req, err := protocol.DecodeJoinGroupRequest(payload)
	if err != nil {
		return b.joinGroupResponse(requestID, protocol.ErrUnknown, 0, "", "", nil)
	}

	result, err := b.coordinator.JoinGroup(req.GroupID, req.MemberID, req.Topics)
	if err != nil {
		return b.joinGroupResponse(requestID, protocol.ErrUnknown, 0, "", "", nil)
	}

	// Assign partitions for the topics
	group, _ := b.coordinator.GetGroup(req.GroupID)
	if group != nil {
		b.coordinator.AssignTopicPartitions(group, req.Topics)
	}

	return b.joinGroupResponse(requestID, protocol.ErrNone, result.Generation, result.LeaderID, result.MemberID, result.Members)
}

func (b *Broker) joinGroupResponse(requestID int32, errorCode int16, generation int32, leaderID, memberID string, members []string) []byte {
	resp := protocol.EncodeJoinGroupResponse(&protocol.JoinGroupResponse{
		ErrorCode:  errorCode,
		Generation: generation,
		LeaderID:   leaderID,
		MemberID:   memberID,
		Members:    members,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)),
		RequestID: requestID,
	})
	return append(header, resp...)
}

// handleLeaveGroup processes a LEAVE_GROUP request.
func (b *Broker) handleLeaveGroup(requestID int32, payload []byte) []byte {
	req, err := protocol.DecodeLeaveGroupRequest(payload)
	if err != nil {
		return b.simpleErrorResponse(requestID, protocol.ErrUnknown)
	}

	if err := b.coordinator.LeaveGroup(req.GroupID, req.MemberID); err != nil {
		return b.simpleErrorResponse(requestID, protocol.ErrUnknown)
	}

	return b.simpleErrorResponse(requestID, protocol.ErrNone)
}

// handleHeartbeat processes a HEARTBEAT request.
func (b *Broker) handleHeartbeat(requestID int32, payload []byte) []byte {
	req, err := protocol.DecodeHeartbeatRequest(payload)
	if err != nil {
		return b.simpleErrorResponse(requestID, protocol.ErrUnknown)
	}

	if err := b.coordinator.Heartbeat(req.GroupID, req.MemberID, req.Generation); err != nil {
		if err.Error() == "stale generation" {
			return b.simpleErrorResponse(requestID, protocol.ErrStaleGeneration)
		}
		if err.Error() == "unknown member" {
			return b.simpleErrorResponse(requestID, protocol.ErrUnknownMember)
		}
		return b.simpleErrorResponse(requestID, protocol.ErrUnknown)
	}

	return b.simpleErrorResponse(requestID, protocol.ErrNone)
}

// handleOffsetCommit processes an OFFSET_COMMIT request.
func (b *Broker) handleOffsetCommit(requestID int32, payload []byte) []byte {
	req, err := protocol.DecodeOffsetCommitRequest(payload)
	if err != nil {
		return b.simpleErrorResponse(requestID, protocol.ErrUnknown)
	}

	if err := b.coordinator.CommitOffset(req.GroupID, req.Topic, req.Partition, req.Offset); err != nil {
		return b.simpleErrorResponse(requestID, protocol.ErrUnknown)
	}

	return b.simpleErrorResponse(requestID, protocol.ErrNone)
}

// handleOffsetFetch processes an OFFSET_FETCH request.
func (b *Broker) handleOffsetFetch(requestID int32, payload []byte) []byte {
	req, err := protocol.DecodeOffsetFetchRequest(payload)
	if err != nil {
		return b.offsetFetchResponse(requestID, protocol.ErrUnknown, -1)
	}

	offset := b.coordinator.FetchOffset(req.GroupID, req.Topic, req.Partition)
	return b.offsetFetchResponse(requestID, protocol.ErrNone, offset)
}

func (b *Broker) offsetFetchResponse(requestID int32, errorCode int16, offset int64) []byte {
	resp := protocol.EncodeOffsetFetchResponse(&protocol.OffsetFetchResponse{
		ErrorCode: errorCode,
		Offset:    offset,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)),
		RequestID: requestID,
	})
	return append(header, resp...)
}

// simpleErrorResponse builds a response with just an error code (2 bytes).
func (b *Broker) simpleErrorResponse(requestID int32, errorCode int16) []byte {
	resp := make([]byte, 2)
	resp[0] = byte(errorCode >> 8)
	resp[1] = byte(errorCode)
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)),
		RequestID: requestID,
	})
	return append(header, resp...)
}
