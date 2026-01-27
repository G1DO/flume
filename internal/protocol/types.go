package protocol

// API Keys - identify which operation the client wants
const (
	APIKeyProduce      int16 = 0
	APIKeyFetch        int16 = 1
	APIKeyJoinGroup    int16 = 2
	APIKeyLeaveGroup   int16 = 3
	APIKeyHeartbeat    int16 = 4
	APIKeyOffsetCommit int16 = 5
	APIKeyOffsetFetch  int16 = 6
)

// Error codes
const (
	ErrNone             int16 = 0
	ErrUnknown          int16 = 1
	ErrTopicNotFound    int16 = 2
	ErrOffsetOutOfRange int16 = 3
	ErrUnknownMember    int16 = 4
	ErrStaleGeneration  int16 = 5
	ErrRebalanceNeeded  int16 = 6
)

// Header sizes
const (
	RequestHeaderSize  = 4 + 2 + 4 // Size(4) + APIKey(2) + RequestID(4)
	ResponseHeaderSize = 4 + 4     // Size(4) + RequestID(4)
)

// RequestHeader is the common header for all requests.
// Wire format: [Size:4][APIKey:2][RequestID:4]
type RequestHeader struct {
	Size      int32 // total bytes after this field
	APIKey    int16 // which operation (PRODUCE=0, FETCH=1)
	RequestID int32 // client-assigned ID for correlation
}

// ResponseHeader is the common header for all responses.
// Wire format: [Size:4][RequestID:4]
type ResponseHeader struct {
	Size      int32 // total bytes after this field
	RequestID int32 // echoed from request for correlation
}

// ProduceRequest is sent by producers to store a message.
// Wire format: [TopicLen:2][Topic:var][Partition:4][KeyLen:4][Key:var][PayloadLen:4][Payload:var]
type ProduceRequest struct {
	Topic     string // which topic to write to
	Partition int32  // partition to write to (-1 = use key hash)
	Key       []byte // message key (for partitioning)
	Payload   []byte // the message data
}

// ProduceResponse is returned after a produce request.
// Wire format: [Partition:4][Offset:8][ErrorCode:2]
type ProduceResponse struct {
	Partition int32 // partition written to
	Offset    int64 // assigned offset (-1 if error)
	ErrorCode int16 // 0 = success
}

// FetchRequest is sent by consumers to read messages.
// Wire format: [TopicLen:2][Topic:var][Partition:4][Offset:8][MaxBytes:4]
type FetchRequest struct {
	Topic     string // which topic to read from
	Partition int32  // which partition to read from
	Offset    int64  // starting offset
	MaxBytes  int32  // max bytes to return (0 = no limit)
}

// FetchResponse is returned with fetched messages.
// Wire format: [ErrorCode:2][RecordCount:4][Records:var]
// Each record: [Offset:8][PayloadLen:4][Payload:var]
type FetchResponse struct {
	ErrorCode int16         // 0 = success
	Records   []FetchRecord // the fetched messages
}

// FetchRecord is a single record in a fetch response.
// Simplified version of storage.Record for wire transfer.
type FetchRecord struct {
	Offset  int64  // record offset
	Payload []byte // message data
}

// --- Consumer Group Types ---

// JoinGroupRequest is sent when a consumer joins a group.
// Wire format: [GroupLen:2][Group:var][MemberIDLen:2][MemberID:var][SessionTimeout:4][TopicCount:2][Topics:var]
// Each topic: [TopicLen:2][Topic:var]
type JoinGroupRequest struct {
	GroupID        string   // consumer group name
	MemberID       string   // empty for new member, existing ID to rejoin
	SessionTimeout int32    // milliseconds before member is considered dead
	Topics         []string // topics this consumer subscribes to
}

// JoinGroupResponse is returned after joining a group.
// Wire format: [ErrorCode:2][Generation:4][LeaderLen:2][Leader:var][MemberIDLen:2][MemberID:var][MemberCount:2][Members:var]
// Each member: [MemberIDLen:2][MemberID:var]
type JoinGroupResponse struct {
	ErrorCode  int16    // 0 = success
	Generation int32    // group generation (increments on rebalance)
	LeaderID   string   // current group leader
	MemberID   string   // assigned member ID
	Members    []string // all members in the group
}

// LeaveGroupRequest is sent when a consumer leaves a group.
// Wire format: [GroupLen:2][Group:var][MemberIDLen:2][MemberID:var]
type LeaveGroupRequest struct {
	GroupID  string // consumer group name
	MemberID string // member leaving
}

// LeaveGroupResponse is returned after leaving a group.
// Wire format: [ErrorCode:2]
type LeaveGroupResponse struct {
	ErrorCode int16 // 0 = success
}

// HeartbeatRequest is sent periodically to keep membership alive.
// Wire format: [GroupLen:2][Group:var][Generation:4][MemberIDLen:2][MemberID:var]
type HeartbeatRequest struct {
	GroupID    string // consumer group name
	Generation int32  // current generation (from join response)
	MemberID   string // member sending heartbeat
}

// HeartbeatResponse is returned after a heartbeat.
// Wire format: [ErrorCode:2]
type HeartbeatResponse struct {
	ErrorCode int16 // 0 = success, ErrRebalanceNeeded if must rejoin
}

// OffsetCommitRequest commits an offset for a partition.
// Wire format: [GroupLen:2][Group:var][TopicLen:2][Topic:var][Partition:4][Offset:8]
type OffsetCommitRequest struct {
	GroupID   string // consumer group name
	Topic     string // topic name
	Partition int32  // partition number
	Offset    int64  // offset to commit
}

// OffsetCommitResponse is returned after committing an offset.
// Wire format: [ErrorCode:2]
type OffsetCommitResponse struct {
	ErrorCode int16 // 0 = success
}

// OffsetFetchRequest fetches the committed offset for a partition.
// Wire format: [GroupLen:2][Group:var][TopicLen:2][Topic:var][Partition:4]
type OffsetFetchRequest struct {
	GroupID   string // consumer group name
	Topic     string // topic name
	Partition int32  // partition number
}

// OffsetFetchResponse is returned with the committed offset.
// Wire format: [ErrorCode:2][Offset:8]
type OffsetFetchResponse struct {
	ErrorCode int16 // 0 = success
	Offset    int64 // committed offset (-1 if none)
}
