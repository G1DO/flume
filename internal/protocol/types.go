package protocol

// API Keys - identify which operation the client wants
const (
	APIKeyProduce int16 = 0
	APIKeyFetch   int16 = 1
)

// Error codes
const (
	ErrNone             int16 = 0
	ErrUnknown          int16 = 1
	ErrTopicNotFound    int16 = 2
	ErrOffsetOutOfRange int16 = 3
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
