# Flume

A message streaming system built from scratch in Go. Not a Kafka wrapper — the core primitives implemented manually to understand log-structured storage, consumer coordination, and backpressure.

## Status

**In Development** — Phase 4 (Consumer Groups)

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Log Storage Engine | Complete |
| 2 | Broker + Wire Protocol | Complete |
| 3 | Topics + Partitions | Complete |
| 4 | Consumer Groups | Not started |
| 5 | Backpressure | Not started |
| 6 | Integration + Benchmarks | Not started |

## What This Is

A learning project that implements:

- **Log-structured storage** — append-only logs with offset-based reads
- **Partitioned topics** — parallelism with ordering guarantees per partition
- **Consumer groups** — coordinated consumption with rebalancing
- **Backpressure** — bounded buffers to handle slow consumers

## What This Is Not

- Not distributed (single broker, no replication)
- Not production-ready
- Not a Kafka clone — intentionally limited scope

## Architecture

```
Producers --> Broker --> Consumers
                |
           +----+----+
           |  Topics |
           | +-----+ |
           | | P0  | |  <-- Partitions (separate log files)
           | | P1  | |
           | | P2  | |
           | +-----+ |
           +---------+
```

### Record Format (Implemented)

```
+----------+----------+----------+-------------------+
| Offset   | Size     | CRC32    | Payload           |
| 8 bytes  | 4 bytes  | 4 bytes  | variable          |
+----------+----------+----------+-------------------+
           |<-- 16-byte header -->|
```

- **Offset**: 64-bit record position in log
- **Size**: 32-bit payload length
- **CRC32**: IEEE checksum for corruption detection
- **Payload**: Raw message bytes

## Project Structure

```
flume/
├── cmd/
│   ├── broker/           # Main broker binary (--port, --data flags)
│   │   └── main.go
│   ├── produce/          # CLI producer (--broker, --topic, --message)
│   │   └── main.go
│   └── consume/          # CLI consumer (--broker, --topic, --offset, --max-bytes)
│       └── main.go
├── internal/
│   ├── storage/          # Phase 1: Log engine
│   │   ├── record.go     # Record struct, encode/decode, CRC, stream reading
│   │   ├── segment.go    # Segment file with Append(), Read(), Recover(), configurable fsync
│   │   ├── index.go      # Sparse offset index for fast lookups
│   │   ├── log.go        # Multi-segment log abstraction
│   │   ├── errors.go     # Storage error types
│   │   └── *_test.go     # Unit tests
│   ├── protocol/         # Phase 2: Wire protocol
│   │   ├── types.go      # Request/response structs, API keys, error codes
│   │   ├── encode.go     # Binary encode/decode for all message types
│   │   └── *_test.go     # Round-trip and edge case tests
│   ├── broker/           # Phase 2: TCP server
│   │   ├── server.go     # TCP listener, connection handling, topic log management
│   │   ├── handlers.go   # PRODUCE and FETCH request handlers
│   │   └── *_test.go     # Integration tests
│   ├── topic/            # Phase 3: Topics + partitions
│   │   ├── partition.go  # Partition wrapping Log
│   │   ├── topic.go      # Topic with multiple partitions
│   │   └── manager.go    # Topic registry with lazy creation
│   └── consumer/         # (planned) Consumer groups
├── pkg/
│   └── client/           # Client libraries
│       ├── producer.go   # Producer with key-based partitioning
│       ├── consumer.go   # Consumer with partition + offset tracking
│       └── partitioner.go # Key -> partition routing (FNV-1a hash)
├── test/
│   └── integration/      # (planned) End-to-end tests
└── docs/
    └── BUILD_PLAN.md     # Detailed phase roadmap
```

## Usage

```bash
# Run tests
go test ./...

# Start the broker (default port 9092, data in ./data)
go run ./cmd/broker --port 9092 --data ./data

# Produce a message (goes to partition 0 by default)
go run ./cmd/produce --broker localhost:9092 --topic test --message "hello world"

# Produce with a key (same key = same partition)
go run ./cmd/produce --broker localhost:9092 --topic test --key user-123 --message "event1"

# Consume from a specific partition and offset
go run ./cmd/consume --broker localhost:9092 --topic test --partition 0 --offset 0

# Build all binaries
go build ./cmd/broker
go build ./cmd/produce
go build ./cmd/consume
```

### Example Session

```bash
# Terminal 1: Start broker (default 3 partitions per topic)
./broker --port 9092 &

# Terminal 2: Produce messages with keys (same key = same partition)
./produce --topic orders --key user-123 --message '{"item": "widget"}'
# Output: Produced to orders partition 1 at offset 0

./produce --topic orders --key user-123 --message '{"item": "gadget"}'
# Output: Produced to orders partition 1 at offset 1

./produce --topic orders --key user-456 --message '{"item": "thing"}'
# Output: Produced to orders partition 2 at offset 0

# Terminal 3: Consume from partition 1
./consume --topic orders --partition 1 --offset 0
# Output:
# [0] {"item": "widget"}
# [1] {"item": "gadget"}

# Consume from partition 2
./consume --topic orders --partition 2 --offset 0
# Output:
# [0] {"item": "thing"}
```

## Current API (internal/storage)

### Record

```go
// Create a new record with payload
record := storage.NewRecord([]byte("message"))

// Encode to bytes for disk storage
data := record.Encode()

// Decode from bytes
decoded, err := storage.DecodeRecord(data)

// Read from an io.Reader stream (returns io.EOF at end)
record, err := storage.ReadRecord(reader)

// Validate integrity
if !record.ValidateCRC() {
    // record is corrupted
}

// Get total bytes on disk (header + payload)
size := record.TotalSize()
```

### Segment

```go
// Configure segment behavior
config := storage.ConfigSegment{
    SyncWrites:    100,  // fsync after every 100 writes (0 = never)
    IndexInterval: 4096, // add index entry every 4096 bytes
}

// Create or open a segment file
segment, err := storage.NewSegment("/path/to/data", baseOffset, config)

// Append a record (returns assigned offset)
// Automatically fsyncs based on SyncWrites config
offset, err := segment.Append(record)

// Read a record by offset (uses sparse index for fast lookup)
record, err := segment.Read(offset)

// Recover state after restart (scans file, updates nextOffset)
err := segment.Recover()

// Close the segment file
err := segment.Close()
```

### Protocol (internal/protocol)

```go
// API keys identify the operation
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

// Encode/decode request headers
header := &protocol.RequestHeader{
    Size:      100,
    APIKey:    protocol.APIKeyProduce,
    RequestID: 42,
}
data := protocol.EncodeRequestHeader(header)
decoded, err := protocol.DecodeRequestHeader(reader)

// Encode/decode produce requests (with partition and key)
req := &protocol.ProduceRequest{
    Topic:     "orders",
    Partition: -1,                    // -1 = use key hash
    Key:       []byte("user-123"),    // for partitioning
    Payload:   []byte(`{"item": "widget"}`),
}
data := protocol.EncodeProduceRequest(req)
decoded, err := protocol.DecodeProduceRequest(data)

// Encode/decode fetch requests (from specific partition)
fetchReq := &protocol.FetchRequest{
    Topic:     "orders",
    Partition: 0,       // which partition to read
    Offset:    0,
    MaxBytes:  65536,
}
data := protocol.EncodeFetchRequest(fetchReq)
decoded, err := protocol.DecodeFetchRequest(data)

// Encode/decode fetch responses (with multiple records)
resp := &protocol.FetchResponse{
    ErrorCode: protocol.ErrNone,
    Records: []protocol.FetchRecord{
        {Offset: 0, Payload: []byte("first")},
        {Offset: 1, Payload: []byte("second")},
    },
}
data := protocol.EncodeFetchResponse(resp)
decoded, err := protocol.DecodeFetchResponse(data)
```

### Broker (internal/broker)

```go
// Create and start a broker
config := broker.BrokerConfig{
    Port:          9092,
    DataDir:       "./data",  // topics stored as subdirectories
    NumPartitions: 3,         // default partitions for new topics
}
b := broker.NewBroker(config)

// Start accepting connections (loads existing topics from disk)
if err := b.Start(); err != nil {
    log.Fatal(err)
}

// Broker auto-creates topics with partitions
// Data stored in: ./data/{topic}/partition-{N}/00000000000000000000.log

// Graceful shutdown
b.Stop()  // closes connections, flushes logs
```

### Client (pkg/client)

```go
// Producer - send messages to broker
producer, _ := client.NewProducer("localhost:9092")
defer producer.Close()

// Simple produce (no key, goes to partition 0)
result, err := producer.Produce("orders", []byte(`{"item": "widget"}`))
// result.Partition = 0, result.Offset = assigned offset

// Produce with key (same key = same partition)
result, err = producer.ProduceWithKey("orders", []byte("user-123"), []byte(`{"item": "gadget"}`))
// result.Partition = hash("user-123") % numPartitions

// Produce to specific partition
result, err = producer.ProduceToPartition("orders", 2, nil, []byte(`{"item": "thing"}`))
// result.Partition = 2

// Consumer - fetch messages from a specific partition
consumer, _ := client.NewConsumer("localhost:9092", "orders", 0, 0) // partition 0, offset 0
defer consumer.Close()

messages, err := consumer.Fetch(65536)  // max 64KB of messages
for _, msg := range messages {
    fmt.Printf("Offset %d: %s\n", msg.Offset, msg.Payload)
}

// Consumer tracks offset automatically within the partition
nextOffset := consumer.Offset()
partition := consumer.Partition()
```

### Partitioner (pkg/client)

```go
// Partitioner interface for custom routing strategies
type Partitioner interface {
    Partition(key []byte, numPartitions int) int
}

// HashPartitioner - FNV-1a hash (default)
// Same key always maps to same partition
partitioner := &client.HashPartitioner{}
partition := partitioner.Partition([]byte("user-123"), 3)  // deterministic

// RoundRobinPartitioner - even distribution, no ordering
partitioner := &client.RoundRobinPartitioner{}
partition := partitioner.Partition(nil, 3)  // 0, 1, 2, 0, 1, 2, ...
```

### Topic Manager (internal/topic)

```go
// TopicConfig configures topic creation
config := topic.TopicConfig{
    NumPartitions: 3,
    LogConfig:     storage.LogConfig{...},
}

// Manager handles topic lifecycle
manager := topic.NewManager("./data", config)

// Load existing topics from disk on startup
err := manager.LoadExisting()

// Get or create a topic (lazy creation)
t, err := manager.GetOrCreate("orders")

// Get existing topic (returns nil if not found)
t := manager.Get("orders")

// List all topics
names := manager.Topics()  // []string{"orders", "users", ...}

// Topic operations
numPartitions := t.NumPartitions()
offset, err := t.Append(partitionID, record)
record, err := t.Read(partitionID, offset)

// Partition operations
p, err := t.Partition(0)
offset, err := p.Append(record)
record, err := p.Read(offset)
oldest := p.OldestOffset()
newest := p.NewestOffset()

// Cleanup
manager.Close()
```

## Benchmarks

Benchmarks will be added after Phase 6.

## Design Decisions

### Record Format
- **Big-endian encoding**: Network byte order for potential future cross-platform compatibility
- **CRC32-IEEE**: Standard checksum algorithm, fast and sufficient for corruption detection
- **Fixed 16-byte header**: Predictable layout simplifies parsing; offset stored per-record enables segment-level recovery

### Segment Files
- **Naming convention**: 20-digit zero-padded offset (e.g., `00000000000000000000.log`) for lexicographic sorting
- **Append-only**: Records written sequentially, never modified in place
- **Recovery**: On restart, Recover() scans the segment sequentially to rebuild nextOffset state
- **Configurable fsync**: `SyncWrites` controls durability/performance tradeoff (0 = OS-managed, N = sync every N writes)
- **Sparse index**: Index entries added at configurable byte intervals for O(1) seek + short scan reads

### Wire Protocol
- **Big-endian encoding**: Network byte order, consistent with storage layer
- **Length-prefixed messages**: Size field first enables framing on TCP stream
- **Request correlation**: RequestID echoed in response for async request matching
- **Separate header/payload encoding**: Headers decoded first to route to correct handler

```
Request:  [Size:4][APIKey:2][RequestID:4][Payload:var]
Response: [Size:4][RequestID:4][Payload:var]

ProduceRequest:  [TopicLen:2][Topic:var][Partition:4][KeyLen:4][Key:var][PayloadLen:4][Payload:var]
ProduceResponse: [Partition:4][Offset:8][ErrorCode:2]

FetchRequest:    [TopicLen:2][Topic:var][Partition:4][Offset:8][MaxBytes:4]
FetchResponse:   [ErrorCode:2][RecordCount:4][Records:var]
  Each record:   [Offset:8][PayloadLen:4][Payload:var]
```

### Broker
- **One goroutine per connection**: Simple concurrency model; connection loop reads requests sequentially
- **Lazy topic creation**: Logs created on first produce to topic (no explicit CREATE_TOPIC API)
- **RWMutex for topic map**: Allows concurrent reads to different topics; write lock only for new topic creation
- **Graceful shutdown**: WaitGroup tracks active connections; quit channel signals shutdown

### Client Libraries
- **Synchronous API**: One request/response at a time per connection (no pipelining yet)
- **Atomic request IDs**: Incrementing counter for request correlation
- **Consumer offset tracking**: Client tracks position within partition; advances after each Fetch
- **No batching**: Producer sends one message per request (batching planned for Phase 5)

### Topics + Partitions
- **Partition = independent log**: Each partition is a separate Log instance in its own directory
- **Directory structure**: `data/{topic}/partition-{N}/` isolates partition data
- **Key-based routing**: FNV-1a hash of key determines partition; same key = same partition = ordering preserved
- **Lazy topic creation**: Topics created on first produce; partition count fixed at creation
- **Recovery**: Manager scans data directory on startup, counts partition-N directories to determine partition count
- **RWMutex for manager**: Read lock for topic lookup; write lock only when creating new topics
- **Partition in protocol**: Both produce and fetch specify partition; -1 means use key hash

## What I Learned

*Notes added after each phase completion.*
