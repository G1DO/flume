# Flume

A message streaming system built from scratch in Go. Not a Kafka wrapper — the core primitives implemented manually to understand log-structured storage, consumer coordination, and backpressure.

## Status

**In Development** — Phase 2 (Broker + Wire Protocol)

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Log Storage Engine | Complete |
| 2 | Broker + Wire Protocol | In Progress |
| 3 | Topics + Partitions | Not started |
| 4 | Consumer Groups | Not started |
| 5 | Backpressure | Not started |
| 6 | Integration + Benchmarks | Not started |

### Phase 2 Progress

| Milestone | Status |
|-----------|--------|
| Wire protocol types (Request/Response headers) | Done |
| ProduceRequest/ProduceResponse encoding | Done |
| FetchRequest/FetchResponse encoding | Done |
| Error codes | Done |
| TCP server | Not started |
| PRODUCE handler | Not started |
| FETCH handler | Not started |
| Producer client | Not started |
| Consumer client | Not started |

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
│   ├── broker/           # (planned) Main broker binary
│   ├── produce/          # (planned) CLI producer
│   └── consume/          # (planned) CLI consumer
├── internal/
│   ├── storage/          # Phase 1: Log engine
│   │   ├── record.go     # Record struct, encode/decode, CRC, stream reading
│   │   ├── segment.go    # Segment file with Append(), Read(), Recover(), configurable fsync
│   │   ├── index.go      # Sparse offset index for fast lookups
│   │   ├── log.go        # Multi-segment log abstraction
│   │   ├── errors.go     # Storage error types
│   │   └── *_test.go     # Unit tests
│   ├── protocol/         # Phase 2: Wire protocol (in progress)
│   │   ├── types.go      # Request/response structs, API keys, error codes
│   │   ├── encode.go     # Binary encode/decode for all message types
│   │   └── *_test.go     # Round-trip and edge case tests
│   ├── broker/           # (planned) TCP server
│   ├── topic/            # (planned) Topics + partitions
│   └── consumer/         # (planned) Consumer groups
├── pkg/
│   └── client/           # (planned) Producer/consumer libraries
├── test/
│   └── integration/      # (planned) End-to-end tests
└── docs/
    └── BUILD_PLAN.md     # Detailed phase roadmap
```

## Usage

```bash
# Run tests
go test ./...
```

CLI tools not yet implemented — see BUILD_PLAN.md for roadmap.

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

// Encode/decode produce requests
req := &protocol.ProduceRequest{
    Topic:   "orders",
    Payload: []byte(`{"user": 123}`),
}
data := protocol.EncodeProduceRequest(req)
decoded, err := protocol.DecodeProduceRequest(data)

// Encode/decode fetch requests
fetchReq := &protocol.FetchRequest{
    Topic:    "orders",
    Offset:   0,
    MaxBytes: 65536,
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

ProduceRequest:  [TopicLen:2][Topic:var][PayloadLen:4][Payload:var]
ProduceResponse: [Offset:8][ErrorCode:2]

FetchRequest:    [TopicLen:2][Topic:var][Offset:8][MaxBytes:4]
FetchResponse:   [ErrorCode:2][RecordCount:4][Records:var]
  Each record:   [Offset:8][PayloadLen:4][Payload:var]
```

## What I Learned

*Notes added after each phase completion.*
