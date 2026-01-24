# Flume

A message streaming system built from scratch in Go. Not a Kafka wrapper — the core primitives implemented manually to understand log-structured storage, consumer coordination, and backpressure.

## Status

**In Development** — Phase 1 (Log Storage Engine)

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Log Storage Engine | In Progress |
| 2 | Broker + Wire Protocol | Not started |
| 3 | Topics + Partitions | Not started |
| 4 | Consumer Groups | Not started |
| 5 | Backpressure | Not started |
| 6 | Integration + Benchmarks | Not started |

### Phase 1 Progress

| Milestone | Status |
|-----------|--------|
| Record format + serialization (CRC32) | Done |
| Segment Append() | Done |
| Segment Recover() | Done |
| Segment Read() | Done |
| Fsync strategy (configurable) | Done |
| Sparse index for offset lookup | Done |
| Multi-segment with rollover | Not started |

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
│   ├── protocol/         # (planned) Wire protocol
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

## What I Learned

*Notes added after each phase completion.*
