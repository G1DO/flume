# Flume

A message streaming system built from scratch in Go. No frameworks, no Kafka wrappers — every component from the binary wire protocol to the log-structured storage engine is hand-implemented.

Built to deeply understand how systems like Kafka work under the hood: how bytes hit disk, how partitions enable parallelism, how consumer groups coordinate, and how backpressure keeps everything stable.

## What It Does

```
Producers ──► Broker (TCP :9092) ──► Consumers
                 │
           ┌─────┴─────┐
           │   Topic    │
           │ ┌───┬───┐  │
           │ │P0 │P1 │  │   Partitions = independent append-only logs
           │ │P2 │...│  │   Same key → same partition → ordered
           │ └───┴───┘  │
           └────────────┘
```

**Core features:**
- **Append-only log storage** with CRC32 integrity checks and sparse indexing
- **Partitioned topics** with key-based routing (FNV-1a hash) for parallel processing
- **Consumer groups** with automatic partition assignment, heartbeat-based liveness, and rebalancing
- **Backpressure** via per-partition bounded buffers (buffered Go channels)
- **Custom binary wire protocol** over TCP with 7 API operations
- **Full test coverage** including integration tests and benchmarks

## Quick Start

```bash
# Start the broker
go run ./cmd/broker --port 9092 --data ./data

# Produce messages (same key = same partition = ordered)
go run ./cmd/produce --broker localhost:9092 --topic orders --key user-123 --message '{"item": "widget"}'
go run ./cmd/produce --broker localhost:9092 --topic orders --key user-123 --message '{"item": "gadget"}'

# Consume
go run ./cmd/consume --broker localhost:9092 --topic orders --partition 0 --offset 0

# Run all tests
go test ./...
```

## How It's Built

The project was built in 6 incremental phases, each adding a layer of complexity:

| Phase | What | Why |
|-------|------|-----|
| 1. Log Engine | Append-only segments, CRC checksums, sparse index | Understand how databases store data durably |
| 2. Wire Protocol | Binary TCP protocol, request routing | Learn network framing and serialization from scratch |
| 3. Partitions | Topics split into N independent logs, key hashing | Solve the single-log throughput bottleneck |
| 4. Consumer Groups | Membership, heartbeats, coordinated assignment | Prevent duplicate processing across consumers |
| 5. Backpressure | Bounded channels, background write goroutines | Handle fast producers without OOM crashes |
| 6. Integration | End-to-end tests, benchmarks | Verify everything works together |

## Architecture

### On Disk

```
data/
├── orders/                    # topic
│   ├── partition-0/
│   │   ├── 00000000.log       # append-only records
│   │   └── 00000000.index     # sparse offset → byte position
│   ├── partition-1/
│   └── partition-2/
└── __consumer_offsets/         # committed offsets per group
```

### Record Format

Every message on disk:

```
┌──────────┬──────────┬──────────┬───────────────────┐
│ Offset   │ Size     │ CRC32    │ Payload           │
│ 8 bytes  │ 4 bytes  │ 4 bytes  │ variable          │
└──────────┴──────────┴──────────┴───────────────────┘
```

### Wire Protocol

```
Request:  [Size:4][APIKey:2][RequestID:4][Payload:variable]
Response: [Size:4][RequestID:4][Payload:variable]

7 operations: PRODUCE, FETCH, JOIN_GROUP, LEAVE_GROUP, HEARTBEAT, OFFSET_COMMIT, OFFSET_FETCH
```

### Backpressure Flow

```
Producer request
      │
      ▼
Broker handler
      │
      ▼
Partition's buffered channel ◄── blocks here if full
      │
      ▼
Background goroutine ──► log.Append() ──► disk
      │
      ▼
Response with offset sent back to producer
```

Each partition has its own channel and goroutine — a slow partition never blocks others.

## Benchmarks

```
BenchmarkProduce              ~160μs/op     6,200 msg/s     (100-byte messages)
BenchmarkProduceLargeMessage  ~219μs/op    46.7 MB/s        (10KB messages)
BenchmarkProduceAndFetchRT    ~842μs/op                     (full round-trip)
BenchmarkConcurrentProducers  ~644μs/op                     (4 parallel producers)
```

Run with: `go test ./test/integration/ -bench=. -benchmem`

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Append-only logs | No random writes, sequential disk I/O, simple recovery |
| Sparse index (not per-record) | O(1) seek + short scan without bloating index files |
| Per-partition bounded channels | Backpressure without cross-partition interference |
| FNV-1a key hashing | Fast, deterministic — same key always hits same partition |
| Generation-based rebalancing | Prevents split-brain when consumers join/leave |
| Heartbeat expiration | Detects dead consumers, triggers automatic reassignment |
| Atomic offset persistence | temp file + rename — no partial writes on crash |

## Project Structure

```
flume/
├── cmd/                        # CLI binaries (broker, produce, consume)
├── internal/
│   ├── storage/                # Log engine: records, segments, index, multi-segment log
│   ├── protocol/               # Binary wire protocol: encode/decode for all 7 APIs
│   ├── broker/                 # TCP server: connection handling, request routing
│   ├── topic/                  # Partitioned topics with bounded write buffers
│   └── consumer/               # Consumer groups: coordinator, membership, offset storage
├── pkg/client/                 # Producer + consumer client libraries
└── test/integration/           # End-to-end tests + benchmarks
```

## Intentional Limitations

This is a learning project, not a production system:

- **Single broker** — no replication, no distributed consensus
- **No batching** — one message per network round-trip
- **No compression** — payloads stored raw
- **No authentication** — open TCP connections
- **No log compaction** — segments grow indefinitely

These are deliberate scope boundaries, not oversights.

## What I Learned

- Building a storage engine from scratch made concepts like CRC checksums, segment rollover, and sparse indexing feel real instead of theoretical.

- Partitions aren't just "a Kafka thing." They solve a real problem: one log = one bottleneck. Split into many = parallelism. That clicked for me.

- Consumer groups sound simple (split partitions across consumers), but handling crashes, detecting dead members with heartbeats, and rebalancing without duplicates was the hardest part of this project.

- I didn't know what backpressure was before this. Turns out the idea is simple: don't crash when a producer is too fast — make it wait. Bounded channels in Go made this easy to build.

- Got much more comfortable with Go's concurrency: goroutines, channels, mutexes, WaitGroups, and interfaces. Also learned to take error handling seriously.
