# Flume

A message streaming system built from scratch in Go. Not a Kafka wrapper — the core primitives implemented manually to understand log-structured storage, consumer coordination, and backpressure.

## Status

🚧 **In Development**

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Single-topic broker | Not started |
| 2 | Multi-topic + partitions | Not started |
| 3 | Consumer groups | Not started |
| 4 | Backpressure | Not started |
| 5 | Benchmarks | Not started |

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
Producers → Broker → Consumers
              │
         ┌────┴────┐
         │  Topics │
         │ ┌─────┐ │
         │ │ P0  │ │  ← Partitions (separate log files)
         │ │ P1  │ │
         │ │ P2  │ │
         │ └─────┘ │
         └─────────┘
```

<!-- TODO: expand after Phase 1 complete -->

## Usage

```bash
# TODO: add commands after implementation
```

## Benchmarks

<!-- TODO: fill after Phase 5 -->

| Metric | Value |
|--------|-------|
| Throughput | TBD |
| p99 Latency | TBD |
| Recovery time (1M msgs) | TBD |

## Design Decisions

<!-- Document trade-offs as you build -->

## What I Learned

<!-- Add insights after each phase -->
