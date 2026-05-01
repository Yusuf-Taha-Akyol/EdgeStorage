# EdgeStorage

EdgeStorage is a lightweight C-based telemetry storage engine for edge devices.

It is being built for constrained environments where low resource usage and compressed telemetry storage matter.

## Current Status

EdgeStorage is currently at a working prototype stage.

Implemented so far:
- schema-based stream and record model
- engine runtime lifecycle
- append-only segmented storage writer
- initial timestamp delta compression
- basic read/query path
- benchmark tooling

## Current Capabilities

- append-only telemetry writes
- segmented `.seg` storage files
- stream-based storage layout
- segment rollover
- single and batch record writes
- initial timestamp delta compression on write path
- stream-based queries
- time range queries
- record type filtering
- multi-segment reads
- basic benchmark measurements

## Not Yet Implemented

- payload-level compression
- full decompression coverage
- advanced indexing
- durability modes / fsync benchmarking
- sync / DDIL transport layer
- production-grade recovery

## Build

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build