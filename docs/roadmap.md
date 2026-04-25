# EdgeStorage Roadmap

## Current Stage

The project currently has a working append-only telemetry write path with initial timestamp delta compression support.

What is already done:

- repository created
- initial README added
- project folder structure created
- CMake-based build flow added
- public C API skeleton added
- example and smoke test added
- stream schema and record model foundation added
- engine runtime lifecycle added
- in-memory stream registration state added
- append-only storage writer added
- stream-based storage directories added
- segment file rollover added
- single and batch append paths added
- timestamp delta compression write path added
- compression-specific tests added

## Phase 1: Foundation

Goal: establish a stable project base

Tasks:

- finalize project documentation
- stabilize public API direction
- clarify data model and stream schema approach
- keep build and test flow clean

## Phase 2: Core Engine

Goal: implement the smallest usable engine core

Tasks:

- implement engine lifecycle more cleanly
- define internal engine state
- validate config handling
- prepare storage initialization flow

## Phase 3: Stream and Record Model

Goal: define how telemetry is represented

Tasks:

- define stream registration flow
- define field types and stream schema
- define record layout
- define ownership and memory rules

## Phase 4: Storage Path

Goal: support real append-only local writes

Tasks:

- design segment file format
- implement append-only write path
- persist metadata
- establish simple on-disk structure

## Phase 5: Compression

Goal: add first practical compression behavior without breaking the append-only write path.

Current status:

- Compression v1 is implemented for timestamp delta encoding.
- Compression is enabled through `compression_enabled`.
- The first record in a segment writes the full `timestamp_ns`.
- Subsequent records in the same segment write a `uint32_t` timestamp delta.
- Segment rollover resets timestamp delta state so each segment remains independently decodable.
- Non-monotonic timestamps are rejected in the compressed write path.
- Timestamp deltas larger than `UINT32_MAX` are rejected for now.

In scope for the current version:

- timestamp delta encoding
- compressed write path
- compressed segment size accounting
- rollover behavior with compression enabled
- compression-focused tests

Out of scope for the current version:

- read/query/decompression path
- payload integer field compression
- float/double compression
- delta-of-delta encoding
- frame-of-reference encoding
- benchmarking

## Phase 6: Read Path

Goal: support basic retrieval

Tasks:

- read stored records back
- filter by stream
- filter by time range
- support minimal result handling

## Phase 7: Benchmarks

Goal: prove technical value

Tasks:

- generate synthetic telemetry data
- measure storage size reduction
- measure build and runtime stability
- measure CPU and memory overhead
- compare against simple raw storage baselines

## Later Phases

Possible later expansions:

- stronger indexing
- richer query support
- multi-language bindings
- offline synchronization
- recovery and durability improvements
- benchmark comparison against SQLite or other alternatives

