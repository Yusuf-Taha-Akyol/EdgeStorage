# EdgeStorage

EdgeStorage is a lightweight C telemetry storage engine for edge devices.

It stores telemetry records in append-only segmented files, supports timestamp and payload-aware compression for numeric telemetry patterns, and provides basic read/query and benchmark tooling.

The project is currently a working prototype, not a production-ready database.

## Current Status

EdgeStorage currently supports:

- schema-based stream and record model
- engine runtime lifecycle
- append-only segmented storage
- single and batch record writes
- self-describing segment headers
- timestamp delta compression
- payload-aware int32 delta compression
- compressed read/query path
- time-range queries
- record type filtering
- multi-segment reads
- workload-based benchmark tooling

The current focus is validating compression behavior, storage format design, and edge-device suitability.

## Why this exists

Telemetry-heavy edge systems such as drones, robots, and industrial devices often produce continuous numeric data under limited storage, bandwidth, and connectivity constraints.

EdgeStorage explores a small, embeddable storage layer for those environments:

- write records quickly
- store data locally in segmented files
- reduce storage footprint for telemetry-like payloads
- read/query stored records later
- prepare for future offline-first sync workflows

## Quick Start

Build and run tests:

    cmake -S . -B build
    cmake --build build
    ctest --test-dir build --output-on-failure

Run the basic example:

    ./build/edgestorage_example

Run a benchmark:

    ./build/edgestorage_benchmark 1000000 100 64 mixed_drone_i32

Benchmark format:

    edgestorage_benchmark <records> <batch_size> <payload_size_bytes> [workload]

Available workloads:

    random
    smooth_i32
    delta_i16
    mixed_drone_i32

## Benchmark Snapshot

The latest benchmark run was measured on a Mac development machine.

1M records, 64B payload, batch size 100:

| Workload | Compression ratio | Storage reduction | Compressed write |
|---|---:|---:|---:|
| random | 0.952x | -5.0% | 705K rec/s |
| smooth_i32 | 2.222x | ~55.0% | 641K rec/s |
| delta_i16 | 1.538x | ~35.0% | 637K rec/s |
| mixed_drone_i32 | 2.222x | ~55.0% | 633K rec/s |

For 1M records with 256B smooth or drone-like int32 telemetry payloads, the current prototype reached **3.237x compression ratio**, reducing approximately **272 MB to 84 MB**.

This means EdgeStorage does not try to show artificial gains on random data. The compression advantage appears on telemetry-like numeric payloads where consecutive values change smoothly.

## What the Benchmark Shows

Current benchmark behavior:

- random payloads do not compress well, as expected
- smooth int32 telemetry reaches around 2.22x compression ratio on 64B payloads
- drone-like int32 telemetry reaches around 2.22x compression ratio on 64B payloads
- 256B smooth or drone-like telemetry reaches around 3.24x compression ratio
- compressed data remains readable through the current query path

Storage reduction examples:

| Compression ratio | Approx. storage reduction |
|---:|---:|
| 1.538x | ~35% |
| 2.222x | ~55% |
| 3.237x | ~69% |

Full benchmark details are in:

[docs/benchmark-results.md](docs/benchmark-results.md)

## Current Limitations

EdgeStorage is still early. Current limitations:

- benchmarks are synthetic
- `mixed_drone_i32` is telemetry-like, not real flight data
- tests were run on a Mac development machine
- Raspberry Pi / Jetson-class validation is still pending
- durability and fsync modes are not benchmarked yet
- no crash recovery benchmark yet
- no power-loss testing yet
- no real sensor dataset benchmark yet
- compression currently focuses on timestamp and int32 delta patterns
- query path is still basic linear scanning
- no encryption at rest yet
- no sync/server layer yet

## Roadmap

Near-term:

- add a simple dump/export tool for inspecting stored records
- document the storage format
- run the same benchmarks on edge-class hardware
- add durability and fsync benchmark modes
- test with real telemetry datasets
- improve query/index support
- move toward segment-based offline sync

Longer-term:

- schema-aware compression
- float/Gorilla-style compression
- adaptive compression strategy
- segment metadata and checksums
- local-first sync to user-controlled machines or servers

## Current Takeaway

EdgeStorage should not be described as a general-purpose compressor.

A more accurate description is:

EdgeStorage uses telemetry-aware payload compression to reduce storage footprint for smooth numeric edge telemetry streams. Random data is not expected to compress well.

Current benchmark headline:

- 64B smooth/drone-like int32 telemetry: ~2.22x compression ratio, ~55% storage reduction
- 256B smooth/drone-like int32 telemetry: ~3.24x compression ratio, ~69% storage reduction

## License

MIT
