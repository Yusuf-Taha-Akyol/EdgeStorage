# EdgeStorage Benchmark Results

This document summarizes the current EdgeStorage benchmark results after adding workload-based payload generation and payload-aware int32 delta compression.

The goal of these benchmarks is not to claim production performance. The goal is to show how the current prototype behaves across different telemetry-like data patterns.

## Context

EdgeStorage now supports:

- append-only segmented storage
- self-describing segment headers
- timestamp delta compression
- encoded compressed record headers
- payload-aware int32 delta compression
- compressed read/query path
- workload-based benchmark tooling

The benchmark tool can generate different payload patterns:

    random
    smooth_i32
    delta_i16
    mixed_drone_i32

This makes compression behavior easier to interpret. Random data should not compress well. Smooth numeric telemetry should compress better.

## Test Environment

These results were measured on a Mac development machine.

Important notes:

- these are synthetic benchmarks
- `mixed_drone_i32` is telemetry-like, not real flight data
- durability / fsync modes are not benchmarked yet
- edge-device validation is still pending
- results should be treated as development-stage measurements, not production guarantees

## How to Run

Build and test:

    cmake -S . -B build
    cmake --build build
    ctest --test-dir build --output-on-failure

Run benchmark examples:

    ./build/edgestorage_benchmark 100000 100 64 random
    ./build/edgestorage_benchmark 100000 100 64 smooth_i32
    ./build/edgestorage_benchmark 100000 100 64 delta_i16
    ./build/edgestorage_benchmark 100000 100 64 mixed_drone_i32

General form:

    ./build/edgestorage_benchmark <records> <batch_size> <payload_size_bytes> [workload]

## Workloads

### random

Deterministic pseudo-random payload.

Purpose: show behavior on data that should not compress well.

Expected behavior: compression ratio around 1.0x or slightly below due to compressed record metadata overhead.

### smooth_i32

Payload is generated as an int32 array with small changes between consecutive records.

Purpose: exercise `DELTA_I32_I8` payload compression.

Expected behavior: strong compression ratio on smooth numeric telemetry.

### delta_i16

Payload is generated as an int32 array where deltas exceed int8 range but fit int16 range.

Purpose: exercise `DELTA_I32_I16` payload compression.

Expected behavior: better than RAW, but less compact than int8 delta compression.

### mixed_drone_i32

Telemetry-like int32 payload pattern with fields similar to:

    ax, ay, az, gx, gy, gz, battery_mv, altitude_cm, speed_cm_s, temperature_centi

Purpose: provide a more realistic synthetic drone/robot telemetry pattern.

Expected behavior: significantly better than random payloads.

## Storage Reduction Formula

Compression ratio is calculated as:

    compression_ratio = uncompressed_size / compressed_size

Storage reduction is calculated as:

    storage_reduction = 1 - (1 / compression_ratio)

Examples:

    2.222x ratio -> ~55.0% storage reduction
    1.538x ratio -> ~35.0% storage reduction
    3.237x ratio -> ~69.1% storage reduction

A ratio below 1.0x means the compressed representation is larger than the uncompressed representation. This can happen on random payloads because compressed records carry metadata.

## 100K Payload Sweep

Records: 100,000  
Batch size: 100

| Payload | Workload | Ratio | Storage reduction | Batch write | Compressed write |
|---:|---|---:|---:|---:|---:|
| 16B | random | 0.889x | -12.5% | 869K rec/s | 788K rec/s |
| 16B | smooth_i32 | 1.333x | 25.0% | 856K rec/s | 739K rec/s |
| 16B | delta_i16 | 1.143x | 12.5% | 860K rec/s | 787K rec/s |
| 16B | mixed_drone_i32 | 1.333x | 25.0% | 824K rec/s | 781K rec/s |
| 32B | random | 0.923x | -8.3% | 802K rec/s | 762K rec/s |
| 32B | smooth_i32 | 1.714x | 41.7% | 873K rec/s | 681K rec/s |
| 32B | delta_i16 | 1.333x | 25.0% | 864K rec/s | 745K rec/s |
| 32B | mixed_drone_i32 | 1.714x | 41.7% | 862K rec/s | 739K rec/s |
| 64B | random | 0.952x | -5.0% | 740K rec/s | 718K rec/s |
| 64B | smooth_i32 | 2.222x | 55.0% | 830K rec/s | 639K rec/s |
| 64B | delta_i16 | 1.538x | 35.0% | 814K rec/s | 620K rec/s |
| 64B | mixed_drone_i32 | 2.222x | 55.0% | 815K rec/s | 630K rec/s |
| 128B | random | 0.973x | -2.8% | 628K rec/s | 600K rec/s |
| 128B | smooth_i32 | 2.769x | 63.9% | 777K rec/s | 495K rec/s |
| 128B | delta_i16 | 1.714x | 41.7% | 758K rec/s | 467K rec/s |
| 128B | mixed_drone_i32 | 2.769x | 63.9% | 739K rec/s | 481K rec/s |
| 256B | random | 0.986x | -1.4% | 482K rec/s | 449K rec/s |
| 256B | smooth_i32 | 3.237x | 69.1% | 666K rec/s | 335K rec/s |
| 256B | delta_i16 | 1.838x | 45.6% | 660K rec/s | 341K rec/s |
| 256B | mixed_drone_i32 | 3.237x | 69.1% | 619K rec/s | 301K rec/s |

## 1M Sanity Results

Records: 1,000,000  
Batch size: 100

| Workload | Payload | Ratio | Storage reduction | Uncompressed | Compressed | Batch write | Compressed write | Query latency |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| random | 64B | 0.952x | -5.0% | 80.0 MB | 84.0 MB | 712K rec/s | 705K rec/s | 135.1 ms |
| smooth_i32 | 64B | 2.222x | 55.0% | 80.0 MB | 36.0 MB | 805K rec/s | 641K rec/s | 132.8 ms |
| delta_i16 | 64B | 1.538x | 35.0% | 80.0 MB | 52.0 MB | 812K rec/s | 637K rec/s | 126.6 ms |
| mixed_drone_i32 | 64B | 2.222x | 55.0% | 80.0 MB | 36.0 MB | 795K rec/s | 633K rec/s | 125.3 ms |
| smooth_i32 | 256B | 3.237x | 69.1% | 272.0 MB | 84.0 MB | 670K rec/s | 349K rec/s | 238.0 ms |
| mixed_drone_i32 | 256B | 3.237x | 69.1% | 272.0 MB | 84.0 MB | 597K rec/s | 330K rec/s | 179.4 ms |

All 1M sanity runs returned the expected query result count:

    query_result_count = 1000000

This confirms that the compressed data remains readable through the current query path.

## Batch Size Sweep

Workload: `mixed_drone_i32`  
Records: 100,000  
Payload: 64B

| Batch size | Ratio | Batch write | Compressed write | Query latency |
|---:|---:|---:|---:|---:|
| 1 | 2.222x | 809K rec/s | 585K rec/s | 12.5 ms |
| 10 | 2.222x | 787K rec/s | 614K rec/s | 16.5 ms |
| 100 | 2.222x | 815K rec/s | 627K rec/s | 12.8 ms |
| 1000 | 2.222x | 817K rec/s | 649K rec/s | 12.7 ms |

Batch size did not change compression ratio in this run. Larger batches slightly improved compressed write throughput.

## Interpretation

The benchmark now shows three useful behaviors.

First, random payloads do not compress well. This is expected. The compressed format carries per-record metadata, so incompressible payloads can become slightly larger than the uncompressed representation.

Second, numeric telemetry-like payloads benefit from int32 delta compression. On 64B `smooth_i32` and `mixed_drone_i32` workloads, EdgeStorage reached **2.222x compression ratio**, or about **55% storage reduction**.

Third, larger telemetry payloads improve the effective ratio because fixed metadata overhead becomes smaller relative to payload size. On 256B smooth and drone-like telemetry payloads, the current prototype reached **3.237x compression ratio**, reducing approximately **272 MB to 84 MB** for 1M records.

## Current Takeaway

EdgeStorage should not be described as a general-purpose compressor.

A more accurate statement is:

> EdgeStorage uses telemetry-aware payload compression to reduce storage footprint for smooth numeric edge telemetry streams. Random data is not expected to compress well.

Current benchmark headline:

    64B smooth/drone-like int32 telemetry:
      ~2.22x compression ratio
      ~55% storage reduction

    256B smooth/drone-like int32 telemetry:
      ~3.24x compression ratio
      ~69% storage reduction

## Limitations

These results are still early.

Known limitations:

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

## Next Steps

Planned follow-up work:

- add a simple dump/export tool for inspecting stored records
- document the storage format
- run the same benchmarks on edge-class hardware
- add durability / fsync benchmark modes
- test with real telemetry datasets
- add schema-aware compression
- improve query/index support
