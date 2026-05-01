# Architecture

EdgeStorage is organized into these layers:

- `src/api/` — public API entry points
- `src/core/` — engine runtime and stream registry
- `src/storage/` — append-only segment writer
- `src/compression/` — compression logic
- `src/query/` — read/query path
- `bench/` — benchmark tooling

Current storage model:
- stream-based layout
- append-only `.seg` files
- segment rollover
- basic timestamp-delta compression