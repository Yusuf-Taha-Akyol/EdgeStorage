# EdgeStorage Roadmap

## Current Stage

The project is currently in the skeleton and design phase.

What is already done:

- repository created
- initial README added
- project folder structure created
- CMake-based skeleton added
- public API skeleton added
- example and smoke test added
- MVP and architecture documents added

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

Goal: add first practical compression behavior

Tasks:

- implement delta encoding
- implement delta-of-delta encoding where appropriate
- measure compression effect on sample telemetry data
- keep CPU and memory overhead low

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

