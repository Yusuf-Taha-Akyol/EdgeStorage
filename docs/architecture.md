# EdgeStorage Architecture

## Overview

EdgeStorage is designed as a small embedded telemetry storage engine for edge devices.

The architecture is intentionally simple in the first phase.  
The goal is to build a low-resource and compressed storage core before adding more advanced features.

## High-Level Structure

The project is organized around these layers:

- public API
- engine core
- storage layer
- compression layer
- query layer

## 1. Public API

The public API is the external entry point of the library.

Responsibilities:

- open and close the engine
- register telemetry streams
- write records
- query records
- expose a stable C interface

This layer should remain small, predictable, and embeddable.

## 2. Engine Core

The core layer manages internal engine state.

Responsibilities:

- hold configuration
- manage runtime state
- coordinate storage and compression
- provide shared internal control flow

This is the central control layer of the system.

## 3. Storage Layer

The storage layer is responsible for persistent local storage.

Responsibilities:

- append-only writes
- segmented file management
- metadata handling
- disk layout management
- future recovery support

The first version should prefer simple and robust file organization over complexity.

## 4. Compression Layer

The compression layer is responsible for reducing telemetry storage size.

Responsibilities:

- encode repeated or predictable telemetry values
- support timestamp-friendly compression
- remain lightweight enough for constrained devices

Initial direction:

- delta encoding
- delta-of-delta encoding

Future versions may add more advanced field-specific strategies.

## 5. Query Layer

The query layer is responsible for basic read access.

Responsibilities:

- read stored records
- filter by stream
- filter by time range
- support minimal retrieval operations

In the first version, query support should remain simple.

## Data Flow

The expected high-level data flow is:

1. the user opens the engine
2. the user registers a stream schema
3. the user writes telemetry records
4. the engine passes records through the storage path
5. the storage path applies compression
6. compressed records are appended to local storage
7. later, records can be read back through the query layer

## Initial Design Principles

The architecture should follow these principles:

- keep the core small
- prefer simple file layouts
- avoid unnecessary abstraction
- design for low memory usage
- design for incremental improvement
- keep the C API stable and clear

## Initial Module Mapping

Planned repository mapping:

```text
include/edgestorage/   -> public headers
src/api/               -> public API implementation
src/core/              -> runtime state and engine coordination
src/storage/           -> on-disk storage logic
src/compression/       -> compression logic
src/query/             -> read/query support
tests/                 -> validation and smoke tests
examples/              -> usage examples
tools/                 -> helper utilities
bench/                 -> performance benchmarks

## Near-Term Implementation Order

Recommended implementation order:

1-engine lifecycle
2-stream schema registration
3-append-only write path
4-segmented storage format
5-basic compression
6-minimal read path
7-benchmark tooling