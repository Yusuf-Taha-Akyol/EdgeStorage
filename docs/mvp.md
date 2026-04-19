# EdgeStorage MVP

## Purpose

The purpose of the MVP is to prove that EdgeStorage can store telemetry data on edge devices with:

- low resource usage
- compressed storage
- a simple embeddable C core

## Core Promise

EdgeStorage is a lightweight telemetry storage engine for edge devices that stores data in compressed form.

## Main Requirements

The MVP must satisfy these two core requirements:

1. Low resource usage
2. Compressed telemetry storage

These are the non-negotiable foundations of the first version.

## MVP Scope

The MVP will include:

- a C-based core library
- append-only telemetry write path
- segmented local storage
- basic compressed storage
- a minimal public API
- basic smoke tests
- a simple example program
- basic build support with CMake

## Supported Data Model

The first version will focus on small telemetry records such as:

- timestamped sensor values
- numeric telemetry streams
- simple boolean state values

The data model will be schema-based and stream-oriented.

## Initial Compression Direction

The initial compression approach will be simple and incremental.

Planned starting point:

- delta encoding
- delta-of-delta encoding for timestamp-like sequences

Compression can be improved in later versions.

## Target Environment

The MVP is designed for edge-style environments such as:

- Raspberry Pi
- Jetson-class devices
- Linux-based constrained systems

## Long-Term Direction

After the MVP, the project may expand toward:

- better compression strategies
- richer read/query support
- benchmarking against alternatives
- embedded integrations with other languages
- offline-first telemetry pipelines