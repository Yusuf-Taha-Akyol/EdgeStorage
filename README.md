# EdgeStorage

EdgeStorage is a lightweight open-source telemetry storage engine for edge devices.

It is designed for constrained environments where low resource usage and compressed storage matter.  
The initial goal is simple:

- use little CPU and memory
- store telemetry in compressed form
- run well on edge devices such as Raspberry Pi, Jetson, and similar Linux systems

## Project Status

Early design stage.  
The first version will focus on a minimal C-based storage engine core.

## Goals

- Low resource usage
- Compressed telemetry storage
- Simple embeddable C core
- Edge-device friendly architecture

## Planned Tech Stack

- Language: C
- Build system: CMake
- Focus: embeddable storage engine for edge environments

## Repository Structure

Planned structure:

```text
include/
src/
tests/
examples/
docs/
tools/
bench/
