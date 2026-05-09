# Apache.Calcite.AdoNet

`Apache.Calcite.AdoNet` is an ADO.NET provider/driver for Apache Calcite, intended to give .NET applications first-class access to Calcite using standard .NET data APIs.

This is a **driver/provider**, not an adapter. It is intended to provide equivalent functionality to Calcite's JDBC driver from a .NET consumer perspective, while using .NET naming, APIs, and conventions.

---

## Goals

- Provide a full ADO.NET implementation for Calcite.
- Preserve feature parity with the Calcite JDBC driver surface (connection properties, model handling, SQL execution workflow, metadata behavior).
- Expose an idiomatic .NET API (`CalciteConnection`, `CalciteCommand`, `CalciteDataReader`, etc.).
- Support standard ADO.NET patterns used by ORMs, tooling, and custom data-access code.
- Implement provider logic natively in .NET; do not proxy, host, or wrap the existing JDBC driver.

## Explicit Non-Goal

- This project is **not** a thin interop layer over JDBC.
- The provider must replicate JDBC driver behavior and capabilities using native .NET implementation components.

---

## High-Level Architecture

The provider is split into layered components so transport, protocol, SQL execution, and ADO.NET abstractions remain cleanly separated.

1. **ADO.NET Surface Layer**
   - Public classes implementing the standard abstractions:
     - `CalciteConnection : DbConnection`
     - `CalciteCommand : DbCommand`
     - `CalciteDataReader : DbDataReader`
     - `CalciteParameter : DbParameter`
     - `CalciteParameterCollection : DbParameterCollection`
     - `CalciteTransaction : DbTransaction` (if transaction semantics are supported by Calcite configuration)
     - `CalciteDataAdapter : DbDataAdapter` (optional but useful for classic ADO.NET scenarios)
   - .NET naming and behavior are prioritized over Java-style naming.

2. **Connection/String & Configuration Layer**
   - Parses and normalizes connection strings.
   - Maps provider options to Calcite configuration properties.
   - Handles model URI/content, schema defaults, planner flags, and execution options.

3. **Protocol/Client Layer**
   - Responsible for client communication with the Calcite runtime endpoint.
   - Encodes command execution requests and decodes responses.
   - Isolated behind interfaces so transport can evolve without changing ADO.NET classes.

4. **Type System & Conversion Layer**
   - Maps Calcite SQL types to CLR types.
   - Handles nullability, precision/scale, temporal types, and binary data.
   - Centralizes `DbType`, provider type names, and conversion logic.

5. **Metadata Layer**
   - Exposes schema/table/column metadata in ADO.NET-compatible forms.
   - Supports `GetSchema(...)`, reader field metadata, and related discovery paths.

6. **Diagnostics & Error Layer**
   - Normalizes Calcite/transport failures into .NET exceptions.
   - Adds provider-specific exception types where useful.
   - Integrates optional logging/tracing hooks.

7. **Compatibility Layer**
   - Ensures behavior aligns with JDBC driver capabilities where feature parity matters.
   - Documents intentional deviations where ADO.NET semantics differ.

---

## Primary Components (Suggested Project Structure)

- `Apache.Calcite.AdoNet`
  - Core provider implementation (public ADO.NET API + internal runtime).
- `Apache.Calcite.AdoNet.Protocol` (optional split)
  - Transport/protocol contracts and concrete client implementation.
- `Apache.Calcite.AdoNet.Tests`
  - Unit + integration tests against known model configurations and SQL scenarios.
- `Apache.Calcite.AdoNet.Benchmarks` (optional)
  - Throughput/latency micro-benchmarks for query execution and materialization.

If kept as a single project initially, these can still be separated with folders/namespaces:

- `Connection/`
- `Command/`
- `Reader/`
- `Parameters/`
- `Protocol/`
- `TypeMapping/`
- `Metadata/`
- `Diagnostics/`
- `Internal/`

---

## End-to-End Execution Order

The following describes the typical runtime path from .NET app code to query results.

1. **Provider Registration / Construction**
   - Consumer creates `CalciteConnection` with a connection string.
   - Optional provider factory use via `DbProviderFactory`.

2. **Connection Initialization (`Open`)**
   - Connection string is parsed.
   - Model and Calcite properties are validated/resolved.
   - Protocol client/session is established.
   - Session state is cached in connection instance.

3. **Command Preparation**
   - Consumer creates `CalciteCommand` with SQL text.
   - Parameters are added and validated.
   - Command builds an execution request payload.

4. **Execution**
   - `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery` routes through a shared execution pipeline.
   - Request is sent through protocol layer.
   - Response metadata and row stream are returned.

5. **Reader Materialization**
   - `CalciteDataReader` exposes rows via typed getters (`GetInt32`, `GetString`, etc.).
   - Type mapping layer performs conversions from Calcite types to CLR.
   - Sequential/streaming behavior is honored where possible.

6. **Metadata Access**
   - Field-level metadata available during read.
   - `GetSchema` and related metadata APIs call metadata layer.

7. **Lifecycle Completion**
   - Reader is disposed.
   - Command can be reused or disposed.
   - Connection closed/disposed; session and client resources released.

---

## Build and Delivery Plan

### Phase 1: Skeleton Provider
- Implement `DbConnection`, `DbCommand`, `DbDataReader` minimal happy path.
- Basic connection string parsing.
- Simple SQL execution returning primitive results.

### Phase 2: Core ADO.NET Semantics
- Parameter collection and parameter typing.
- Command behaviors (timeouts, cancel where supported).
- Reader schema table and type-safe getters.

### Phase 3: JDBC Parity Features
- Model file options and property parity with JDBC behavior.
- Metadata APIs and catalog/schema discovery.
- Error-code/exception mapping consistency.

### Phase 4: Hardening
- Integration tests across representative model files.
- Concurrency, disposal, and failure-path validation.
- Performance tuning for row materialization and network round-trips.

### Phase 5: Packaging & Tooling
- Provider factory registration guidance.
- NuGet packaging metadata and versioning policy.
- Usage samples for raw ADO.NET and common .NET data-access patterns.

---

## Contract with .NET Consumers

- Public API follows .NET conventions (PascalCase, `IDisposable` semantics, `Db*` inheritance patterns).
- Behavior should feel native to ADO.NET users.
- JDBC concepts are translated, not copied literally, unless needed for parity.

---

## Relationship to Apache Calcite Source

The reference implementation for behavior and feature parity is the existing Calcite JDBC driver in the Calcite source tree (available locally at `D:\calcite`).

Key parity checkpoints:

- Connection properties and model handling behavior.
- SQL execution lifecycle.
- Metadata surface.
- Type handling and conversion edge cases.

---

## Initial API Shape (Planned)

The provider is expected to expose at least:

- `CalciteConnection`
- `CalciteCommand`
- `CalciteDataReader`
- `CalciteParameter`
- `CalciteProviderFactory`
- `CalciteConnectionStringBuilder`

Additional classes may be introduced as needed to provide complete ADO.NET compatibility.

---

## Development Priorities

1. Correctness and compatibility with Calcite behavior.
2. ADO.NET consistency for .NET consumers.
3. Maintainable architecture with clear layering.
4. Performance and observability.

