# Apache.Calcite.Data — Design

`Apache.Calcite.Data` is an ADO.NET provider for Apache Calcite. It exposes Calcite to .NET
applications through the standard `System.Data.Common` abstractions while running Calcite's
planner and runtime in-process via IKVM.

This document describes how the provider is structured and how a request flows from a .NET
caller into the Calcite engine and back.

---

## Scope

- **Driver/provider**, not an adapter. The provider is the consumer's entry point into Calcite,
  not a way to expose ADO.NET data sources to Calcite.
- **Native in-process engine.** Calcite's Java code is loaded through IKVM and called directly.
  The provider does **not** wrap Calcite's JDBC driver, Avatica, or any remote protocol.
- **JDBC parity at the behavior level.** Connection properties, model handling, SQL execution,
  and metadata semantics aim to match the Calcite JDBC driver, but the public surface is
  idiomatic .NET (`Db*` base classes, PascalCase, `IDisposable`).

---

## Layered Architecture

The implementation is organized as a small set of layers with clear responsibilities. The
public ADO.NET surface only sees the engine through an interface; the engine is the only thing
that touches Calcite's Java types directly.

### 1. ADO.NET Surface

Public, consumer-facing classes that implement the `System.Data.Common` contracts.

| Class | Base | Role |
| --- | --- | --- |
| `CalciteConnection` | `DbConnection` | Owns connection state, opens/closes the engine session, exposes Calcite-native accessors. |
| `CalciteCommand` | `DbCommand` | Holds SQL text and parameters, executes against the connection's session. |
| `CalciteDataReader` | `DbDataReader` | Streams rows produced by an `ICalciteResult`. |
| `CalciteParameter` / `CalciteParameterCollection` | `DbParameter` / `DbParameterCollection` | Provider parameter model. |
| `CalciteTransaction` | `DbTransaction` | Transaction handle (semantics bounded by Calcite). |
| `CalciteProviderFactory` | `DbProviderFactory` | Standard ADO.NET factory registration. |
| `CalciteConnectionStringBuilder` | `DbConnectionStringBuilder` | Strongly typed connection string keys (`Model`, `Schema`, `CaseSensitive`, `Conformance`, …). |
| `CalciteException` | `DbException` | Provider-specific failures, including engine errors. |

`CalciteConnection` also exposes Calcite-native accessors directly so consumers can interact
with the engine without an `Unwrap`-style escape hatch:

- `RootSchema` → `org.apache.calcite.schema.SchemaPlus`
- `TypeFactory` → `org.apache.calcite.adapter.java.JavaTypeFactory`
- `Config` → `org.apache.calcite.config.CalciteConnectionConfig`

These properties throw `InvalidOperationException` if the connection is not open.

### 2. Session Boundary

`CalciteSession` is the per-connection runtime state created when `CalciteConnection.Open()`
succeeds. It is the boundary between the public ADO.NET surface and the internal Calcite
engine.

- Holds the resolved `CalciteConnectionStringBuilder` and the `ICalciteClient` for the open
  connection.
- Owns the engine client lifetime; disposing the session disposes the client.
- Accessed internally through `CalciteConnection.RequireSession()`, which enforces that the
  connection is open.

### 3. Engine Abstraction (`ICalciteClient`)

`ICalciteClient` is the internal contract between the ADO.NET surface and the Calcite engine.
It hides the concrete implementation so the surface layer never references Calcite Java types
directly through internal flows (it only does so when the public connection deliberately
re-exposes them).

The interface exposes:

- `RootSchema`, `TypeFactory`, `Config` — engine state surfaced upward to `CalciteConnection`.
- `ExecuteAsync(CalciteExecuteRequest, CancellationToken)` — executes a prepared SQL request
  and returns an `ICalciteResult` row stream.

### 4. Engine Implementation (`CalciteEngineClient`)

`CalciteEngineClient` is the only component that drives Calcite directly. It takes the place
of `CalciteConnectionImpl`, `CalciteStatement`, and `CalciteResultSet` from the JDBC driver,
but it does so without depending on Avatica or any JDBC plumbing.

Construction:

- Builds the root schema from the connection options via `RootSchemaBuilder`.
- Creates a `CalciteConnectionConfigImpl` from engine properties produced by the same builder.
- Creates a `JavaTypeFactoryImpl`.
- Resolves the default schema path from the `Schema` connection option.

Execution:

1. Validates the request and cancellation token.
2. Constructs a `StatementDataContext` and a `PrepareContext` that mirror what the JDBC driver
   builds internally (root schema, type factory, config, default schema path, cancel flag, and
   command timeout).
3. Calls `CalcitePrepare.DEFAULT_FACTORY` to obtain a prepare instance and invokes
   `prepareSql(...)` while the prepare context is pushed onto Calcite's thread-local stack.
4. Materializes the result `Enumerable`, registers cancellation against the cancel flag, and
   wraps the `Enumerator`, signature, and column mapping in a `CalciteEngineResult`.
5. Surfaces engine errors as `CalciteException`, leaving cancellation/disposal exceptions
   unwrapped.

### 5. Result Stream (`ICalciteResult` / `CalciteEngineResult`)

`ICalciteResult` represents an active row stream. `CalciteEngineResult` adapts Calcite's
`Enumerator`, the prepared signature, and the column metadata into something the ADO.NET
reader can consume. It also owns the cancellation registration produced during execution and
disposes it with the result.

### 6. Type Mapping & Materialization

- `ColumnAdapter` translates the columns described by a `CalcitePrepare.CalciteSignature` into
  `CalciteColumn` descriptors (name, ordinal, Calcite/CLR types).
- `CalciteTypeMap` centralizes the mapping between Calcite SQL types and CLR types, including
  nullability, precision/scale, temporal types, and binary data.
- `RowMaterializer` converts a single Calcite row (typically `Object[]`) into the values
  surfaced by `CalciteDataReader`'s typed getters.

### 7. Parameters

- `CalciteParameter` / `CalciteParameterCollection` implement the standard ADO.NET parameter
  model.
- `CalciteParameterValue` normalizes parameter values for the engine prior to execution.
- `CalciteExecuteRequest` is the payload consumed by `ICalciteClient.ExecuteAsync`, capturing
  the SQL text, parameter values, and command-level options such as the timeout.

### 8. Configuration

- `CalciteConnectionStringBuilder` defines the supported keys and provides typed accessors
  while preserving any unknown keys for the engine.
- `RootSchemaBuilder` (engine-internal) consumes the builder to produce both the
  `CalciteSchema` and the engine properties used to build `CalciteConnectionConfigImpl`.

### 9. Diagnostics & Errors

- `CalciteException` is the provider-specific exception type. The engine wraps non-cancellation
  failures in `CalciteException` so callers see a single, consistent error type from the
  provider.
- Cancellation surfaces as `OperationCanceledException` propagated from the cancel flag.
- `ObjectDisposedException` is thrown if the engine client is used after disposal.

---

## End-to-End Execution Flow

A typical query traverses the layers in the following order:

1. **Construct.** Caller creates a `CalciteConnection` (directly or through
   `CalciteProviderFactory`) with a connection string.
2. **Open.** `CalciteConnection.Open()` parses the connection string into a
   `CalciteConnectionStringBuilder`, asks `CalciteClientFactory` to create an `ICalciteClient`
   (a `CalciteEngineClient` today), and stores both in a new `CalciteSession`.
3. **Build command.** Caller creates a `CalciteCommand`, sets `CommandText`, and adds
   parameters. The command is bound to the connection.
4. **Execute.** `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery` builds a
   `CalciteExecuteRequest` (SQL, parameters, timeout) and calls
   `RequireSession().Client.ExecuteAsync(...)`.
5. **Engine prepare.** `CalciteEngineClient` builds the prepare/data contexts and invokes
   `CalcitePrepare.prepareSql` to obtain a `CalciteSignature`.
6. **Engine execute.** The signature's `Enumerable` is materialized into an `Enumerator`,
   cancellation is wired to the cancel flag, and the enumerator is returned inside a
   `CalciteEngineResult`.
7. **Read.** `CalciteDataReader` wraps the result and exposes rows through typed getters,
   using `ColumnAdapter`/`CalciteTypeMap`/`RowMaterializer` to convert values to CLR types.
8. **Dispose.** Disposing the reader releases the engine result (and its cancellation
   registration). Closing/disposing the connection disposes the session, which disposes the
   engine client.

---

## Direct Engine Access

`CalciteConnection` deliberately exposes selected Calcite-native objects as public properties
instead of providing a JDBC-style `unwrap` API. This keeps the contract typed and discoverable
while still allowing advanced consumers to:

- Register schemas, tables, functions, and views on `RootSchema`.
- Build types using `TypeFactory`.
- Inspect resolved configuration through `Config`.

The provider does not currently expose the prepare/plan APIs publicly; those remain inside
`CalciteEngineClient`.

---

## Project Layout

```
src/
  Apache.Calcite.Data/                ADO.NET provider (this design)
    CalciteConnection.cs              Public DbConnection + native engine accessors
    CalciteCommand.cs                 Public DbCommand
    CalciteDataReader.cs              Public DbDataReader
    CalciteParameter.cs               Public DbParameter
    CalciteParameterCollection.cs     Public DbParameterCollection
    CalciteTransaction.cs             Public DbTransaction
    CalciteProviderFactory.cs         Public DbProviderFactory
    CalciteConnectionStringBuilder.cs Public DbConnectionStringBuilder
    CalciteException.cs               Public provider exception
    CalciteSession.cs                 Internal per-connection state
    ICalciteClient.cs                 Internal engine contract
    CalciteEngineClient.cs            Internal Calcite-backed engine
    CalciteClientFactory.cs           Internal engine factory
    CalciteExecuteRequest.cs          Internal execute payload
    ICalciteResult.cs                 Internal result handle
    CalciteEngineResult.cs            Internal Calcite result adapter
    CalciteColumn.cs                  Internal column descriptor
    ColumnAdapter.cs                  Internal signature → column mapping
    CalciteTypeMap.cs                 Internal SQL ↔ CLR type mapping
    RowMaterializer.cs                Internal row materialization
    StatementDataContext.cs           Internal Calcite DataContext implementation
    PrepareContext.cs                 Internal Calcite Context implementation
    RootSchemaBuilder.cs              Internal schema/config builder
    CalciteParameterValue.cs          Internal parameter normalization
  Apache.Calcite.Data.Tests/          xUnit test suite
```

---

## Design Constraints

- **No JDBC dependency.** The provider must not call into `org.apache.calcite.avatica.*` or
  Calcite's JDBC entry points. It uses `CalcitePrepare`, `CalciteSchema`, `SchemaPlus`,
  `JavaTypeFactory`, and `CalciteConnectionConfig` directly.
- **Layer isolation.** The ADO.NET surface only talks to the engine through `ICalciteClient`
  (plus the deliberate re-exposure of three engine objects on `CalciteConnection`). The
  engine implementation never reaches back into the public surface.
- **Idiomatic .NET.** Public types follow .NET naming and `IDisposable` conventions. JDBC
  concepts are translated, not copied verbatim.
- **Targeting.** The provider targets .NET 8 (C# 12).
