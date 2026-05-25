# Apache Calcite for .NET

This repository provides .NET projects that expose Apache Calcite capabilities through ADO.NET and IKVM.

## Projects

### `Apache.Calcite.Data` (`src/Apache.Calcite.Data`)

Primary ADO.NET provider package for Apache Calcite. It exposes `DbConnection`, `DbCommand`, `DbDataReader`,
`DbBatch`, connection-string configuration, and provider-factory support so .NET applications can query Calcite.

NuGet package: `Apache.Calcite.Data`

### `Apache.Calcite.Adapter.AdoNet` (`src/Apache.Calcite.Adapter.AdoNet`)

Calcite adapter package that maps external ADO.NET data sources into Calcite schemas and relational operators.
This enables query pushdown and federated SQL planning across connected data sources.

NuGet package: `Apache.Calcite.Adapter.AdoNet`

### `Apache.Calcite.Extensions` (`src/Apache.Calcite.Extensions`)

Supporting extension package that provides .NET-friendly wrappers and helpers for Calcite/IKVM interop (for example,
connection property helpers and related utility types used by the provider and adapter projects).

NuGet package: `Apache.Calcite.Extensions`

### Test projects

- `Apache.Calcite.Tests`
- `Apache.Calcite.Data.Tests`
- `Apache.Calcite.Adapter.AdoNet.Tests`

### Distribution projects

- `dist-nuget`: packages the NuGet artifacts
- `dist-tests`: packages test artifacts for CI
