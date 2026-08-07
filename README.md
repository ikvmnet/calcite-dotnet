# Apache Calcite for .NET

This repository provides .NET projects that expose [Apache Calcite](https://calcite.apache.org/) — the SQL parser, optimizer, and execution framework — through standard ADO.NET abstractions, powered by [IKVM](https://github.com/ikvmnet/ikvm).

## Packages

### [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) · `src/Apache.Calcite.Data`

The core ADO.NET provider. Exposes `CalciteConnection`, `CalciteCommand`, `CalciteDataReader`, `CalciteBatch`, `CalciteDataSource`, `CalciteProviderFactory`, and `CalciteConnectionStringBuilder` so any .NET code that speaks `DbConnection` / `DbCommand` / `DbDataReader` can execute Calcite SQL.

The Calcite engine runs fully in-process via IKVM — no JDBC driver, no Avatica server, no extra process.

```sh
dotnet add package Apache.Calcite.Data
```

### [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) · `src/Apache.Calcite.Adapter.AdoNet`

Federated query adapter that bridges Calcite to external ADO.NET data sources. Exposes any database reachable via `DbProviderFactory` or `DbDataSource` as a Calcite schema, with pushdown of filters, projections, joins, aggregations, sorts, and set operations.

Ships built-in metadata for SQL Server, SQLite, ODBC, OLE DB, and any `INFORMATION_SCHEMA`-compliant database.

```sh
dotnet add package Apache.Calcite.Adapter.AdoNet
```

### [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) · `src/Apache.Calcite.Extensions`

`ClrEnumerableConvention` — a calling convention that runs a query plan as a compiled `System.Linq.Expressions` tree instead of generating Java source and compiling it with Janino. It mirrors Calcite's own `EnumerableConvention` node for node and uses the same row types, and converters exist in both directions, so a plan can mix the two.

With it, the prepare pipeline that takes a statement from SQL text to such a plan, and the interop helpers both need — including `CalciteConnectionProperties`, a strongly-typed wrapper over Calcite's `java.util.Properties`, so you can configure the engine with compile-time-safe .NET properties instead of raw string keys.

Because the plan holds a method rather than its name, a user-defined function written in .NET runs in this convention — Janino cannot resolve the `cli.`-prefixed class name IKVM gives a CLR type, so such a query has no plan under `EnumerableConvention`.

Targets .NET 8, and is verified on .NET 8 and .NET 10.

```sh
dotnet add package Apache.Calcite.Extensions
```

## Test and distribution projects

| Project | Purpose |
|---------|---------|
| `Apache.Calcite.Tests` | Core engine integration tests, and the convention and prepare pipeline tests — including the differential suites that run the same SQL through `ClrEnumerableConvention` and `EnumerableConvention` and require the same rows |
| `Apache.Calcite.Data.Tests` | Provider integration tests |
| `Apache.Calcite.Adapter.AdoNet.Tests` | Adapter integration tests |
| `dist-nuget` | Packages NuGet artifacts |
| `dist-tests` | Packages test artifacts for CI |

## Quick example

```csharp
using Apache.Calcite.Data;

const string model = """
{
  "version": "1.0",
  "defaultSchema": "HR",
  "schemas": [{
    "name": "HR",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.csv.CsvSchemaFactory",
    "operand": { "directory": "hr" }
  }]
}
""";

await using var conn = new CalciteConnection($"Model=inline:{model}");
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT \"NAME\", \"DEPTNO\" FROM \"EMPS\" ORDER BY \"NAME\"";

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
```

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters](https://calcite.apache.org/docs/adapter.html)
- [JSON model reference](https://calcite.apache.org/docs/model.html)
- [IKVM](https://github.com/ikvmnet/ikvm)

## License

Apache License 2.0.
