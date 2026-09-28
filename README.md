# Apache Calcite for .NET

[Apache Calcite](https://calcite.apache.org/) — the SQL parser, optimizer and query engine — for .NET, running
in-process under [IKVM](https://github.com/ikvmnet/ikvm) and exposed through ADO.NET. There is no JDBC driver,
no Avatica server and no separate process: a `DbCommand` goes to Calcite's planner and the rows come back
through a `DbDataReader`. Queries are compiled to .NET code rather than to Java.

All packages target .NET 8 and are tested on .NET 8 and .NET 10.

## Packages

| Package | What it is |
|---|---|
| [`Apache.Calcite.Data`](src/Apache.Calcite.Data/README.md) | The ADO.NET provider: `CalciteConnection`, `CalciteCommand`, `CalciteDataReader`, `CalciteDataSource`, `CalciteProviderFactory` and the rest. Start here. |
| [`Apache.Calcite.Adapter.AdoNet`](src/Apache.Calcite.Adapter.AdoNet/README.md) | Exposes any ADO.NET database as a Calcite schema, pushing filters, projections, joins, aggregations, sorts and set operations down to it. Built-in support for SQL Server, SQLite, ODBC, OLE DB and `INFORMATION_SCHEMA` databases. |
| [`Apache.Calcite.Extensions`](src/Apache.Calcite.Extensions/README.md) | The query engine the provider runs on: a Calcite calling convention that compiles a plan to `System.Linq.Expressions`, the pipeline that prepares a statement for it, and typed Calcite connection properties. Reference it directly to drive Calcite's planner without ADO.NET. |
| [`Apache.Calcite.Data.Common`](src/Apache.Calcite.Data.Common/README.md) | The mapping between Calcite SQL types and .NET types, shared by the provider and the adapter, and extensible with your own resolvers. |
| [`Apache.Calcite.Geography`](src/Apache.Calcite.Geography/README.md) | Optional. `CLR_ST_GEOG_*` functions that treat coordinates as WGS84 and answer geodesically, in metres, using Google's S2 and GeographicLib. |
| [`Apache.Calcite.FullText`](src/Apache.Calcite.FullText/README.md) | Optional. `CLR_FT_*` full-text search functions for adapters to push down to a store that supports them. It declares the functions; it does not evaluate them. |

```sh
dotnet add package Apache.Calcite.Data
```

## Example

Query a SQLite database through Calcite:

```csharp
using Apache.Calcite.Adapter.AdoNet;
using Apache.Calcite.Data;
using Microsoft.Data.Sqlite;

var sqlite = SqliteFactory.Instance.CreateDataSource("Data Source=sales.db");

await using var dataSource = new CalciteDataSourceBuilder()
    .ConfigureRootSchema(root => root.add("SALES", AdoSchema.Create(root, "SALES", sqlite, null, null)))
    .Build();

await using var conn = await dataSource.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT \"NAME\", \"DEPTNO\" FROM \"SALES\".\"EMPS\" ORDER BY \"NAME\"";

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
```

Each package's README covers configuration, JSON models, code-driven schemas and user-defined functions.

## Building from source

```sh
dotnet build Apache.Calcite.slnx
```

The build resolves Calcite from Maven through `IKVM.Maven.Sdk`. The IKVM packages come from the organization's
GitHub Packages feed named in `nuget.config`, which requires a GitHub token to read; see the comment in that file.

| Test project | Covers |
|---|---|
| `Apache.Calcite.Tests` | The calling convention and prepare pipeline, including differential tests that run each query through both this convention and Calcite's own and require the same rows |
| `Apache.Calcite.Data.Tests` | The ADO.NET provider |
| `Apache.Calcite.Adapter.AdoNet.Tests` | The ADO.NET adapter |
| `Apache.Calcite.Geography.Tests` | The geography functions |
| `Apache.Calcite.FullText.Tests` | The full-text function declarations |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters](https://calcite.apache.org/docs/adapter.html)
- [JSON model reference](https://calcite.apache.org/docs/model.html)
- [IKVM](https://github.com/ikvmnet/ikvm)

## License

Apache License 2.0. This project is not an Apache Software Foundation project.
