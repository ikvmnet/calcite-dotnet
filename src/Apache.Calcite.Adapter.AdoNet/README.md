# Apache.Calcite.Adapter.AdoNet

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Adapter.AdoNet)](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet)

**Apache.Calcite.Adapter.AdoNet** lets [Apache Calcite](https://calcite.apache.org/) treat any ADO.NET data source as a first-class relational schema. Calcite can then plan and execute federated SQL queries across those sources — pushing filters, projections, joins, aggregations, and sorts down to the underlying database wherever possible.

Use this package together with [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) to federate SQL Server, SQLite, PostgreSQL, MySQL, and any other ADO.NET-capable database under a single Calcite connection.

## How it works

1. You create an `AdoDataSource` (or use one of the built-in subclasses) that describes how to open connections and how to discover the remote schema.
2. You register it with Calcite — either via a JSON model or programmatically via `SchemaPlus`.
3. Calcite's planner generates an optimal query plan, pushing as much SQL as possible back to the source using the correct SQL dialect.
4. Results are merged in-process and delivered through the standard `DbDataReader` API.

## Install

```sh
dotnet add package Apache.Calcite.Adapter.AdoNet
dotnet add package Apache.Calcite.Data
```

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

## Quick start — JSON model

Wire up a SQL Server database as a Calcite schema using the JSON model:

```json
{
  "version": "1.0",
  "defaultSchema": "SQLSERVER",
  "schemas": [
    {
      "name": "SQLSERVER",
      "type": "custom",
      "factory": "Apache.Calcite.Adapter.AdoNet.AdoSchemaFactory",
      "operand": {
        "providerName":      "Microsoft.Data.SqlClient",
        "connectionString":  "Server=localhost;Database=AdventureWorks;Integrated Security=true",
        "metadataType":      "SqlServer"
      }
    }
  ]
}
```

Then query it normally:

```csharp
using Apache.Calcite.Data;

await using var conn = new CalciteConnection("Model=path/to/model.json");
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = """
    SELECT p."ProductID", p."Name", SUM(d."OrderQty") AS "TotalQty"
    FROM "SQLSERVER"."SalesOrderDetail" d
    JOIN "SQLSERVER"."Product"          p ON p."ProductID" = d."ProductID"
    GROUP BY p."ProductID", p."Name"
    ORDER BY "TotalQty" DESC
    """;

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetInt32(0)}\t{reader.GetString(1)}\t{reader.GetInt32(2)}");
```

## Quick start — code-driven registration

Register an ADO.NET data source directly on the Calcite root schema without a JSON model:

```csharp
using System.Data.Common;
using Apache.Calcite.Adapter.AdoNet;
using Apache.Calcite.Adapter.AdoNet.Metadata;
using Apache.Calcite.Data;
using Microsoft.Data.SqlClient;

// 1. Describe the data source.
var factory    = SqlClientFactory.Instance;
var connString = "Server=localhost;Database=AdventureWorks;Integrated Security=true";
var metadata   = new SqlServerDatabaseMetadata(
    new SqlClientDataSource(connString));   // DbDataSource from Microsoft.Data.SqlClient

var dataSource = new DbProviderAdoDataSource(factory, connString, metadata);

// 2. Open a Calcite connection and attach the schema.
await using var conn = new CalciteConnection();
await conn.OpenAsync();

AdoSchema.Register(conn.RootSchema, "AW", dataSource);

// 3. Query across schemas.
await using var cmd = conn.CreateCommand();
cmd.CommandText = """
    SELECT "Name" FROM "AW"."Product" WHERE "ListPrice" > ? ORDER BY "Name"
    """;
cmd.Parameters.Add(new CalciteParameter("price", 500m));

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine(reader.GetString(0));
```

## Federated query across two databases

One of the key strengths of Calcite is federating queries across completely different databases:

```csharp
// Attach two data sources under different schema names.
AdoSchema.Register(conn.RootSchema, "SQL",    sqlServerDataSource);
AdoSchema.Register(conn.RootSchema, "SQLITE", sqliteDataSource);

// Join them in a single SQL statement — Calcite handles the cross-source planning.
await using var cmd = conn.CreateCommand();
cmd.CommandText = """
    SELECT s."CustomerId", s."Name", COUNT(o."OrderId") AS "Orders"
    FROM   "SQL"."Customers"  s
    JOIN   "SQLITE"."Orders"  o ON o."CustomerId" = s."CustomerId"
    GROUP BY s."CustomerId", s."Name"
    """;
```

## Built-in metadata implementations

The adapter ships provider-specific `AdoDatabaseMetadata` subclasses that configure the correct SQL dialect, quoting style, and schema-discovery queries for each database:

| Class | Database |
|-------|----------|
| `SqlServerDatabaseMetadata` | Microsoft SQL Server |
| `SqliteDatabaseMetadata` | SQLite |
| `OdbcDatabaseMetadata` | Generic ODBC sources |
| `OleDbDatabaseMetadata` | Generic OLE DB sources |
| `AdoInformationSchemaDatabaseMetadata` | Any database exposing an `INFORMATION_SCHEMA` |

For databases not listed above, extend `AdoDatabaseMetadata` to supply the correct `SqlDialect`, table enumeration, and column-type mappings.

## Key types

| Type | Purpose |
|------|---------|
| `AdoDataSource` | Abstract base — implement to connect Calcite to any ADO.NET source. |
| `DbProviderAdoDataSource` | `AdoDataSource` backed by a `DbProviderFactory` and a connection string. |
| `DbDataSourceAdoDataSource` | `AdoDataSource` backed by a .NET 7+ `DbDataSource`. |
| `AdoSchema` | The Calcite `Schema` implementation that enumerates tables from a data source. |
| `AdoDatabaseSchema` | Represents a single database-level schema within the adapter. |
| `AdoSchemaFactory` | `SchemaFactory` used to instantiate `AdoSchema` from a Calcite model JSON. |
| `AdoDatabaseMetadata` | Abstract base for schema/column/dialect discovery. |

## Pushdown support

The adapter includes Calcite conversion rules for the following relational operators, which are pushed to the source as SQL when the dialect supports them:

- `AdoFilter` — `WHERE` predicates
- `AdoProject` — column projections
- `AdoJoin` — inner and outer joins
- `AdoAggregate` — `GROUP BY` / aggregate functions
- `AdoSort` — `ORDER BY` / `LIMIT` / `OFFSET`
- `AdoUnion` / `AdoIntersect` / `AdoMinus` — set operations
- `AdoValues` — constant value sets

Any operator that cannot be pushed down is executed in-process by Calcite's enumerable runtime.

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | The core ADO.NET provider — required to open connections and execute SQL. |
| [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) | .NET helper types for Calcite connection properties and IKVM interop. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters overview](https://calcite.apache.org/docs/adapter.html)
- [Calcite model JSON reference](https://calcite.apache.org/docs/model.html)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
