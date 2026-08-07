# Apache.Calcite.Adapter.AdoNet

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Adapter.AdoNet)](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet)

**Apache.Calcite.Adapter.AdoNet** lets [Apache Calcite](https://calcite.apache.org/) treat any ADO.NET data source as a first-class relational schema. Calcite can then plan and execute federated SQL queries across those sources — pushing filters, projections, joins, aggregations, sorts, and set operations down to the underlying database wherever possible.

Use this package together with [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) to federate SQL Server, SQLite, and any other ADO.NET-capable database under a single Calcite connection.

## How it works

1. You describe the remote database with an `AdoDataSource` — a `DbProviderFactory` plus a connection string, or a .NET 7+ `DbDataSource`.
2. You register it with Calcite as a schema, either programmatically or through a JSON model.
3. Calcite's planner produces a query plan, pushing as much SQL as possible back to the source in the right dialect.
4. Whatever cannot be pushed down runs in-process, and the results arrive through the standard `DbDataReader`.

## Install

```sh
dotnet add package Apache.Calcite.Adapter.AdoNet
dotnet add package Apache.Calcite.Data
```

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

## Quick start — code-driven registration

`AdoSchema.Create` builds the schema; `SchemaPlus.add` puts it on the connection under a name:

```csharp
using Apache.Calcite.Adapter.AdoNet;
using Apache.Calcite.Data;
using Microsoft.Data.Sqlite;

// 1. Any DbDataSource will do. The matching metadata provider is chosen for you.
var dataSource = SqliteFactory.Instance.CreateDataSource("Data Source=sales.db");

// 2. Open a Calcite connection and attach the schema.
await using var conn = new CalciteConnection("Lex=JAVA;CaseSensitive=false");
await conn.OpenAsync();

var root = conn.RootSchema;
root.add("ADO", AdoSchema.Create(root, "ADO", dataSource, null, null));

// 3. Query it.
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT NAME FROM ADO.EMPS WHERE SALARY > ? ORDER BY NAME";
cmd.Parameters.Add(new CalciteParameter("salary", 100.0));

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine(reader.GetString(0));
```

The last two arguments to `AdoSchema.Create` are the database and schema name to restrict discovery to; pass `null` for either to take the source's default. Overloads accept an `AdoDataSource`, an `AdoDatabaseMetadata`, or an `AdoDatabaseMetadataFactory` where you need to choose the metadata provider yourself.

## Quick start — JSON model

The adapter can also be named from a [Calcite model](https://calcite.apache.org/docs/model.html). **Two things are required and neither is obvious:**

- The `factory` value must be the IKVM name of the CLR class — `cli.` followed by its .NET full name. Calcite resolves a model's factory through Java's `Class.forName`, which does not know a bare .NET type name.
- The assembly must be on IKVM's boot class path *before* the connection is opened, because `Class.forName` cannot see a type that is only in a referenced assembly.

```csharp
// Once, at startup — before opening any connection that uses the model.
ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
DbProviderFactories.RegisterFactory("Microsoft.Data.Sqlite", SqliteFactory.Instance);
```

```json
{
  "version": "1.0",
  "defaultSchema": "ADO",
  "schemas": [
    {
      "name": "ADO",
      "type": "custom",
      "factory": "cli.Apache.Calcite.Adapter.AdoNet.AdoSchemaFactory",
      "operand": {
        "adoProviderName": "Microsoft.Data.Sqlite",
        "adoConnectionString": "Data Source=sales.db"
      }
    }
  ]
}
```

```csharp
await using var conn = new CalciteConnection($"Model=inline:{model};Lex=JAVA;CaseSensitive=false");
await conn.OpenAsync();
```

### Operand reference

| Operand | Required | Meaning |
|---------|----------|---------|
| `adoProviderName` | yes, unless `adoDataSource` is given | Invariant name registered with `DbProviderFactories`. |
| `adoConnectionString` | yes, with `adoProviderName` | Connection string handed to that factory. |
| `adoDataSource` | — | Assembly-qualified .NET type name of a `DbDataSource` with a parameterless constructor. Used instead of the two above. |
| `adoDatabaseMetadata` | — | Assembly-qualified .NET type name of an `AdoDatabaseMetadata` to use instead of the detected one. |
| `adoDatabaseMetadataFactory` | — | Assembly-qualified .NET type name of an `AdoDatabaseMetadataFactory`. |
| `adoDatabase` | — | Restrict discovery to one database. |
| `adoSchema` | — | Restrict discovery to one schema. |

A missing `adoProviderName` or `adoConnectionString` throws `AdoCalciteException`, and so does a type name that cannot be loaded.

## Federated query across two databases

Federating across unrelated databases is the point of the adapter:

```csharp
var root = conn.RootSchema;
root.add("SQL",    AdoSchema.Create(root, "SQL",    sqlServerDataSource, null, null));
root.add("SQLITE", AdoSchema.Create(root, "SQLITE", sqliteDataSource,    null, null));

await using var cmd = conn.CreateCommand();
cmd.CommandText = """
    SELECT s.CustomerId, s.Name, COUNT(o.OrderId) AS Orders
    FROM   SQL.Customers  s
    JOIN   SQLITE.Orders  o ON o.CustomerId = s.CustomerId
    GROUP BY s.CustomerId, s.Name
    """;
```

Each side is pushed to its own database as far as it can go, and the join runs in-process.

## Provider support

`AdoDatabaseMetadataFactoryImpl` — the default, used when you do not name one — inspects the connection the data source produces and selects a metadata provider:

| Connection type | Discovery |
|---|---|
| `Microsoft.Data.SqlClient.SqlConnection`, `System.Data.SqlClient.SqlConnection` | SQL Server, via `INFORMATION_SCHEMA` |
| `Microsoft.Data.Sqlite.SqliteConnection` | SQLite |
| `System.Data.Odbc.OdbcConnection` | Generic ODBC, via `INFORMATION_SCHEMA` |
| `System.Data.OleDb.OleDbConnection` | Generic OLE DB, via `INFORMATION_SCHEMA` |

Anything else throws `AdoCalciteException` naming the connection type. To support it, derive from `AdoDatabaseMetadata` — the abstract base that supplies the `SqlDialect`, table and column enumeration, and type mapping — and pass your implementation to an `AdoSchema.Create` overload, or name it in the `adoDatabaseMetadata` operand. The built-in implementations are internal; `AdoDatabaseMetadata` and `AdoDatabaseMetadataFactory` are the extension points.

## Key public types

| Type | Purpose |
|------|---------|
| `AdoSchema` | The Calcite `Schema` that enumerates tables from a data source. `AdoSchema.Create(...)` is how you build one. |
| `AdoSchemaFactory` | The `SchemaFactory` a JSON model names. |
| `AdoDataSource` | Abstract base — implement to connect Calcite to any ADO.NET source. |
| `DbProviderAdoDataSource` | `AdoDataSource` over a `DbProviderFactory`, a connection string, and an `AdoDatabaseMetadata`. |
| `DbDataSourceAdoDataSource` | `AdoDataSource` over a .NET 7+ `DbDataSource` and an `AdoDatabaseMetadata`. |
| `AdoDatabaseMetadata` | Abstract base for schema, column, and dialect discovery. |
| `AdoDatabaseMetadataFactory` | Chooses the metadata provider for a data source. `AdoDatabaseMetadataFactoryImpl.Instance` is the default. |
| `AdoDatabaseSchema` | A single database-level schema within the adapter. |
| `AdoConvention` | The calling convention a pushed-down subtree is planned into. |
| `AdoRules` | The adapter's conversion rules. |
| `DbCommandEnricher` | Hook for adjusting each `DbCommand` before it runs. |
| `AdoCalciteException` | What the adapter throws. |

## Pushdown support

The adapter provides Calcite conversion rules for these operators, which become SQL against the source when the dialect supports them:

- `AdoFilter` — `WHERE` predicates
- `AdoProject` — column projections
- `AdoJoin` — inner and outer joins
- `AdoAggregate` — `GROUP BY` and aggregate functions
- `AdoSort` — `ORDER BY`, `LIMIT`, `OFFSET`
- `AdoUnion` / `AdoIntersect` / `AdoMinus` — set operations
- `AdoValues` — constant value sets
- `AdoTableScan` — the scan itself

Anything that cannot be pushed down runs in-process. Converters exist into both execution conventions — `AdoToClrEnumerableConverter` for the compiled-.NET convention that `Apache.Calcite.Data` plans into, and `AdoToEnumerableConverter` for Calcite's own — so the adapter works under either.

Correlated sub-queries are supported: `AdoCorrelationDataContext` carries the outer row's values into the inner query.

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | The ADO.NET provider — required to open connections and execute SQL. |
| [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) | The calling convention plans are compiled into, the prepare pipeline behind it, and the IKVM interop helpers. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters overview](https://calcite.apache.org/docs/adapter.html)
- [Calcite model JSON reference](https://calcite.apache.org/docs/model.html)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
