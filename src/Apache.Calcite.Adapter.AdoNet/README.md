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

| Connection type | Discovery | Dialect |
|---|---|---|
| `Microsoft.Data.SqlClient.SqlConnection`, `System.Data.SqlClient.SqlConnection` | `INFORMATION_SCHEMA`, via `GetSchema` | SQL Server, at the version the server reports |
| `Microsoft.Data.Sqlite.SqliteConnection` | `PRAGMA table_xinfo` | SQLite |
| `System.Data.Odbc.OdbcConnection` | The ODBC catalog — `SQLTables` and `SQLColumns`, via `GetSchema` | Whatever the driver names as the product behind it |
| `System.Data.OleDb.OleDbConnection` | The OLE DB schema rowsets, via `GetSchema` | Whatever the provider names as the product behind it |

Anything else throws `AdoCalciteException` naming the connection type. To support it, derive from `AdoDatabaseMetadata` — the abstract base that supplies the `SqlDialect`, table and column enumeration, and type mapping — and pass your implementation to an `AdoSchema.Create` overload, or name it in the `adoDatabaseMetadata` operand. The built-in implementations are internal; `AdoDatabaseMetadata` and `AdoDatabaseMetadataFactory` are the extension points.

### ODBC and OLE DB

Both front an unknown database, so both take the product name from the driver's `DataSourceInformation` collection and match it the way Calcite's own `SqlDialectFactoryImpl` does. An unrecognised name gets the generic ANSI dialect, which is Calcite's answer too. Neither has a default schema — a null schema means every schema rather than a particular one — so pass `adoSchema` (or the `schemaName` argument) where the database has more than one and the table names collide.

The parameter marker for both is `?`, bound by position.

Two limitations worth knowing before choosing one of these over a native provider:

- A dialect matched from a product name alone is, in Calcite's words, an approximation. The version is carried where it changes the SQL — SQL Server below 2012 gets `TOP (n)` rather than `OFFSET`/`FETCH` — but a driver that will not report its product gets generic SQL. Name a metadata provider through `adoDatabaseMetadata` where that is not good enough.
- `System.Data.Odbc` has no mapping for SQL Server's `time` or `datetimeoffset` and throws `ArgumentException` on reading either. The columns are still discovered and typed; only reading one fails. That is the driver, not the adapter.
- `System.Data.OleDb` cannot bind a `DateTimeOffset` parameter at all — the Variant marshal refuses it on the client — and binds a `TimeSpan` through OLE DB's `DBTIME`, which has no fractional seconds, so a bound time reaches the server truncated to the whole second. Reading both types works; only a correlated comparison on one is affected.

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
