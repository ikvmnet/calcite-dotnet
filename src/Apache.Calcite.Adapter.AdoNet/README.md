# Apache.Calcite.Adapter.AdoNet

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Adapter.AdoNet)](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet)

**Apache.Calcite.Adapter.AdoNet** exposes a database reached through ADO.NET as an [Apache Calcite](https://calcite.apache.org/) schema. Calcite can then query its tables alongside any other schema, and sends as much of each query as it can to the database as a single SQL statement in that database's dialect: filters, projections, joins, aggregations, sorts and set operations. It is the ADO.NET counterpart of Calcite's JDBC adapter.

Use it with [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data), the ADO.NET provider for Calcite, to query SQL Server, SQLite, and ODBC or OLE DB sources through one connection, including joins between them.

## Install

```sh
dotnet add package Apache.Calcite.Adapter.AdoNet
dotnet add package Apache.Calcite.Data
```

The package targets .NET 8.

## Registering a schema in code

`AdoSchema.Create` builds the schema. It needs the parent schema it will be added to, so register it through `CalciteDataSourceBuilder.ConfigureRootSchema`, which hands you the root as a `SchemaPlus`:

```csharp
using Apache.Calcite.Adapter.AdoNet;
using Apache.Calcite.Data;
using Microsoft.Data.Sqlite;

// Any DbDataSource will do; the metadata provider is chosen from its connection type.
var sqlite = SqliteFactory.Instance.CreateDataSource("Data Source=sales.db");

await using var calcite = new CalciteDataSourceBuilder("Lex=JAVA;CaseSensitive=false")
    .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", sqlite, null, null)))
    .Build();

await using var conn = await calcite.OpenConnectionAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT NAME FROM ADO.EMPS WHERE SALARY > ? ORDER BY NAME";
cmd.Parameters.Add(new CalciteParameter("salary", 100.0));

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine(reader.GetString(0));
```

The name passed to `AdoSchema.Create` must be the name the schema is added under. The last two arguments restrict the schema to one database and one schema of the source; pass `null` for either to take the provider's default (see [Supported providers](#supported-providers)). Other overloads take an `AdoDatabaseMetadataFactory`, an `AdoDatabaseMetadata`, or an `AdoDataSource` where you want to choose the metadata provider or supply connections yourself.

`AdoSchema.Create` reads the database's dialect as it runs, which for SQL Server, ODBC and OLE DB opens a connection.

## Registering a schema in a model

A [Calcite JSON model](https://calcite.apache.org/docs/model.html) can create the schema through `AdoSchemaFactory`. Name the factory by its assembly-qualified .NET type name:

```json
{
  "version": "1.0",
  "defaultSchema": "ADO",
  "schemas": [
    {
      "name": "ADO",
      "type": "custom",
      "factory": "Apache.Calcite.Adapter.AdoNet.AdoSchemaFactory, Apache.Calcite.Adapter.AdoNet",
      "operand": {
        "adoProviderName": "Microsoft.Data.Sqlite",
        "adoConnectionString": "Data Source=sales.db"
      }
    }
  ]
}
```

Calcite loads a class a model names only if the `calcite.model.classes.allowed` system property allows it, and it reads the property once, the first time any Calcite class is used. Set it, and register the ADO.NET provider the operands name, at startup before anything touches Calcite:

```csharp
// A comma-separated list; an entry ending in "." allows every class in that namespace.
java.lang.System.setProperty("calcite.model.classes.allowed", "Apache.Calcite.Adapter.AdoNet.");

DbProviderFactories.RegisterFactory("Microsoft.Data.Sqlite", SqliteFactory.Instance);

await using var conn = new CalciteConnection($"Model=inline:{model};Lex=JAVA;CaseSensitive=false");
await conn.OpenAsync();
```

`Model` also accepts the path of a model file.

### Operands

Every operand is a string.

| Operand | Meaning |
|---------|---------|
| `adoProviderName` | The invariant name of a provider registered with `DbProviderFactories`. Required unless `adoDataSource` is given. |
| `adoConnectionString` | The connection string to give that provider. Required with `adoProviderName`. |
| `adoDataSource` | The assembly-qualified name of a `DbDataSource` type with a public parameterless constructor. Used instead of the two above. |
| `adoDatabaseMetadata` | The assembly-qualified name of an `AdoDatabaseMetadata` type with a public constructor taking a `DbDataSource`. Used instead of the provider `AdoDatabaseMetadataFactoryImpl` would choose. |
| `adoDatabase` | The database whose tables to expose. Omit it for the provider's default. |
| `adoSchema` | The schema whose tables to expose. Omit it for the provider's default. |

A missing `adoProviderName` or `adoConnectionString`, or a type name that cannot be loaded, throws `AdoCalciteException`. An `adoDatabaseMetadataFactory` operand is currently ignored.

## Joining two databases

Each schema is its own source. A query can join them, and each side is sent to its own database as far as it can go; the join itself runs in process.

```csharp
var sqlServer = SqlClientFactory.Instance.CreateDataSource(sqlServerConnectionString);
var sqlite = SqliteFactory.Instance.CreateDataSource("Data Source=orders.db");

await using var calcite = new CalciteDataSourceBuilder("Lex=JAVA;CaseSensitive=false")
    .ConfigureRootSchema(root =>
    {
        root.add("CRM", AdoSchema.Create(root, "CRM", sqlServer, null, null));
        root.add("SHOP", AdoSchema.Create(root, "SHOP", sqlite, null, null));
    })
    .Build();

await using var conn = await calcite.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = """
    SELECT c.CustomerId, c.Name, COUNT(o.OrderId) AS Orders
    FROM   CRM.Customers c
    JOIN   SHOP.Orders   o ON o.CustomerId = c.CustomerId
    GROUP BY c.CustomerId, c.Name
    """;
```

## Supported providers

`AdoDatabaseMetadataFactoryImpl`, the default, chooses the metadata provider from the type of connection the data source creates. The metadata lists the tables and their columns, maps column types to Calcite types, and supplies the dialect SQL is written in.

| Connection type | Tables and columns from | Dialect | Default database / schema |
|---|---|---|---|
| `Microsoft.Data.SqlClient.SqlConnection`, `System.Data.SqlClient.SqlConnection` | The `Tables` and `Columns` schema collections | SQL Server, for the version the server reports | `Initial Catalog` or `Database` from the connection string, else the connection's / `dbo` |
| `Microsoft.Data.Sqlite.SqliteConnection` | `sqlite_master` and `PRAGMA table_xinfo` | SQLite | none; SQLite has neither |
| `System.Data.Odbc.OdbcConnection` | The ODBC catalog (`SQLTables`, `SQLColumns`) | Chosen from the product name the driver reports | The connection's catalog / every schema |
| `System.Data.OleDb.OleDbConnection` | The OLE DB schema rowsets | Chosen from the product name the provider reports | The connection's catalog / every schema |

Any other connection type throws `AdoCalciteException`. To support one, derive from `AdoDatabaseMetadata` and pass an instance to `AdoSchema.Create`, or name its type in the `adoDatabaseMetadata` operand. The built-in implementations are internal.

A column whose type the metadata does not recognise is typed `OTHER`, and its values are passed through as the provider returns them.

### ODBC and OLE DB

Neither says what database is behind it except through the product name in its `DataSourceInformation` schema collection. The adapter matches that name the way Calcite's `SqlDialectFactoryImpl` does, and uses the ANSI dialect for a name it does not recognise or a driver that reports none. SQL Server's dialect is built for the reported version, so a server older than SQL Server 2012 gets `TOP (n)` rather than `OFFSET`/`FETCH`. Where the chosen dialect is not good enough, supply your own `AdoDatabaseMetadata`.

With no default schema, a schema of `null` exposes the tables of every schema. Name the schema (the `schemaName` argument or `adoSchema` operand) where table names repeat across schemas.

Parameters are written `?` and bound by position.

Some limitations come from the drivers rather than the adapter:

- `System.Data.Odbc` cannot read SQL Server's `time` or `datetimeoffset` columns and throws `ArgumentException`. The columns are still listed with their types.
- `System.Data.OleDb` cannot bind a `DateTimeOffset` parameter, and binds a `TimeSpan` with no fractional seconds. This matters only where such a value is sent as a parameter, as in a correlated sub-query that compares one of these columns.

## What is pushed down

A part of a query is sent to the database when every node in it belongs to that database's schema and can be written in its dialect:

| Operation | Pushed down unless |
|---|---|
| Table scan | |
| Filter (`WHERE`, `HAVING`) | the condition calls a user-defined function |
| Projection | it calls a user-defined function, uses a window function the dialect does not support, or reads a correlation variable |
| Join | it is a semi- or anti-join, or its condition uses anything but column references, literals, parameters, `AND`, `OR`, comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS NOT DISTINCT FROM` and `CAST` |
| Aggregate (`GROUP BY`) | it has several grouping sets (`GROUPING SETS`, `ROLLUP`, `CUBE`), or uses an aggregate function or `FILTER` clause the dialect does not support |
| Sort, offset and fetch | |
| `UNION`, `UNION ALL` | |
| `INTERSECT`, `EXCEPT` | it is `INTERSECT ALL` or `EXCEPT ALL` |
| `VALUES` | |

Everything else runs in process over the rows the database returns. Query parameters (`?`) in a pushed-down part are sent to the database as command parameters. Where a correlated sub-query stays correlated after planning and its inner side is pushed down, the outer row's values are sent as command parameters too, and the inner statement runs once per outer row.

For SQL Server, the adapter's dialect also corrects some of what Calcite writes: an unbounded `VARCHAR` or `VARBINARY` in a `CAST` becomes `VARCHAR(MAX)` or `VARBINARY(MAX)`, a `UUID` becomes `UNIQUEIDENTIFIER` (and a `UUID` literal a cast of its text), `MOD` is parenthesised correctly as `%`, and `TOP`, `OFFSET` and `FETCH` counts are written as integers.

A pushed-down statement runs over a new connection, which is closed when its rows have been read and the reader disposed. It is executed when the plan opens it, which under `Apache.Calcite.Data` normally happens inside `ExecuteReader` or `ExecuteReaderAsync`, so a statement the database rejects fails there rather than at the first `Read`. Where a pushed-down part is the whole query, each `Read` or `ReadAsync` of the reader you get advances the database's reader once, and the cancellation token you pass to `ReadAsync` is passed on to it. Listing tables and columns also opens a connection each time.

## Key public types

| Type | Purpose |
|------|---------|
| `AdoSchema` | The Calcite schema over one database schema. `AdoSchema.Create` builds one. |
| `AdoSchemaFactory` | The `SchemaFactory` a JSON model names. |
| `AdoDataSource` | Opens connections and supplies the metadata. Derive from it to supply connections your own way. |
| `DbDataSourceAdoDataSource` | An `AdoDataSource` over a `DbDataSource`. |
| `DbProviderAdoDataSource` | An `AdoDataSource` over a `DbProviderFactory` and a connection string. |
| `AdoDatabaseMetadata` | Lists schemas, tables and columns, and supplies the dialect and parameter syntax. Derive from it to support another provider. |
| `AdoDatabaseMetadataFactory` | Chooses the metadata for a data source. `AdoDatabaseMetadataFactoryImpl.Instance` is the default. |
| `IAdoSqlSyntax` | How a driver names parameters, and a hook to rewrite each generated statement. |
| `AdoCalciteException` | What the adapter throws. |

`AdoConvention`, `AdoRules`, the node types under `Apache.Calcite.Adapter.AdoNet.Rel`, and the converters `AdoToClrCursorConverter` and `AdoToEnumerableConverter` are public for callers that drive Calcite's planner themselves. The first converter serves `Apache.Calcite.Data`; the second serves plans in Calcite's own `EnumerableConvention`.

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | The ADO.NET provider for Calcite: opens connections and runs SQL. |
| [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) | The calling convention plans are compiled into, and the pipeline that prepares them. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters overview](https://calcite.apache.org/docs/adapter.html)
- [Calcite model JSON reference](https://calcite.apache.org/docs/model.html)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
