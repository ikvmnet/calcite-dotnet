# Apache.Calcite.Data

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Data)](https://www.nuget.org/packages/Apache.Calcite.Data)

**Apache.Calcite.Data** is a native, in-process ADO.NET provider for [Apache Calcite](https://calcite.apache.org/) — the SQL parser, optimizer, and execution framework that powers many leading database and data-virtualization products.

The Calcite engine runs directly inside your .NET process via [IKVM](https://github.com/ikvmnet/ikvm). There is no JDBC driver, no Avatica server, and no separate process. SQL flows from your `DbConnection` straight into Calcite's planner.

## Why use this?

- **Standard ADO.NET** — works with any code that understands `DbConnection` / `DbCommand` / `DbDataReader`, including Dapper, EF Core conventions, and generic data-access layers.
- **Federated queries** — join CSV files, in-memory collections, REST adapters, JDBC databases, and custom Calcite schemas in a single SQL statement.
- **Rich SQL** — standards-conformant SQL with window functions, lateral joins, `MATCH_RECOGNIZE`, and much more.
- **Code-driven schemas** — register .NET objects as Calcite schemas, tables, and user-defined functions at runtime via the `SchemaPlus` API; no JSON model required.
- **No external dependencies** — everything runs in-process; no server to provision or maintain.
- **Queries run as .NET code** — a plan is compiled to a `System.Linq.Expressions` tree and executed as .NET, rather than generated as Java source and compiled at runtime.

## Supported platforms

Targets **.NET 8** and is verified on **.NET 8** and **.NET 10**.

## Install

```sh
dotnet add package Apache.Calcite.Data
```

## Quick start — inline JSON model

The quickest way to connect is with an inline [Calcite model](https://calcite.apache.org/docs/model.html) that wires up one or more adapters:

```csharp
using Apache.Calcite.Data;

const string model = """
{
  "version": "1.0",
  "defaultSchema": "SALES",
  "schemas": [
    {
      "name": "SALES",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.csv.CsvSchemaFactory",
      "operand": { "directory": "sales" }
    }
  ]
}
""";

await using var conn = new CalciteConnection($"Model=inline:{model}");
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT \"NAME\", \"DEPTNO\" FROM \"EMPS\" WHERE \"DEPTNO\" = 10";

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
```

## Quick start — model file

Point `Model` at a JSON file on disk:

```csharp
await using var conn = new CalciteConnection("Model=path/to/model.json;Schema=SALES");
await conn.OpenAsync();
```

## Parameterized queries

Calcite uses positional `?` placeholders (ODBC-style). Parameters are matched to placeholders by the order they are added to `Parameters`; the `ParameterName` is informational only.

```csharp
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT \"NAME\" FROM \"EMPS\" WHERE \"DEPTNO\" = ? AND \"SALARY\" > ?";
cmd.Parameters.Add(new CalciteParameter("deptno", 10));
cmd.Parameters.Add(new CalciteParameter("salary", 50_000m));

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine(reader.GetString(0));
```

## Code-driven schemas

Register .NET objects as Calcite schemas directly — no JSON model required:

```csharp
using org.apache.calcite.schema;

await using var conn = new CalciteConnection();
await conn.OpenAsync();

SchemaPlus root = conn.RootSchema;
root.add("MEM", new MyCustomSchema());   // any org.apache.calcite.schema.Schema implementation

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM \"MEM\".\"USERS\" ORDER BY \"ID\"";
await using var reader = await cmd.ExecuteReaderAsync();
```

## Using `DbDataSource` (.NET 7+)

`CalciteDataSource` implements the modern `DbDataSource` pattern for dependency-injection and pooled-connection scenarios:

```csharp
using Apache.Calcite.Data;

await using var dataSource = new CalciteDataSource("Model=path/to/model.json");

await using var conn = await dataSource.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM \"ORDERS\"";
var count = await cmd.ExecuteScalarAsync();
Console.WriteLine($"Order count: {count}");
```

## Using `DbProviderFactory`

```csharp
using System.Data.Common;
using Apache.Calcite.Data;

// Register once at startup.
DbProviderFactories.RegisterFactory("Apache.Calcite.Data", CalciteProviderFactory.Instance);

// Resolve anywhere.
var factory = DbProviderFactories.GetFactory("Apache.Calcite.Data");
await using var conn = factory.CreateConnection()!;
conn.ConnectionString = "Model=path/to/model.json";
await conn.OpenAsync();
```

## Connection string reference

All keys are exposed as typed properties on `CalciteConnectionStringBuilder`. Unknown keys are preserved and forwarded to the engine.

| Key | Type | Description |
|-----|------|-------------|
| `Model` | `string` | Path/URI to a Calcite model JSON file, or `inline:<json>` for an embedded model. |
| `Schema` | `string` | Default schema name when identifiers are unqualified. |
| `CaseSensitive` | `bool` | Whether identifier lookup is case-sensitive (default: `true`). |
| `Conformance` | `string` | SQL conformance level: `DEFAULT`, `STRICT_2003`, `PRAGMATIC_2003`, etc. |
| `Lex` | `string` | Lexical policy: `ORACLE` (default), `MYSQL`, `MYSQL_ANSI`, `SQL_SERVER`, `JAVA`, `BIG_QUERY`. |
| `Quoting` | `string` | Quote style: `DOUBLE_QUOTE`, `BACK_TICK`, `BACK_TICK_BACKSLASH`, `BRACKET`. |
| `QuotedCasing` | `string` | How quoted identifiers are stored: `UNCHANGED`, `TO_UPPER`, `TO_LOWER`. |
| `UnquotedCasing` | `string` | How unquoted identifiers are stored: `UNCHANGED`, `TO_UPPER`, `TO_LOWER`. |
| `Fun` | `string` | Extra function libraries: `standard` (default), `oracle`, `spatial`, or comma-separated combinations. |
| `TimeZone` | `string` | Session time zone, e.g. `UTC` or `gmt-3`. Defaults to the JVM time zone. |
| `Conformance` | `string` | SQL conformance level. |
| `DefaultNullCollation` | `string` | How NULLs sort when `NULLS FIRST`/`NULLS LAST` is not specified. Default: `HIGH` (Oracle behaviour). |
| `ForceDecorrelate` | `bool` | Whether the planner aggressively de-correlates subqueries (default: `true`). |
| `MaterializationsEnabled` | `bool` | Whether the planner may use materializations (default: `false`). |
| `TypeCoercion` | `bool` | Whether implicit type coercion is applied during validation (default: `true`). |
| `parserFactory` | `string` | Custom SQL parser factory class, e.g. `org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY`. |

## Identifier casing

Calcite's default lexer (`Lex=ORACLE`) follows standard SQL rules:

| Identifier kind | Normalized to | Compared |
|-----------------|--------------|---------|
| Unquoted (`emps`) | Upper case (`EMPS`) | Case-sensitive |
| Quoted (`"Emps"`) | Unchanged (`Emps`) | Case-sensitive |

Most built-in adapters (CSV, JDBC against H2/HSQLDB/Oracle) expose names in upper case, so unquoted identifiers work naturally with them.

If your schema uses mixed- or lower-case names, quote them:

```csharp
cmd.CommandText = """SELECT "Name", "DeptNo" FROM "Emps" WHERE "DeptNo" = 10""";
```

Or switch to a case-insensitive lexer:

```csharp
// Lex=MYSQL_ANSI: unquoted identifiers are left unchanged, matching is case-insensitive.
await using var conn = new CalciteConnection("Model=inline:{...};Lex=MYSQL_ANSI");
```

## How queries are executed

Every plan is compiled into a `System.Linq.Expressions` tree and run as .NET code. Calcite's own engine
generates Java source and compiles it at runtime with Janino; this provider does not, so no Java compiler
runs when your query is prepared, and a user-defined function written in .NET can be called straight from
a plan.

There is nothing to configure — it is what a connection does by default. To use Calcite's own engine
instead, set `PrepareFactory` before opening the connection:

```csharp
await using var conn = new CalciteConnection(connectionString);
conn.PrepareFactory = () => (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
await conn.OpenAsync();
```

A single plan may use both. Anything the .NET convention has no implementation for is planned by Calcite
as usual, and rows cross between the two untouched.

## Accessing the Calcite engine directly

`CalciteConnection` exposes Calcite-native objects as typed .NET properties for advanced scenarios:

| Property | Java type | Purpose |
|----------|-----------|---------|
| `RootSchema` | `org.apache.calcite.schema.SchemaPlus` | Add/remove schemas and tables at runtime. |
| `TypeFactory` | `org.apache.calcite.adapter.java.JavaTypeFactory` | Construct Calcite `RelDataType` instances. |
| `Config` | `org.apache.calcite.config.CalciteConnectionConfig` | Inspect resolved connection configuration. |

These properties are only valid while the connection is open.

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Expose any ADO.NET data source as a federated Calcite schema with query pushdown. |
| [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) | .NET helper types for working with Calcite connection properties and IKVM interop. |
| [`Apache.Calcite.Linq`](https://www.nuget.org/packages/Apache.Calcite.Linq) | The calling convention this provider executes plans with. Referenced for you. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters](https://calcite.apache.org/docs/adapter.html)
- [Calcite model JSON reference](https://calcite.apache.org/docs/model.html)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
