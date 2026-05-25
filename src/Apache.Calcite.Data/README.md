# Apache.Calcite.Data

[Apache Calcite](https://calcite.apache.org/) for ADO.NET.

NuGet package: `Apache.Calcite.Data`

`Apache.Calcite.Data` is a native, in-process ADO.NET provider for Apache Calcite. It exposes
Calcite's SQL parser, planner, and runtime through the standard `System.Data.Common`
abstractions — so any code that knows how to talk to a `DbConnection` / `DbCommand` /
`DbDataReader` can talk to Calcite.

The Calcite engine itself is loaded into the same process via [IKVM](https://github.com/ikvmnet/ikvm).
There is no JDBC driver, no Avatica server, and no separate process: SQL flows directly from
your .NET code into Calcite's planner.

## Why this provider?

Apache Calcite is a SQL parser, optimizer, and execution framework. It is the engine behind
many database and data-virtualization products. With this provider you can:

- Run **Calcite SQL** — a standards-conformant SQL dialect with rich semantics — from .NET.
- **Federate** heterogeneous data sources (files, REST APIs, JDBC databases, in-memory
  collections, custom adapters) through a single SQL surface.
- Author **schemas, tables, views, and user-defined functions** in .NET and expose them to
  SQL via Calcite's `SchemaPlus` API.
- Use any tool that consumes a `DbProviderFactory` / ADO.NET connection string.

## Install

```sh
dotnet add package Apache.Calcite.Data
```

Targets **.NET 8** and is verified on **.NET 8** and **.NET 10**.

## Quick start

The simplest connection uses an inline JSON model that defines one or more schemas. This
mirrors the [Calcite tutorial](https://calcite.apache.org/docs/tutorial.html) and the
[JDBC connection string](https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters)
form, with .NET-style key/value pairs.

```csharp
using System.Data;
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

await using var conn = new CalciteConnection($"Model=inline:{model};Schema=SALES");
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT name, deptno FROM emps WHERE deptno = 10";

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
```

### Parameters

Calcite uses positional `?` placeholders, like ODBC. Parameters are bound by the order they
are added to the command's `Parameters` collection; `ParameterName` is informational.

```csharp
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT name FROM emps WHERE deptno = ? AND salary > ?";
cmd.Parameters.Add(new CalciteParameter("?", 10));
cmd.Parameters.Add(new CalciteParameter("?", 50_000m));

await using var reader = await cmd.ExecuteReaderAsync();
```

### Registering schemas in code

Instead of (or in addition to) a JSON model, you can drive Calcite directly from .NET by
adding schemas, tables, and functions to the connection's root schema:

```csharp
using org.apache.calcite.schema;

await using var conn = new CalciteConnection();
await conn.OpenAsync();

SchemaPlus root = conn.RootSchema;
root.add("MEM", new MyInMemorySchema());

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM MEM.users";
await using var reader = await cmd.ExecuteReaderAsync();
```

### Using `DbProviderFactory`

Like other ADO.NET providers (Npgsql, MySqlConnector, `Microsoft.Data.SqlClient`, etc.),
the factory is not auto-registered. Register it once at application startup:

```csharp
using System.Data.Common;
using Apache.Calcite.Data;

DbProviderFactories.RegisterFactory("Apache.Calcite.Data", CalciteProviderFactory.Instance);

var factory = DbProviderFactories.GetFactory("Apache.Calcite.Data");
using var conn = factory.CreateConnection()!;
conn.ConnectionString = "Model=path/to/model.json";
conn.Open();
```

## Connection string keys

Exposed by [`CalciteConnectionStringBuilder`](src/Apache.Calcite.Data/CalciteConnectionStringBuilder.cs):

| Key | Description |
| --- | --- |
| `Model` | Path or URI to a Calcite model file, or `inline:<json>` for an inline model. |
| `Schema` | Default schema name used when a query does not qualify identifiers. |
| `CaseSensitive` | Whether identifier matching is case-sensitive. |
| `Conformance` | SQL conformance level (e.g. `DEFAULT`, `STRICT_2003`, `PRAGMATIC_2003`). |

Unknown keys are preserved and forwarded to the engine, so any Calcite property that is not
listed above can still be supplied by name.

## Identifier casing

Calcite's default lexer (`Lex.ORACLE`) follows standard SQL rules:

- The quote character is `"` (double quote).
- **Unquoted** identifiers are folded to **upper case** at parse time.
- **Quoted** identifiers are left **unchanged**.
- Identifier matching against the schema is **case-sensitive**.

So in the quick-start example above, `emps` and `deptno` are normalized to `EMPS` and
`DEPTNO` before lookup. This works against most built-in adapters (CSV, JDBC against
Oracle/H2/HSQLDB, etc.) because they expose names in upper case as well.

If your underlying schema uses mixed- or lower-case names, quote them:

```csharp
cmd.CommandText = """SELECT "Name", "DeptNo" FROM "Emps" WHERE "DeptNo" = 10""";
```

You can also relax matching by setting `CaseSensitive=false` in the connection string, or
switch to a different lexical convention (e.g. `MYSQL_ANSI`, which leaves unquoted
identifiers unchanged) by passing the `Lex` property through the connection string.

## Direct engine access

`CalciteConnection` exposes selected Calcite-native objects as typed properties for advanced
scenarios — no `Unwrap` escape hatch required:

- `RootSchema` — `org.apache.calcite.schema.SchemaPlus`
- `TypeFactory` — `org.apache.calcite.adapter.java.JavaTypeFactory`
- `Config` — `org.apache.calcite.config.CalciteConnectionConfig`

These are valid only while the connection is open.

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters](https://calcite.apache.org/docs/adapter.html)
- [JSON model reference](https://calcite.apache.org/docs/model.html)
- [Provider design notes](src/Apache.Calcite.Data/DESIGN.md)

## License

Apache License 2.0.
