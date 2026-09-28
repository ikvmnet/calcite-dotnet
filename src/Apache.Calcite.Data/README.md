# Apache.Calcite.Data

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Data)](https://www.nuget.org/packages/Apache.Calcite.Data)

**Apache.Calcite.Data** is an in-process ADO.NET provider for [Apache Calcite](https://calcite.apache.org/),
the SQL parser, optimizer and query engine.

Calcite runs inside your .NET process through [IKVM](https://github.com/ikvmnet/ikvm). There is no server,
no JDBC driver and no wire protocol: a `DbCommand` is parsed, validated and optimized by Calcite, and the
resulting plan is compiled to a `System.Linq.Expressions` tree and executed as .NET code.

## Why use it

- **Standard ADO.NET.** `CalciteConnection`, `CalciteCommand` and `CalciteDataReader` work with code written
  against `DbConnection`, `DbCommand` and `DbDataReader`, including Dapper and `DataTable.Load`.
- **Federated SQL.** Query any schema Calcite can reach, including schemas you implement in .NET, and join
  across them in one statement.
- **Calcite's SQL.** Window functions, `MATCH_RECOGNIZE`, lateral joins, arrays, maps, and the rest of what
  Calcite supports.
- **Schemas from code.** Register schema objects your application builds, without writing a JSON model.
- **Synchronous and asynchronous reads.** Every reader supports both `Read` and `ReadAsync`, and a table that
  produces rows asynchronously is awaited rather than blocked on.

## Supported platforms

Targets **.NET 8**, and is tested on **.NET 8** and **.NET 10**.

## Install

```sh
dotnet add package Apache.Calcite.Data
```

## Quick start: a schema from code

A schema your application constructs is registered on a `CalciteDataSource` and shared by every connection
opened from it. Tables implement Calcite's `Table` interface; `ScannableTable` is the simplest kind:

```csharp
using Apache.Calcite.Data;

using org.apache.calcite;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

await using var dataSource = new CalciteDataSourceBuilder("Schema=HR")
    .ConfigureRootSchema(root => root.add("HR", new AbstractSchema()).add("PEOPLE", new PeopleTable()))
    .Build();

await using var conn = await dataSource.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = ?";
cmd.Parameters.Add(new CalciteParameter("id", 2));

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine(reader.GetString(0));

class PeopleTable : AbstractTable, ScannableTable
{
    public override RelDataType getRowType(RelDataTypeFactory typeFactory) =>
        typeFactory.builder()
            .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR, 20))
            .build();

    // each row is an object[] of the values Calcite's runtime uses for the column types:
    // java.lang.Integer for INTEGER, string for VARCHAR
    public org.apache.calcite.linq4j.Enumerable scan(DataContext root) =>
        org.apache.calcite.linq4j.Linq4j.asEnumerable(new object[][]
        {
            [java.lang.Integer.valueOf(1), "Alice"],
            [java.lang.Integer.valueOf(2), "Bob"],
        });
}
```

`AddSchema(name, schema)` registers a whole schema object. `ConfigureRootSchema` gives you the root as
Calcite's mutable `SchemaPlus`, after any model has been applied, so you can add schemas, tables, functions and
views. Steps run in the order they were added, each time a root is built.

## Quick start: a JSON model

A [Calcite model](https://calcite.apache.org/docs/model.html) describes schemas in JSON. Pass it inline, or
as the path of a model file:

```csharp
const string model = """
{
  "version": "1.0",
  "defaultSchema": "SALES",
  "schemas": [
    {
      "name": "SALES",
      "tables": [
        {
          "name": "EMPS",
          "type": "view",
          "sql": "SELECT * FROM (VALUES (1, 'Alice', 10), (2, 'Bob', 20)) AS t (ID, NAME, DEPTNO)"
        }
      ]
    }
  ]
}
""";

await using var conn = new CalciteConnection($"Model=inline:{model}");
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT NAME FROM EMPS WHERE DEPTNO = 10";
var name = (string?)await cmd.ExecuteScalarAsync();
```

```csharp
await using var conn = new CalciteConnection("Model=path/to/model.json");
```

A model file must exist, or `Open` throws. A `defaultSchema` in the model takes precedence over the `Schema`
connection string key.

### Models that name classes

Calcite loads a class named in a model (a custom schema's `factory`, a function's `className`, a JDBC
driver) only when the Java system property `calcite.model.classes.allowed` allows it. The value is a
comma-separated list of class names, or of package prefixes ending in `.`; it is empty by default, which
allows nothing, not even Calcite's own classes. Calcite reads the property once, the first time it reads its
system properties, so set it at startup before opening any connection:

```csharp
java.lang.System.setProperty(
    "calcite.model.classes.allowed",
    "org.apache.calcite.,MyCompany.Calcite.,cli.MyCompany.Calcite.");
```

A .NET class must be listed under its .NET name and also under its IKVM name, which is the .NET name prefixed
with `cli.`. The same applies to the class a `SchemaFactory` or `SchemaType` connection string key implies,
since the provider describes that schema to Calcite as a model.

The class must also be visible to Calcite's class loader. A class in a Java library you reference with
`MavenReference` is found once its assembly is on IKVM's boot class path:

```csharp
ikvm.runtime.Startup.addBootClassPathAssembly(typeof(SomeClassInThatLibrary).Assembly);
```

## Parameters

Placeholders are positional `?` markers, bound in the order the parameters appear in `Parameters`. The
parameter name is used only to look a parameter up in the collection.

```csharp
cmd.CommandText = "SELECT NAME FROM EMPS WHERE DEPTNO = ? AND ID > ?";
cmd.Parameters.Add(new CalciteParameter("deptno", 10));
cmd.Parameters.Add(new CalciteParameter("id", 0));
```

Calcite infers a SQL type for each placeholder, and the value is converted to that type. A parameter's
`DbType`, or its `CalciteDbType` for types `DbType` cannot name, chooses which .NET type the value is converted
from. `null` and `DBNull.Value` both bind SQL null.

## Reading values

`CalciteDataReader` returns .NET values; Calcite's Java representations never reach your code unless you ask
for them with `GetCalciteValue`.

| Calcite type | `GetFieldType` / `GetValue` |
|---|---|
| `BOOLEAN` | `bool` |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `sbyte`, `short`, `int`, `long` |
| `TINYINT UNSIGNED` … `BIGINT UNSIGNED` | `byte`, `ushort`, `uint`, `ulong` |
| `DECIMAL` | `decimal` |
| `REAL`, `DOUBLE`, `FLOAT` | `float`, `double`, `double` |
| `CHAR`, `VARCHAR` | `string` |
| `BINARY`, `VARBINARY` | `byte[]` |
| `DATE`, `TIMESTAMP` | `DateTime` |
| `TIME` | `TimeSpan` |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE`, `TIME WITH TIME ZONE`, `TIME WITH LOCAL TIME ZONE` | `DateTimeOffset` |
| `UUID` | `Guid` |
| `GEOMETRY` | `string` (well-known text) |
| year-month interval | `int` (months) |
| day-time interval | `TimeSpan` |
| `ARRAY`, `MULTISET` | an array of the elements' type, for example `int[]`, or `int?[]` where elements may be null |
| `MAP` | `Dictionary<TKey, TValue>` |
| `ROW` | `object[]` |
| `ANY`, `OTHER`, `VARIANT` | `object`; the value's own type decides |

`GetFieldValue<T>` also accepts the other readings a type has, such as `DateOnly` for a `DATE` or `TimeOnly`
for a `TIME`, and element types for a collection, such as `GetFieldValue<DateOnly[]>` for a `DATE ARRAY`.
`GetArray` and `GetArray<T>` read collections.

**Typed getters do not convert.** As in `Microsoft.Data.SqlClient`, `GetInt64` on an `INTEGER` column
throws `InvalidCastException`; use `GetInt32`. `GetGuid` does not parse a string; `CAST(x AS UUID)` in SQL does.
The typed getters throw `InvalidCastException` on SQL null, so test `IsDBNull` first.

`GetRelDataType` returns a column's exact Calcite type, including nested element, key, value and field types
that `GetDataTypeName` cannot express.

## Batches

`CalciteBatch` runs several statements in order on one connection:

```csharp
await using var batch = conn.CreateBatch();

var insert = batch.CreateBatchCommand();
insert.CommandText = "INSERT INTO T VALUES (1, 'a')";
batch.BatchCommands.Add(insert);

var update = batch.CreateBatchCommand();
update.CommandText = "UPDATE T SET NAME = 'b' WHERE ID = 1";
batch.BatchCommands.Add(update);

var total = await batch.ExecuteNonQueryAsync();          // total rows affected
var first = batch.BatchCommands[0].RecordsAffected;      // rows affected by the first command
```

`ExecuteReader` on a batch plans every command and opens its result before returning, and the reader holds one result set per
command; call `NextResult` to move between them. Hooks registered on the connection do not apply to batches.

## Data sources and connection lifetime

A connection plans against a root schema that belongs to a `CalciteDataSource`: the schemas the model defines,
the schemas and steps registered on the data source, and the tables DDL creates. The root is built when the
first connection opens, and every connection of the data source shares it, so a table created on one
connection is visible on the others.

Create a data source once, share it for the life of the application, and dispose it at shutdown:

```csharp
await using var dataSource = new CalciteDataSource("Model=path/to/model.json");

await using var conn = await dataSource.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM ORDERS";
var count = await cmd.ExecuteScalarAsync();
```

A connection created from a connection string alone, `new CalciteConnection(connectionString)`, draws on a
data source the provider keeps for that connection string and shares with every connection opened with an
equivalent string, so the model is read once per process rather than once per connection. The provider
releases it once no connection has been open on it for `Connection Idle Lifetime` seconds (default 300),
checking every `Connection Pruning Interval` seconds (default 10). `CalciteConnection.ClearPool(connection)`
releases the one for a connection's string at once, which makes the next connection read a changed model
file, and `CalciteConnection.ClearAllPools()` releases all of them. `CalciteDataSource.Clear()` does the same
for a data source you created. Connections already open keep the root they have until they are disposed.

`Pooling=false` in the connection string gives each connection a root of its own, built when it first opens
and released when it is disposed.

When a root is released, every schema on it that implements `IDisposable` is disposed, once the last
connection using that root has been disposed.

Because connections share the root, a schema you register may be read from several threads at once and must
tolerate concurrent reads. Statements plan under a shared lock on the root and DDL runs under an exclusive
one; execution does not hold the lock.

For a single connection:

- The first `Open()` creates the connection's session: its configuration, type factory and type mappings.
  `Close()` only changes the state, and a later `Open()` reuses the session.
- `Dispose()` releases the session. The root is not affected, except under `Pooling=false`.
- `ConnectionString` and `TypeMapper` cannot be changed once the connection has been opened.

## Using `DbProviderFactory`

```csharp
using System.Data.Common;
using Apache.Calcite.Data;

DbProviderFactories.RegisterFactory("Apache.Calcite.Data", CalciteProviderFactory.Instance);

var factory = DbProviderFactories.GetFactory("Apache.Calcite.Data");
await using var conn = factory.CreateConnection()!;
conn.ConnectionString = "Model=path/to/model.json";
await conn.OpenAsync();
```

## DDL

DDL (`CREATE TABLE`, `CREATE VIEW`, `DROP`, …) needs Calcite's DDL parser, which is in the `calcite-server`
artifact. Reference it with `MavenReference`, at the same version of `calcite-core` this package uses, put its
assembly on IKVM's boot class path at startup, and name its parser factory in the connection string:

```xml
<MavenReference Include="org.apache.calcite:calcite-server" Version="..." />
```

```csharp
ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.server.ServerDdlExecutor).Assembly);

await using var conn = new CalciteConnection(
    "parserFactory=org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY");
```

A DDL statement takes effect while it is prepared, before the execute method returns, and produces no rows.
It changes the data source's root, so every connection of the data source sees it. `CREATE MATERIALIZED VIEW`
and `CREATE TABLE ... AS SELECT` are not supported.

## Behaviour to be aware of

- **No transactions.** `BeginTransaction` and `EnlistTransaction` throw `NotSupportedException`.
- **`CommandType.Text` only.** Any other `CommandType` throws `NotSupportedException`.
- **No statement cache.** Every execution parses and plans the statement again; `Prepare` does nothing.
- **Errors surface at execute.** Executing a query opens its plan, which is when a sort drains its input and a
  table adapter sends its query, so failures from starting the query are thrown by `ExecuteReader`, not by the
  first `Read`.
- **`ExecuteNonQuery`** returns the number of rows affected for `INSERT`, `UPDATE`, `DELETE` and `MERGE`, 0 for
  DDL, and -1 for a query, which it plans but does not run.
- **`RecordsAffected` on a reader is 0.** A data modification executed through a reader returns its count as
  a single `ROWCOUNT` column.
- **`HasRows` does not look ahead.** It reports whether the last `Read` returned a row.
- **Cancellation.** A `CancellationToken` passed to an asynchronous execute method is checked before planning
  and stays linked to the statement. A token passed to `ReadAsync` reaches every operator of the plan; if it is
  cancelled, the statement is cancelled. `DbCommand.Cancel()` does nothing.
- **`CommandTimeout`** is passed to Calcite as the statement's query timeout, which Calcite applies to queries
  its JDBC adapter sends to a database. It does not otherwise stop a long-running statement.
- **Materialized views are not substituted**, whatever `materializationsEnabled` says.
- **`spark=true` has no effect.**
- **Hooks are attached during planning and opening only.** A hook registered with `RegisterHook` is attached to
  the executing thread while the statement is prepared and its result opened, not while rows are read.

## Diagnostics

Calcite's `Hook` points can be attached for every command on a connection, or for one command:

```csharp
using org.apache.calcite.runtime;

conn.RegisterHook(Hook.PLAN_BEFORE_IMPLEMENTATION, root => Console.WriteLine(root));
cmd.RegisterHook(Hook.PARSE_TREE, args => Console.WriteLine(((object[])args)[0]));
```

Overloads accept a Java `Consumer`, a .NET `Action<object>`, or a primitive value for a property hook.
Connection hooks are attached before command hooks.

`EXPLAIN PLAN FOR <query>` returns the plan as a single row. A plan's nodes are `ClrCursor*` nodes, with
Calcite's `Enumerable*` nodes beneath a converter wherever this provider has no node of its own.

## Connection string reference

Keys are matched ignoring case. Every key is also a typed property on `CalciteConnectionStringBuilder`. A key
the builder does not recognize is passed to Calcite unchanged, and a `schema.`-prefixed key is an operand of
the schema `SchemaFactory` or `SchemaType` creates. Defaults are Calcite's own.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Model` | `string` | — | Path to a model file, or inline JSON (prefixed `inline:` or starting with `{`). |
| `Schema` | `string` | — | Default schema for unqualified names. The model's `defaultSchema` takes precedence. |
| `Pooling` | `bool` | `true` | Whether connections opened with an equivalent connection string share one root schema. |
| `Connection Idle Lifetime` | `int` | `300` | Seconds a kept data source may go with no open connection before it is released. |
| `Connection Pruning Interval` | `int` | `10` | Seconds between checks for data sources to release. Must be positive and not exceed `Connection Idle Lifetime`. |
| `Lex` | `string` | `ORACLE` | Lexical policy: `ORACLE`, `MYSQL`, `MYSQL_ANSI`, `SQL_SERVER`, `JAVA`, `BIG_QUERY`. |
| `CaseSensitive` | `bool` | from `Lex` | Whether identifiers are matched case-sensitively. |
| `Quoting` | `string` | from `Lex` | `DOUBLE_QUOTE`, `BACK_TICK`, `BACK_TICK_BACKSLASH` or `BRACKET`. |
| `QuotedCasing` | `string` | from `Lex` | How quoted identifiers are stored: `UNCHANGED`, `TO_UPPER`, `TO_LOWER`. |
| `UnquotedCasing` | `string` | from `Lex` | How unquoted identifiers are stored: `UNCHANGED`, `TO_UPPER`, `TO_LOWER`. |
| `Conformance` | `string` | `DEFAULT` | SQL conformance level: `DEFAULT`, `STRICT_2003`, `PRAGMATIC_2003`, … |
| `Fun` | `string` | `standard` | Function libraries, comma-separated: `standard`, `oracle`, `mysql`, `spatial`, … |
| `DefaultNullCollation` | `string` | `HIGH` | How nulls sort when a query does not say: `HIGH`, `LOW`, `FIRST`, `LAST`. |
| `TimeZone` | `string` | process default | Session time zone, for example `UTC` or `gmt-3`. |
| `TypeCoercion` | `bool` | `true` | Whether implicit type coercion is applied during validation. |
| `ForceDecorrelate` | `bool` | `true` | Whether the planner decorrelates as much as possible. |
| `TopDownGeneralDecorrelationEnabled` | `bool` | `false` | Whether `TopDownGeneralDecorrelator` does the decorrelation rather than `RelDecorrelator`. |
| `MaterializationsEnabled` | `bool` | `true` | Has no effect here; see above. |
| `CreateMaterializations` | `bool` | `true` | Whether Calcite creates materializations. |
| `ApproximateDecimal` | `bool` | `false` | Allow approximate results from `DECIMAL` aggregates. |
| `ApproximateDistinctCount` | `bool` | `false` | Allow approximate `COUNT(DISTINCT ...)`. |
| `ApproximateTopN` | `bool` | `false` | Allow approximate Top-N results. |
| `DruidFetch` | `int` | `16384` | Rows Calcite's Druid adapter fetches at a time. |
| `Spark` | `bool` | `false` | Has no effect here. |
| `SchemaFactory` | `string` | — | Java class (or `Class#FIELD`) of a schema factory. Without a model, creates one schema named by `Schema` (default `adhoc`) with every `schema.`-prefixed key as an operand. |
| `SchemaType` | `string` | — | Without a model or `SchemaFactory`: `MAP` for an empty schema, `JDBC` for a `JdbcSchema`. |
| `TypeSystem` | `string` | — | A .NET type or static member providing a Calcite `RelDataTypeSystem`, such as `"[org.apache.calcite.sql.dialect.PostgresqlSqlDialect, calcite.core]::POSTGRESQL_TYPE_SYSTEM"`. |
| `parserFactory` | `string` | — | Java class (or `Class#FIELD`) of a parser factory; see [DDL](#ddl). |

## Identifier casing

Under the default `Lex=ORACLE`, unquoted identifiers are converted to upper case and quoted identifiers are
kept as written, and both are matched case-sensitively. A schema whose names are not upper case needs quoted
identifiers:

```csharp
cmd.CommandText = """SELECT "Name" FROM "Emps" WHERE "DeptNo" = 10""";
```

or a lexical policy that matches case-insensitively:

```csharp
// Lex=MYSQL_ANSI: unquoted identifiers are kept as written and matched case-insensitively
await using var conn = new CalciteConnection("Model=path/to/model.json;Lex=MYSQL_ANSI");
```

## Accessing Calcite directly

| Property | Type | Purpose |
|----------|------|---------|
| `RootSchema` | `org.apache.calcite.schema.Schema` | The root schema the connection plans against, shared by the data source's connections. To add to it, use `CalciteDataSourceBuilder.ConfigureRootSchema` or DDL. |
| `TypeFactory` | `org.apache.calcite.adapter.java.JavaTypeFactory` | Builds Calcite `RelDataType` instances, for example for `CalciteParameter.RelDataType`. |
| `Config` | `org.apache.calcite.config.CalciteConnectionConfig` | The effective Calcite configuration. |

These require an open connection and otherwise throw `InvalidOperationException`.

## Errors

Failures to load a model, open a connection, or parse, plan or execute a statement are thrown as
`CalciteException`, a `DbException`, with Calcite's exception as the `InnerException`.

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Exposes an ADO.NET data source as a Calcite schema, pushing queries down to it. |
| [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) | The calling convention and prepare pipeline this provider executes plans with. Referenced for you. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters](https://calcite.apache.org/docs/adapter.html)
- [Calcite model reference](https://calcite.apache.org/docs/model.html)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
