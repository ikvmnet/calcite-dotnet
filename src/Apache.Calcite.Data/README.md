# Apache.Calcite.Data

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Data)](https://www.nuget.org/packages/Apache.Calcite.Data)

**Apache.Calcite.Data** is a native, in-process ADO.NET provider for [Apache Calcite](https://calcite.apache.org/) — the SQL parser, optimizer, and execution framework that powers many leading database and data-virtualization products.

The Calcite engine runs directly inside your .NET process via [IKVM](https://github.com/ikvmnet/ikvm). There is no Avatica server, no wire protocol, and no separate process — your `DbCommand` goes straight into Calcite's planner and the rows come straight back.

## Why use this?

- **Standard ADO.NET** — works with any code that understands `DbConnection` / `DbCommand` / `DbDataReader`, including Dapper and generic data-access layers.
- **Federated queries** — join CSV files, in-memory collections, REST adapters, JDBC databases, and custom Calcite schemas in a single SQL statement.
- **Rich SQL** — standards-conformant SQL with window functions, lateral joins, `MATCH_RECOGNIZE`, and much more.
- **Code-driven schemas** — register .NET objects as Calcite schemas, tables, and user-defined functions at runtime via the `SchemaPlus` API; no JSON model required.
- **No external dependencies** — everything runs in-process; no server to provision or maintain.
- **Queries run as .NET code** — a plan is compiled to a `System.Linq.Expressions` tree and executed as .NET, rather than generated as Java source and compiled at runtime.

## Supported platforms

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

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

Point `Model` at a JSON file on disk. The file must exist, or `Open` throws:

```csharp
await using var conn = new CalciteConnection("Model=path/to/model.json;Schema=SALES");
await conn.OpenAsync();
```

If the model declares a `defaultSchema`, it wins over the `Schema` connection-string key.

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

Register .NET objects as Calcite schemas directly — no JSON model required. A schema an application
constructs is handed to a `CalciteDataSource`, which every connection opened from it shares:

```csharp
using org.apache.calcite.schema;

await using var dataSource = new CalciteDataSourceBuilder()
    .AddSchema("MEM", new MyCustomSchema())   // any org.apache.calcite.schema.Schema implementation
    .Build();

await using var conn = await dataSource.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM \"MEM\".\"USERS\" ORDER BY \"ID\"";
await using var reader = await cmd.ExecuteReaderAsync();
```

`ConfigureRootSchema` is the general form: it hands you the root as Calcite's mutable `SchemaPlus`, after
the model has been applied, so a table, a function or a view macro can be added as well as a schema, and a
schema that needs its parent can have it:

```csharp
var dataSource = new CalciteDataSourceBuilder("Model=path/to/model.json")
    .ConfigureRootSchema(root => root.add("STAFF", ViewTable.viewMacro(root, "SELECT ...", null, null, null)))
    .Build();
```

The data source is the application's: create it once, register it as a singleton, and dispose it when the
application is done. Disposing it disposes every schema on its root that implements `IDisposable`, which is
the release Calcite's own schema SPI has no hook for.

## Batches

`CalciteBatch` runs several statements sequentially on one connection:

```csharp
await using var batch = conn.CreateBatch();

var insert = batch.CreateBatchCommand();
insert.CommandText = "INSERT INTO \"T\" VALUES (1, 'a')";
batch.BatchCommands.Add(insert);

var update = batch.CreateBatchCommand();
update.CommandText = "UPDATE \"T\" SET \"NAME\" = 'b' WHERE \"ID\" = 1";
batch.BatchCommands.Add(update);

var total = await batch.ExecuteNonQueryAsync();          // cumulative rows affected
var first = batch.BatchCommands[0].RecordsAffected;      // per-command count
```

`ExecuteReader` on a batch produces one result set per command; call `NextResult` to advance between them.

## Using `DbDataSource` (.NET 7+)

`CalciteDataSource` implements the modern `DbDataSource` pattern, and it is where the long-lived half of a
Calcite connection lives. It reads the model and builds the schemas once, on the first connection to open,
and every connection it produces plans against that root:

```csharp
using Apache.Calcite.Data;

await using var dataSource = new CalciteDataSource("Model=path/to/model.json");

await using var conn = await dataSource.OpenConnectionAsync();
await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM \"ORDERS\"";
var count = await cmd.ExecuteScalarAsync();
Console.WriteLine($"Order count: {count}");
```

A bare `new CalciteConnection(connectionString)` draws on a data source too — one the provider keeps for
that connection string, made the first time the string is seen and shared by every connection opened with
an equivalent string afterwards. So the idiomatic ADO.NET shape, a connection per unit of work, costs a
model read once per process rather than once per request. That is the same bargain every ADO.NET provider
makes with its connection pool, and it has the same switch: `Pooling=false` in the connection string gives
each connection a root of its own, built when it opens and released when it is disposed.

What the provider keeps is bounded by time, as a connection pool is. A data source that has gone
`Connection Idle Lifetime` seconds (default 300) with no connection open on it is released — dropped, and
every schema on its root that implements `IDisposable` disposed — checked every `Connection Pruning
Interval` seconds (default 10); the next connection opened with that string builds again. So a process
that varies its connection strings does not keep a root for every string it ever wrote.
`CalciteConnection.ClearPool(connection)` releases one on demand, which is how a changed model file reaches
a running process sooner, and `ClearAllPools()` releases them all. A connection already open keeps the
root it has, and the root is disposed once the last such connection is. A data source you built yourself
is not kept by the provider and is never released this way; `CalciteDataSource.Clear()` is its equivalent.

Sharing a root means an adapter's schema may be read from several threads at once. Calcite serialises
nothing, so a schema reachable from a data source has to tolerate concurrent reads. The provider holds
the root's read lock while a statement plans and its write lock while DDL alters it, so a statement never
plans against a root another connection is changing; a table is looked up once more when a plan runs, and
that lookup is not under the lock.

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

## Connection lifetime

A connection is cheap and short-lived. What it plans against — the model, the schemas the model built, the
tables DDL has created — belongs to its data source and outlives it; what it keeps to itself is its
configuration, its type factory and the convention it plans into, created on the **first** `Open()` and
reused for the life of the `CalciteConnection` object.

- `Close()` only moves the connection to the `Closed` state; the next `Open()` is free.
- `Dispose()` releases what the connection holds. The data source's root is untouched — unless the
  connection string said `Pooling=false`, in which case the root was the connection's own and goes with it.
- A table created by DDL on one connection is visible on every connection of the same data source, as it
  is in any database.
- `ConnectionString` cannot be changed once the connection has been opened. Create a new `CalciteConnection` for different settings.

## Behaviour worth knowing

**Transactions are not supported.** `BeginTransaction` throws `NotSupportedException`, as do `CalciteTransaction.Commit` and `Rollback`. The type exists only so frameworks that require a non-null `DbTransaction` can be satisfied. `EnlistTransaction` throws too.

**`CommandType.Text` only.** Setting any other `CommandType` throws `NotSupportedException`. There is no stored-procedure concept in Calcite.

**Every query is planned asynchronously unless the connection says otherwise.** The plan is the connection's, not the entry point's: by default it is built in the asynchronous convention, so `ReadAsync` is genuinely asynchronous wherever the schema can be, and a synchronous `Read` blocks per row only where the source really is asynchronous — which is what `Read` over an asynchronous source means in every ADO.NET provider. `Synchronous=true` plans in the synchronous convention instead: nothing ever waits, `ReadAsync` answers with completed tasks, and a query touching a table that can *only* produce rows asynchronously fails to plan.

**Cancellation is per-statement.** A `CancellationToken` is observed before a statement is planned. On a DML statement it is wired to Calcite's cancel flag while the rows are drained. The token given to `ExecuteReaderAsync` also reaches the plan's enumerator, so cancelling it stops the leaf between rows; in `Synchronous` mode it is not wired to a reader's enumeration. `DbCommand.Cancel()` is a no-op.

**`ExecuteNonQuery` return values** follow ADO.NET convention: `-1` for a `SELECT`, `0` for DDL, and the row count Calcite reports for `INSERT` / `UPDATE` / `DELETE` / `MERGE`.

**DDL runs at prepare time.** A `CREATE` or `DROP` has already taken effect by the time the call returns, and produces no rows; it took effect on the data source's root, so every connection of that data source sees it. DDL also needs a parser that understands it — set `parserFactory=org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY`.

**Materialized views are not substituted**, whatever `materializationsEnabled` says. Calcite builds them through a package-private class this provider cannot reach.

**`spark=true` does nothing here.** This provider never enables a Spark handler; a plan of its convention is an expression tree, not generated Java source.

**A one-column result is the value, not a row of one.** `GetValue(0)` returns it directly.

## Connection string reference

All keys are exposed as typed properties on `CalciteConnectionStringBuilder`. Keys are matched case-insensitively, and unknown keys are preserved and forwarded to the engine.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Model` | `string` | — | Path to a Calcite model JSON file, or `inline:<json>` for an embedded model. |
| `Schema` | `string` | — | Default schema name when identifiers are unqualified. |
| `Synchronous` | `bool` | `false` | Plan queries in the synchronous convention instead of the asynchronous one. A provider option, not forwarded to the engine — see [Behaviour worth knowing](#behaviour-worth-knowing). |
| `Pooling` | `bool` | `true` | Whether connections opened with this connection string share one root schema. A provider option, not forwarded to the engine — see [Using `DbDataSource`](#using-dbdatasource-net-7). |
| `Connection Idle Lifetime` | `int` | `300` | Seconds a shared root schema is kept with no connection open on it before it is released. A provider option. |
| `Connection Pruning Interval` | `int` | `10` | Seconds between checks for shared root schemas to release. A provider option. |
| `Lex` | `string` | `ORACLE` | Lexical policy: `ORACLE`, `MYSQL`, `MYSQL_ANSI`, `SQL_SERVER`, `JAVA`, `BIG_QUERY`. |
| `CaseSensitive` | `bool` | from `Lex` | Whether identifier lookup is case-sensitive. |
| `Quoting` | `string` | from `Lex` | Quote style: `DOUBLE_QUOTE`, `BACK_TICK`, `BACK_TICK_BACKSLASH`, `BRACKET`. |
| `QuotedCasing` | `string` | from `Lex` | How quoted identifiers are stored: `UNCHANGED`, `TO_UPPER`, `TO_LOWER`. |
| `UnquotedCasing` | `string` | from `Lex` | How unquoted identifiers are stored: `UNCHANGED`, `TO_UPPER`, `TO_LOWER`. |
| `Conformance` | `string` | `DEFAULT` | SQL conformance level: `DEFAULT`, `STRICT_2003`, `PRAGMATIC_2003`, … |
| `Fun` | `string` | `standard` | Extra function libraries: `oracle`, `spatial`, or comma-separated combinations. |
| `DefaultNullCollation` | `string` | `HIGH` | How NULLs sort when `NULLS FIRST`/`NULLS LAST` is not specified. |
| `TimeZone` | `string` | JVM default | Session time zone, e.g. `UTC` or `gmt-3`. |
| `TypeCoercion` | `bool` | `true` | Whether implicit type coercion is applied during validation. |
| `ForceDecorrelate` | `bool` | `true` | Whether the planner aggressively de-correlates subqueries. |
| `MaterializationsEnabled` | `bool` | `true` | Whether the planner may use materializations. None are supplied here — see above. |
| `CreateMaterializations` | `bool` | `true` | Whether materializations are created on the fly. |
| `ApproximateDecimal` | `bool` | `false` | Allow approximate DECIMAL aggregate results. |
| `ApproximateDistinctCount` | `bool` | `false` | Allow approximate `COUNT(DISTINCT ...)`. |
| `ApproximateTopN` | `bool` | `false` | Allow approximate Top-N results. |
| `DruidFetch` | `int` | `16384` | Rows the Druid adapter fetches at a time. |
| `Spark` | `bool` | `false` | Ignored by this provider. |
| `SchemaFactory` | `string` | — | Schema factory class name, when not using a model: the factory makes one schema, named by `Schema` (default `adhoc`), with every `schema.`-prefixed key as an operand. |
| `SchemaType` | `string` | — | Schema type when not using a model, `MAP` or `JDBC`, naming the factory Calcite ships for it. |
| `TypeSystem` | `string` | — | Type system class name. |
| `parserFactory` | `string` | — | Custom SQL parser factory, e.g. `org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY`. |

Defaults are Calcite's own, from `CalciteConnectionProperty` in the version this package references (1.42).

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

Every plan is compiled into a `System.Linq.Expressions` tree and run as .NET code. Calcite's own engine generates Java source and compiles it at runtime with Janino; this provider does not, so no Java compiler runs when your query is prepared, and a user-defined function written in .NET can be called straight from a plan.

There is nothing to configure and nothing to switch off. This provider owns the prepare pipeline rather than subclassing Calcite's: it never calls `CalcitePrepare.prepareSql`, and a statement never produces a Calcite `Bindable`. The rows a reader returns come from the compiled delegate's own enumerator, with nothing in between.

Execute runs the plan; reading pulls rows. `ExecuteReader` obtains the plan's enumerator, and — as in Calcite's own linq4j — obtaining an enumerator is where each operator acquires its input, a sort drains, and an underlying source opens or executes its statement. A failing plan therefore throws at `ExecuteReader`, not at the first `Read`.

A single plan may still use both engines. Anything the .NET convention has no rule for is planned by Calcite as usual, and rows cross between the two untouched.

## Diagnostics

Calcite's `Hook` points can be attached for the duration of every statement on a connection, or of one command:

```csharp
using org.apache.calcite.runtime;

conn.RegisterHook(Hook.PLAN_BEFORE_IMPLEMENTATION, plan => Console.WriteLine(plan));
cmd.RegisterHook(Hook.PROGRAM, /* ... */);
```

Overloads accept a Java `Consumer`, a .NET `Action<object>`, or a primitive value to set as the hook's property. Connection hooks run before command hooks.

`EXPLAIN PLAN FOR <query>` also works, and returns the rendered plan as a single row. It explains the plan the connection would run: `ClrAsyncEnumerable*` nodes by default, `ClrEnumerable*` ones when the connection string says `Synchronous` — the same plan from `ExecuteReader` and `ExecuteReaderAsync`, since the convention is the connection's choice rather than the entry point's.

## Accessing the Calcite engine directly

`CalciteConnection` exposes Calcite-native objects as typed .NET properties for advanced scenarios:

| Property | Java type | Purpose |
|----------|-----------|---------|
| `RootSchema` | `org.apache.calcite.schema.Schema` | Inspect the root the connection plans against. It is the data source's, shared by every connection opened on it, and handed out as Calcite's read interface; to add to it, use `CalciteDataSourceBuilder.ConfigureRootSchema` or DDL. |
| `TypeFactory` | `org.apache.calcite.adapter.java.JavaTypeFactory` | Construct Calcite `RelDataType` instances. |
| `Config` | `org.apache.calcite.config.CalciteConnectionConfig` | Inspect resolved connection configuration. |

These properties are only valid while the connection is open; otherwise they throw `InvalidOperationException`.

## Errors

Planning and execution failures surface as `CalciteException`, a `DbException`, with the underlying Calcite or Java exception as `InnerException`.

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Expose any ADO.NET data source as a federated Calcite schema with query pushdown. |
| [`Apache.Calcite.Extensions`](https://www.nuget.org/packages/Apache.Calcite.Extensions) | The calling convention this provider executes plans with, the prepare pipeline behind it, and the IKVM interop and connection-property helpers. Referenced for you. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Calcite adapters](https://calcite.apache.org/docs/adapter.html)
- [Calcite model JSON reference](https://calcite.apache.org/docs/model.html)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
