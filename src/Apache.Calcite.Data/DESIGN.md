# Apache.Calcite.Data — Design

`Apache.Calcite.Data` is an ADO.NET provider for Apache Calcite. It exposes Calcite to .NET
applications through the standard `System.Data.Common` abstractions while running Calcite's parser,
validator and planner in-process via IKVM.

A statement is not handed to Calcite's own prepare framework. `Apache.Calcite.Extensions` owns the
pipeline that takes SQL text to a chosen plan and compiles that plan into a
`System.Linq.Expressions` tree; this project drives that pipeline and adapts what comes back to
`DbDataReader`. Janino never runs.

This document describes how the provider is structured and how a statement flows from a .NET caller
into Calcite and back.

---

## Scope

- **Driver/provider**, not an adapter. The provider is the consumer's entry point into Calcite, not
  a way to expose ADO.NET data sources to Calcite. (`Apache.Calcite.Adapter.AdoNet` is the other
  direction.)
- **Native in-process engine.** Calcite's Java code is loaded through IKVM and called directly. The
  provider does not go through Calcite's JDBC driver on the statement path and speaks no Avatica
  wire protocol.
- **JDBC parity at the behavior level.** Connection properties, model handling, SQL execution and
  metadata semantics aim to match the Calcite JDBC driver, but the public surface is idiomatic .NET
  (`Db*` base classes, PascalCase, `IDisposable`).

### What is still Calcite's, and named honestly

Three things carry Java names through this design and are not going away:

- **Avatica's metadata value types.** `ColumnMetaData`, `AvaticaParameter`, `Meta.CursorFactory` and
  `Meta.StatementType` come from `org.apache.calcite.avatica`, and the prepare pipeline produces
  them because that is what Calcite's own prepare produces. They are plain descriptors. Nothing here
  constructs an Avatica `Meta`, a service, or a connection.
- **`CalcitePrepare.Context` and `CalcitePrepare.Dummy`.** `PrepareContext` implements the former —
  it is the interface Calcite's validator, catalog reader and `SqlToRelConverter` read the schema,
  type factory and configuration from. The latter is a thread-local stack Calcite's own parse-to-rel
  reads the context off, so `CalciteSession.Plan` pushes onto it for the duration of a prepare.
  `CalcitePrepare.DEFAULT_FACTORY` and `prepareSql` are never called.
- **A view's definition is analysed under Calcite's default configuration, not the connection's, and
  that is reproduced rather than fixed.** `ViewTableMacro.apply` analyses through
  `MaterializedViewTable.MATERIALIZATION_CONNECTION` — a process-wide
  `DriverManager.getConnection("jdbc:calcite:")` — and `CalcitePrepareImpl.parse_` builds the catalog
  reader and the validator from that connection's configuration, so `fun`, `conformance` and
  `caseSensitive` never reach a view definition even though `ClrPreparingStmt.expandView` uses the real
  configuration when the view is later expanded. Measured against stock Calcite: a plain
  `jdbc:calcite:` connection with `fun=standard,oracle` evaluates `NVL` in a query and fails on the
  same expression inside a model view, with no part of this project involved. `Schemas.makeContext`
  does have a branch that would avoid it — a null connection makes it read
  `CalcitePrepare.Dummy.peek()` — and reaching it needs only a `ViewTableMacro` subclass, but taking it
  would be a divergence we own alone and could diff against nothing; the argument belongs upstream.
  (The *parser* is Calcite's default for a view definition regardless — `parse_` calls
  `createParser(sql)`, as `ClrPreparingStmt.ParserConfig` does — so the quoting and casing a `lex`
  implies were never taken from the connection.)
  A view registered from code goes through `ViewTable.viewMacro` like any other; pass
  `CalciteSchema.from(schema).path(name)` as the view path, or Calcite cannot detect a view defined in
  terms of itself and recurses instead of raising `CyclicDefinitionException`.
- **Calcite's JDBC driver, registered once, for views.** `CalciteSession`'s static constructor puts
  `org.apache.calcite.jdbc.Driver`'s assembly on IKVM's boot class path and constructs one.
  `ViewTableMacro.apply` reads `MaterializedViewTable.MATERIALIZATION_CONNECTION`, whose initializer
  is `DriverManager.getConnection("jdbc:calcite:")` — so expanding any view, however declared, goes
  through the driver. Without the registration every view fails at validation. No statement executed
  by this provider goes through it.

---

## Layered Architecture

### 1. ADO.NET Surface

Public, consumer-facing classes implementing the `System.Data.Common` contracts.

| Class | Base | Role |
| --- | --- | --- |
| `CalciteConnection` | `DbConnection` | Owns connection state and the `CalciteSession`; exposes Calcite-native accessors and hook registration. |
| `CalciteCommand` | `DbCommand` | Holds SQL text and parameters; builds a `CalciteExecuteRequest` and calls the session. |
| `CalciteDataReader` | `DbDataReader` | Streams rows out of one or more `CalciteResult`s; `NextResult` walks a batch's results. |
| `CalciteBatch` / `CalciteBatchCommand` / `CalciteBatchCommandCollection` | `DbBatch` / `DbBatchCommand` / `DbBatchCommandCollection` | Runs several statements sequentially on one session. |
| `CalciteParameter` / `CalciteParameterCollection` | `DbParameter` / `DbParameterCollection` | Provider parameter model. Placeholders are positional `?`; `ParameterName` is informational. |
| `CalciteTransaction` | `DbTransaction` | Exists to satisfy frameworks that require a non-null transaction. `Commit` and `Rollback` throw `NotSupportedException`, and `BeginDbTransaction` throws before one is ever handed out. |
| `CalciteDataSource` | `DbDataSource` | The `DbDataSource` entry point. |
| `CalciteProviderFactory` | `DbProviderFactory` | Standard ADO.NET factory registration. |
| `CalciteConnectionStringBuilder` | `DbConnectionStringBuilder` | Typed connection-string keys (`Model`, `Schema`, `Synchronous`, `CaseSensitive`, `Conformance`, …). Unknown keys are preserved and forwarded. |
| `CalciteException` | `DbException` | Provider failures, including planning and execution errors. |

`CalciteConnection` also exposes Calcite-native objects directly, so there is no `Unwrap`-style
escape hatch:

- `RootSchema` → `org.apache.calcite.schema.SchemaPlus`
- `TypeFactory` → `org.apache.calcite.adapter.java.JavaTypeFactory`
- `Config` → `org.apache.calcite.config.CalciteConnectionConfig`

All three go through `RequireSession()` and throw `InvalidOperationException` when the connection is
not open.

`CalciteConnection` and `CalciteCommand` each carry `RegisterHook` overloads — for a Java
`Consumer`, for an `Action<object>`, and for each primitive, which are wrapped in `Hook.propertyJ`.
The connection's hooks and the command's are concatenated per request, connection first.

### 2. Session (`Internal/CalciteSession`)

`CalciteSession` is the per-connection engine state, created on the first `CalciteConnection.Open()`
and kept alive across `Close`/`Open` cycles — a schema registered on `RootSchema` or a table created
by DDL survives a close. `Dispose` is what ends it, and only marks the session disposed so later
execute calls throw `ObjectDisposedException`.

Construction, from the `CalciteConnectionStringBuilder`:

- Builds the root schema with `CalciteSchema.createRootSchema(addMetadataSchema: true)`, then
  applies the `Model` key through Calcite's `ModelHandler` — either `inline:`-prefixed (or
  brace-leading) JSON, or a file path, which must exist. The handler's `defaultSchemaName()` wins
  over the `Schema` key.
- Builds a `java.util.Properties` from every remaining key, translating each to the camelCase name
  Calcite expects via a static map from connection-string key to `CalciteConnectionProperty`, and
  wraps it in a `CalciteConnectionConfigImpl`.
- Creates a `JavaTypeFactoryImpl`, and resolves the default schema path to zero or one name.
- Reads the `Synchronous` key — a provider option, excluded from the engine properties like `Model` —
  which decides the convention every query on this connection is planned into.

Anything thrown here that is not already a `CalciteException` is wrapped in one.

The session exposes three private steps and the execute entry points.

**`Plan`** constructs a `PrepareContext`, pushes it onto `CalcitePrepare.Dummy`, and calls
`ClrPrepareImpl.Prepare(ctx, sql, Object[], -1, async)`, returning a `ClrSignature`. The element
type is what makes the pipeline ask for array-shaped rows; `-1` means no row limit; `async` is the
connection's mode, `true` unless `Synchronous` was set. Nothing is executed and no per-statement
state is created.

**`Bind`** builds the execution-time `DataContext`: a fresh `AtomicBoolean` cancel flag, the
positional parameters converted by `ParameterBinder`, the command timeout in milliseconds, and
`signature.InternalParameters` — assembled into one `StatementDataContext`. This mirrors what
`CalciteConnectionImpl.enumerable()` does immediately before `signature.enumerable(dataContext)`.

**`ActivateHooks` / `DeactivateHooks`** bind each `CalciteHookEntry` to the current thread with
`Hook.addThread` for the duration of one request and close the handles in a `finally`.

**`ExecuteReader` / `ExecuteReaderAsync`** share one core: plan into the connection's convention,
bind, and — unless the statement is DDL — take the plan's enumerator. **Which convention is the
connection's choice, not the entry point's**, the way Calcite's own connection can ask for the
bindable convention. By default the plan is `ClrAsyncEnumerableConvention` and the core takes
`signature.BindAsync(dataContext).GetAsyncEnumerator(cancellationToken)` into a
`CalciteAsyncEnumerableResult` — the token enters here, at the enumerator, which is the only place a
token can enter an `IAsyncEnumerable`, and cancelling it stops the leaf between rows. With
`Synchronous` set the plan is `ClrEnumerableConvention` and the core takes
`signature.Bind(dataContext).GetEnumerator()` into a `CalciteEnumerableResult`; the token is then
only checked before planning. `ExecuteReaderAsync` is the core in a completed task — planning is
synchronous work — and `ExecuteReader` is the core with `CancellationToken.None`.

**`ExecuteNonQueryAsync`** plans and binds, then branches on `signature.StatementType`:

- DDL (`CREATE`, `ALTER`, `DROP`, `OTHER_DDL`, dispatched on `name()`) has already taken effect
  during prepare, so there is nothing to enumerate and the count is `0`.
- `SELECT` reports `-1`, by ADO.NET convention.
- DML drains the enumerator. Here — and only here, where the enumeration is synchronous and
  Calcite's check-points can see it — the cancellation token is registered against the cancel flag,
  scoped to the drain. Because the plan was prepared for `Object[]` rows, the single row's element
  `[0]` is the `ROWCOUNT BIGINT` column of `RelOptUtil.createDmlRowType`, read through a `ToInt64`
  that accepts a Java boxed number or a CLR primitive.

Both entry points wrap any non-`CalciteException` failure in a `CalciteException`.

### 3. Prepare pipeline (`Apache.Calcite.Extensions/Prepare`)

This is where a statement becomes a plan. It replaces `CalcitePrepareImpl`'s driver and nothing
below it: validation, sql-to-rel, view expansion, field trimming and `optimize` are Calcite's own,
reused as they stand. The driver had to be replaced because its one exit is a `Bindable` — a linq4j
`Enumerable` — and a plan of `ClrEnumerableConvention` is a compiled delegate.

| Type | Counterpart in Calcite | Role |
| --- | --- | --- |
| `ClrPrepareImpl` | `CalcitePrepareImpl.prepare_` / `prepare2_` | The driver. Builds the catalog reader, the planner and the preparing statement; parses; executes DDL; describes the result. |
| `ClrPrepare` | `Prepare` | The algorithm: convert, checked arithmetic, flatten, decorrelate, trim, optimize, implement — with `EXPLAIN`'s two exits where Calcite has them. Knows nothing of a cluster or a schema. |
| `ClrPreparingStmt` | `CalcitePrepareImpl.CalcitePreparingStmt` | The wiring: cluster, convertlet table, schema, validator, view expansion. Also the `RelOptTable.ViewExpander` given to `SqlToRelConverter`. |
| `ClrEnumerablePreparingStmt` | — | What one convention adds: the result convention, the program, the root trait set, and the compiler. A second convention writes only these four. |
| `ClrPrepareResult` | `Prepare.PreparedResult` | What preparing produces, less `getBindable`. |
| `ClrEnumerablePrepareResult` | `PreparedResultImpl` (anonymous, in `implement`) | Carries an `IClrBindable`. |
| `ClrExplainResult` / `ClrExplainBindable` | `Prepare.PreparedExplain` / `CalcitePreparedExplain.getBindable` | An `EXPLAIN`: the text is rendered at prepare time and yielded as one row. |
| `ClrSignature` | `CalcitePrepare.CalciteSignature` | The planned statement, member for member, with `Bindable` swapped for `IClrBindable` and `enumerable` for `Bind`. |

`ClrPrepareImpl.Prepare` is the entry point this provider uses. It creates a `VolcanoPlanner` with
`RelOptUtil.registerDefaultRules` **plus** one convention's rules — `ClrAsyncEnumerableRules.Rules()`
when the `async` parameter says so, `ClrEnumerableRules.Rules()` otherwise, and never both — so
Calcite's own rules stay on the planner and a statement the chosen convention has no node for is
still planned and run in `EnumerableConvention`, with a converter carrying its rows. That is how a
table modification works here. `PrepareRel`, the second entry point, plans a `RelNode` that was
built rather than parsed; it is exercised by tests and not reached from this project.

A DDL statement is executed inside `Prepare2` rather than planned, exactly as Calcite does. The
`ClrSignature` it returns has no row type, no columns, a null bindable, `CursorFactory.OBJECT` and
`StatementType.OTHER_DDL`.

`Describe` builds one `AvaticaParameter` per dynamic parameter and one `ColumnMetaData` per result
column, deduces the `CursorFactory` from the columns and the compiled plan's element type, and
assembles the `ClrSignature`. All of that metadata is ported rather than reused: every piece of it
is a private static of `CalcitePrepareImpl`.

`ClrSignature.Bind(DataContext)` runs the plan and returns `IEnumerable<object>`, applying the row
limit when `MaxRowCount` is not negative — the limit lives on the signature, not on the bindable, so
a caller reaching past it would silently lose it. This provider always passes `-1`.

`IClrBindable` (in `Apache.Calcite.Extensions/Runtime`) is the compiled plan: `Bind(DataContext)`
returning rows, plus the `ElementType` the cursor factory is deduced from. It merges Calcite's
`Bindable` and `Typed`.

### 4. Execution contexts (`Apache.Calcite.Extensions/Prepare`)

- **`PrepareContext`** implements `CalcitePrepare.Context` over the session's type factory, root
  schema, config and default schema path. `getDataContext()` returns a throwaway
  `StatementDataContext` with only the timestamp variables, which is what backs `RexExecutorImpl`
  for constant folding during optimisation — the same thing `CalciteConnectionImpl.ContextImpl`
  does. **`getRelRunner()` refuses, with a message naming what that costs.** Calcite's
  `ContextImpl` unwraps the connection — the connection *is* the runner — and
  `RelRunner.prepareStatement` is declared to return a `java.sql.PreparedStatement`. Its one caller is
  `ServerDdlExecutor.populate`, which uses two members of it, so supporting it means a hundred-odd
  members of a JDBC interface this project exists to not have. The planning half is already here:
  `ClrPrepareImpl.PrepareRel` is the `prepare2_` branch Calcite's own runner uses, ready for a runner
  that wants it. So `CREATE MATERIALIZED VIEW` and `CREATE TABLE ... AS SELECT` are unsupported, and
  fail *after* `ServerDdlExecutor` has added the table — that ordering is upstream's. Both are pinned
  by tests. `populate` also resolves its INSERT against `getRootSchema()` unconditionally, so neither
  would work in a sub-schema even with a runner; that is a second, independent upstream limitation,
  and it has its own test.

  Note that **this is the only place JDBC is declined, not the only place it appears.** Expanding any
  view goes through `MaterializedViewTable.MATERIALIZATION_CONNECTION`, a `public static final`
  eagerly initialised from `DriverManager.getConnection("jdbc:calcite:")` and read unconditionally by
  `ViewTableMacro.apply` — which is why `CalciteSession` registers the driver. That connection supplies
  only config, type factory and a `DataContext`; `Schemas.makeContext` takes `getRootSchema()` from the
  `schema` argument, so its own empty schema is never consulted. `makeContext` has a `connection == null`
  branch that would use `CalcitePrepare.Dummy.peek()` instead — our context — but a `static final`
  cannot be null, so that path is unreachable from a view.
- **`StatementDataContext`** implements `DataContext` for execution. It holds the root schema, the
  type factory, the well-known per-statement variables (`utcTimestamp`, `currentTimestamp`,
  `localTimestamp`, `sysTimestamp`, `cancelFlag`, `queryTimeout`), the values stashed at plan time
  through `signature.InternalParameters`, and the bound positional parameters, which Calcite
  addresses as `?0`, `?1`, …. `getQueryProvider()` returns `Linq4j.DEFAULT_PROVIDER`, the non-JDBC
  equivalent of the delegation `CalciteConnectionImpl` performs.

### 5. Result stream (`Internal/CalciteResult` and friends)

`CalciteResult` is what an execute call hands back: the `ClrSignature`, a
`CalciteResultColumns` built from it, the plan's enumerator, and a records-affected count. Two
subclasses, one per convention — `CalciteEnumerableResult` over an `IEnumerator<object>`,
`CalciteAsyncEnumerableResult` over an `IAsyncEnumerator<object>` — and both answer both `Read` and
`ReadAsync`, because `DbDataReader` is a contract: a synchronous plan answers `ReadAsync` with a
completed task, and an asynchronous plan blocks in `Read`, with the synchronization context
suppressed before the plan runs so a thread carrying one does not wait on a continuation promised
to itself. The enumerator is the plan's own — a compiled delegate returns it — so nothing stands
between a row and the reader. A read wraps the current row in a `CalciteResultRow`; a null
enumerator (DDL, or a non-query) reads as an empty result. `Dispose` completes the enumerator's
disposal — blocking for it on the asynchronous result, under the same suppression — and holds
nothing else.

`CalciteResultColumns` reads the signature's Avatica `ColumnMetaData` list — name, nullability,
provider type name — and maps each to a CLR type. The SQL type name takes precedence over the
runtime representation for date, time and binary columns, because Calcite's `rep` there is the
internal storage form (`int` days, `long` millis, `ByteString`) rather than what an ADO.NET consumer
expects. Unsigned SQL types map to the unsigned CLR types. `GetSqlType` reads the signature's
`RelDataType` instead, and throws where there is none.

`CalciteResultRow` addresses a column within one row without copying it, dispatching on the cursor
factory's style: `OBJECT` (a one-column result is the value, so only ordinal `0` is valid), `ARRAY`,
or `LIST`. Any other style throws `NotSupportedException`.

`CalciteResultValue` is the final conversion, from what Calcite produced to what the caller asked
for: `GetValue` for the reader's untyped path, `GetFieldValue<T>` for the generic one, and a typed
getter per ADO.NET accessor. Each typed getter is strict — it accepts the representations Calcite
actually produces for that SQL type and throws `InvalidCastException` otherwise, naming the runtime
type, the value and the SQL type. `BigDecimalConverter` carries `java.math.BigDecimal` to and from
`decimal`.

`CalciteDataReader` holds an array of `CalciteResult`s and delegates every accessor to
`ActiveResult.Current.GetValue(ordinal)`. `NextResult` disposes the result it leaves and advances.

### 6. Parameters

- `CalciteParameter` / `CalciteParameterCollection` implement the ADO.NET parameter model. Where
  `DbType` was not set explicitly it is inferred from the value's CLR type by `CalciteTypeMap`.
- `CalciteParameterValue` is the `(DbType, object?)` pair carried into the request.
- `CalciteExecuteRequest` is the payload the session executes: SQL text, an
  `ImmutableArray<CalciteParameterValue>` in placeholder order, the command timeout in seconds, and
  the request's hooks. It also carries `ClampToInt32`, which the `ExecuteNonQuery` surfaces use to
  narrow a `long` row count.
- `ParameterBinder` converts each value to the representation Calcite's runtime expects — Java boxed
  primitives, `BigDecimal`, `ByteString`, `joou` unsigned types, and the internal forms for
  temporals: days since epoch for `DATE`, milliseconds since epoch for `TIMESTAMP`, milliseconds
  since midnight for `TIME`. Where the `DbType` is `Object` or unrecognised it infers from the CLR
  type instead.

### 7. Metadata and configuration

- `CalciteConnectionStringBuilder` defines the supported keys and preserves unknown ones;
  `CalciteSession` is what turns it into a `Properties` and a `CalciteConnectionConfigImpl`.
- `CalciteSchemaInfo` builds the `DataTable`s behind `DbConnection.GetSchema` —
  `MetaDataCollections` and `Restrictions` without a session, and `DataSourceInformation`,
  `DataTypes`, `ReservedWords`, `Tables` and `Columns` from an open one. **`Tables` and `Columns` are
  two enumerations concatenated, because a view is not a table.** Both routes to a view register it
  as a `TableMacro` of no arguments — `ModelHandler.visit(JsonView)` and
  `ServerDdlExecutor.execute(SqlCreateView, …)` both call `schema.add(name, ViewTable.viewMacro(…))` —
  and that lands in the schema's function map, so `getTableNames()` never returns one whatever its
  javadoc says. `TablesOf` reads the function map for names that resolve to a nullary `TableMacro`.
  **A view is expanded to be described, so the name restriction is applied before the expansion.**
  `ViewTableMacro.apply` opens the materialization connection and parses, validates and converts the
  view's SQL — the whole front end, per view. `CalciteMetaImpl.tables` concatenates
  `getTablesBasedOnNullaryFunctions()`, which builds that map eagerly for the whole schema; this asks
  `getTableBasedOnNullaryFunction` for the names a caller actually gave. That divergence is
  deliberate: `CalciteMetaImpl` is Avatica's JDBC metadata and this is not a port of it, so there is
  no behaviour to reproduce, only a schema SPI to read correctly. An *unrestricted* listing is still
  eager, because `TABLE_TYPE` comes from `Table.getJdbcTableType()` and typing a view means expanding
  it; short-cutting that from the macro's class would be a guess, since `ViewTableMacro.apply` is
  overridable and `MaterializedViewTable.MaterializedViewTableMacro` overrides it.
- `CalciteTypeMap` maps between `DbType` and CLR types for the parameter surface. Result columns do
  not go through it; `CalciteResultColumns` maps those from the Avatica metadata.

### 8. Diagnostics and errors

- `CalciteException` is the provider's exception type. The session wraps every non-`CalciteException`
  planning or execution failure in one, so a caller sees a single error type.
- `ObjectDisposedException` is thrown when a disposed session or result is used.
- Cancellation is honoured before planning on both paths, and during the drain of a DML statement.
  In the default mode the token given to `ExecuteReaderAsync` also reaches the plan's enumerator, so
  cancelling it stops the leaf between rows; in synchronous mode it is not wired to a reader's
  enumeration.

---

## End-to-End Execution Flow

**A query.**

1. **Construct.** The caller creates a `CalciteConnection` (directly, through
   `CalciteProviderFactory`, or from a `CalciteDataSource`) with a connection string.
2. **Open.** `Open()` creates the `CalciteSession` on first call: root schema, model, config, type
   factory, default schema path.
3. **Build command.** The caller sets `CommandText` and adds parameters to a `CalciteCommand`.
4. **Request.** `ExecuteReader` builds a `CalciteExecuteRequest` from the text, the parameters, the
   timeout and the resolved hooks, and hands it to the session's reader core.
5. **Plan.** The session pushes a `PrepareContext` onto `CalcitePrepare.Dummy` and calls
   `ClrPrepareImpl.Prepare`, which parses, validates, converts to relational algebra, optimises into
   the connection's convention — `ClrAsyncEnumerableConvention` by default,
   `ClrEnumerableConvention` when the connection string says `Synchronous` — and compiles the chosen
   plan to a delegate. The result is a `ClrSignature`.
6. **Bind.** Parameters are converted and assembled with the cancel flag, the timeout and the
   signature's internal parameters into a `StatementDataContext`.
7. **Execute.** The plan's enumerator is taken — `BindAsync(...).GetAsyncEnumerator(token)` or
   `Bind(...).GetEnumerator()` by mode — and wrapped in the matching `CalciteResult`. Nothing has
   been enumerated yet.
8. **Read.** `CalciteDataReader` pulls rows through `CalciteResult.ReadAsync`, and each accessor
   goes `CalciteResultRow` → `CalciteResultValue` → CLR value.
9. **Dispose.** Disposing the reader disposes the result and its enumerator. Disposing the
   connection disposes the session.

**A non-query** follows steps 1–6, then branches on the statement type as described above and
returns a `CalciteResult` with a count and no enumerator.

**DDL** never reaches step 7: it took effect during step 5, and the signature it produced has no
plan to bind.

**`EXPLAIN`** never reaches `Implement`. `ClrPrepare.PrepareSql` renders the plan or the type as
text and returns a `ClrExplainResult`; `Describe` wraps that text in a `ClrExplainBindable`, which
yields one row.

It is read by either reader, and `ClrExplainBindable` is the only bindable that is both an
`IClrBindable` and an `IClrAsyncBindable`. Not a relaxation of the rule that each convention refuses
the other's reader — an `EXPLAIN` is of neither convention, holding a string rather than a plan, so
there is no pull for an awaited reader to hide. **Which convention gets explained is still decided
by which method was called**, because the plan is optimized under that program before it is
rendered: `ExecuteReaderAsync` on an `EXPLAIN` renders `ClrAsyncEnumerable*` nodes, and fails to
plan wherever the query itself would.

---

## Direct Engine Access

`CalciteConnection` exposes selected Calcite-native objects as public properties rather than
providing a JDBC-style `unwrap`. This keeps the contract typed and discoverable while letting
advanced consumers register schemas, tables, functions and views on `RootSchema`, build types with
`TypeFactory`, and inspect resolved configuration through `Config`.

A user-defined function written in .NET works here and in no plan Janino compiles: IKVM names a CLR
class `cli.Namespace.Type`, which `EnumerableConvention` would write into generated Java source and
Janino cannot resolve. This convention holds the method rather than its name.

The prepare and plan APIs are not exposed on the ADO.NET surface. A caller who wants them references
`Apache.Calcite.Extensions` and uses `ClrPrepareImpl` directly.

---

## Project Layout

```
src/
  Apache.Calcite.Data/                    ADO.NET provider (this design)
    CalciteConnection.cs                  DbConnection, native accessors, hook registration
    CalciteCommand.cs                     DbCommand
    CalciteDataReader.cs                  DbDataReader over one or more results
    CalciteBatch.cs                       DbBatch
    CalciteBatchCommand.cs                DbBatchCommand
    CalciteBatchCommandCollection.cs      DbBatchCommandCollection
    CalciteParameter.cs                   DbParameter
    CalciteParameterCollection.cs         DbParameterCollection
    CalciteTransaction.cs                 DbTransaction (Commit/Rollback throw)
    CalciteDataSource.cs                  DbDataSource
    CalciteProviderFactory.cs             DbProviderFactory
    CalciteConnectionStringBuilder.cs     DbConnectionStringBuilder
    CalciteException.cs                   DbException
    Internal/
      CalciteSession.cs                   Per-connection engine state; plan, bind, execute
      CalciteExecuteRequest.cs            Execute payload
      CalciteParameterValue.cs            (DbType, value) pair
      ParameterBinder.cs                  CLR value → Calcite runtime representation
      BigDecimalConverter.cs              decimal ↔ java.math.BigDecimal
      CalciteResult.cs                    Row stream over a ClrSignature
      CalciteResultColumns.cs             Avatica ColumnMetaData → ADO.NET column metadata
      CalciteResultRow.cs                 Column addressing within one row, by cursor style
      CalciteResultValue.cs               Final value conversion and typed getters
      CalciteTypeMap.cs                   DbType ↔ CLR type, for parameters
      CalciteSchemaInfo.cs                GetSchema collections
      CalciteHookEntry.cs                 (Hook, Consumer) pair
      CalciteColumn.cs                    Unreferenced

  Apache.Calcite.Extensions/              The convention and the prepare pipeline
    Prepare/
      ClrPrepareImpl.cs                   The driver: parse, plan, execute DDL, describe
      ClrPrepare.cs                       The algorithm, less any wiring
      ClrPreparingStmt.cs                 Cluster, validator, view expansion
      ClrPrepareResult.cs                 What preparing produces
      ClrSignature.cs                     The planned statement
      ClrExplainResult.cs                 EXPLAIN, rendered at prepare time
      ClrExplainBindable.cs               …and yielded as one row
      PrepareContext.cs                   CalcitePrepare.Context
      StatementDataContext.cs             DataContext for execution
      Enumerable/
        ClrEnumerablePreparingStmt.cs     Convention, program, traits, compiler
        ClrEnumerablePrepareResult.cs     Carries the IClrBindable
    Runtime/IClrBindable.cs               The compiled plan
    Adapter/Enumerable/                   ClrEnumerableConvention: nodes, rules, implementor
    Linq4j/, Interop/                     linq4j → System.Linq.Expressions, Java ↔ CLR values

  Apache.Calcite.Data.Tests/              xUnit tests for this provider
  Apache.Calcite.Tests/                   xUnit tests for the convention and the pipeline
```

---

## Design Constraints

- **Nothing on the statement path goes through JDBC or Avatica plumbing.** Calcite's JDBC driver is
  registered once, for view expansion, and no statement executed here reaches it. Avatica's
  `ColumnMetaData`, `AvaticaParameter` and `Meta.*` are used as the metadata value types Calcite's
  prepare produces, and nothing more.
- **No `Bindable` and no `PreparedResult` on the row path.** A plan is a compiled delegate behind
  `IClrBindable`; `ClrSignature` and `ClrPrepareResult` exist because Calcite's equivalents are
  declared in terms of the linq4j types this convention does not produce.
- **The ADO.NET surface owns no engine logic.** Everything that touches Calcite's planner is in
  `CalciteSession` and below it. The `Internal` types are reachable from the public surface; the
  reverse does not happen.
- **Idiomatic .NET.** Public types follow .NET naming and `IDisposable` conventions. JDBC concepts
  are translated, not copied.
- **Targeting.** The provider targets .NET 8, and is verified on .NET 8 and .NET 10.
