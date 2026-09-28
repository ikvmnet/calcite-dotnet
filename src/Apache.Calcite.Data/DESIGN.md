# Apache.Calcite.Data — Design

`Apache.Calcite.Data` is an ADO.NET provider for Apache Calcite. It exposes Calcite through the
`System.Data.Common` abstractions and runs Calcite's parser, validator and planner in-process through IKVM.

Statements do not go through Calcite's own prepare driver. `Apache.Calcite.Extensions` owns the pipeline that
takes SQL text to a plan in `ClrCursorConvention` and compiles that plan to a `System.Linq.Expressions` tree;
this project drives the pipeline and adapts what it produces to `DbDataReader`. Janino is not used on the
statement path.

This document describes how the provider is structured, how a statement flows from a caller into Calcite and
back, and why.

---

## Scope

- **A provider, not an adapter.** This is a .NET application's entry point into Calcite.
  `Apache.Calcite.Adapter.AdoNet` goes the other way, exposing an ADO.NET data source to Calcite.
- **In-process.** Calcite's Java code is loaded through IKVM and called directly. No statement goes through
  Calcite's JDBC driver or the Avatica wire protocol.
- **JDBC parity in behaviour, .NET in shape.** Connection properties, model handling, SQL semantics and
  metadata follow Calcite's JDBC driver; the public surface follows ADO.NET conventions (`Db*` base classes,
  PascalCase, `IDisposable`/`IAsyncDisposable`).

### What remains Calcite's

- **Avatica's metadata value types.** `ColumnMetaData`, `AvaticaParameter`, `Meta.CursorFactory` and
  `Meta.StatementType` come from `org.apache.calcite.avatica`, because Calcite's prepare produces them. They are
  used as plain descriptors; no Avatica `Meta`, service or connection is constructed.
- **`CalcitePrepare.Context` and `CalcitePrepare.Dummy`.** `PrepareContext` implements the former, which is how
  Calcite's validator, catalog reader and `SqlToRelConverter` reach the schema, type factory and
  configuration. The latter is a thread-local stack that Calcite's parse-to-rel reads the context from, so
  `CalciteSession.Plan` pushes onto it for the duration of a prepare. `CalcitePrepare.DEFAULT_FACTORY` and
  `prepareSql` are not called for statements.
- **Calcite's JDBC driver, for views only.** `ViewTableMacro.apply` analyses a view through
  `MaterializedViewTable.MATERIALIZATION_CONNECTION`, a static field initialized with
  `DriverManager.getConnection("jdbc:calcite:")`, so expanding any view needs the driver registered.
  `CalciteSession`'s static constructor puts the driver's assembly on IKVM's boot class path (so
  `UnregisteredDriver` can load its factory by name) and constructs a `Driver`, whose static initializer
  registers it.
- **A view's definition is analysed under Calcite's default configuration.** Because analysis goes through
  that materialization connection, `Schemas.analyzeView` builds its context from that connection's
  configuration, and the connection's `fun`, `conformance` and `caseSensitive` do not apply to a view
  definition, although `ClrPrepareImpl.PreparingStmt.expandView` uses the connection's configuration when the
  view is later expanded into a query. Calcite's own JDBC driver behaves the same way, and this provider
  reproduces it rather than diverging. A view registered from code uses `ViewTable.viewMacro` like any other;
  pass `CalciteSchema.from(schema).path(name)` as the view path so that Calcite can detect a view defined in
  terms of itself and raise `CyclicDefinitionException`.

---

## The driver this one is modelled on

Where ADO.NET leaves a provider a choice, **`Microsoft.Data.SqlClient` decides it.** It is the
`DbDataReader` most .NET code is written against, so code pointed at this provider should meet the same rules.
Questions are settled by reading its source, `SqlBuffer.cs` and `SqlDataReader.cs` in `dotnet/SqlClient`,
rather than its documentation. Where the source does not settle a question plainly, run it.

This is unrelated to `Apache.Calcite.Adapter.AdoNet`'s SQL Server support, which pushes a plan down to SQL
Server as a back end.

What it settles:

- **A typed getter is a cast, not a conversion.** `SqlBuffer.Int32` returns the stored `int` where the storage
  type is `Int32` and otherwise `(int)Value`; `Guid` accepts `Guid` and `SqlGuid` and otherwise
  `(Guid)Value`. So `GetInt32` over a `bigint` throws and `GetGuid` never parses text. Here, `GetInt64` over an
  `INTEGER` column throws, and `GetGuid` over a `VARCHAR` column throws.
- **`GetFieldValue<T>` is the same cast**, with its fast paths chosen by the value's storage type and
  `(T)GetValue()` as the fallback, plus narrowing conveniences such as `DateOnly` over a `DateTime` store. Here,
  `GetFieldValue<T>` answers the column's default reading and the other readings the type-mapping registry
  has for the column's type.
- **`sql_variant` is `ANY`.** SqlClient gives `sql_variant` the class type `object` and reads the inner value
  with the same reader the non-variant path uses. Here, `ANY`, `OTHER` and `VARIANT` columns report `object`,
  and the value's own type decides which getter reads it, with the same strictness as a declared column.
- **`ReadAsync(token)` registers the token against the statement before checking it,** so a token that is
  already cancelled cancels the statement rather than only the call.

SqlClient has no counterpart for Calcite's `ARRAY`, `MULTISET`, `MAP`, `ROW` and `VARIANT`, whose runtime
forms are Java objects. `CalciteValues`, `CalciteVariants` and the collection mappings in
`Apache.Calcite.Data.Common` exist for that gap.

---

## Layers

### 1. ADO.NET surface

| Class | Base | Role |
| --- | --- | --- |
| `CalciteConnection` | `DbConnection` | Owns the connection state and a `CalciteSession`; draws its root schema from a `CalciteDataSource`; exposes Calcite objects and hook registration. |
| `CalciteCommand` | `DbCommand` | Holds the SQL text and parameters; builds a `CalciteExecuteRequest` and calls the session. |
| `CalciteDataReader` | `DbDataReader` | Reads one or more `CalciteResult`s; `NextResult` moves through a batch's results. |
| `CalciteBatch`, `CalciteBatchCommand`, `CalciteBatchCommandCollection` | `DbBatch`, `DbBatchCommand`, `DbBatchCommandCollection` | Runs several statements in order on one session. |
| `CalciteParameter`, `CalciteParameterCollection` | `DbParameter`, `DbParameterCollection` | Positional parameters; `ParameterName` is used only for lookup. |
| `CalciteTransaction` | `DbTransaction` | The declared transaction type. Never instantiated, because `BeginTransaction` throws. |
| `CalciteDataSource` | `DbDataSource` | Holds the root schema for the connections it produces. |
| `CalciteDataSourceBuilder` | — | Builds a data source from a connection string plus schema instances, root-schema steps and type resolvers. |
| `CalciteProviderFactory` | `DbProviderFactory` | Factory registration. |
| `CalciteConnectionStringBuilder` | `DbConnectionStringBuilder` | Typed keys; unrecognized keys are kept and passed to Calcite. |
| `CalciteException` | `DbException` | Wraps every failure from Calcite. |

`CalciteConnection` exposes Calcite objects as typed properties rather than through an `Unwrap` method:
`RootSchema` (as Calcite's read interface `Schema`, since the root is shared), `TypeFactory` and `Config`. All
three require an open connection.

`CalciteConnection` and `CalciteCommand` each have `RegisterHook` overloads for a Java `Consumer`, an
`Action<object>`, and each primitive (wrapped with `Hook.propertyJ`). A command execution attaches the
connection's hooks and then the command's to the executing thread with `Hook.addThread` while the statement is
planned and opened, and detaches them before returning. Batches pass no hooks.

### 2. Data source, root and session

Calcite's JDBC connection is long-lived and owns its root schema; an ADO.NET connection is short-lived. The
provider therefore splits Calcite's connection in two: the part that lives as long as the application, held
by a data source, and the part that lives as long as an ADO.NET connection.

**`CalciteDataSourceRoot`** (`Internal/`) is the application-lived part: the root schema, `DUAL` where the
conformance supports it, and the model. It runs the steps of `CalciteConnectionImpl`'s constructor that build
the root and `DUAL`, then the model step of the driver's `onConnectionInit`, in that order, so a model can
replace `DUAL`. Without a `Model` key, a `SchemaFactory` or `SchemaType` key produces an inline model with one
custom schema, named by the `Schema` key (default `adhoc`), with every `schema.`-prefixed key as an operand,
as Calcite's `Driver.createHandler` does. Steps registered on a `CalciteDataSourceBuilder` run last.

A root counts the sessions using it. `Retire` marks it unwanted; it is disposed when retired and unused.
Disposing a root disposes every schema on it that implements `IDisposable`, sub-schemas first, which gives
adapters the release point Calcite's schema SPI lacks.

**`CalciteDataSource`** builds its root once, under a lock, when the first connection opens; a failed build
leaves nothing behind, so the next connection retries. Under `Pooling=false` it builds a root per connection,
which the connection's session owns and retires. `Clear` retires the current root so the next connection
builds a new one; `Dispose` retires it and refuses further connections. In both cases connections already open
keep working until disposed.

**`CalciteDataSources`** (`Internal/`) is the process-wide dictionary of data sources the provider keeps for
connections created from a connection string alone, keyed by `CalciteConnectionStringBuilder.DataSourceKey`
(the connection string with its keys lower-cased and sorted). An empty connection string gets a new,
unregistered data source. Entries are held strongly, because an entry exists to be there for the next
connection when nothing references it; time bounds the set instead. Each entry's timer fires every
`Connection Pruning Interval` seconds and removes the entry once its root has had no session for
`Connection Idle Lifetime` seconds. `ClearPool` and `ClearAllPools` remove entries on demand, and remaining
entries are released at process exit and domain unload. A connection that finds its entry pruned between
lookup and use looks it up again. The two keywords and their validation follow the established .NET
providers.

**`CalciteSession`** (`Internal/`) is the connection-lived part, created on the connection's first `Open` and
kept across `Close`/`Open`. Its constructor is the rest of `CalciteConnectionImpl`'s:

- It builds a `java.util.Properties` from the engine's keys through `CalciteEngineProperties`, which renames
  known keys to Calcite's camelCase names and passes unknown ones through, and wraps it in a
  `CalciteConnectionConfigImpl`. `Model`, `Pooling`, the two pruning keys and `TypeSystem` are the provider's
  and are left out.
- It creates a `JavaTypeFactoryImpl` over the type system the `TypeSystem` key names, resolved by
  `ClrPlugin`, wrapped to convert ragged unions to varying types where the conformance asks. The type factory
  is per session and the root per data source, as when Calcite opens an internal connection over an existing
  root (`CalciteMetaImpl.connect(schema.root(), null)`); this is required, because
  `JavaTypeFactoryImpl.syntheticTypes` is an unsynchronized `HashMap` written while planning aggregates and
  windows.
- It binds the connection's type-resolver chain to that factory in a `ClrTypeRegistry`. The chain is read
  once, which is why `CalciteConnection.TypeMapper` is unavailable after the first `Open`.
- It resolves the default schema path to zero or one name; the model's `defaultSchema` takes precedence over
  the `Schema` key.

`Dispose` marks the session disposed, releases its count on the root, and retires the root first where the
session owns it. Anything thrown during construction that is not a `CalciteException` is wrapped in one.

**Concurrency.** Sharing a root means an adapter's schema can be read from several threads at once, and
Calcite does not serialize access to a root: a `CalciteSchema` keeps its tables and sub-schemas in `NameMap`s
over `TreeMap`s, and DDL writes into them. The root therefore carries a `ReaderWriterLockSlim`. `Plan` holds
the read lock from the snapshot `PrepareContext` takes until the signature is built, and the `GetSchema`
builders hold it while they read. DDL is recognized only after parsing, inside the prepare, so
`ClrPrepareImpl` finds the lock on the context, releases the read lock, takes the write lock for the DDL, and
takes the read lock again for `Plan` to release. No statement plans against a root that DDL is changing.
Execution is not covered: the lock is thread-affine and a result may be read across awaits, and a running plan
resolves tables again from its snapshot, whose table map is shared with the live root. A table lookup during
execution is therefore not protected against concurrent DDL.

**Session operations.**

- **`Plan`** constructs a `PrepareContext`, pushes it onto `CalcitePrepare.Dummy`, and calls
  `ClrPrepareImpl.PrepareSql(ctx, query, typeof(object[]), -1)`, which returns a `Signature` whose plan is a
  compiled cursor factory. The element type asks for array-shaped rows, and `-1` means no row limit. DDL is
  executed inside this call; nothing else is.
- **`Bind`** builds the execution `DataContext`, a `StatementDataContext` over the signature's snapshot root,
  holding the parameters converted by `ParameterBinder`, the command timeout in milliseconds, the values
  planning stashed in `signature.InternalParameters`, and a cancel flag tied to the statement's cancellation
  token. This mirrors what `CalciteConnectionImpl.enumerable()` does before `signature.enumerable(dataContext)`.
- **`ExecuteReader`** and **`ExecuteReaderAsync`** plan, bind and, except for DDL, open the plan's cursor:
  synchronously through `signature.Open`, or with await through `signature.OpenAsync`. Opening runs the
  plan's acquisition, as obtaining an enumerator does in linq4j: every operator acquires its input, a sort
  drains, a table adapter sends its query. The cursor supports both `Read` and `ReadAsync(token)` whichever way
  it was opened, so neither method decides how rows are read. The statement gets its own
  `CancellationTokenSource`, linked to the caller's token on the asynchronous path, so a token given later to
  `ReadAsync` has something to cancel.
- **`ExecuteNonQuery`** plans and binds, then branches on `signature.StatementType`: DDL has already taken
  effect and reports 0; a query reports -1 and is not run; DML is run by reading the first row of its cursor,
  whose single `ROWCOUNT` column (from `RelOptUtil.createDmlRowType`) is the count. A one-column result is the
  value itself (`Meta.CursorFactory.deduce` answers `OBJECT`), so the row is the boxed count.
  `ExecuteNonQueryAsync` returns `ExecuteNonQuery`'s result as a completed task: the modification is Calcite's
  `EnumerableTableModify` under a converter, which has nothing to await.

Every execute method wraps any failure that is not a `CalciteException` in one.

### 3. Prepare pipeline (`Apache.Calcite.Extensions/Prepare`)

The pipeline replaces `CalcitePrepareImpl`'s driver and nothing below it: validation, sql-to-rel, view
expansion, field trimming and optimization are Calcite's. The driver is replaced because its only output is a
`Bindable` producing a linq4j `Enumerable`, and a plan of `ClrCursorConvention` is a factory that opens a
cursor.

| Type | Counterpart in Calcite | Role |
| --- | --- | --- |
| `ClrPrepareImpl` | `CalcitePrepareImpl.prepare_` / `prepare2_` | The driver: builds the catalog reader, planner and preparing statement; parses; executes DDL; describes the result. |
| `ClrPrepare` | `Prepare` | The algorithm: convert, flatten, decorrelate, trim, optimize, implement, with `EXPLAIN`'s exits. |
| `ClrPrepareImpl.PreparingStmt` | `CalcitePrepareImpl.CalcitePreparingStmt` | Cluster, convertlet table, schema, validator, view expansion. |
| `ClrCursorPreparingStmt` | — | The result convention, root traits, and the implement step that builds the factory. |
| `ClrExplainBindable` | `CalcitePreparedExplain.getBindable` | An `EXPLAIN`, rendered at prepare time and returned as one row. |
| `IClrPrepare.Signature` | `CalcitePrepare.CalciteSignature` | The planned statement, with `Open` and `OpenAsync` in place of `enumerable`. |

`ClrPrepareImpl.CreatePlanner` registers Calcite's default rules and then `ClrCursorRules.Rules()`, so
Calcite's own rules stay on the planner and a node this convention does not implement is planned in
`EnumerableConvention` under a converter. That is how table modification runs.

A DDL statement is executed inside the prepare, as Calcite does. Its signature has no row type and no columns,
`CursorFactory.OBJECT`, and a DDL statement type.

`Describe` builds one `AvaticaParameter` per dynamic parameter and one `ColumnMetaData` per result column and
deduces the `CursorFactory`. This code is ported, because the corresponding members of `CalcitePrepareImpl`
are private.

### 4. Execution contexts (`Apache.Calcite.Extensions/Prepare`)

- **`PrepareContext`** implements `CalcitePrepare.Context` over the session's type factory, root schema,
  configuration and default schema path, and carries the root lock. `getDataContext()` returns a
  `StatementDataContext` with no parameters, cancellation or timeout, for constant folding during planning, as
  `CalciteConnectionImpl.ContextImpl` does. `getRelRunner()` throws: Calcite's runner is a JDBC connection
  whose `prepareStatement` returns a `java.sql.PreparedStatement`, and its one caller,
  `ServerDdlExecutor.populate`, serves `CREATE MATERIALIZED VIEW` and `CREATE TABLE ... AS SELECT`. Those two
  statements are therefore unsupported, and fail after `ServerDdlExecutor` has created the table, which is the
  order Calcite uses.
- **`StatementDataContext`** implements `DataContext` for execution: the root schema, the type factory, the
  per-statement variables (the timestamps, time zone, locale, cancel flag and `timeout`), the values planning
  stashed, and the positional parameters, addressed as `?0`, `?1`, …

The `timeout` variable carries `CommandTimeout`. Calcite reads it only in `ResultSetEnumerable`, which applies
it to statements its JDBC adapter sends; nothing else in a plan enforces it.

### 5. Results (`Internal/CalciteResult` and related types)

`CalciteResult` is what every execute path returns and what a reader holds per result set: the columns, the
affected-row count and the current row. Its one subclass, `CalciteCursorResult`, owns the plan's cursor, the
data context and the cancellation source. `Read` is the cursor's synchronous advance, which blocks only where a
table can produce rows asynchronously and no other way, and does so with the synchronization context
suppressed. `ReadAsync(token)` is the awaiting advance with that call's token, which also registers the token
against the statement's cancellation for the length of the call, so Calcite's operators, which poll the cancel
flag, stop too. Both are always supported because `DbDataReader` is a contract that generic consumers call
`Read` on.

`CalciteDataReader.CloseAsync` and `DisposeAsync` are overridden so that `await using` awaits the cursor's
release instead of falling back to the synchronous `Close`.

`CalciteResultColumns` answers naming questions (label, SQL type name, nullability) from Avatica's
`ColumnMetaData`, and everything about a column's .NET type from its `RelDataType` through the registry:
`GetFieldType` and every value accessor answer from the same place. Avatica's type cannot serve here, because
its representation for date, time and binary columns is the storage form (`int` days, `long` milliseconds,
`ByteString`), it has no representation for `UUID`, and for an array it carries the element's
representation but not the element's nullability. A column whose type no mapping covers is refused rather
than reported as `object`, because `object` is the real answer for `ANY`, `OTHER` and `VARIANT` and a caller
reading `GetFieldType` would otherwise be told to ask for `object` and then refused. Each column's type and
mapping are resolved once per result and cached, since the registry lookup costs several times the conversion
it selects; the cache arrays are shared by the struct copies each row receives.

The registry's own cache is keyed on the `RelDataType` instance. Calcite canonizes types through a
process-wide cache keyed on the type's digest, so equal types are the same instance across type factories.
Interning makes the instance key fast; it is not what makes it correct, since two instances describing one
type would each resolve their own, correct entry.

`CalciteResultRow` addresses a column of the current row in place, by the cursor factory's style: `OBJECT`
(a one-column result, whose row is the value), `ARRAY` or `LIST`. Any other style throws.

`CalciteResultValue` is the final conversion from what Calcite produced to what the caller asked for.

- **Every accessor is a registry lookup**, and nothing in it switches on a Java class or a `SqlTypeName`. A
  typed getter answers where the registry pairs the column's `RelDataType` with the getter's type, and throws
  `InvalidCastException` naming the value, its SQL type and the target otherwise. The same table decides what
  `GetFieldValue<T>` accepts and what a parameter may be written as, so reading and writing cannot drift apart.
- **The column's type is asked about, never the class the value is held in.** Calcite holds a `DATE` as a
  count of days in a `java.lang.Integer`, so matching on the class would let `GetInt32` return a day count from
  a column `GetFieldType` reports as `DateTime`. `ClrTypeMapping.RepresentationType` and `ClrType` keep the
  holding class and the presented type apart.
- **`ANY`, `OTHER` and `VARIANT` describe nothing**, identified by `ClrTypeMapping.DescribesValue`. For them the
  value's own class stands in for the declared type, with the same strictness: a `java.lang.Integer` in an
  `ANY` column reads through `GetInt32` and not `GetInt64`, and a `java.time.LocalDate` reads through
  `GetDateOnly`. A column with a declared type is unaffected.
- **Nulls** are the mapping's answer too (`ClrTypeMapping.IsNull`), because a `VARIANT`'s nulls
  (`VariantSqlNull`, `VariantNull`) are objects. Every accessor asks `IsDbNull`.
- **`GetFieldValue<T>`** tries the column's default reading first, so `GetFieldValue<object>` is `GetValue` and
  `GetFieldValue<int[]>` reads an `INTEGER ARRAY`. It then tries the registry's mapping between the column's
  type and `T`, which reaches readings that are not the default (`DateOnly` for a `DATE`). Finally it reshapes a
  collection or map to element types `T` names, where the elements already have those types; naming
  `DateOnly[]` for a `DATE ARRAY` puts `DateOnly` into the element lookup. Naming the Java class a value is held
  in is refused; `CalciteDataReader.GetCalciteValue` is the only route to a Java object.
- **`GetArray<T>`** is `GetFieldValue<T[]>` plus a refusal of a null column, so the two cannot disagree. A null
  element is refused where `T` cannot hold one, because `Array.SetValue` would silently write `default(T)`.

`CalciteReaderMatrixTests` records every accessor against every Calcite type, as a column and as an array
element, and compares the grid with `ReaderMatrix.txt`. Most of the grid is refusals, so a change to what any
accessor accepts shows up as a diff to justify.

**Values.** The conversions themselves live in `Apache.Calcite.Data.Common` (the mappings) and in
`Internal/CalciteValues` (recursive conversion of collections, maps and rows, and reshaping for
`GetFieldValue<T>`). Calcite holds an `ARRAY` or `MULTISET` as a `java.util.List`, a `MAP` as a
`java.util.Map` and a `ROW` as an `Object[]`. A list becomes an array whose element type is the type its
converted elements share (`int[]`, `int?[]` where one is null, `object[]` where they differ); a map becomes a
`Dictionary<TKey, TValue>` by the same rule, or a `KeyValuePair<,>[]` where a key is null, since Calcite allows
one and `Dictionary` does not; a row stays `object[]`. The `RelDataType` descends with the conversion, because
a `DATE` inside an array is a count of days and only the component type says so.

**Variants.** `VariantClrTypeMapping` reads a `VARIANT`, whose runtime form is a `VariantValue`, using two
public calls: `getTypeString()` names the payload's type, and `cast()` to a `BasicSqlTypeRtti` of that same
type returns the payload in Calcite's storage form for the mapping of that type to decode. `cast` is Calcite's
SQL cast and converts, so it is only called with the variant's own type. An array is walked with `item(1)`,
`item(2)`, … until null, because a variant records only `ARRAY`, not its element type. A map's keys are read by
casting to `MAP<VARCHAR, VARCHAR>`, which returns the keys without their values, and each value with
`item(key)`. An interval names itself by its scale rather than by a `SqlTypeName`, so the year-month family
(a count of months) and the day-time family (milliseconds) are decoded separately, reading as `int` and
`TimeSpan` as declared intervals do. A `MULTISET`, a `ROW`, and a map whose keys are not character values have
no public route to their contents and are refused with an exception naming the type, rather than returning the
Java object or inventing a text form. `Internal/CalciteVariants` is a second implementation of the same reading, reached
only when `CalciteValues.TryConvertTo` meets a variant inside a collection.

`CalciteDataReader` holds an array of `CalciteResult`s and delegates every accessor to
`ActiveResult.Current.GetValue(ordinal)`. `NextResult` disposes the result it leaves.

### 6. Parameters

- `CalciteParameter` states its type three ways, `DbType`, `CalciteDbType` and `RelDataType`, kept consistent:
  setting one sets the others to the nearest equivalent. Where none is set, `DbType` is inferred from the
  value's CLR type by `CalciteTypeMap`.
- `CalciteExecuteRequest` is what the session executes: the SQL text, a snapshot of each parameter's `DbType`
  and value (`CalciteParameterValue`) in placeholder order, the command timeout in seconds, and the hooks. It
  also provides `ClampToInt32` for narrowing row counts.
- `ParameterBinder` converts each value through the session's registry to the type Calcite inferred for its
  placeholder. The parameter's `DbType` selects which .NET type the value is converted from, where the registry
  has a mapping between that type and the placeholder's; otherwise the placeholder type's default mapping is
  used. Converting to the inferred type matters because the plan reads the value as that type whatever the
  caller said; converting at all matters because a .NET object in a plan whose row types are Java classes fails
  the first comparison it meets.

### 7. Metadata and configuration

- `CalciteConnectionStringBuilder` defines the recognized keys and keeps unrecognized ones;
  `CalciteEngineProperties` turns it into Calcite's `Properties`.
- `ClrPlugin` resolves a plugin named in the connection string (the type system) the way
  `AvaticaUtils.instantiatePlugin` does, with .NET naming: `Namespace.Type, Assembly` for a type,
  `[Namespace.Type, Assembly]::Member` for a static member, and Calcite's `Type#MEMBER`.
- `CalciteSchemaInfo` builds the `GetSchema` tables: `MetaDataCollections` and `Restrictions` without a
  session, and `DataSourceInformation`, `DataTypes`, `ReservedWords`, `Tables` and `Columns` from an open one.
  `Tables` and `Columns` read tables and views separately, because Calcite registers a view, from a model or
  from `CREATE VIEW`, as a no-argument `TableMacro` in the schema's function map, where `getTableNames()` does
  not see it. Describing a view means expanding it (`ViewTableMacro.apply` parses, validates and converts its
  SQL), so the name restriction is applied before expansion, through `getTableBasedOnNullaryFunction`, and only
  views that pass it are expanded. Calcite's `CalciteMetaImpl.tables` expands every view; this is not a port of
  Avatica's JDBC metadata, and restricting first keeps one unresolvable view from breaking metadata calls about
  other tables. The table-type restriction is applied after expansion, because the type comes from
  `Table.getJdbcTableType()` and a macro's class does not reliably say what it produces.
- `CalciteTypeMap` maps between `DbType` and CLR types for parameters. Result columns do not use it.

### 8. Errors and cancellation

- `CalciteException` is the provider's exception type. The session wraps every other failure in one, so a
  caller catches one type.
- `ObjectDisposedException` is thrown when a disposed connection, session or result is used.
- The caller's token is checked before planning. On the asynchronous reader path it is linked into the
  statement's cancellation source, which is tied to Calcite's cancel flag; each `ReadAsync` token is passed to
  every operator for that advance and also cancels the statement. `DbCommand.Cancel` does nothing.

---

## End-to-end flow

**A query.**

1. **Construct.** The caller creates a `CalciteConnection` directly, through `CalciteProviderFactory`, or from a
   `CalciteDataSource`.
2. **Open.** `Open` resolves the connection's data source (the one it came from, or the one the provider keeps
   for its connection string) and acquires its root, which the first connection builds, model included. It
   then creates the `CalciteSession`: configuration, type factory, type registry, default schema path.
3. **Command.** The caller sets `CommandText` and adds parameters.
4. **Request.** `ExecuteReader` builds a `CalciteExecuteRequest` from the text, the parameters, the timeout and
   the hooks, and passes it to the session.
5. **Plan.** The session pushes a `PrepareContext` onto `CalcitePrepare.Dummy` and calls
   `ClrPrepareImpl.PrepareSql`, which parses, validates, converts and optimizes into `ClrCursorConvention`. The
   root is implemented through both its bodies into one cursor factory, whose two opens are compiled on first
   use. The result is a `Signature`.
6. **Bind.** Parameters are converted and assembled with the cancel flag, the timeout and the stashed values
   into a `StatementDataContext`.
7. **Open the plan.** `Open(dataContext)` or `await OpenAsync(dataContext, token)` opens the cursor, running
   the plan's acquisition, so failures and side effects of starting the plan surface here. The cursor is
   wrapped in a `CalciteCursorResult`.
8. **Read.** `CalciteDataReader` advances `CalciteResult.Read` or `ReadAsync`, and each accessor goes
   `CalciteResultRow` → `CalciteResultValue` → .NET value.
9. **Dispose.** Disposing the reader disposes the result and its cursor. Disposing the connection disposes the
   session; the root goes with it only under `Pooling=false`.

**A non-query** follows steps 1 to 6 and then branches on the statement type as described under
`ExecuteNonQuery`.

**DDL** takes effect in step 5 and produces no cursor.

**`EXPLAIN`** is rendered as text in step 5 and read as a one-row cursor. There is one plan whichever execute
method is called, so the text shows `ClrCursor*` nodes, with Calcite's `Enumerable*` nodes beneath a converter
wherever this convention has no node. It cannot say whether a read will await; that is decided per read.

---

## Direct engine access

`CalciteConnection` exposes Calcite objects as typed properties rather than a JDBC-style `unwrap`.
`RootSchema` is typed as `Schema`, Calcite's read interface, rather than `SchemaPlus`, because the root is
shared by every connection of the data source; the root is changed through
`CalciteDataSourceBuilder.ConfigureRootSchema` or DDL. The type does not prevent a caller casting it.

A user-defined function written in .NET needs no class name: this convention holds the method itself, not its
name. Where a plan falls back to `EnumerableConvention`, Janino resolves the IKVM name (`cli.Namespace.Type`)
through the class loader `IKVM.Maven.Sdk` stamps on `calcite-core`, which requires IKVM 8.16.0 or later and
sees only assemblies already loaded in the application domain.

The prepare and plan APIs are not on the ADO.NET surface. A caller who wants them references
`Apache.Calcite.Extensions` and uses `ClrPrepareImpl` directly.

---

## Project layout

```
src/
  Apache.Calcite.Data/                    ADO.NET provider (this design)
    CalciteConnection.cs                  DbConnection, Calcite accessors, hook registration
    CalciteCommand.cs                     DbCommand
    CalciteDataReader.cs                  DbDataReader over one or more results
    CalciteBatch.cs                       DbBatch
    CalciteBatchCommand.cs                DbBatchCommand
    CalciteBatchCommandCollection.cs      DbBatchCommandCollection
    CalciteParameter.cs                   DbParameter
    CalciteParameterCollection.cs         DbParameterCollection
    CalciteTransaction.cs                 DbTransaction (never instantiated)
    CalciteDataSource.cs                  DbDataSource; holds the root schema
    CalciteDataSourceBuilder.cs           Builds a data source from a string and objects
    CalciteProviderFactory.cs             DbProviderFactory
    CalciteConnectionStringBuilder.cs     DbConnectionStringBuilder
    CalciteException.cs                   DbException
    Internal/
      CalciteSession.cs                   Per-connection state over a root; plan, bind, execute
      CalciteDataSourceRoot.cs            The root schema, DUAL and the model; lifetime and lock
      CalciteDataSources.cs               The data sources the provider keeps, one per connection string
      CalciteEngineProperties.cs          Connection string keys to Calcite Properties
      ClrPlugin.cs                        Resolves a plugin named in the connection string
      CalciteExecuteRequest.cs            Execute payload
      CalciteParameterValue.cs            (DbType, value) pair
      CalciteHookEntry.cs                 (Hook, Consumer) pair
      ParameterBinder.cs                  Parameter values to Calcite's representations
      CalciteValues.cs                    Recursive Java/.NET value conversion and reshaping
      CalciteVariants.cs                  VARIANT reading for CalciteValues
      CalciteResult.cs                    One result set
      CalciteCursorResult.cs              A result over the plan's cursor
      CalciteResultColumns.cs             Column metadata and cached mappings
      CalciteResultRow.cs                 Column addressing within one row
      CalciteResultValue.cs               Final conversion and typed getters
      CalciteTypeMap.cs                   DbType to and from CLR types, for parameters
      CalciteSchemaInfo.cs                GetSchema collections
      CalciteColumn.cs                    Column descriptor (unused)

  Apache.Calcite.Data.Common/             Type mappings: ClrTypeRegistry, resolvers, CalciteDbType

  Apache.Calcite.Extensions/              The convention and the prepare pipeline
    Prepare/                              ClrPrepareImpl, ClrPrepare, PrepareContext, StatementDataContext, …
    Runtime/                              IClrCursor, IClrCursorFactory
    Adapter/Cursor/                       ClrCursorConvention: nodes, rules, implementor

  Apache.Calcite.Data.Tests/              Tests for this provider
```

---

## Design constraints

- **Nothing on the statement path goes through JDBC or Avatica.** Calcite's JDBC driver is registered for view
  expansion only. Avatica's metadata types are used as the descriptors Calcite's prepare produces.
- **No `Bindable` on the row path.** A plan is a compiled cursor factory. `IClrPrepare.Signature` exists
  because Calcite's equivalent is declared in terms of linq4j types this convention does not produce.
- **The ADO.NET surface holds no engine logic.** Everything that touches the planner is in `CalciteSession` and
  below it.
- **No Java object reaches a caller** except through `CalciteDataReader.GetCalciteValue`.
- **`Microsoft.Data.SqlClient` is the precedent** for choices ADO.NET leaves open; see *The driver this one is
  modelled on*.
- **Targeting.** The provider targets .NET 8 and is tested on .NET 8 and .NET 10.
