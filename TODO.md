# Outstanding work

## ADO.NET adapter: gaps against Calcite's JDBC adapter

Established by comparing `src/Apache.Calcite.Adapter.AdoNet` against
`org.apache.calcite.adapter.jdbc` class by class. The rule sets are otherwise identical — join,
project, filter, aggregate, sort, union, intersect, minus, values and the to-enumerable converter
are all present on both sides, with the same node classes behind them, and correlated sub-queries
work.

None of the items below are covered by tests either way: the 98 adapter tests exercise the query path
only. Every bug found in this adapter so far has been in code nothing executed, so write the
failing test first.

### 0. Correlated sub-queries — done, but thinly tested

Fixed. Kept here only for the note at the end.

The feature was present, reachable, and broken in five separate ways, none of which could be seen
without `forceDecorrelate=false` — Calcite rewrites a correlated sub-query into a join before it
reaches the adapter, so eleven passing tests in `AdoCorrelationTests` never touched any of this code.

`AdoToEnumerableConverter` built the correlation data context and never called `Build()` on it.
`AdoCorrelationDataContext.get` ignored the parameters it was constructed with.
`AdoCorrelationDataContextBuilderImpl` cast `typeof(X)` to `java.lang.reflect.Type`, which a
`System.RuntimeType` fails, so the class could never initialize — hidden because C# defers a static
field until first read and the only read was in `Build()`. The generated SQL carried Calcite's
positional `?` where ADO.NET matches by name. And correlation values arrived as boxed Java types no
provider can bind.

`AdoEnumerable.ToProviderValue` unwraps those boxed values, and is now exercised for an integer, a
real, a string and a date. `Boolean`, `Double`, `BigDecimal` and `ByteString` remain unexercised —
SQLite has no column of those types in the fixture, so covering them needs either a wider fixture or
a second provider.

### 0b. ODBC and OleDb cannot answer what they are

`OdbcDatabaseMetadata` and `OleDbDatabaseMetadata` throw from `Dialect`, `GetDefaultSchema` and
`ParseDbType`. That reads like laziness; it is not. ODBC is a transport, not a database — one
`OdbcConnection` may front SQL Server, Oracle, PostgreSQL, DB2 or Teradata, each with a different
grammar. No dialect follows from "this is ODBC", so there is nothing to return without connecting.

Calcite treats this as the general case rather than a special one. `SqlDialectFactoryImpl.create`
takes a live `DatabaseMetaData`, reads `getDatabaseProductName()`, and returns the matching dialect;
`JdbcUtils.DialectPool` caches the answer per data source so it is asked once. The ADO.NET input is
the same: `DbConnection.GetSchema(DbMetaDataCollectionNames.DataSourceInformation)` yields
`DataSourceProductName` and version.

This is the clearest case for keeping the dialect and the parameter syntax apart. For ODBC the
dialect is unknown until a connection is opened, because it belongs to the server behind the bridge;
the parameter form is known statically, because ODBC always binds positional `?`. One member could
not answer both.

And `?` is the case `GetParameterName(int)` cannot express — a bare marker carries no position. So
ODBC needs more than a different string: `AdoEnumerable.SetParameter` sets `ParameterName` and relies
on the name matching the marker, and ODBC binds by the order of the parameter collection instead.
Supporting it means a positional path through the enricher.

### 1. DML never reaches the provider

`INSERT` / `UPDATE` / `DELETE` do not push down. Calcite has three pieces we have none of:

| Calcite | ours |
|---|---|
| `JdbcRules.JdbcTableModificationRule` | — |
| `JdbcRules.JdbcTableModify` (`JdbcRules.java:1019`) | — |
| `JdbcTable implements … ModifiableTable` | `AdoTable : AbstractQueryableTable, TranslatableTable, ScannableTable` |

The runtime half already exists and is orphaned: `AdoUpdateEnumerable` executes a non-query and
returns a row count, reachable only from `AdoEnumerable.CreateUpdate`, which no rule calls. So
execution was written and planning never was.

This is the only entry that is a missing *feature* rather than a rough edge.

### 2. Views are reported as tables

Every discovered object is hardcoded to `Schema.TableType.TABLE`, so a view is indistinguishable
from a table and system tables are not marked. Calcite maps the driver's reported type name through
`Util.enumVal`, giving `VIEW`, `SYSTEM_TABLE`, and `OTHER` as a fallback
(`JdbcSchema.java:310-324`).

Fix is in the `AdoDatabaseMetadata` implementations that enumerate tables. Most likely of the six to
surprise someone.

### 3. The dialect's calendar policy is ignored

`JdbcToEnumerableConverter:124` reads `dialect.getCalendarPolicy()` and, for `LOCAL`, threads a
`Calendar` built from the connection's time zone through every temporal read. ClickHouse, MySQL and
Presto override it. We read dates and times with no calendar at all.

This is the family the `DATE`-as-milliseconds bug came from. The day count is now correct for a
UTC-agnostic reading, but a dialect needing `SHIFT` or `LOCAL` is still wrong. Policy values are
`NONE, NULL, LOCAL, DIRECT, SHIFT`.

### 4. No catalog-level schema

`JdbcCatalogSchema` exposes a whole catalog as sub-schemas — one `JdbcSchema` per back-end schema,
computed lazily so the schema list is not fetched until asked for. `AdoDatabaseSchema` exists but
does not do this, so a multi-schema database (SQL Server, Postgres) cannot be browsed as a tree from
a single registration.

### 5. No query provider

`JdbcQueryProvider` is a `QueryProviderImpl` that makes `JdbcTableQueryable` executable.
`AdoTableQueryable` takes a `QueryProvider` in its constructor but we ship no implementation, so
`asQueryable` only works if the caller supplies one.

Decide whether this path is wanted at all before adding it — symmetry with Calcite is not a reason
on its own.

### 6. Dialect and data source lookups are not cached

`JdbcUtils.DialectPool` and `JdbcUtils.DataSourcePool` cache by key, per data source, across schemas.

`SqlServerDatabaseMetadata` now memoizes its own dialect, which was the pressing part: deriving it
opens a connection to read the server version, and `AdoConvention.Dialect` is read for every rule
that matches while planning. What is left is caching *across* metadata instances, which only matters
when several schemas point at one database. Lowest priority; measure before assuming it matters.

## Test suites not yet written

Sized against measured coverage: `Apache.Calcite.Data` 69.9%, `Apache.Calcite.Adapter.AdoNet` ~60%.
Listed worst-first by uncovered lines.

- **`CalciteResultValue`** — 56%, **282 uncovered**, the largest single gap anywhere. It is the whole
  type-conversion surface, and the `DATE`-as-milliseconds bug lived in exactly this kind of code.
- **`AdoSchemaFactory` from a Calcite model** — 0%. The operand-driven path is the primary documented
  way anyone configures an adapter, and nothing proves it works.
- **`AdoInformationSchemaDatabaseMetadata`** — 130 lines at 0%, shared by the SqlServer, Odbc and
  OleDb providers, so one suite lifts four.
- **Connection strings, parameters, batches** — `CalciteConnectionStringBuilder` 35% (148 uncovered),
  `CalciteParameterCollection` 55% (78), `CalciteBatchCommandCollection` 21% (62). Mechanical, high
  line yield.

Also at 0% and worth deciding about rather than covering: `AdoTableQueryable` (no provider ships, see
§5), `AdoUpdateEnumerable` (orphaned, see §1), and the twelve `Ado*Factory` relational factories with
`AdoRules.GetRules(convention, relBuilderFactory)`, which nothing calls.

`CalciteTransaction` is 46 lines at 0% because `BeginDbTransaction` always throws and the tests
correctly assert that. It is unreachable, not untested, and it stays — it is part of the ADO.NET
surface whether or not the operation succeeds.

## Smaller items

- `AdoSchema.Create(null, …)` throws `NullReferenceException` from inside `Schemas.expression`
  rather than saying a parent schema is required. First thing anyone calling the API by hand hits.
- `AdoSetOpFactory.createSetOp` is covered only indirectly, through `UNION` / `INTERSECT` / `EXCEPT`
  queries. Direct tests need a planner fixture that does not exist yet.
- The AdoNet adapter is tested against SQLite only. `SqlServerDatabaseMetadata`,
  `OdbcDatabaseMetadata` and `OleDbDatabaseMetadata` have no coverage.

## Apache.Calcite.Linq: where the CLR conventions stand

`ClrEnumerableConvention` runs. 72 tests pass. `ClrAsyncEnumerableConvention` does not exist yet — not
one file of it, deferred deliberately until the sync side is finished.

### Done

Scan, values, calc, project, filter, sort, limit, offset, limit-with-sort, union, intersect, minus,
hash/semi/anti join, nested loop join, correlate, aggregate, collect and uncollect. Converters in
both directions, so one plan can hold nodes of both conventions and the rows cross untouched.

### The rules that hold

- **linq4j appears in a node only where a Calcite generator produced one or takes one**, and is
  translated where it is produced rather than composed into a larger linq4j tree first. That is four
  things: Rex (`translateCondition`, `translateProjects`, `translateLiteral`, everything `RexImpTable`
  reaches); `PhysType`'s expression members and `JavaRowFormat.field`; a table's own
  `getExpression(Queryable.class)`, which the schema SPI defines as linq4j; and the block Calcite's
  implementor produces for an `EnumerableConvention` sub-plan at a converter.
- **`JavaCast` is for what Java the language converts and an expression tree will not** — boxing,
  unboxing, numeric promotion. It is not a way to make one type into another where they ought already
  to agree. Measured across every plan the tests run: four reference conversions and one boxing.
- **`Rules()` and `CalcRules()` are two passes.** `VolcanoCost.isLt` compares the row count and nothing
  else, so a project and a calc are never cheaper than one another and the planner keeps whichever it
  saw first. `Programs.standard` runs the calc rules afterwards as a hep pass. A caller must do the
  same, must run `Programs.subQuery` before the planner, and — to reach `ClrEnumerableCorrelate` at all
  — must *not* decorrelate.
- **A join boxes its rows.** Calcite builds the selector and predicate against boxed rows because
  linq4j's `Function2` and `Predicate2` erase to `Object`, and because an outer join compares a row to
  null. A delegate is typed where those interfaces were not, so the sequence is boxed to agree.
- **A block consumed apart from what reads it must come from a non-optimising `BlockBuilder`.** An
  optimising one inlines a declaration used once, which leaves a reference already built into a
  translated sub-plan pointing at a variable that no longer exists. `ClrEnumerableCorrelate` needs this.

### Recursive CTE: the nodes exist, and a scan of the scratch table does not convert

`ClrEnumerableRepeatUnion` and `ClrEnumerableTableSpool` exist and register the transient table in the
runtime root schema exactly as Calcite does. Nothing about them is a write: a spool is a tee that
passes rows through and leaves the round behind it, and `WITH RECURSIVE` is a read-only query.

They are not reachable, and the reason is deliberate on Calcite's side rather than an oversight to
work around. `SpoolRelOptTable.getExpression` returns null on purpose — "so EnumerableTableScanRule
won't try to convert spool table scans" — and `EnumerableTableScan.canHandle` refusing a
`TransientTable` (CALCITE-3673) is the matching half. Dropping our half of that guard only moves the
failure from planning to implementation, because there is still no expression for the table.

So the behaviour here matches `EnumerableConvention`, refusal included, which is what being feature
compatible with it means. Neither node has a test. Whatever makes a recursive query run in Calcite's
own convention is what would make it run here, and finding that out is the next step, not a
workaround in this rule.

### Window works, and translates far more linq4j than it should

`ClrEnumerableWindow` lets `EnumerableWindow` build the whole block and translates it. Seven queries
agree with `EnumerableConvention` row for row, so it is correct, and it is the largest single piece of
linq4j translated anywhere in this port -- which is the opposite of the rule the rest of it holds to.

About half of that block is legitimately linq4j: the window aggregate implementors, the frame bound
expressions, and the `PhysType` calls. The other half is not, and is the same shape as the loop
`ClrEnumerables.Calc` already owns:

- `getPartitionIterator` -- a `SortedMultiMap`, `putMulti` per row, `arrays(comparator)`
- the per-partition `Object[]` buffering and the index loop over it
- frame maintenance: `prevStart`/`prevEnd`, clamping start and end, deciding when to recompute
- buffering the output into an `ArrayList` and turning it back into a sequence

Those should be a `ClrEnumerables.Window` written once, taking the translated per-expression pieces --
reset, add, result, the bound expressions, the key selector, the comparator -- as arguments. Calcite
generates an anonymous `Enumerator` for the same reason it does in a calc: generated Java source is the
only place it can put a loop.

The seven differential tests are what makes that refactor safe to attempt, and they did not exist when
the current shape was written. Keep the working node registered until the replacement passes them.

The one part of the split that is not obvious: Calcite declares each window aggregate's state as
block-local variables and mutates them in place, so it cannot be lifted into a lambda as it stands.
`ClrEnumerableAggregate` already solves the same problem — it builds a synthetic record for the
accumulator and points `agg.state` at `accPhysType.fieldReference(acc_, j)`, so the state becomes a value
passed in and out. The loop variables the aggregate contexts read (`i`, `startX`, `endX`, `hasRows`,
`frameRowCount`, `partitionRowCount`, `rows`) become linq4j parameters bound to the CLR parameters of the
lambdas, the same way a row parameter is bound everywhere else.

### Nodes not done

The standing scope is feature compatibility with `EnumerableConvention`: whatever it does, this does.
Against the referenced 1.41, that leaves:

- **Window** and **Match**. These are the substantial ones and they are not started. Window is 1007
  lines and Match 547, and neither is mostly its `implement` method: `translateBound`,
  `declareAndResetState`, `getPartitionIterator`, `getRowCollationKey` and `WindowRelInputGetter` are
  all private, so all of them have to be ported, and they are order sensitive code over nested block
  builders mutating an `Object[]` of partition rows. Getting one detail wrong gives wrong analytic
  results rather than a failure, which is the reason not to rush them.
- **TableFunctionScan**, **MergeJoin**, **BatchNestedLoopJoin**, **SortedAggregate**, **MergeUnion**,
  **Interpreter**, **Bindable**.
- **TableModify**, once the convention is more than read-only.
- An aggregate call carrying its own ordering, which needs `LazyAggregateLambdaFactory` and a
  `SourceSorter` per call. Only the unordered `BasicAggregateLambdaFactory` is built today.

`AsofJoin` is **not** in scope: `org.apache.calcite.rel.core.Asof` does not exist in 1.41. `Combine`
has not been checked and is probably the same, being newer still.

Worth knowing while these are outstanding: a query using OVER or MATCH_RECOGNIZE still runs. The
planner puts the node in `EnumerableConvention` and the converters carry the rows in and out. What is
missing is the node in *this* convention, not the feature.

### Two facts that will bite

- **The Calcite checked out at `D:\calcite` is 1.42.0-SNAPSHOT; the projects reference 1.41.0.**
  `PhysType.generateNullAwareAccessor` and `JoinInfo.nullExclusionFlags` are 1.42 only, and the hash
  join was written against 1.41 because of it. Read the source, then check the member exists.
- **`global.json` has `rollForward: latestMajor`,** so every build here picks the .NET 11 preview SDK
  rather than 10.0.302. It also makes `dotnet sln add` rewrite every project in the solution with
  x64/x86 configurations — edit the solution by hand instead.

Also unwired: nothing connects the convention to `Apache.Calcite.Data`, and nothing exposes the
three-pass program as something a caller can just use; the tests wire it by hand.

The old line is still on the `clr-conventions` branch with a working copy at
`D:\calcite-dotnet-conventions-aside`. Nothing from it has been used, and `ClrPhysType`,
`ClrValueConverter`, `ClrFunctions` and `ClrEnumeratorAdapter.ToCalciteRow` still should not be.
