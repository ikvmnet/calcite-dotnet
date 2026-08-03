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

`ClrEnumerableConvention` runs. Scan, values, calc, project, filter, sort, limit, offset, union,
intersect, minus and hash/semi/anti join all plan into it, compile to `System.Linq.Expressions` and
return rows; 59 tests pass. `ClrAsyncEnumerableConvention` does not exist yet — not one file of it.

### The rules that hold

- **linq4j appears in a node only where a Calcite generator produced one or takes one**, and is
  translated where it is produced rather than composed into a larger linq4j tree first. That is four
  things: Rex (`translateCondition`, `translateProjects`, `translateLiteral`, everything `RexImpTable`
  reaches); `PhysType`'s expression members and `JavaRowFormat.field`; a table's own
  `getExpression(Queryable.class)`, which the schema SPI defines as linq4j; and the block Calcite's
  implementor produces for an `EnumerableConvention` sub-plan at a converter.
- The translator exists because reimplementing `RexToLixTranslator` and `RexImpTable` is not on the
  table yet. It is not a licence to keep everything else in linq4j.
- **`ClrEnumerableRules.Rules()` and `CalcRules()` are two passes, not one.** `VolcanoCost.isLt`
  compares the row count and nothing else, so a project and a calc are never cheaper than one another
  and the planner keeps whichever it saw first. `Programs.standard` runs the calc rules afterwards as
  a hep pass; anything registering these rules must do the same or a project will be chosen and will
  refuse to implement itself.

### Not done

- **The whole async convention.** Every node, rule, converter and runtime operator, mirroring the
  sync side. `System.Linq.AsyncEnumerable` in .NET 10 covers most operators.
- **Nodes**: Aggregate, SortedAggregate, Window, Match, Correlate, ConditionalCorrelate,
  NestedLoopJoin, MergeJoin, AsofJoin, BatchNestedLoopJoin, Collect, Uncollect, TableModify,
  TableSpool, RepeatUnion, TableFunctionScan, MergeUnion, Combine, LimitSort, Interpreter, Bindable.
  The runtime methods for the joins and correlate exist; the nodes do not.
- **Aggregate is the big one.** `EnumerableAggregateBase.createAggStateTypes`,
  `declareParentAccumulator`, `createAccumulatorAdders` and `implementLambdaFactory` are `protected`,
  so they need porting rather than reusing, exactly as `EnumUtils.joinSelector` and
  `generatePredicate` did. Each accumulator lambda then has to be wrapped back into a `Function0` or
  `Function2` for `AggregateLambdaFactory`, which is the `SamAdapters` table in the other direction.
- **`ClrEnumerableToEnumerableConverter`.** The forward converter exists and is untested — no query
  has yet mixed conventions. The reverse one has to compile its sub-plan, stash the delegate, and let
  a linq4j tree call it.
- **Nothing connects the convention to `Apache.Calcite.Data`,** and nothing exposes the two-pass
  program as something a caller can just use; the tests wire it by hand.

### Two facts that will bite

- **The Calcite checked out at `D:\calcite` is 1.42.0-SNAPSHOT; the projects reference 1.41.0.**
  `PhysType.generateNullAwareAccessor` and `JoinInfo.nullExclusionFlags` are 1.42 only, and the hash
  join was written against 1.41 because of it. Read the source, then check the member exists.
- **`global.json` has `rollForward: latestMajor`,** so every build here picks the .NET 11 preview SDK
  rather than 10.0.302. It also makes `dotnet sln add` rewrite every project in the solution with
  x64/x86 configurations — edit the solution by hand instead.

The old line is still on the `clr-conventions` branch with a working copy at
`D:\calcite-dotnet-conventions-aside`. Nothing from it has been used, and `ClrPhysType`,
`ClrValueConverter`, `ClrFunctions` and `ClrEnumeratorAdapter.ToCalciteRow` still should not be.
