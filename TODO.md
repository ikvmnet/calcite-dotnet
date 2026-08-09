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

- **`AdoEnumerable.ToProviderValue`** — done. `Boolean`, `Double`, `BigDecimal` and `ByteString` were
  unexercised because SQLite's fixture has no column of those types;
  `GenericProviderCorrelationTests.CorrelatingOnAColumnConvertsItsValueForTheProvider` correlates on one of
  each in `SqlServerFixture`'s `TYPES`, through all three drivers. `Character` is still unreached, Calcite
  having no type that arrives as one.
- **`CalciteResultValue`** — 56%, **282 uncovered**, the largest single gap anywhere. It is the whole
  type-conversion surface, and the `DATE`-as-milliseconds bug lived in exactly this kind of code.
- **`AdoSchemaFactory` from a Calcite model** — 0%. The operand-driven path is the primary documented
  way anyone configures an adapter, and nothing proves it works.
- **`AdoInformationSchemaDatabaseMetadata`** — was 130 lines at 0%, and the whole of it was wrong:
  `DataRow.Field<int?>` on SQL Server's `tinyint` precision threw on every table with a numeric column, so
  no query against SQL Server had ever run. `SqlServerQueryTests` covers it now, on a Windows machine with
  LocalDB. It is no longer shared with Odbc and OleDb: neither driver's collections have the information
  schema's shape, and both now read their own.
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
- The SQL Server, ODBC and OLE DB suites all need a Windows machine with LocalDB and skip everywhere else,
  so the Linux and macOS legs of the matrix still see SQLite alone. `AdoSqlDialectsTests` is the part of it
  that runs everywhere.
- Both generic providers are covered against SQL Server and against nothing else, which is the one backend
  that proves least: an ODBC driver over Oracle or DB2 reports its catalog differently in ways only that
  driver will show. The type-code tables are from ODBC's `sql.h` and OLE DB's `oledb.h` rather than from
  one driver, but only SQL Server's codes have been seen.
- A temporal correlation value is now decoded before binding — a `DATE` left the plan as a day count and
  SQL Server answered "Operand type clash: date is incompatible with int" through all three drivers; only
  SQLite had tolerated the raw count. Two driver limits remain, pinned by tests: `System.Data.OleDb` cannot
  bind a `DateTimeOffset` (Variant marshal refuses it) and binds a `TimeSpan` through `DBTIME`, which drops
  fractional seconds — measured, `01:02:03.500` compares equal to `01:02:03` and unequal to itself.
- **Upstream, and worth reporting**: `MssqlSqlDialect` does not override `supportsGroupByLiteral`, and SQL
  Server cannot group by a constant in either form — `GROUP BY (1 = 1)` is "Incorrect syntax near '='" and
  `GROUP BY 1` is "Each GROUP BY expression must contain at least one column that is not an outer
  reference". It costs every correlated sub-query, because `EXISTS` becomes an aggregate over a constant
  true and `SqlImplementor.visitRoot` only runs `AggregateProjectConstantToDummyJoinRule` when the dialect
  has asked for it. Postgres, Redshift and Informix each override it. `AdoSqlDialects.Mssql` says it here;
  the fix belongs in Calcite.

## Translate a CLR expression tree into a linq4j one

`ClrEnumerableToEnumerableConverter` is the only place a plan of these conventions has to call back out of
generated Java. Calcite compiles its side with Janino from source that cannot mention a CLR object, so the
converter stashes the sub-plan's tree on the `DataContext` and emits a call to `JavaPlans.Bind`, which
compiles it on first use and wraps the result as a linq4j `Enumerable`.

What that costs is a boundary in the middle of a plan: a compiled delegate on one side, a Janino class on
the other, and a sequence adapter between them. The rows still cross untouched, so it is correct — it is
not cheap.

The fix is the inverse of `LixToClrTranslator`: translate the `System.Linq.Expressions` tree into a linq4j
one, hand it to Calcite's implementor as an ordinary block, and let Janino compile the whole thing. Then
there is no callback, no stash, no adapter, and one compilation unit.

It is a real piece of work and not a transcription. `LixToClrTranslator` had the easier direction — linq4j's
tree is smaller than the CLR's, so going the other way means deciding what to do with everything linq4j has
no node for, and the answer for some of it will be "nothing". Worth doing when the boundary starts to
matter; not before.

Note the asynchronous convention needs none of this. It reads a Calcite sub-plan and never feeds one — no
plan can put a Calcite node above an asynchronous one, because Calcite cannot read an
`IClrAsyncScannableTable` — so it has no converter out and nothing to translate.

## Audit findings: 45 operators, twelve agents, one method group each

**The audit has run, and its findings are fixed.** 45 operators, twelve agents, one method group each:
**30 equivalent, 17 divergent, 1 uncertain.** None of the 17 was visible to the differential suite, and all
17 are now transcribed from Calcite's body in both conventions — `HashEquiJoin`'s leftover order,
`NestedLoopJoin`'s five, `RepeatUnion`'s termination test and clean-up ordering, `SemiJoin`'s two algorithms
and its memoization, `Take`'s n+1 draw, `CorrelateJoin`'s refusal and null guard, `Cartesian`'s eagerness and
`int` overflow, the call-time fold in `GroupBy`/`GroupByMultiple`/`AsofJoin`/`Window`, `JavaSequences`'
`reset`, and the deletion of `Count`/`IntersectAll`/`ExceptAll`.

Every one of those was then re-checked against the 1.42 source rather than against the agent report that
found it, and one had been filed wrongly: `Window`'s laziness was recorded as a divergence to keep, on the
grounds that it returns the same rows in the same order and only reaches them sooner. `EnumerableWindow`
generates an `ArrayList`, appends each output row to it and evaluates to `Linq4j.asEnumerable(list)`, once
per window group — so Calcite computes the whole window where the expression is evaluated, and keeping ours
lazy was a decision the port is not entitled to make. It collects now.

What no query can reach is pinned directly instead: `ClrEnumerableNestedLoopJoinTests`,
`ClrRepeatUnionTests`, `ClrEnumerableDefaultsContractTests`.

Two of the asynchronous twins cannot follow Calcite exactly, and say so at the site: a method returning an
`IAsyncEnumerable` cannot await before it returns, so the fold in `GroupBy`/`GroupByMultiple`/`AsofJoin` and
the list in `NestedLoopJoinAsList` happen on the first `MoveNextAsync` rather than at the call. Every row is
still computed before the first is yielded, which is the property the ordering rests on.

What remains below is what was deliberate, and what is still unproven.

### Defects of Calcite's that are reproduced, and tested as such

- **`Window`'s EXCLUDE over an UNBOUNDED/UNBOUNDED frame**: the outer guard is still false after row 0, so
  the exclusion never takes effect.
- **A RANGE bound with an offset over a nullable order key throws.** `translateBound` boxes the key type
  only where the bound has no offset, so with one the key stays `java.lang.Integer` and the `subtract` built
  on it unboxes a null. `ShouldAgreeOnFailingARangeFrameWithAnOffsetOverANullableKey` asserts that *both*
  conventions throw, so if Calcite ever fixes it we are told to follow.

### The SPI contract, which needs no enforcing

A table of this convention's SPI returns what the type factory says, which is Java's — the same contract
Calcite puts on `ScannableTable`, and for the same reason. Nothing checks it and nothing needs to: what
reads a field is `SqlFunctions.toInt` or a cast to the boxed type the row type declares, both Calcite's own,
so a table that gets it wrong stops on its first row.
`ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys` holds that, so that the failure is not one day read
as a defect in the scan and answered with a per-row conversion every correct table would pay for.
