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

- **`AdoEnumerable.ToProviderValue`** — `Boolean`, `Double`, `BigDecimal` and `ByteString` are unexercised.
  SQLite has no column of those types in the fixture, so covering them needs a wider fixture or a second
  provider. All that is left of the correlated sub-query work.
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

**The audit has run.** 45 operators, twelve agents, one method group each. Result: **30 equivalent, 17
divergent, 1 uncertain.** Everything below was found by reading Calcite beside ours; none of it was visible
to the differential suite.

Fixed so far, in both conventions: `HashEquiJoin`'s leftover order, `NestedLoopJoin` (five defects), and
`RepeatUnion`'s termination test and clean-up ordering. Each carries a test that fails against the old
behaviour -- `ClrEnumerableNestedLoopJoinTests` and `ClrRepeatUnionTests` for what no query can reach.

### Eager where Calcite is eager, or the reverse

Calcite builds at call time and returns an enumerable over finished state; our `yield` iterators defer and
re-execute. In `AsofJoin`, `GroupBy`, `GroupByMultiple`, `nestedLoopJoinAsList`, `Cartesian`, `Window`.
Identical for a single pass; on re-enumeration ours rescans, and **`RepeatUnion` re-enumerates its
iterative branch every round**, so a `GROUP BY` under a recursive CTE recomputes here and replays a frozen
map there. `Window` is the reverse and in our favour: we stream output where Calcite collects it.

### Memory and laziness

- **`Cartesian` is fully eager**: `new List(outer.Count * inner.Count)` before the first row, where
  Calcite's is lazy. Quadratic per merge-join key run, and that `int` multiply overflows around 2^31 pairs.
- **`SemiJoin` holds every inner row** where linq4j holds distinct keys, and drains the inner even for an
  empty outer where linq4j memoizes (CALCITE-2909).
- **`Take` draws n rows where linq4j draws n+1**, because `takeWhile` reads `current()` before rejecting.
  With `FETCH 0` we never open the input, so `LazyCollectionSpool`'s write-back and `RepeatUnion`'s
  `cleanUp` are skipped where Calcite opens and closes.
### Robustness

- `CorrelateJoin` does not reject RIGHT/FULL (Calcite throws, we silently inner-join) and does not guard a
  null correlated inner (Calcite substitutes empty, we throw). `LeftMarkJoin` next door does guard.
- `JavaSequences.ToJava`'s `reset()` throws where linq4j re-acquires. Only `CartesianProductEnumerator`
  calls it live.

### Dead code to delete

`Count`, `IntersectAll`, `ExceptAll`, in both conventions, found independently by two agents. Latent
defects if wired up: CLR `Dictionary` keying that bypasses `JavaWrapped`, source-order output where linq4j
yields in `HashMultiset` order, a throw on a null key.

### Deliberate: record, do not fix

- **`Window` reproduces a Calcite bug**: with UNBOUNDED/UNBOUNDED plus EXCLUDE the outer guard is false
  after row 0, so the exclusion never takes effect.
- **RIGHT/FULL unmatched-right order**: Calcite walks an `IdentityHashMap`, whose buckets key on
  `System.identityHashCode`. We now dedup on identity as it does, but there is no CLR counterpart to that
  number, so the unmatched rows come out in insertion order. The set of rows is Calcite's; the order is not,
  and cannot be.
- **`SemiJoin` applies the comparer** where linq4j's `contains` ignores it. Ours is more correct, and
  unreachable: the key projection optimises to LIST/SCALAR, so `comparer()` is null.
- **`CompareNullsLastForMergeJoin`** returns 1 for two nulls where Calcite throws and its caller converts to
  1. Composed behaviour identical; the contract no longer signals two-nulls to a future caller.

### Still to settle

- **`JavaValues.From` is a no-op wherever a row type instantiates it.** It branches on
  `typeof(T).IsValueType`, and a `PhysType.RowType` is always `ClrPrimitive.Box(...)`, a Java class. The
  protection at those sites comes from that boxing, not from `From`, which earns its keep only in the
  `Delegate*` SAM adapters. If it is meant as a boundary guard it should test `value.GetType().IsValueType`
  as `As` does.
- **`arrayComparer` compares `object[]` elements by `equals`, and `From` does not recurse into an array.** A
  set operation with one CLR-native input and one carried across a converter would compare CLR-boxed values
  against `java.lang.Integer`; hashes agree, so order survives and only deduplication breaks. Reachability
  unproven.
- A RANGE window bound with an offset over a nullable order key builds `subtract(boxedKey, offs)`, which
  NPEs in Java. Confirm the CLR translation fails the same way.
