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

Three divergences have now been found by being asked about, not by testing:

| | ours | Calcite's |
|---|---|---|
| `ClrAsyncEnumerableTableScan.Format` | no `Row` case, tested `IsArray` | `EnumerableTableScan.format()` |
| direct `ScannableTable.scan` in the scan | re-derived `deduceElementType`'s precedence | `getExpression(Queryable.class)` |
| `OrderByWithFetchAndOffset` | full sort, then skip and take | bounded `TreeMap` (CALCITE-3920, CALCITE-4157) |

All three passed the differential tests, and they had to: **those tests compare answers, and every one of
these returns the right answer.** They are an oracle for correctness and a null oracle for equivalence. So
"the suite is green" cannot be the stopping condition for a port.

What found all three was reading Calcite's source for that one member. The audit is therefore: for every
member of `ClrEnumerableDefaults` against `EnumerableDefaults`, and every node against its `Enumerable*`
counterpart, read both and record the verdict — ported unchanged, or the exact divergence and why.

**The audit has run.** 45 operators, twelve agents, one method group each. Result: **30 equivalent, 17
divergent, 1 uncertain.** Everything below was found by reading Calcite beside ours; none of it was visible
to the differential suite.

### Fixed

- **A null sort key threw.** `SortedDictionary` rejects a null key *before* consulting the comparer, where a
  `TreeMap` built with a comparator hands one to it, which is the whole job of `Functions.nullsComparator`.
  Introduced by the limit-sort port itself. Reachable only for a **single**-column collation over a nullable
  column: a multi-field collation keys on a `FlatLists` row, never null however many fields in it are, which
  is why four existing NULLS FIRST/LAST tests could not reach it. An agent fuzzed 20,000 cases against
  `OrderBy().Skip().Take()`: 0 row or order mismatches, 5,714 throws.

### Wrong rows, latent

- **`NestedLoopJoin` SEMI passes a null right row** where both of Calcite's bodies pass the matched inner
  row. Invisible only because `ClrEnumUtils.JoinSelector` declares the right parameter for SEMI and never
  reads it. Reachable from `MergeJoin`'s non-equi SEMI path.
- **`RepeatUnion` stops one round early.** Calcite sets `current` to `DUMMY` at the end of each round but
  never restores it across the seed/iteration boundary, so a non-empty seed followed by an empty round 0
  does not stop it. Differs iff the seed emitted at least one row and round 0 emits nothing new; changes
  rows only for a non-monotone step. Upstream behaviour at HEAD.
- **`HashEquiJoin` emits unmatched right rows in the wrong order.** linq4j iterates the `HashSet` copy of
  the key set; we iterate the `HashMap` `entrySet()` filtered by membership. Measured on a JVM: they
  disagree at 12, 24, 48, 96 distinct build keys, 199/200 trials at n=12. `SalesTable` has six rows, so no
  test can reach it. One-line fix. Same in the async twin.

### Eager where Calcite is eager, or the reverse

Calcite builds at call time and returns an enumerable over finished state; our `yield` iterators defer and
re-execute. In `AsofJoin`, `GroupBy`, `GroupByMultiple`, `nestedLoopJoinAsList`, `Cartesian`, `Window`.
Identical for a single pass; on re-enumeration ours rescans, and **`RepeatUnion` re-enumerates its
iterative branch every round**, so a `GROUP BY` under a recursive CTE recomputes here and replays a frozen
map there. `Window` is the reverse and in our favour: we stream output where Calcite collects it.

### Memory and laziness

- **`Cartesian` is fully eager**: `new List(outer.Count * inner.Count)` before the first row, where
  Calcite's is lazy. Quadratic per merge-join key run, and that `int` multiply overflows around 2^31 pairs.
- **`NestedLoopJoin` buffers its inner** where Calcite's optimized body re-enumerates per outer row. The
  asymmetry is Calcite's own, so ours is a divergence rather than a choice.
- **`SemiJoin` holds every inner row** where linq4j holds distinct keys, and drains the inner even for an
  empty outer where linq4j memoizes (CALCITE-2909).
- **`Take` draws n rows where linq4j draws n+1**, because `takeWhile` reads `current()` before rejecting.
  With `FETCH 0` we never open the input, so `LazyCollectionSpool`'s write-back and `RepeatUnion`'s
  `cleanUp` are skipped where Calcite opens and closes.
- **`Ordered` finds its last key in O(n)**, `SortedDictionary.Keys` being neither an `IList` nor a LINQ
  partition. The CALCITE-3920 memory bound is correct; its constant is not.

### Robustness

- `CorrelateJoin` does not reject RIGHT/FULL (Calcite throws, we silently inner-join) and does not guard a
  null correlated inner (Calcite substitutes empty, we throw). `LeftMarkJoin` next door does guard.
- `RepeatUnion`'s `cleanUp` runs after the child enumerators close rather than before, and not at all if the
  plan is created and never pulled.
- `JavaSequences.ToJava`'s `reset()` throws where linq4j re-acquires. Only `CartesianProductEnumerator`
  calls it live.

### Dead code to delete

`Count`, `IntersectAll`, `ExceptAll`, in both conventions, found independently by two agents. Latent
defects if wired up: CLR `Dictionary` keying that bypasses `JavaWrapped`, source-order output where linq4j
yields in `HashMultiset` order, a throw on a null key.

### Deliberate: record, do not fix

- **`Window` reproduces a Calcite bug**: with UNBOUNDED/UNBOUNDED plus EXCLUDE the outer guard is false
  after row 0, so the exclusion never takes effect.
- **RIGHT/FULL unmatched-right dedup**: Calcite uses an identity set, so two unmatched rows that are the
  same object emit once; ours emits twice. Ours is the SQL-correct answer.
- **`HashEquiJoin` retires keys by the comparer** where linq4j uses natural equality on unwrapped keys, and
  **`SemiJoin` applies the comparer** where linq4j's `contains` ignores it. Ours is more correct in both;
  both unreachable, the key projection optimising to LIST/SCALAR so `comparer()` is null.
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

## A limit sort sorts more than it needs to — fixed

Ported now, and kept here as the worked example of what the audit is for.

`ClrEnumerableDefaults.OrderByWithFetchAndOffset` sorted the whole input and then skipped and took:

    var ordered = OrderBy(source, keySelector, comparator);
    if (offset > 0) ordered = ordered.Skip(offset);
    if (fetch != int.MaxValue) ordered = ordered.Take(fetch);

linq4j does not. CALCITE-3920 and CALCITE-4157 changed `EnumerableDefaults.orderBy(source, keySelector,
comparator, offset, fetch)` to keep a `TreeMap` bounded at `offset + fetch` rows: a row whose key sorts at
or after the last key held is discarded without being stored, and adding one evicts the last. A comment in
the source says why a `TreeMap` rather than a heap — it behaves like the plain `orderBy` and is better when
there are few distinct keys.

So `ORDER BY x FETCH 10` over a million rows holds ten in Calcite and a million here. The rows and their
order are identical, which is why the differential tests never saw it — this is a divergence in what it
costs, not in what it answers.

Both conventions have it: the asynchronous `OrderByWithFetchAndOffset` drains into a buffer and hands it to
the synchronous one, so fixing the synchronous one fixes both.

A plain sort is not this. It has to read its whole input before it can yield anything, in linq4j and here
alike, and both do it lazily on the first read rather than when the sequence is built. That part is
faithful.

