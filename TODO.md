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

`ClrEnumerableConvention` runs against **calcite-core 1.42.0**. 211 tests pass, measured 2026-08-04. `ClrAsyncEnumerableConvention` does not
exist yet — not one file of it, deferred deliberately until the sync side is finished.

`PARITY.md` was rebuilt from the two sources on 2026-08-04 rather than carried forward, and §9 of it lists
the dozen rows that turned out to be wrong. Four of them were differences nothing had recorded, and they are
in the list below. **The lesson is the one this file already states about Calcite: a row written from memory
of the code reads exactly like a row written from the code, and hides a difference behind the appearance of
having accounted for it.** Rebuild it against the source when it is next relied on.

### What this convention is for, and how it is built

**The goal.** `ClrEnumerableConvention` executes a Calcite plan as `System.Linq.Expressions` instead of as
Java source compiled by Janino. Nothing about *what* a query means changes: the same planner, the same
rules, the same `RexToLixTranslator`, the same `PhysType`, the same answers. Only the thing at the end
differs — an expression tree compiled by the CLR rather than a generated class compiled by Janino. The
standing scope is feature compatibility with `EnumerableConvention`: whatever it does, this does, refusals
included.

**The method: a faithful port.** Every node here is `EnumerableX` ported across, and the default is a
**line-by-line port of each individual method**. Where a question comes up — why is this expression built
that way, why this trait, why this order — the answer is to go and read Calcite's method and translate it
statement by statement, not to reason out what it ought to be. That is the way to move forward whenever
something is unclear, and it is faster than the alternative every time it has been tried.

**The rule is to copy Calcite, and to invent only where copying cannot possibly be done.** Not "where ours
would be better", not "where ours would be equivalent", not "where ours is simpler to write" — where it
*cannot be done*. And the corollary, which is the one that catches things: **a defect we have that Calcite
does not have means we copied Calcite wrongly.** It does not mean Calcite is wrong. Reach for "this is a
Calcite defect" last, and only with a run behind it.

**Divergence is a last resort, and it is always written down.** There are exactly three reasons to depart
from Calcite's text, and each one has to be argued in this file:

1. *An expression tree cannot express it.* A loop, an anonymous class, a mutable local shared across
   methods. Calcite generates those because source text is all it has; we own the loop instead. Window and
   Calc are this.
2. *Calcite's own member is unreachable.* Package private, and no public route. Port it, or find the public
   equivalent — a `ConverterRule`'s `convert` is public even when the node's constructor is not.
3. *It is a defect in Calcite.* Rare, and it must be demonstrated, not asserted.

Anything else is a bug waiting to be found. Every divergence invented rather than forced has turned out to
be wrong: a gratuitous difference in `Calc`, a cost analysis that was simply false, and the harness
hand-listing planner rules instead of calling what `CalcitePrepareImpl` calls, which is where three
"planning mysteries" came from.

**And a divergence argued from a misreading is the same bug wearing a proof.** `ClrEnumerableValuesRule` was
cut down to one statement to explain a `RelCompositeTrait to RelCollation` failure, on the claim that
Calcite's rule is one line. It is two — `create(…)` and then `copy` onto the logical node's traits — and the
failure was `ClrEnumerableValues.passThrough` being missing. Both are fixed. Quote the text before departing
from it.

**So when stuck, diff against Calcite.** Not "what should this do" but "what does `EnumerableX` do here,
statement by statement, and where does ours stop matching".

### What is left, in the order worth doing it

The sections after this one are how each of these was arrived at. This is the list, and each item names the
`PARITY.md` point it comes from — that file numbers every point `section.item`, so 5.3 and 6.9 are addresses,
not page references. `PARITY.md` §5 in full is item 1 below plus the four nodes not written, which are items
4 and 5. Item 2 is not a difference from `EnumerableConvention` at all; it is the thing that would make this
convention usable.

**0. Done — the port is at 1.42.** `PARITY.md` is re-derived against `calcite-1.42.0`, and everything 1.42
added is written and tested:

- **`LEFT_MARK`**, the join type behind it all. A mark join returns every left row with one column appended
  saying whether the right side matched, and that column is three-valued — true, false, or null where a
  comparison was unknown. The third value is the point: it is what makes `IN` over a nullable column answer
  UNKNOWN. `ClrEnumerableHashJoin` and `ClrEnumerableNestedLoopJoin` both have the path, over
  `LeftMarkHashJoin` and `LeftMarkNestedLoopJoin`.
- **`ClrEnumerableConditionalCorrelate`**, the correlated form of the same thing, and
  **`ClrEnumerableCombine`**, for multi-root optimisation. Both rules are in `Rules()`, because both are in
  `ENUMERABLE_RULES` at 1.42.
- Four deltas to nodes already written: the merge join's strict-equality `joinInfo` and its rule's refusal
  of IS NOT DISTINCT FROM, the merge union's collations check, and the merge union rule's BigDecimal fetch.

`ENUMERABLE_RULES` is 26 at 1.42 and `Rules()` is 26, and the membership matches: Calcite's 26 less match
and table modify, plus the two converters. **The rule sets agree exactly for the first time.**

Eleven of the seventeen files 1.42 changed are classes this convention reuses rather than ports, so their
changes arrived free with the assembly. `PARITY.md` 9.21 has the reckoning.

**1. The small differences are closed.** `PARITY.md` 5.3, 5.4 and 5.5 were all "copying Calcite is possible
and we did not", which is the one thing §6 does not admit, and all three were found by rebuilding that file
against the source rather than by anything failing.

- **5.3 — resolved**, and 1a below is what it cost.
- **5.4 — resolved.** `ImplementRoot` wraps a failing node now, so the failure names the plan that reached
  it, as Calcite's does. It catches `System.Exception` where Calcite catches `RuntimeException`, because
  .NET has no checked exceptions and that is the same set, and carries the original as the inner exception
  because .NET has no suppressed one. `ClrEnumerableProject` refuses to implement itself by design, which is
  what makes the wrap testable: one built by hand is the only way to reach that refusal, since the calc
  rules rewrite every project the planner produces.
- **5.5 — resolved.** `ToBindable` takes all four of Calcite's arguments in its order. The Spark branch is
  the only part that could not be copied — it hands `SparkHandler.compile` the generated `ClassDeclaration`
  and its source text, and there is neither here — so a handler that says it is enabled is refused rather
  than ignored. `PARITY.md` 6.16.
- **5.2 — still open**, and it belongs with item 3 rather than here: the window table function path is
  refused because it fails two ways nobody has explained, not because a decision went unrecorded.

**1a. 5.3 is resolved, and it was not free.** `Rules()` held the limit-sort rule that `ENUMERABLE_RULES`
leaves out. Taking it out is one line — and `ShouldAgreeOnLimitAndOffset` then failed with
`InvalidCastException: Unable to cast object of type 'java.lang.Integer' to type 'System.Int32'`. With the
rule gone the whole plan lands in `EnumerableConvention` and `EnumerableToClrEnumerableConverter` carries a
one-column `INTEGER NOT NULL` result: physical type SCALAR, Java row type `int`, linq4j yielding
`java.lang.Integer`. **`JavaSequences.FromJava` cast where it had to convert**, and now goes through
`JavaValues.As`. Two lessons, both of which this file already had in other words: an adapter converts, it
never casts; and "the converters carry it" is a claim to run, not to assert — the converter had never met a
one-column primitive because the extra rule meant this convention planned the query itself. Five
differential tests now run with the limit-sort rule on **on both sides**, which is the first time
`EnumerableLimitSort` has been the oracle for `ClrEnumerableLimitSort`; two more assert which node each side
chose, with the rule and without it. `PARITY.md` 9.18.

**The outbound half of that adapter is unfixed and unmeasured.** `JavaSequences.JavaEnumerator.current()`
returns `source.Current` as it is, and `Bind` reaches it through `Cast<object>`, so a CLR sub-plan of one
primitive column crossing `ClrEnumerableToEnumerableConverter` would hand Calcite a CLR-boxed int where the
type factory says `java.lang.Integer` — the exact mirror of what 9.18 fixed, and the failure mode
`ClrEnumerableRelImplementor.BoxScalars` exists to prevent at the root. It is **not** demonstrated: every
`ToJava` call site today passes `TSource = object`, so the adapter is a no-op and nothing reaches it. Find
the query that puts a one-column primitive CLR sub-plan under a Calcite node before changing anything —
guessing at this is how the first half got written.

**1b. `PARITY.md` 6.9 is argued and not measured.** Sort, limit, limit-sort, spool and
repeat union turn off the row-format optimisation Calcite leaves on. The argument is from Calcite's source
and it is a good one — `EnumerableTableScan.toRows` reshapes to match the optimised type, so a scan is
consistent; `EnumerableTableFunctionScan` cannot reshape and so passes `optimize = false` to keep an honest
ARRAY; a pass-through node above it then optimises that ARRAY away without touching the rows, and a parent
reads field 0 as the row itself. What is missing is `EnumerableConvention` doing it. **The experiment is a
one-column table function Janino can name** — ours is a CLR class, which is the same wall the whole
table-function comparison hits. Until then this is reading, not measurement, and the file says so. Nothing
about our code is suspected: it gives right answers and the tests hold it. What is suspected is the word
"demonstrated".

**2. Wire the convention up.** Nothing connects it to `Apache.Calcite.Data` — checked, and only the test
project references `Apache.Calcite.Linq` at all — and nothing exposes the three-pass program
(`Programs.subQuery`, then the rules, then the calc rules as a hep pass, and no decorrelation) as anything a
caller can use; every test wires it by hand. This is worth more to anyone using the library than another
join algorithm, and it is small.

**3. Two open mysteries, either of which may stay open.**
- A window table function does not run by either route: through the node, translating
  `EnumUtils.tumblingWindowSelector` leaves an `Object[] _input` referenced from no scope that declares
  it; through the converter, the same. Four explanations tried and disproved — they are listed so they
  are not tried again.
- `EnumerableMatch` emits `new Object[]()` when the input format is ARRAY, which is not Java and which a
  translator cannot complete either. **One experiment is worth doing before concluding**: a fixture table
  whose rows are objects rather than `Object[]` gives CUSTOM format, which is how Calcite's own
  MATCH_RECOGNIZE tests pass — that would give Match an oracle and might move the whole thing.

**4. Nodes not started and not investigated.** Interpreter and Bindable.

**5. Blocked, and not by effort.** Match cannot be written as a node: `PassedRowsInputGetter` and
`PrevInputGetter` are package private *types* that Calcite's own translator casts to, and reflection is
not an acceptable way in. TableModify waits on the convention being more than read-only. A recursive CTE
is refused by Calcite too, deliberately.

**Not on this list, and worth saying so.** `EnumUtils.markJoinSelector` stood here and in `PARITY.md` §5 as
a gap. It does not exist in 1.41 — it was read off `D:\calcite`'s working tree, which is the trap the top of
both files warns about, caught this time by checking the tag. It does exist in 1.42, which the projects now
reference, and `ClrEnumUtils.MarkJoinSelector` is written. Nor is `joinSelectorCompact` a gap:
`ClrEnumUtils.JoinSelector` deliberately has one form where Calcite has two, because the second exists only
to keep a generated method under the Java class-file size limit and an expression tree has none. That is now
argued in `PARITY.md` 6.7 rather than simply absent, and the version mistake is `PARITY.md` 9.2.

`Combine` and `ConditionalCorrelate` were out of scope because neither exists in 1.41. Both are in 1.42, and
both are now written — item 0.
**`AsofJoin` is not out of scope and never was** — `rel.core.AsofJoin`, `EnumerableAsofJoin` and `ENUMERABLE_ASOFJOIN_RULE` are all
in 1.41 and in the referenced assembly. The line that said otherwise was written against the wrong class
name (`rel.core.Asof`, which is really 1.43) and stood unchecked. The node is now written.

### Done

Scan, values, calc, project, filter, sort, limit, offset, limit-with-sort, union, intersect, minus,
hash/semi/anti join, nested loop join, batch nested loop join, merge join, ASOF join, mark join in both the
hash and the nested loop form, correlate, conditional correlate, combine, aggregate — ordered calls included
— sorted aggregate, window, merge union, table function scan, collect and uncollect. Converters in both directions, so one plan can hold nodes of both conventions and the rows
cross untouched. 211 tests pass.

`PARITY.md` is the member-by-member comparison against **1.42.0**, the version the projects reference,
rebuilt from the source at the tag and checked against the assembly. Every point in it is numbered
`section.item`, so a defect can be cited as 6.9 rather than described. What is left of **§5** is 5.1 and 5.2 — items 3 to 5 of the list above. 5.3,
5.4 and 5.5 are resolved. §6 is the differences that have an argument and nothing in it is work, except that 6.9 wants the
measurement item 1b describes.

### Trait derivation, and the only thing that calls it

Every node that has one now carries Calcite's `passThroughTraits`, `deriveTraits` and `getDeriveMode`, and
`ClrEnumerableTraitsUtils` is the port of the package-private `EnumerableTraitsUtils` behind them. So do
`EnumerableTableScan.passThrough`, `EnumerableValues.passThrough` and the cost overrides on the two joins
and the window.

**None of it runs unless the planner optimises top down.** `passThroughTraits`, `deriveTraits` and
`getDeriveMode` are reached only from `TopDownRuleDriver`, and `CalciteSystemProperty.TOPDOWN_OPT` is false
by default — so with the default planner they are dead on both sides and a comparison says nothing about
them. `DefaultRulesProgram` takes a `topDown` flag and eleven differential tests use it: a projection, a
cast in a sorted projection, a filter under a sort, an equi join, a left join, a nested loop join, a join
over collated inputs, a correlated sub-query, VALUES, an aggregate and a window. That is what makes those
methods more than text.

This was checked the other way round too, and it is worth knowing how: an exception planted in
`passThroughTraits` did **not** fire with the default planner, which looked like the C# override never
reaching the interface slot. It was the planner, not the binding.

### MergeJoin: written, and it flushed out three defects that had nothing to do with it

`ClrEnumerableMergeJoin` and its rule exist and are registered, and the node is chosen: eight tests reach
`Implement`, checked by making it throw and reading off which failed. Four of them are the tests written
before the node existed, which compared our hash join against Calcite's merge join and now compare the two
merge joins; eight more were added for the join types and the shapes the algorithm branches on — a run of
equal keys on both sides, a key missing from one side, several keys, an extra condition that is not an
equality, and a nullable key, where the comparator refuses to call two nulls equal.

The node is `EnumerableMergeJoin` member for member, including the three private helpers and the six cases
of `passThroughTraits`. One line is not Calcite's and is argued in `PARITY.md` 6.8: a required trait set of
another convention is refused rather than copied onto. `ClrEnumerableDefaults.MergeJoin` is
`EnumerableDefaults.mergeJoin` as an iterator instead of an enumerator with a state machine — the same
walk, the same runs of equal keys, the same nulls-last rule.

**What it cost was three defects elsewhere, none of them in the merge join.** Every one was reachable before
and unreached, and each is now covered by a test:

- **A pass-through node re-optimised its input's row format.** `PhysTypeImpl.of(typeFactory, rowType,
  format)` optimises, and for a one-column row that turns ARRAY into SCALAR — so a sort over a one-column
  table function declared "the row is the value" while still yielding `Object[]`. Sort, limit, limit-sort,
  spool and repeat union now use the overload that does not optimise.
- **A correlate did not box its rows.** `joinSelector` types both parameters boxed, as every other join
  here accounts for; an EXISTS whose right side is one primitive boolean column is where the selector and
  the call disagreed.
- **A synthetic record of no fields got two identical constructors.** The empty one and the all-fields one
  are the same signature when there are no fields, and `Type.GetConstructor` cannot tell them apart. A semi
  join whose right input projects nothing is one.

A claim written here first and checked afterwards, which is the wrong order: that the chosen plan varies from
run to run. It does not, and there was never evidence for it — the failures that looked like variation were
the same plan meeting different code of mine, and a probe that fired on a different test than the one
failing. `JavaHashingTests` settles the mechanism it was blamed on: Java specifies `String.hashCode`, IKVM
implements it, and a `java.util.HashMap` iterates identically in every process.

### ASOF join: written, and on the chosen plan

`ClrEnumerableAsofJoin` and its rule exist and are registered. The node is what `EnumerableAsofJoin` is,
member for member, including the two private helpers — `generateTimestampComparator`, which turns the match
condition's kind into a collation on the right input's timestamp field, and `getTimestampFieldIndex`, which
finds that field without knowing which side of the comparison it was written on.

The algorithm is `EnumerableDefaults.asofJoin` reimplemented in `ClrEnumerableDefaults` over typed delegates, which
is the choice the hash join already made and for the same reason. One detail is not free: the rows are
emitted in the iteration order of the map the left input is indexed by, so the index is a
`java.util.HashMap` and not a `Dictionary`. Nothing else could agree with linq4j's, and it is the same
lesson the window's partition order taught.

Six differential tests: an inner ASOF join, a left one, one looking forward rather than back, one over a
two-field key, one with a null key, and one with no ORDER BY at all — that last is the one that holds the
emit order to Calcite's. The node is on the chosen plan rather than the converter carrying it, checked by
making `Implement` throw and watching all six fail through it.

### An ordered aggregate call is implemented, not refused

`SUM(x ORDER BY y)` and `LISTAGG(x, ',') WITHIN GROUP (ORDER BY y)` hold the rows of a group and fold them
once the call's own ordering has been applied. `EnumerableAggregateBase.implementLambdaFactory` has two
branches and now so does ours: `BasicAggregateLambdaFactory` where no call is ordered, and
`LazyAggregateLambdaFactory` over a `SourceSorter` per ordered call and a `BasicLazyAccumulator` per
unordered one where any is. All four classes are Calcite's and public, so none of them is written again.

The accumulator changes type on that path — it becomes a `LazySource` holding the rows rather than the state
record — and nothing here had to change for it, because the runtime passes an accumulator as an object and
only the factory knows what it is.

Four differential tests: one ordered call, a global aggregate with no GROUP BY, an ordered and an unordered
call in one aggregate, and an ordering on a nullable column. The node is reached, checked the usual way.

### Two things reshaped to match Calcite's own division

**The aggregate is two classes**, as Calcite's is. `ClrEnumerableAggregateBase` holds what
`EnumerableAggregateBase` holds — `hasOrderedCall`, `declareParentAccumulator`, `implementLambdaFactory`,
`createAccumulatorAdders`, `createAggStateTypes`, and the two implementor contexts — and
`ClrEnumerableAggregate` holds the constructor, `copy` and `implement`. The accumulator helpers had been on
`ClrEnumUtils`, which put them on a class Calcite has no counterpart of. They are static because
`ClrEnumerableWindow` needs two of them and is not an aggregate; that is the whole reason.

The reason written here first — that Calcite's four are instance methods that never read `this`, so making
them static costs nothing — is true of three of them and **not of `createAggStateTypes`**, which constructs
`AggContextImpl`. That is an inner class, and it reads `getInput().getRowType()`, `groupSet` and `groupSets`
off the enclosing node. `CreateAggStateTypes` takes those three as parameters and hands them to
`ClrAggContext`, and that is the whole of why its signature is longer than Calcite's. Nothing else about the
port changes; it is worth having right because a signature that differs for a reason nobody wrote down is
the next person's hour.

**`ClrEnumerablePrefer` is this convention's own**, against `EnumerableRel.Prefer`. Same five values and the
same three questions asked of them, plus `ToCalcite` and `FromCalcite`, which are called at the two
converters and nowhere else. What a node asks its input for belongs to the convention asking.

### The order of a query that asks for none is the collection's, and it has to be Calcite's collection

A GROUP BY with no ORDER BY has an order, and it is the order of the map the operator grouped in. Ours
grouped in a `Dictionary` and yielded the keys in the order first seen; Calcite groups in a
`java.util.HashMap` and yields them in the map's. Both are defensible and they are not the same, so the two
conventions answered the same query differently — found by asking the question the harness exists to ask,
after the hashing measurement raised it.

Six operators held rows in a CLR collection whose order then escaped, and each now holds them the way linq4j
does: `GroupBy` in a `java.util.HashMap`, `Distinct` and `Union` in a `java.util.HashSet`, `Intersect` and
`Except` in a `HashSet` or a Guava `HashMultiset` depending on ALL, and `HashJoin` in a `java.util.HashMap`
— that last one because a right or full join ends with the right rows that matched nothing, in the lookup's
order. `EnumerableDefaults.Wrapped` is ported as `JavaWrapped`, because a comparer has to reach a Java
collection as the rows' own `hashCode` and `equals`.

Nine differential tests hold it: an unordered GROUP BY, UNION, INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL,
DISTINCT, RIGHT JOIN and FULL JOIN. Every one of them failed before the change or would have.

**What makes this reproducible rather than lucky** is that Java specifies `String.hashCode` and IKVM
implements it — `JavaHashingTests` measures both that and the map order that follows from it. The CLR's
string hash is randomised per process; nothing here may depend on it.

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

### Window: split, and holding the rule the rest of the port holds

`ClrEnumerableWindow` no longer lets `EnumerableWindow` build the block. The loop is
`ClrEnumerableDefaults.Window`, written once: partitioning, ordering each partition, walking its rows, clamping
the bounds, deciding when the frame has changed enough to be computed again, folding rows in, and
yielding. What the node translates is what a generator of Calcite's produced and nothing else — the
implementors' reset, add and result blocks, the two frame bounds, the partition key, the comparator, the
collation key, and the literals of `constants`.

Five private members of `EnumerableWindow` are ported rather than reused: `translateBound`,
`declareAndResetState`, `implementResult`, `WindowRelInputGetter`, and the anonymous
`WinAggFrameResultContext`. `getPartitionIterator`, `getRowCollationKey`, `implementAdd` and
`buildExcludeGuard` went into the loop instead.

Calcite declares each aggregate's state, and the variable holding its last result, as locals of the
generated method and mutates them in place. Both become fields of one synthetic record — the answer
`ClrEnumerableAggregate` already gives to the same problem — so the state is a value passed in and out of
a lambda. The loop variables the implementors read (`i`, `startX`, `endX`, `hasRows`, `frameRowCount`,
`partitionRowCount`, `rows`, `j`) are linq4j parameters bound, per lambda, to CLR locals read off a
`WindowFrame`.

Three of Calcite's decisions are reference-identity tests over the expressions it has just built. They are
made at plan time here and passed to the loop as flags: whether a bound needs clamping to the partition
(`startUnchecked == i_`), whether the frame's start can move at all (`isUnboundedPreceding`), and whether
the frame is always non-empty. The fourth, `startX == minX` in the frame row count, is only Calcite
avoiding a subtraction of zero, so the loop subtracts and the arithmetic is the same.

Two lambdas are absent rather than empty, and that is load bearing. No adder means no aggregate reads the
frame's rows, which is Calcite dropping the `for` loop. No cached result means every aggregate is
recomputed per row, which is Calcite dropping the whole frame-maintenance block — `prevStart`, `prevEnd`
and the reset with it. `LEAD`/`LAG`/`NTILE` are the shapes that reach it.

Thirty-one differential queries agree with `EnumerableConvention` row for row: the original seven, plus a
whole-partition window, several partition keys, a null partition key, a primitive partition key, an
always-empty frame, a frame entirely following, a RANGE frame with an offset, a RANGE frame over several
order keys — the only shape that reaches the five-argument binary search — LEAD/LAG, LAG with an offset and
a default, FIRST_VALUE/LAST_VALUE, MIN/MAX, AVG, NTILE, NTH_VALUE, the three EXCLUDE clauses, a cached and
an uncached aggregate in one window, two COUNTs in one window, ROW_NUMBER with no ordering, a window over a
filtered input, a window over no rows at all, and the partition order itself.

Two COUNTs in one window is there for a reason: `CountWinImplementor` keeps `justFrameRowCount` on the
instance `RexImpTable` hands out, and `getStateType` is called for every aggregate before `implementAdd` is
called for any. `COUNT(*)` takes the frame's row count and `COUNT` of a nullable column accumulates, so the
two disagree about it. Calcite has the same ordering, and this says we reproduce it.

Two of those found defects rather than confirming anything. **NTILE**: a partition is an `Object[]`, and a
row that is a primitive was going into it boxed the CLR way, where the comparator ordering the partition is
Calcite's and takes a `java.lang.Integer`. The sequence is boxed before it reaches the loop now, by
`ClrEnumUtils.BoxRows`, which is what a join already does and for the same reason; the partition key is
boxed too, because what hashes it is a `java.util.HashMap`. **The partition order**: partitioning by hand
gave the order the keys were first seen in, and Calcite's is a `HashMap`'s. `ClrEnumerableDefaults.Window` now
partitions with `SortedMultiMap` itself — a runtime class of Calcite's, not a generated tree, so the rule
about where linq4j may appear is untouched. Nothing else could have worked, because the order *is* the map's
and only the same map has it. It settles the sort inside a partition as well — `arrays` uses `Arrays.sort`,
which is stable.

The reason written here originally was wrong, and it is worth keeping the correction: it said that under
IKVM a String hashes as .NET hashes it, randomised per process, so no two runs would agree. Measured in
`JavaHashingTests`, in two processes: `java.util.Objects.hashCode("EAST")` is 2120701 both times — the value
the Java language specifies — while `"EAST".GetHashCode()` differs every time. IKVM implements Java's hash,
so a `java.util.HashMap` iterates identically in every run. The fix was right; the reason for it was not,
and the true reason is the stronger one, because it means the agreement is reproducible rather than lucky.

A user-defined aggregate is covered, and it is the one place the differential harness cannot be the oracle.
A function written in C# is a class IKVM names `cli.Apache.Calcite.Linq.Tests.SumAggregate`;
`EnumerableConvention` writes that name into generated Java source and hands it to Janino, which does not
resolve a `cli.` name, so its plan fails to compile — as a windowed aggregate and as a grouped one alike.
This convention holds the method rather than its name and runs both. So those two tests state the rows SQL
says the query gives, which is the only check available; the values were worked out first and matched on
the first run. Nothing else in the suite is asserted by hand.

That is worth knowing beyond the tests: **a user-defined function written in .NET is reachable from this
convention and from no plan Janino compiles.** It is the first thing this convention can do that
`EnumerableConvention` cannot, rather than a thing it merely does differently.

Worth knowing about the fixture: over a frame that is the whole partition Calcite reports no row as any
other's peer, so `EXCLUDE GROUP` behaves as `EXCLUDE CURRENT ROW` and `EXCLUDE TIES` excludes nothing. The
three exclusion tests use a running frame, which is where they differ.

### TableFunctionScan: done, with one of its two paths untested

`ClrEnumerableTableFunctionScan` and its rule exist and are registered, and the node is on the chosen plan
rather than the converter carrying it — checked by making `Implement` throw and watching the tests fail
through it.

It wears two different things, and both ends of both are linq4j because a table function returns Calcite's
own `Enumerable`: a schema defines it that way, exactly as it defines a table's
`getExpression(Queryable.class)`. A function the schema defines is a call that yields a sequence, and
translating the call is the whole of it — no loop, nothing to compose. A window table function (TUMBLE,
HOP, SESSION) is the other way round: a generator of Calcite's that *takes* the input sequence, so the
child's goes out to linq4j and the result comes back.

Three tests, hand-asserted for the same reason `MY_SUM` is: a table function is a class, Janino cannot name
a CLR class, so `EnumerableConvention` has no plan to compare against. They cover the function alone, under
a join, and under an aggregate.

**The window table function path is refused, and does not run by the other route either.** It was written
first and tested afterwards, which is the wrong way round and is how it came to be shipped broken for a
turn. An `EVENTS` fixture with a `TIMESTAMP` column now exists, and `TUMBLE` fails two different ways:

- Through this node, the translated tree ends with an `Object[] _input` referenced from no scope that
  declares it. `EnumUtils.tumblingWindowSelector` names its selector's parameter `_input`, the same name
  the node gives the input sequence, and one of the two escapes its lambda. Three explanations were tried
  and each disproved by experiment: an optimising `BlockBuilder` lifting a shared sub-expression out (a
  non-optimising one changes nothing), the two names colliding (renaming ours changes nothing), and linq4j
  declaring the method with a different parameter object than the body was built against (resolving an
  unbound parameter by name changes nothing). The cause is still unknown.
- Through the converter, `ExpressionTranslator.Anonymous` refuses the anonymous `Enumerator` Calcite wraps
  the result in: four methods, no single one of which is the body.

So the rule refuses a window table function — refused in `matches`, never in `Implement` — and the node
carries only the path that works. `EVENTS` is left in the fixture because it is what a test needs the
moment either route does.

**That second failure was the general one, and fixing it was worth more than this node.** The converter was
not the safety net it had been assumed to be: it could carry an `EnumerableConvention` sub-plan only when
the block Calcite generates held no anonymous class beyond a single-method one, and `EnumerableCalc` —
among others — generates a four-method `Enumerator` over a field. Nothing had noticed, because in every
mixed-convention test this convention owns the calc and the Enumerable sub-plan is a bare scan.

That is fixed. An anonymous class of one method is a lambda, as before; one of several is a thing, and
becomes an object holding a delegate per method — `DelegateEnumerator`, with `DelegateEnumerable` for the
`AbstractEnumerable` wrapped around it. The state they share is not held by the adapter: the class's fields
were already becoming variables of the block that builds the lambdas, and all of the delegates close over
them, which is the lifetime one instance of the anonymous class would have had.

`ShouldCarryACalcAcrossTheConverter` proves it, and it is load bearing — checked by dropping the
`Enumerator` entry and watching it fail with the old message. It plans with only Calcite's rules plus this
convention's one converter rule, so the whole plan lands in `EnumerableConvention` and the converter has to
survive a real generated block. The two existing mixed-convention tests could not have caught this, because
they give the planner both rule sets and this convention wins nearly everything.

### The sorted nodes: all four written

MergeJoin, MergeUnion, SortedAggregate and BatchNestedLoopJoin are only ever chosen over their hash and
buffering counterparts when the input already carries a collation, and a table is where one comes from:
`getStatistic().getCollations()` is what both conventions' scans put in their trait set. Every fixture
table advertised none, so none of the four could be planned **in either convention**. `SortedTable`,
registered as `SORTED`, advertises a collation on its first field, and that unblocked two of them.

**MergeJoin is written**, and the section above records it.

**MergeUnion is written, and the question in front of it had an answer.** This file said the rule never
matches because a SELECT list over a UNION puts a `LogicalProject` between the sort and the union, and that
adding `PROJECT_REMOVE` and `PROJECT_MERGE` does not help. The first half is right and the second was never
the point: the operand wants the sort *directly* over the union, and

    SELECT * FROM "SORTED" UNION ALL SELECT * FROM "SORTED" ORDER BY 1

is that shape. Calcite plans it as `EnumerableMergeUnion`; naming the columns instead of `SELECT *` plans it
as a sort over a union. Dumping the chosen plan for three candidate queries is all it took, and it is worth
doing that before calling a node unreachable again.

Five differential tests: UNION ALL, UNION distinct, with a limit, with an offset and a limit, and three
inputs. Three of them reach the node — the two carrying a limit plan to a limit-sort instead, which is a
cost decision and not a failure, and they agree either way.

**SortedAggregate is written, and the guess in front of it was wrong.** This file said it most likely loses
on the row-count-only cost model and could never be chosen, so a test would have to force it. Dumping three
plans with the rule registered says otherwise:

    SELECT "K", COUNT(*) FROM "SORTED" GROUP BY "K"                 → EnumerableAggregate
    SELECT "K", COUNT(*) FROM "SORTED" GROUP BY "K" ORDER BY "K"    → EnumerableSortedAggregate
    SELECT "REGION", COUNT(*) FROM "SALES" GROUP BY "REGION"        → EnumerableAggregate

It is chosen when the query wants its output ordered by the group key over an input that carries the
collation, because then the ordering is free and the hash aggregate would need a sort on top. Cost was never
the obstacle; nobody had turned the rule on.

Five differential tests, three of which reach the node. The rule is not in `ClrEnumerableRules.Rules()`,
because Calcite does not put its own in `ENUMERABLE_RULES` — a caller turns it on, and the harness turns on
each side's own.

**Turning that rule on found a defect in Calcite too.** For `GROUP BY ()` the collation the node would tell
groups apart with is empty, and Calcite's rule builds the node anyway: `SELECT COUNT(*) FROM t` with the
rule registered plans to `EnumerableSortedAggregate` and then fails with "Unable to implement". Ours refuses
an empty group set, which is one line and is recorded in `PARITY.md` 6.11.

**BatchNestedLoopJoin is written, and it needed nothing but its rule.** The third question in a row whose
answer was one plan dump away: register `ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE` and it is chosen at once,
for an equi-join and a non-equi one alike, over a hash join. Nothing about a collation was ever involved —
it belongs in this section only because this file put it here.

The rule rewrites the right input into a filter over a disjunction of the batch's conditions, one
correlation variable per batch position, so one pass of the right input serves a hundred left rows. The node
declares those variables out of the list a batch arrives in; `ClrEnumerableDefaults.CorrelateBatchJoin` is
`correlateBatchJoin`. Five differential tests, three of which reach the node — the semi and anti ones plan to
a correlate instead.

**It read the right input eagerly for a while, and that was a defect of ours wearing a justification.** The
note said "an expression tree cannot express it, narrowly", and nothing about an expression tree was
involved: `ClrEnumerableDefaults` is hand-written C# and can do exactly what linq4j does. It now does —
the batch's first left row pulls from the right input and caches as it goes, every row after it reads the
cache, and a semi or anti join finishes reading before it stops, because the rest of the batch reads what
the first row cached. That last clause is the whole reason Calcite's version looks convoluted, and it is
`correlateBatchJoin` statement for statement. The cost the old note claimed for the divergence — a full pass
where a batch of one matched early — was wrong in both directions, because Calcite drains there too; what
laziness buys is a consumer that stops early, which is a LIMIT above the join.

**It also found a defect in the translator, and a general one.** A condition over a nullable column is a
`Boolean`, and a disjunction of a hundred of them is `||` over two boxed booleans. Java unboxes there and
the CLR has no operator for two references, so `ExpressionTranslator.Binary` now writes the unboxing out for
`&&` and `||`. Nothing else had produced a boxed operand to a conditional operator.

Also: `ENUMERABLE_SORTED_AGGREGATE_RULE` and `ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE` are **not** in
`EnumerableRules.ENUMERABLE_RULES`; Calcite turns them on by configuration. The harness does the same, per
test and per side — registering the sorted aggregate rule for every query breaks queries that have nothing
to do with it, because of the defect above.

**There is a third such rule, and this convention does not treat it the same way.**
`ENUMERABLE_LIMIT_SORT_RULE` is a field of `EnumerableRules` outside `ENUMERABLE_RULES` as well, and nothing
in core registers it — but `ClrEnumerableRules.Rules()` holds ours. That is item 1 of the list at the top of
this part of the file. `ENUMERABLE_RULES` is 24 and `Rules()` is 25, and this is the entire difference once
match and table modify are set against the two converters.

### Match: not done, and here is exactly what stops it

Three separate blockers, found in this order. The first two are now fixed and are worth having on their
own; the third is where it stands.

**1. The converter could never carry a MATCH_RECOGNIZE.** The line that used to be in this file — that such
a query still runs because the planner puts the node in `EnumerableConvention` and the converters carry the
rows — was wrong, and had been since it was written. `ExpressionTranslator.Anonymous` took a
single-method anonymous class and threw on anything else, and linq4j's `DeterministicCodeOptimizer` hoists
a sub-expression it can prove constant into a *field* of the anonymous class it is generating — a
MATCH_RECOGNIZE predicate carries `$L4J$C$0_1 = 0 * -1`. `Anonymous` now turns each such field into a
variable of the block that builds the lambda, assigned once where the class would have been constructed and
closed over, which is what Java does with it.

**2. Calcite relies on Java's name shadowing across two different variables.** `EnumerableMatch` translates
a pattern definition against a `row_` that is the input row, then declares the predicate method's parameter
as a `row_` that is the `MemoryFactory.Memory` around it, and expects generated source to line the two up
by name. linq4j keys a variable by object identity, so they were two variables and the body tried to call
`Memory.get` on an `Object[]`. `ExpressionTranslator` now gives an anonymous method's parameters a lexical
scope: a linq4j parameter of the same name resolves to them while inside that method, whether or not the
name is already bound, and means the outer variable again once the method is left.

**3. `EnumerableMatch` emits `new Object[]()`, which is not Java.** It builds the measures row with
`Expressions.new_(physType.getJavaRowType())`, and where the input's format is ARRAY that type is
`Object[]`. Calcite's own tests do not hit it because they run over a `ReflectiveSchema` whose rows are
objects, so the format is CUSTOM and the same line emits `new Record2_int_int()`. Over a `ScannableTable`
returning `Object[]` — which is what this project's fixture is, and what an ADO.NET table is — Janino
fails with `'{' expected instead of '('`. A translator cannot complete it either: the array's length is
only implied by the field assignments that follow it.

So there is no oracle for MATCH_RECOGNIZE over an ARRAY-format input, and no way to translate Calcite's
block for one. The next step is to decide between giving the differential fixture a CUSTOM-format table, so
that Calcite's own plan compiles and can be compared against, and treating the `Object[]` case as a Calcite
defect to report upstream. That decision comes before any more code.

### Why the node cannot simply be written, either

Every part of `EnumerableMatch` can be rewritten except two classes, and those two cannot be rewritten for
a reason that has nothing to do with how hard they are — both are about twenty lines.
`EnumerableMatch.PassedRowsInputGetter` and `PrevInputGetter` are package private *types* that Calcite's
own code casts to by name: `RexToLixTranslator.implementPrev` casts the input getter to `PrevInputGetter`
to set the offset, `RexImpTable.LastImplementor` casts it to `PassedRowsInputGetter` to set the index, and
`RexToLixTranslator` suppresses its field-read cache for a `PrevInputGetter` specifically. A ported class
of the same shape fails all three casts, so any DEFINE mentioning PREV and any LAST measure would throw.
IKVM compiles them to `internal` CLR types, so C# can name them and cannot construct them.

That leaves three routes and no fourth. Reflect into Calcite's constructors — rejected, and rightly.
Let Calcite build the block and translate it, which is the converter, and which blocker 3 stops. Or rewrite
the Rex before translating, substituting the PREV and LAST calls so Calcite's translator never enters those
paths — which is no longer porting `EnumerableMatch` but inventing a second implementation of
MATCH_RECOGNIZE semantics, and is not worth it for a node this rarely used.

### What was left behind

`SamAdapters` now takes a non-generic adapter and knows `java.util.function.Predicate` and
`Enumerables.Emitter`, with `DelegatePredicate` and `DelegateEmitter` behind them. Nothing exercises them
while Match is out — they are what blocker 3 stops short of needing, and they are the first thing that
becomes live if it is solved.

### Nodes not done

The standing scope is feature compatibility with `EnumerableConvention`: whatever it does, this does.
What is missing against the referenced version is listed once, at the top of this part of the file, with the
order to take it in. The referenced version is 1.42.0 now, and item 0 is what that changed. What follows is only the note that used to sit here.

The line that used to sit here — that a query using OVER or MATCH_RECOGNIZE still runs because the planner
puts the node in `EnumerableConvention` and the converters carry the rows in and out — was **wrong for
MATCH_RECOGNIZE**, and had been since it was written. The fallback held for OVER and never held for
MATCH_RECOGNIZE. The converter now carries far more than it did, and a MATCH_RECOGNIZE still does not
cross it — see the Match section for the three separate things in the way. Never repeat the general claim
without running it.

### Two facts that will bite

- **Three Calcite versions are in play, so "it is in the tree" settles nothing.** `D:\calcite` is
  **1.43.0-SNAPSHOT**; the projects reference **1.42.0**; `Apache.Calcite.Data.Tests` references
  **1.43.0-SNAPSHOT** from `https://repository.apache.org/content/repositories/snapshots/`, for
  `calcite-server` and the `EnumerableTableModify` rewrite. `PhysType.generateNullAwareAccessor` and
  `JoinInfo.nullExclusionFlags` were called 1.42-only and are — which now means *available*, because 1.42 is
  what is referenced, and both are used by the mark join.
  Check a member with `git cat-file -e calcite-1.42.0:<path>`, never by reading the tree.
- **The unreleased 1.43 cannot compile a DELETE over a one-column table, and two tests are red for it.**
  Root-caused, not worked around. `EnumerableTableModify.deleteFromCollection` (CALCITE-7510, `5cdc09b8c`,
  the newest commit on that file and on `main`) declares the sink row as `Object` and then writes
  `Expressions.convert_(sinkRow, tablePhysType.getJavaRowType())`. For one column that row type is a
  primitive: `EnumerableTableScan.deduceFormat` says ARRAY because `MutableArrayTable.getElementType()` is
  `Object[]`, and the *optimising* `PhysTypeImpl.of` then turns ARRAY into SCALAR for a single field. So the
  generated source says `(int) sinkRow`.

  `(int) someObject` is legal Java — JLS 5.5, narrowing reference conversion then unboxing — and javac
  compiles it. **Janino does not implement it.** Measured against the Janino on this classpath: `(int) o`
  gives `Cannot cast "java.lang.Object" to "int"`, `(java.lang.Integer) o` compiles. Calcite compiles with
  Janino, so it must not emit the first form.

  **The fix is one line, upstream**: `Expressions.convert_(sinkRow, Primitive.box(tablePhysType
  .getJavaRowType()))`. `Primitive.box` leaves `Object[]` alone, so the multi-column path — the only one
  CALCITE-7510 added tests for, all of them `create table t (i int not null, j int not null)` — generates
  exactly what it does today.

  **Filed and submitted**: [CALCITE-7690](https://issues.apache.org/jira/browse/CALCITE-7690) and
  [apache/calcite#5150](https://github.com/apache/calcite/pull/5150), with the fix and two `ServerTest`
  cases, verified against Calcite's own build. **When it lands and reaches a snapshot these two tests go
  green with no change here** — at that point delete the long `<remarks>` from both of them, since the
  test names then say all that is left to say.

  **The version trade is measured**, and neither end is clean: on 1.43.0-SNAPSHOT these two one-column
  DELETE tests fail; on 1.42.0 they pass and *four* UPDATE tests fail instead, because fixing UPDATE is what
  CALCITE-7510 is for. 1.43 is the smaller loss and is what the test project references. The two are left
  failing rather than skipped — a suite that says what is broken is worth more than a green one that does
  not — and the reason is written on both tests.

  **This is also `PARITY.md` 6.9's family, in Calcite's own code**: a physical type optimised to SCALAR
  while the thing that produced it still says the rows are `Object[]`. There the consequence was a physType
  that lies about a pass-through node's rows; here it is a cast Janino cannot compile. Worth remembering
  when 6.9 is finally measured — the pattern is not hypothetical.

  **This is the changeset that would carry `EnumerableTableModify`**, so anyone starting that node meets it
  first.
- **`global.json` has `rollForward: latestMajor`,** so every build here picks the .NET 11 preview SDK
  rather than 10.0.302. It also makes `dotnet sln add` rewrite every project in the solution with
  x64/x86 configurations — edit the solution by hand instead.

The old line is still on the `clr-conventions` branch with a working copy at
`D:\calcite-dotnet-conventions-aside`. Nothing from it has been used, and `ClrPhysType`,
`ClrValueConverter`, `ClrFunctions` and `ClrEnumeratorAdapter.ToCalciteRow` still should not be.
