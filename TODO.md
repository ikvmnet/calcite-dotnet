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

`ClrEnumerableConvention` runs. 159 tests pass. `ClrAsyncEnumerableConvention` does not exist yet — not
one file of it, deferred deliberately until the sync side is finished.

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

The sections after this one are how each of these was arrived at. This is the list.

**1. Wire the convention up.** Nothing connects it to `Apache.Calcite.Data`, and nothing exposes the
three-pass program — `Programs.subQuery`, then the rules, then the calc rules as a hep pass, and no
decorrelation — as anything a caller can use; every test wires it by hand. This is worth more to anyone
using the library than another join algorithm, and it is small.

**2. Three planning questions, each to be answered before any code.** They are not one question: each of
these nodes needs a different thing that a collated input does not give it. Why `EnumerableMergeUnionRule`
does not match when its operand — a `LogicalSort` directly over a `LogicalUnion` — looks satisfiable;
whether `EnumerableSortedAggregate` loses purely on the row-count-only cost model, in which case it can
never be chosen and a test has to force it; and what `BatchNestedLoopJoin` needs. Detail in *The four sorted
nodes*. The harness now registers what Calcite registers and can plan top down, so both are available to
answer these with.

**3. Two open mysteries, either of which may stay open.**
- A window table function does not run by either route: through the node, translating
  `EnumUtils.tumblingWindowSelector` leaves an `Object[] _input` referenced from no scope that declares
  it; through the converter, the same. Four explanations tried and disproved — they are listed so they
  are not tried again.
- `EnumerableMatch` emits `new Object[]()` when the input format is ARRAY, which is not Java and which a
  translator cannot complete either. **One experiment is worth doing before concluding**: a fixture table
  whose rows are objects rather than `Object[]` gives CUSTOM format, which is how Calcite's own
  MATCH_RECOGNIZE tests pass — that would give Match an oracle and might move the whole thing.

**4. Nodes not started and not investigated.** Interpreter and Bindable. Also an aggregate call carrying its
own ordering, which needs `LazyAggregateLambdaFactory` and a `SourceSorter` per call: it is refused in
`ClrEnumerableAggregate`'s constructor for now, so such a query plans in `EnumerableConvention` and the rows
cross a converter, which a differential test holds to.

**5. Blocked, and not by effort.** Match cannot be written as a node: `PassedRowsInputGetter` and
`PrevInputGetter` are package private *types* that Calcite's own translator casts to, and reflection is
not an acceptable way in. TableModify waits on the convention being more than read-only. A recursive CTE
is refused by Calcite too, deliberately.

Out of scope: `Combine` and `ConditionalCorrelate`, neither of which exists in 1.41. **`AsofJoin` is not out
of scope and never was** — `rel.core.AsofJoin`, `EnumerableAsofJoin` and `ENUMERABLE_ASOFJOIN_RULE` are all
in 1.41 and in the referenced assembly. The line that said otherwise was written against the wrong class
name (`rel.core.Asof`, which really is 1.42) and stood unchecked. The node is now written.

### Done

Scan, values, calc, project, filter, sort, limit, offset, limit-with-sort, union, intersect, minus,
hash/semi/anti join, nested loop join, merge join, ASOF join, correlate, aggregate, window, table function
scan, collect and uncollect. Converters in both directions, so one plan can hold nodes of both conventions and the rows
cross untouched. 159 tests pass.

`PARITY.md` is the member-by-member comparison against 1.41, rebuilt from the source at the tag and checked
against the assembly. What is left is section 6 of it and the list above.

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
of `passThroughTraits`. One line is not Calcite's and is argued in `PARITY.md` §6: a required trait set of
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

Worth knowing for the next node: **the chosen plan varies from run to run.** `VolcanoCost.isLt` compares
the row count alone, so equal-cost plans are decided by which was seen first, and under IKVM a string hashes
as .NET hashes it — randomised per process. Two of the three defects above appeared in one run and not the
next. Run the suite more than once before believing it.

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

### Two things reshaped to match Calcite's own division

**The aggregate is two classes**, as Calcite's is. `ClrEnumerableAggregateBase` holds what
`EnumerableAggregateBase` holds — `hasOrderedCall`, `declareParentAccumulator`, `implementLambdaFactory`,
`createAccumulatorAdders`, `createAggStateTypes`, and the two implementor contexts — and
`ClrEnumerableAggregate` holds the constructor, `copy` and `implement`. The accumulator helpers had been on
`ClrEnumUtils`, which put them on a class Calcite has no counterpart of. They are static because
`ClrEnumerableWindow` needs two of them and is not an aggregate; that is the whole reason.

**`ClrEnumerablePrefer` is this convention's own**, against `EnumerableRel.Prefer`. Same five values and the
same three questions asked of them, plus `ToCalcite` and `FromCalcite`, which are called at the two
converters and nowhere else. What a node asks its input for belongs to the convention asking.

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
about where linq4j may appear is untouched. Nothing else could have worked: under IKVM a String hashes as
.NET hashes it, which is randomised per process, so the order is not the same from one run to the next and
only reading the same map in the same process makes the two conventions agree. It settles the sort inside a
partition as well — `arrays` uses `Arrays.sort`, which is stable.

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

### The three sorted nodes left, and each is blocked on its own thing

MergeJoin, MergeUnion, SortedAggregate and BatchNestedLoopJoin are only ever chosen over their hash and
buffering counterparts when the input already carries a collation, and a table is where one comes from:
`getStatistic().getCollations()` is what both conventions' scans put in their trait set. Every fixture
table advertised none, so none of the four could be planned **in either convention** — writing any of them
would have produced a node no test could reach, which is the mistake TUMBLE already cost a turn to.
`SortedTable`, registered as `SORTED`, advertises a collation on its first field, and that unblocked the
first of the four.

**MergeJoin is written**, and the section above records it. It needed less than the others: a collated
input and nothing more.

The other three:

- `EnumerableMergeUnionRule` is not a converter rule. Its operand is a `LogicalSort` with a `LogicalUnion`
  **as its immediate input**, and a SELECT list over a UNION puts a `LogicalProject` between the two, so it
  never matches. Adding `PROJECT_REMOVE` and `PROJECT_MERGE` to the same pass does not fix it — the
  projections here are identities and should go, so either they are not removed before the rule is tried or
  something else also blocks it. That is the next thing to find out.
- `EnumerableSortedAggregateRule` asks, in `convert`, for an input carrying a collation on the group set,
  and `SORTED` supplies exactly that — yet the plain `EnumerableAggregate` still wins. Most likely the cost
  model rather than the rule: `VolcanoCost.isLt` compares the row count and nothing else, so a sorted
  aggregate and a hash one tie and the planner keeps whichever it saw first. The same quirk makes a project
  and a calc indistinguishable. If that is the reason it can never be chosen on cost, and a test has to
  force it.
- `BatchNestedLoopJoin` has not been investigated at all.

Also: `ENUMERABLE_SORTED_AGGREGATE_RULE` and `ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE` are **not** in
`EnumerableRules.ENUMERABLE_RULES`; Calcite turns them on by configuration, so the harness has to add them
to both sides for a comparison to mean anything.

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
What is missing against the referenced 1.41 is listed once, at the top of this part of the file, with the
order to take it in. What follows is only the note that used to sit here.

The line that used to sit here — that a query using OVER or MATCH_RECOGNIZE still runs because the planner
puts the node in `EnumerableConvention` and the converters carry the rows in and out — was **wrong for
MATCH_RECOGNIZE**, and had been since it was written. The fallback held for OVER and never held for
MATCH_RECOGNIZE. The converter now carries far more than it did, and a MATCH_RECOGNIZE still does not
cross it — see the Match section for the three separate things in the way. Never repeat the general claim
without running it.

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
