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

### 6. Dialect and data source lookups are not cached across metadata instances

`JdbcUtils.DialectPool` and `JdbcUtils.DataSourcePool` cache by key, per data source, across schemas.
Every metadata implementation that derives its dialect from a live connection memoizes it, which was
the pressing part: deriving it opens a connection to read the server version, and
`AdoConvention.Dialect` is read for every rule that matches while planning. What is left is caching
*across* metadata instances, which only matters when several schemas point at one database. Lowest
priority; measure before assuming it matters.

## ADO.NET adapter: what more it could push, audited 2026-09-07

The section above compares us to `org.apache.calcite.adapter.jdbc` and lists where we fall short of it.
This one asks the other question — what the adapter could do that Calcite's does not — and the answer is
that the JDBC adapter pushes far less than `RelToSqlConverter` can write. `RelToSqlConverter` has a `visit`
for `Join`, `Correlate`, `Filter`, `Project`, `Window`, `Aggregate`, `TableScan`, `Union`, `Intersect`,
`Minus`, `Calc`, `Values`, `Sample`, `Sort`, `TableModify`, `Match`, `Uncollect` and `TableFunctionScan`.
Nine of those have a node in this adapter. **Every gap below is a node rel2sql can already write SQL for**,
so the work is a rel class and a converter rule, not a SQL generator.

### How this was measured

Thirty statements against the SQLite fixture through the Calcite JDBC driver, each run twice: once as
`EXPLAIN PLAN FOR` to read the physical plan, once for real with a `Hook.QUERY_PLAN` handler counting the
statements the adapter sent. A query that pushes fully sends one statement; a query that does not sends one
per pushed subtree, and the plan names what was left in `EnumerableConvention`. The probe was a scratch test
class in `Apache.Calcite.Adapter.AdoNet.Tests`, deleted afterwards; every number below came out of it.

**Pushing fully today**, one statement each: filter, project, `HAVING`, `ORDER BY` with `OFFSET`/`FETCH`,
`COUNT(DISTINCT)`, aggregate `FILTER`, `UNION ALL`, `INTERSECT` (through Calcite's count rewrite), `VALUES`
joined to a table, a non-equi inner join, `DISTINCT`, and `NOT EXISTS` after decorrelation. The nine nodes
we have carry a lot.

### 1. A window function does not merely fail to push — it throws

**`SELECT NAME, SUM(SALARY) OVER (PARTITION BY DEPTNO) FROM ADO.EMPS` fails with an `AssertionError`**:
*"Relational expression LogicalWindow.ADO.ADO … has calling-convention ADO.ADO but does not implement the
required interface AdoRel"*. So does a second one with two different `OVER` clauses, and so does
`… ORDER BY x.SALARY DESC LIMIT 1` inside a `LATERAL`, which Calcite rewrites to `ROW_NUMBER()`.

The cause is that `AdoProjectRule` deliberately admits a project containing `OVER` when the dialect
supports window functions (`AdoProjectRule.cs:38`, upstream's condition), and
`CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` — registered by `RelOptUtil.registerDefaultRules` and
matching any `Project` in any convention — then rewrites that `AdoProject` into a `LogicalWindow` carrying
the trait set it took from the project. The trait says ADO; the class is `LogicalWindow`; the convention
declares `AdoRel`; the assertion fires. Nothing in the suite reaches it because no adapter test uses `OVER`.

`AdoWindow` is both the fix and the capability: `RelToSqlConverter.visit(Window)` writes the `OVER` list
already. Gate the rule on `dialect.supportsWindowFunctions()`, which is what the project rule's condition
was for.

### 2. `GROUPING SETS`, `ROLLUP` and `CUBE` fetch the whole table

`AdoAggregateRule.cs:44` refuses any aggregate with more than one group set, which is upstream's
CALCITE-734 refusal. Measured: `GROUP BY ROLLUP(DEPTNO)`, `GROUP BY CUBE(DEPTNO, NAME)` and an explicit
`GROUPING SETS` each plan to an `EnumerableAggregate` over `SELECT * FROM "EMPS"` — the whole table crosses
the wire and is grouped in memory.

`RelToSqlConverter.generateGroupList` writes `CUBE`, `ROLLUP` and `GROUPING SETS`, picking the form from
`dialect.supportsGroupByWithRollup()` and `supportsGroupByWithCube()`, and `SqlImplementor` handles the
`HAVING GROUPING(…) <> 0` case where the group set is wider than the union of the group sets. The refusal
is older than the renderer. Lift it and gate on the two dialect methods.

### 3. Semi- and anti-joins are two statements

`AdoJoinRule.cs:83` returns null for `SEMI` and `ANTI`, on upstream's stated grounds that "it's not
possible to convert semi-joins or anti-joins; they have fewer columns than regular joins". That is no longer
true of the renderer: `RelToSqlConverter.visit(Join)` dispatches both to `visitAntiOrSemiJoin`, which writes
`EXISTS` / `NOT EXISTS`. `Join.deriveRowType` gives a semi-join the left row type without help.

Measured, `WHERE DEPTNO IN (SELECT …)` and `WHERE EXISTS (SELECT …)` both plan to `EnumerableHashJoin
(joinType=[semi])` over two `AdoToEnumerableConverter`s — two statements, and the join done here.

### 4. A `Correlate` that survives decorrelation runs one statement per row

`AdoCorrelationDataContext` and its builder exist so that a correlation variable referenced from inside a
pushed subtree becomes a dynamic parameter, bound per outer row. That is the JDBC adapter's answer and it
costs a statement — and, because `AdoEnumerable.enumerator()` opens its own connection
(`AdoEnumerable.cs:353`), a connection — for every row of the left input.

`RelToSqlConverter.visit(Correlate)` writes the whole thing as `CROSS JOIN LATERAL`. An `AdoCorrelate` would
turn N+1 statements into one wherever both sides are the same convention. The parameter path stays for the
cases it is still the only answer: the right side in another convention, or a dialect without `LATERAL`.

Top-N-per-group is the shape that makes this matter, and it is also blocked by §1 today.

### 5. `TABLESAMPLE` becomes `RAND()`, and SQLite has no `RAND()`

`SELECT * FROM ADO.EMPS TABLESAMPLE BERNOULLI(50)` plans to `AdoFilter(condition=[<(RAND(), 0.5)])` — 
`CoreRules.SAMPLE_TO_FILTER` rewrote the sample and we pushed the rewrite — and execution fails with
*"SQLite Error 1: 'no such function: RAND'"*. `RelToSqlConverter.visit(Sample)` writes a real `TABLESAMPLE`
clause, so an `AdoSample` gated on the dialect both pushes the operator and stops the rewrite from being
pushed in its place.

This is one instance of a general hole — see §7.

### 6. `MATCH_RECOGNIZE` over an ADO table cannot be implemented at all

`SELECT * FROM (SELECT EMPNO, SALARY FROM ADO.EMPS) MATCH_RECOGNIZE (…)` plans to an `EnumerableMatch` over
`AdoToEnumerableConverter` and then fails: *"Unable to implement EnumerableMatch … AdoToEnumerableConverter"*.
So the operator is unreachable over this adapter today by either route. `RelToSqlConverter.visit(Match)`
writes the clause, and Oracle and SQL Server 2022 have it. Whether that is worth a node is a separate
question from the fact that the query currently has no plan; the failure should at least be understood.

### 7. Nothing checks whether the target has the function or the type

`SqlDialect.supportsFunction(SqlOperator, RelDataType, List<RelDataType>)` and
`SqlDialect.supportsDataType(RelDataType)` are declared, are overridden by Firebolt, JethroData, Postgres,
Vertica and Oracle — and **are called from nowhere in `calcite-core`**, measured by `git grep` over the
1.42.0 tag. So `AdoFilterRule` and `AdoProjectRule` push any expression at all and the provider decides.
§5 is that hole firing on `RAND`; `CHAR_LENGTH` happens to survive because `SqliteSqlDialect` renders it as
`LENGTH`.

A `RexVisitor` over the condition and the projects, refusing where `supportsFunction` says no, is the same
shape as `CheckingUserDefinedFunctionVisitor`, which is already there and already runs on both. It costs a
walk that is already being done and it converts a class of run-time provider errors into an operator that
stays in memory.

The same visitor is what would let a *user-defined* function push when the target has one. The current
refusal is blanket: any `SqlFunction` whose `getFunctionType().isUserDefined()` stops the whole project or
filter, whether or not the target could evaluate it.

### 8. A `FULL JOIN` is pushed to dialects that cannot do one

Upstream's `JdbcJoinRule.matches` refuses a join whose type the dialect rejects
(`JdbcRules.java:372`, `dialect.supportsJoinType(joinType)`). `AdoJoinRule` has no `matches` override, so a
`FULL JOIN` planned against MySQL is pushed and the server refuses it. This is a divergence that pushes
*more* than upstream, which is why it has not shown up: SQLite and SQL Server both do full joins.

### 9. The converters are not cheap, and it costs whole joins — measured

`JdbcToEnumerableConverter.computeSelfCost` multiplies by `.1` (`JdbcToEnumerableConverter.java:88`).
Neither `AdoToEnumerableConverter` nor `AdoToClrEnumerableConverter` overrides `computeSelfCost` at all, so
leaving the adapter is priced at full row count and a plan that leaves it twice is not obviously worse than
one that leaves it once.

Measured by adding the `.1` to both converters and re-running the probe:

| statement | statements before | after |
|---|---|---|
| `EMPS FULL JOIN DEPTS` | 2, `EnumerableHashJoin(full)` | 1, `AdoJoin(full)` |
| `SELECT DNAME, (SELECT COUNT(*) FROM EMPS e WHERE e.DEPTNO = d.DEPTNO) FROM DEPTS d` | 2, `EnumerableMergeJoin(left)` | 1, the whole decorrelated tree pushed |

Nothing else in the thirty changed except the semi-join, which got *worse*: with cheap converters the
planner stops pushing the `GROUP BY` under the semi-join and pulls both tables whole, because §3 leaves it
no way to push the join itself. The two belong together — the multiplier is upstream's number and the reason
it is safe upstream is that upstream's rule set covers the join it makes attractive.

### 10. The adapter tells the planner nothing about the data

`AdoTable` does not implement `Statistic`; `RelOptTable.getStatistic()` answers Calcite's default, so every
table has 100 rows, no keys, no collations and no referential constraints. That is not only a costing
problem — three rules Calcite registers by default cannot fire without key metadata:
`CoreRules.AGGREGATE_REMOVE`, `CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN` and the join-removal rules. Measured:
`SELECT DISTINCT EMPNO FROM ADO.EMPS` pushes as `SELECT "EMPNO" FROM "EMPS" GROUP BY "EMPNO"`, and `EMPNO`
is the table's key — with a `Statistic` saying so the aggregate disappears entirely.

`AdoDatabaseMetadata` (`Metadata/AdoDatabaseMetadata.cs`) has `GetSchemas`, `GetTables` and `GetFields` and
nothing else, so this starts with an SPI addition. The inputs exist on every provider:
`DbConnection.GetSchema("Indexes")` / `"IndexColumns"`, `"ForeignKeys"`, and the information schema's
`TABLE_CONSTRAINTS` / `KEY_COLUMN_USAGE`. Row counts are per-target — `sys.dm_db_partition_stats` on SQL
Server, `pg_class.reltuples` on Postgres, `sqlite_stat1` where `ANALYZE` has run — which is an argument for
a virtual method with a null default rather than a required one, and for the time-to-live the plan-cache
section already argues for.

### 11. Two schemas over one database do not join server-side

`AdoConvention` is created per `AdoSchema`. Measured: two `AdoSchema`s registered over the *same* SQLite
file join through `EnumerableMergeJoin` and two statements, because the two conventions are distinct and
neither rule can see across. Calcite has the same shape and the same limitation.

Keying the convention by data source rather than by schema — the pool `JdbcUtils.DataSourcePool` already
implies, listed as §6 above — makes a multi-schema database one convention, and a three-way join across
`dbo`, `sales` and `staging` becomes one statement. This is the cheapest of the structural items and it is
the one a catalog schema (§4 above) makes reachable, since a catalog exposes exactly this shape.

### 12. Cross-source: ship the small side rather than pulling both

Nothing above helps a join whose sides are genuinely different databases. ADO.NET has three ways to make
that a server-side join that JDBC's adapter never used: a table-valued parameter, a temporary table filled
by a bulk copy, or a `VALUES` list inlined into the statement. The planner already knows which side is
smaller — it is the one whose estimated row count is lower, which needs §10 to be true rather than
defaulted.

The rule shape is a converter that takes a join with one side in another convention, plans that side
independently, and emits a statement whose right operand is the shipped rows. It is the largest item here
and the only one with no Calcite precedent to copy; it is also the one that makes the adapter something
other than a JDBC adapter.

### 13. `INSERT` / `UPDATE` / `DELETE` still do not plan

Already §1 of the section above; the probe confirms the failure mode is not a fallback but a planning
error — *"There are not enough rules to produce a node with desired properties: convention=ENUMERABLE …
Missing conversion is LogicalTableModify\[convention: NONE -> ENUMERABLE\]"* — for `INSERT … VALUES`,
`INSERT … SELECT`, `UPDATE` and `DELETE` alike. Noted here because two capabilities hang off it that
Calcite's adapter does not have: a `DbBatch` for a multi-row modify, and a bulk-copy path
(`SqlBulkCopy`, `NpgsqlBinaryImporter`) for `INSERT … SELECT` whose source is another convention, which is
§12's machinery pointed at a write.

### 14. The connect is synchronous, and timeout and `Cancel()` are not wired

The row loop awaits; the statement does not. It is sent at `GetAsyncEnumerator`, synchronously, through
`OpenConnection()` and `ExecuteReader()`. That is where this convention acquires — linq4j acquires inside
`enumerator()`, `ClrEnumerableDefaultsAcquisitionTests` holds that the whole cascade runs there, and
`ClrEnumerableConventionAsyncAdoNetTests` states that acquisition-time work is synchronous work because
`GetAsyncEnumerator` cannot await.

- **An asynchronous connect.** Getting `OpenConnectionAsync` and `ExecuteReaderAsync` as well means
  awaiting somewhere earlier than the first row, and the only place is `ExecuteReaderAsync` itself.
  Priming one row there was written and reverted: it fails `ShouldReadNothingUntilTheFirstRead`,
  `ExecuteAsyncShouldAcquireTheLeafWithoutReading` and
  `AnAsynchronousSortShouldAcquireAtExecuteAndDrainAtTheFirstRead`, which hold the opposite promise
  deliberately. So this is a change to the convention's execution contract rather than to the adapter, and
  it is the decision that gates it.
- **`DbCommand.Cancel()` is a no-op.** `StatementCancellation.Cancel()` is what it would call and the
  ADO.NET contract is what it costs: the command would have to hold the live statement, and the reader,
  which outlives the execute call, owns it now.
- **Timeout.** `DbCommand.CommandTimeout` is never set, so every statement takes the provider default.
- **Connection lifetime.** `AdoEnumerable.enumerator()` and `AdoSequences` open a connection per enumeration
  (`AdoEnumerable.cs:353`). For a plan with two pushed subtrees that is two connections, and for the
  per-row correlated path it is one per row. A connection held on the `DataContext` for the life of the
  execution, and `DbCommand.Prepare()` on the statement that is about to be run per row, are both things
  ADO.NET offers and this does not use. `CommandBehavior` is left at its default too — `SequentialAccess`
  matters for wide rows and `SingleResult` is free.

### 15. The catalog shows tables only

`AdoSchema.cs:76` builds every discovered object as `Schema.TableType.TABLE` — already §2 above. Beyond
that, `AdoBaseSchema.getFunctions` and `getTypeNames` return the empty defaults, so a server-side
table-valued function or stored procedure cannot be named in a query. `RelToSqlConverter.visit
(TableFunctionScan)` writes `TABLE(f(…))`, and ADO.NET reads a procedure's shape through
`CommandType.StoredProcedure` and `DbDataReader.GetSchemaTable`. The same reader-shape route would give a
raw-SQL passthrough table macro, which is the escape hatch every adapter of this kind ends up wanting.

### What this adds up to

Ordered by what a query gains per unit of work:

| | item | why first |
|---|---|---|
| 1 | `AdoWindow` (§1) | a live crash, and the fix is the capability |
| 2 | converter cost multiplier (§9) | two lines, measured to turn 2 statements into 1 twice |
| 3 | semi/anti join (§3) | one rule; pairs with §9, which makes its absence worse |
| 4 | grouping sets (§2) | one guard removed, two dialect calls added |
| 5 | `supportsFunction` gate (§7) | converts run-time provider failures into in-memory operators |
| 6 | `supportsJoinType` guard (§8) | a divergence that is wrong today |
| 7 | `AdoCorrelate` (§4) | N+1 statements to one, but needs §1 first to be reachable |
| 8 | statistics SPI (§10) | unlocks three core rules and everything cost-based |
| 9 | DML (§13) | the known feature gap |
| 10 | convention per data source (§11) | multi-schema databases join server-side |
| 11 | `AdoSample`, `AdoUncollect`, `AdoTableFunctionScan`, `AdoMatch` | renderable, narrower demand |
| 12 | cross-source shipping (§12) | the largest, and the one with no precedent to copy |

`Calc` is on rel2sql's list and is deliberately not on this one: `JdbcCalc` exists upstream with no rule
that produces it, and `AdoProject` and `AdoFilter` already push everything a calc would.

## A plan cache on the data source's root — *medium, and measured*

Every statement pays parse, validate, Volcano, translation and `LambdaExpression.Compile` on every
execution; parameters are bound at execution through the data context, so the same text with `?`
placeholders is the same plan, and the EF Core provider emits exactly that shape repeatedly. The root a
`CalciteDataSource` holds is where a cache of those plans belongs: a plan references the root's tables, the
root's lifetime bounds the plans' validity, and the root's write lock is the one place the root changes.

### What was measured, 2026-09-07

Through `ClrPrepareImpl.PrepareSql` over the `ClrPrepareFixture` schema (six-row `SALES`, three-row
`NUMS`), Debug build, medians of 15 runs after 3 warm-ups, milliseconds. The plan column is start to
`Hook.PLAN_BEFORE_IMPLEMENTATION`; compile is `Compile()` alone, instrumented for the run and reverted.

| statement | convention | parse+validate+plan | translate | compile | total prepare | execute |
|---|---|---|---|---|---|---|
| filter | sync | 19.2 | 1.4 | 0.6 | 22.8 | 0.35 |
| aggregate | sync | 40.8 | 1.2 | 2.6 | 45.3 | 1.93 |
| self join | sync | 46.5 | 1.2 | 2.2 | 51.4 | 1.56 |
| window | sync | 38.1 | 1.6 | 2.2 | 42.2 | 1.84 |
| sort limit | sync | 42.5 | 1.1 | 1.2 | 45.2 | 2.11 |
| union | sync | 42.1 | 0.3 | 0.6 | 43.2 | 0.24 |
| exists | sync | 89.9 | 0.4 | 0.9 | 91.8 | 0.34 |
| parameter | sync | 34.4 | 2.3 | 1.0 | 36.5 | 0.82 |
| filter | async | 14.6 | 1.6 | 0.7 | 17.6 | 0.56 |
| aggregate | async | 26.1 | 0.9 | 2.4 | 29.6 | 1.86 |
| self join | async | 34.6 | 0.9 | 2.0 | 37.9 | 1.55 |
| window | async | 30.4 | 1.3 | 1.9 | 33.6 | 1.88 |
| sort limit | async | 38.5 | 0.7 | 1.0 | 40.4 | 1.91 |
| union | async | 42.5 | 0.3 | 0.6 | 43.7 | 0.22 |
| exists | async | 92.2 | 0.4 | 0.8 | 93.5 | 0.32 |
| parameter | async | 30.8 | 2.6 | 1.2 | 36.3 | 1.14 |

**Planning is the cost, not compilation.** Translation and compilation together are 1 to 4 ms of a 18 to
94 ms prepare. So Calcite's own shape — `EnumerableInterpretable`'s static cache of compiled `Bindable`s
keyed by the generated source, `calcite.bindable.cache.maxSize` — would save a tenth of it here, and a
cache that skips planning is the one worth having. Its key is therefore the SQL text and everything that
changes what the text means, not a digest of a plan already made.

**A compiled plan is re-bindable.** The same signature, bound a second time and then from 8 tasks 25 times
each with a fresh `StatementDataContext` per bind, answered the same rows as its first bind for every
statement above, read either way. This is the property a prepared statement relies on in Calcite —
`Bindable.bind` is called per execution — and the translation keeps it: an anonymous class's fields become
variables of the block that builds the lambdas, and that block runs per bind.

**A compiled plan does not reach back to the type factory it was planned with.** Bound with a data context
carrying a fresh `JavaTypeFactoryImpl`, every statement above answered the same rows read either way.
The emitted record types are baked into the delegate, and nothing read at execution asks the factory for
one. So a cache on the root can hand a plan compiled under one connection's factory to another connection,
which is what a cache on the root means, the factory being per connection.

### The shape, when it is built

- **Where:** `CalciteDataSourceRoot`. Retiring the root retires its plans.
- **Key:** the SQL text; the engine configuration as a canonical string — lex, casing, conformance,
  function library, type system, null collation all change what a statement means; the default schema
  path; the convention. That is `CalciteConnectionStringBuilder.DataSourceKey` plus the two things it
  leaves out, so the root owns the cache and the session contributes its part of the key.
- **Invalidation, three kinds.** Schema change: DDL runs under the root's write lock, the one place the
  root changes, so a version bumped there drops the cache. Learnt facts: an adapter's statistics expire —
  calcite-cosmos on a five-minute lifetime — and a plan chosen while a table was small stays chosen after
  it grows; Calcite has no answer, SQL Server recompiles on a statistics change, and a time-to-live on
  entries is the cheap one. Size: bounded by count with LRU eviction, a keyword in the string with a
  default, zero to turn it off.
- **Semantics:** a cached plan is an implicit prepared statement, and Avatica's prepared statement already
  defines them — it holds the signature's snapshot until closed, so a cached plan executes against the
  snapshot it was planned on. `CalciteCommand.Prepare()`, a no-op today, becomes "plan it now and keep it".
- **Hooks bypass it.** A connection or command with hooks registered plans every time; a hook exists to
  watch planning happen.
- **Precedents:** SQL Server's server-side plan cache — automatic, keyed by text plus the session's `SET`
  options, invalidated by schema and statistics change — is the shape, since the engine is in-process and
  we are the server. Npgsql's auto-prepare is per connector and off by default; per connector is the wrong
  unit here, a plan not belonging to a connection.

### What is still unproven

- The numbers are one machine, a Debug build, six-row tables and a warm process. The split is what
  matters and it is not close; the absolute figures are not a benchmark. A cold first statement costs far
  more than any row above and is not what a cache saves.
- Re-bindability was measured over eight statement shapes. `ClrEnumerableConventionDifferentialTests` has far more,
  and the cache's own test should be that list bound twice, since a state a bind leaves behind would show
  as a differential failure on the second bind and nowhere else.
- Nothing above measured a cache hit's cost — key canonicalisation and lookup — against the 15 ms floor.

## `ClrDataCursorConvention`: what is not yet written

The convention, its implementor, its factory and cursor, its operator table, both converters against
`EnumerableConvention`, and ten nodes — scan, `VALUES`, calc (with the project and filter that become
one), sort, limit, union, ASOF join, correlate, conditional correlate and batch nested loop join — exist,
and every query the differential suite runs through them answers
Calcite's rows however the cursor is opened and however each row is advanced to. What is left is the
rest of the port and the pipeline that would make the provider use it.

- **The nodes not yet ported** — *large, and each is a transcription*. Aggregate and sorted aggregate,
  the hash, merge and nested loop joins, combine, intersect and minus, merge union, window, table
  function scan, collect and uncollect, repeat union and table spool, the interpreter, and limit-sort.
  Each is its `ClrEnumerable*` counterpart with the sequence replaced by the open, and the operator
  becomes a cursor class with `Read` and `ReadAsync` over one set of fields. Two things to carry over
  deliberately: an operator that acquires a source later than at its own open takes both openers of it —
  the nested loop join's inner side, as the correlate's already does, the CALCITE-2909 memoized
  hash-join lookups, the spool — and a drain the enumerable convention had to leave
  to the first advance, because `GetAsyncEnumerator` cannot await, moves into the awaiting open here,
  where it belongs. Until a node is written, Calcite plans it and a converter carries the rows.
- **The prepare pipeline does not prepare into it, and the provider does not read through it** —
  *medium*. `ClrPrepareImpl` prepares into `ClrEnumerableConvention` and `IClrPrepare.Signature` holds an
  `IClrBindableBase`; a `ClrDataCursorPreparingStmt` and a signature carrying the factory are the shape,
  after which `CalciteDataReader.Read` is `Read` and `ReadAsync(token)` is `ReadAsync(token)`, the per-call
  token reaching the leaf, and the `Synchronous` connection key has nothing left to select. That last is
  the reason the convention exists, and it is unproven until the provider is on it.
- **`ClrRelOptUtil.RegisterDefaultRules` does not register it** — *small, and a decision*. Registering a
  third convention's rules on every planner gives the planner a third set of nodes at equal cost, and
  which convention a tied plan lands in is then whichever it saw first. Leave it to the caller until the
  provider prepares into this convention alone, then decide whether the enumerable one stays registered.
- **A table SPI for cursors** — *small*. A table whose natural shape is a cursor — a provider's
  `DbDataReader`, opened with `ExecuteReader` or `ExecuteReaderAsync` and advanced with `Read` or
  `ReadAsync(token)` — has no way to hand one in: `IClrScannableTable` and `IClrQueryableTable` produce
  sequences, and the scan opens a cursor over them, losing the per-advance token at the leaf. The
  ADO.NET adapter's leaf is the case that matters.

## Test suites not yet written

Sized against measured coverage: `Apache.Calcite.Data` 78.0%, `Apache.Calcite.Adapter.AdoNet` ~60%.
Listed worst-first by uncovered lines. The `Apache.Calcite.Data` figures are `dotnet test -f net8.0
--collect:"XPlat Code Coverage"` over `Apache.Calcite.Data.Tests`.

- **`CalciteVariants`** — **0%, 65 uncovered**, the largest single gap in the reader now. Nothing
  reaches a `VARIANT` at all, which is the one type whose nulls are objects and whose payload carries
  its own type.
- **`CalciteValues`** — 67.2%, **95 uncovered**. The write direction and the map shapes are the bulk of
  it: `ToJava` has a case per CLR type and the tests exercise a handful.
- **`AdoSchemaFactory` from a Calcite model** — 0%. The operand-driven path is the primary documented
  way anyone configures an adapter, and nothing proves it works.
- **Connection strings, parameters, batches** — `CalciteConnectionStringBuilder` 55% (67 uncovered),
  `CalciteParameterCollection` 54.6% (39), `CalciteBatchCommandCollection` 22.8% (27). Mechanical,
  high line yield.

- **`Internal.CalciteVariants` is dead and should be deleted**, with the `VariantValue` arm of
  `Internal.CalciteValues.FromRuntime` that is its only reference. It is an earlier copy of what
  `VariantClrTypeMapping` does; it measured 0% because nothing reaches it, and the two have already
  drifted — the interval reading was added to the live one alone. Confirm the arm is unreachable
  before removing it.

Also at 0% and worth deciding about rather than covering: `AdoTableQueryable` (no provider ships, see
§5), `AdoUpdateEnumerable` (orphaned, see §1), and the twelve `Ado*Factory` relational factories with
`AdoRules.GetRules(convention, relBuilderFactory)`, which nothing calls.

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
- **A fractional row count is pushed as written to every dialect but SQL Server** — *small*. CALCITE-7624
  let a `FETCH` or an `OFFSET` carry a `BigDecimal`, and a count with a fractional part means the rows up
  to its ceiling: `EnumerableDefaults.take` and `skip` each count while the zero-based index is below the
  bound, and `RexUtil.makeOffsetFetchSum` states it as rounding "to whole row counts". `AdoSqlDialects.Mssql`
  writes that whole number; nothing else does. Measured against the SQLite fixture:
  `FETCH FIRST 2.9 ROWS ONLY` goes down as `LIMIT 2.9` and answers "SQLite Error 20: 'datatype mismatch'",
  and `OFFSET 1.5 ROWS` the same. A parameter is not affected there — `LIMIT $P0` with a decimal bound
  answers correctly, which is why issue 164 was SQL Server's alone. The fix is one more `SqlDialect`
  subclass per product, overriding `unparseOffsetFetch`; the literal arm alone is needed, and the `CAST`
  the SQL Server dialect wraps a parameter in must **not** be copied to a `LIMIT` dialect, since
  `SqlDialect.unparseLimit` refuses a fetch that is neither a literal nor a dynamic parameter.
- **Upstream, and worth reporting**: `SqlDialect.getCastSpec` writes an unbounded `VARCHAR` as the bare
  keyword, and a bare `varchar` in T-SQL is not unbounded — it is one character in a declaration and thirty
  in a `CAST`. So `CAST(<uniqueidentifier> AS VARCHAR)` is "Insufficient result space to convert
  uniqueidentifier value to char" and the same cast over a long `nvarchar` returns its first thirty
  characters with no error at all — both measured. `MssqlSqlDialect` does not override `getCastSpec`, and
  CALCITE-6565 made bare `CHAR` the intended rendering for SQL Server, which has the same defect.
  `AdoSqlDialects.Mssql` writes `VARCHAR(MAX)` / `VARBINARY(MAX)` for the unbounded case here; the fix
  belongs in Calcite. Found while there and not fixed: SQL Server's `varchar` tops out at 8000 and
  `MSSQL_TYPE_SYSTEM` inherits `getMaxPrecision` of 65536, so a stated length between the two is written
  out as it stands and the server refuses it — measured, `CAST('a' AS varchar(20000))` is "The size (20000)
  given to the type 'varchar' exceeds the maximum allowed for any data type (8000)". The clamp is not one
  number: `nvarchar` stops at 4000, and nothing in the dialect distinguishes the two today.

## `Apache.Calcite.FullText`: the capabilities left out of the first vocabulary

The nine operators cover everything Cosmos DB offers, which is the bar #157 set, and the survey behind them
turned up four capabilities that several stores have and this package does not. None is a gap in what was
promised; each is a decision not taken.

**A `FREETEXT` mode — exact words against meaning.** SQL Server splits `CONTAINS` from `FREETEXT`, and MySQL
`IN BOOLEAN MODE` from `IN NATURAL LANGUAGE MODE`; both distinguish matching the words written from matching
what they mean, stemming and thesaurus applied. PostgreSQL, Cosmos and SQLite FTS5 stem unconditionally and
have no such switch, so a second name would be a real distinction in two stores and a synonym in three. The
question is whether a synonym is harmless — it returns the same rows in those three — or whether a name that
only sometimes means something is worse than none.

**Proximity.** SQL Server's `NEAR((a, b), n)`, FTS5's `NEAR(a b, N)`, PostgreSQL's `<N>`, Elasticsearch's
`slop`, Atlas's `near`. Every one of them exists and they do not agree on what the number counts: tokens
between the terms in some, positions in others, and SQL Server's excludes the search terms themselves. A
shared `CLR_FT_NEAR(a, b, n)` whose `n` means something different per adapter would answer differently per
store without failing, which is the failure this package exists to prevent. Worth doing only with a
definition stated in the package and adapters held to it — which may mean the count is in tokens and a store
that cannot honour that declines.

**Highlighting and snippets.** `ts_headline`, FTS5's `highlight()` and `snippet()`, and the Elasticsearch and
Atlas highlighters. These are a different shape from the rest — they return marked-up text rather than
answering about a row — and Cosmos and SQL Server have nothing at all. Worth adding when two stores agree on
a shape; the FTS5 and PostgreSQL signatures are not close.

**Vector search.** `CLR_FT_RRF` takes any numeric score, so an adapter's own vector-distance operator already
fuses with a full text one and hybrid search is expressible. But Cosmos's `VectorDistance(v1, v2,
[bruteForce], [options])` has no shared spelling, and nor do pgvector's `<->`, `<#>` and `<=>`, Atlas's
`knnBeta`, or Elasticsearch's `knn`. A `CLR_VEC_*` vocabulary is the obvious sibling package and is a
separate piece of work — the types are the hard part there, a vector being an array of floats in some stores
and an opaque type in others.

## `Apache.Calcite.Geography`: a schema declaration cannot take an array or a geography

The same two limits `Apache.Calcite.FullText` measured apply there and are unmeasured for spatial.
`SqlUtil.lookupSubjectRoutines` runs a type-precedence pass whenever two candidates survive, and that pass
throws rather than declining for an argument whose precedence list cannot compare the parameter type —
`ArraySqlType` for an array, and the same shape of failure that `No assign rules for OTHER defined` was.
Geography's README says to register one route or the other because a name found twice resolves to whichever
is reached first; the stronger reason is that with both, some argument types stop working. Worth reproducing
against `CLR_ST_GEOG_*` and saying so in that README.

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

Note this is the same one boundary whether or not the plan awaits, and that the awaiting side of it is
settled: generated Java cannot await, so a sub-plan handed to Calcite is implemented synchronously and an
awaiting leaf inside it is read across, blocking a thread per row. Translating the tree would remove the
callback and the stash; it would not remove that, and nothing can, short of Calcite compiling something other
than Java.

## Decide whether to turn on 1.43's top-down decorrelator

A correlated `EXISTS` whose inner relation contains an `UNNEST` does not decorrelate — issue 125.
`RelDecorrelator` rewrites the correlate into a join and leaves the correlation live inside the right
input, so the implementor is handed `Calc($cor1.ID) / NestedLoopJoin(condition=true) / [scan,
Aggregate/Calc($cor1)]` and nothing binds `$cor1`; a join does not bind a correlation variable, only a
`Correlate` does. Both conventions throw alike, which
`ShouldAgreeOnFailingACorrelatedExistsOverAnUncollect` holds, and with `forceDecorrelate=false` the
correlate survives and the statement answers correctly, so the plan the decorrelator produces is
malformed and the plan it leaves alone is not.

`Programs.DecorrelateProgram` chooses between `RelDecorrelator` and `TopDownGeneralDecorrelator` on
`topDownGeneralDecorrelationEnabled`, and the top-down one answers this statement correctly *with*
decorrelation — measured, `OK [1]` where the default throws. `RelDecorrelator` itself is unchanged in
the parts at fault: 288 lines differ between 1.42 and 1.43 and none of them touch
`removeCorrelationViaRule`, its three rules, or the bail-outs. The property is 1.43 only.

**That lever is reachable now.** `CalciteConnectionStringBuilder.TopDownGeneralDecorrelationEnabled` and
`CalciteConnectionProperties.TopDownGeneralDecorrelationEnabled` write the property Calcite reads, and
nothing else in this project reads it: the choice is `Programs.DecorrelateProgram`'s, out of the connection
config, and the pipeline already skips its own decorrelation where the property is set.
`ShouldRunACorrelatedExistsOverAnUncollectWithTopDownDecorrelation` holds the statement through the prepare
pipeline and `Should_execute_a_correlated_exists_over_an_uncollect_with_top_down_decorrelation` through the
connection string. Turning it on *by default* is still a decision not taken — it is a different algorithm
over every statement, not a fix aimed at this one.
