# Outstanding work

## ADO.NET adapter: gaps against Calcite's JDBC adapter

Established by comparing `src/Apache.Calcite.Adapter.AdoNet` against
`org.apache.calcite.adapter.jdbc` class by class. The rule sets are otherwise identical — join,
project, filter, aggregate, sort, union, intersect, minus, values and the to-enumerable converter
are all present on both sides, with the same node classes behind them, and correlated sub-queries
work.

None of the six below are covered by tests either way: the 75 adapter tests exercise the query path
only. The last four bugs found in this adapter were all in code nothing executed, so write the
failing test first.

### 0. Correlated sub-queries do not execute — one defect left of four

The feature is not missing; it is present, reachable, and was broken in four separate ways. Three are
fixed. It is invisible by default because Calcite decorrelates correlated sub-queries into joins
before they reach the adapter — eleven passing tests in `AdoCorrelationTests` confirm the answers are
right that way, and none of them touch this code at all. Three further tests, marked `[Ignore]`, use
`forceDecorrelate=false` to leave the `Correlate` in the plan. Un-ignore them when this is finished.

**Fixed.** `AdoToEnumerableConverter` created the correlation data context builder, handed it to
`GenerateSql` so the SQL got its parameter markers, and never called `Build()`, so nothing carried
the values to the command. It now builds an enricher and takes the four-argument `CreateReader`, the
way Calcite does at `JdbcToEnumerableConverter:222`.

**Fixed.** `AdoCorrelationDataContext.get` delegated every lookup and ignored `_parameters`
entirely — the interception its own doc comment described was never implemented.

**Fixed.** `AdoCorrelationDataContextBuilderImpl` cast `typeof(X)` to `java.lang.reflect.Type`, which
is a plain runtime cast a `System.RuntimeType` fails; only `(java.lang.Class)` is converted by IKVM.
Every use of the class threw. This stayed hidden because C# defers a static field until it is first
read, and the only read was in `Build()` — the method nobody called.

**Remaining.** The provider still rejects the command for unbound parameters. The SQL carries
Calcite's `?` marker while the enricher adds parameters named by
`AdoDatabaseMetadata.GetParameterName`, which is `$P0` for SQLite. Confirming that mismatch by
inspecting the generated SQL is the next step; the fix is most likely a dialect that unparses a
dynamic parameter using the provider's marker, rather than the JDBC default.

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

`JdbcUtils.DialectPool` and `JdbcUtils.DataSourcePool` cache by key. We rebuild each time, so dialect
detection re-queries provider metadata on every schema construction. Lowest priority; measure before
assuming it matters.

## Smaller items

- `AdoSchema.Create(null, …)` throws `NullReferenceException` from inside `Schemas.expression`
  rather than saying a parent schema is required. First thing anyone calling the API by hand hits.
- `AdoSetOpFactory.createSetOp` is covered only indirectly, through `UNION` / `INTERSECT` / `EXCEPT`
  queries. Direct tests need a planner fixture that does not exist yet.
- The AdoNet adapter is tested against SQLite only. `SqlServerDatabaseMetadata`,
  `OdbcDatabaseMetadata` and `OleDbDatabaseMetadata` have no coverage.

## Elsewhere

- The CLR enumerable / async-enumerable conventions are parked on the `clr-conventions` branch, with
  a working copy at `D:\calcite-dotnet-conventions-aside` (and its tests alongside). When they come
  back, the constraint to hold is that the adapter returns normal Calcite types in the normal Calcite
  format, and the conventions pass rows through untouched.
