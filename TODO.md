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

### 0. Correlation values are never bound

Found by writing the tests for it, and worse than the rest of this list because the feature is not
missing — it is present, reachable, and broken.

`AdoToEnumerableConverter` creates an `AdoCorrelationDataContextBuilderImpl` and hands it to
`GenerateSql`, so the generated SQL correctly gets a parameter marker per correlation variable. It
then never calls `Build()` on it. Nothing carries the values to the `DbCommand`, so execution fails
with the provider complaining about unbound parameters. Calcite does the missing step at
`JdbcToEnumerableConverter:222`, wrapping `dataContextBuilder.build()` in an enricher and passing it
to the prepared-statement enumerable.

Everything needed is already here and unused: `AdoEnumerable.CreateReader(dataSource, sql,
rowBuilderFactory, DbCommandEnricher)`, `AdoEnumerable.CreateEnricher(metadata, indexes, context)`,
and the `DbCommandEnricher` interface. Note the two do not currently line up — `CreateEnricher`
returns `Action<DbCommand>` where `CreateReader` wants a `DbCommandEnricher`.

This is invisible by default because Calcite decorrelates correlated sub-queries into joins before
they reach the adapter, and eleven passing tests in `AdoCorrelationTests` confirm the answers are
right that way. Three further tests, marked `[Ignore]`, use `forceDecorrelate=false` to leave the
`Correlate` in the plan; un-ignore them when this is fixed.

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
