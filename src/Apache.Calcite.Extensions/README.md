# Apache.Calcite.Extensions

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Extensions)](https://www.nuget.org/packages/Apache.Calcite.Extensions)

**Apache.Calcite.Extensions** is what .NET adds on top of [Apache Calcite](https://calcite.apache.org/) running under [IKVM](https://github.com/ikvmnet/ikvm): a calling convention that runs a query plan as compiled .NET code, the prepare pipeline that takes a statement from SQL text to such a plan, and the interop types both need.

No Java compiler runs when a statement is prepared, and a .NET user-defined function can be called from SQL.

Most people get this package as a dependency of [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) or [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) and never call it directly — every statement on a `CalciteConnection` is already planned and run this way, with nothing to configure. Reference it yourself when you want to drive Calcite's planner without an ADO.NET connection, or want typed access to Calcite's connection properties.

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

## Install

```sh
dotnet add package Apache.Calcite.Extensions
```

## Why you might want it

Calcite normally executes a query by generating Java source and compiling it at runtime with Janino. Under IKVM that works, but a Java compiler runs every time you prepare a statement, and any function you call from SQL has to be reachable by a Java class name.

This package replaces that step. A query plan is compiled into a `System.Linq.Expressions` tree and turned into a delegate, so:

- **No Java compiler runs when you prepare a statement.**
- **A .NET method can be a SQL function, and no class name is written out.** Calcite's own engine reaches one only through the class-loader stamp `IKVM.Maven.Sdk` puts on `calcite-core`, which IKVM 8.14.0 and 8.15.0 could not read — under those a .NET user-defined function had no plan under `EnumerableConvention` at all, Janino refusing the `cli.`-prefixed name IKVM gives a CLR class. IKVM 8.16.0 fixes it; here it never mattered, because nothing writes a name.

`ClrCursorConvention` mirrors Calcite's `EnumerableConvention` node for node and uses the same row types, and converter rules exist in both directions. A plan may hold nodes of both conventions: anything this convention has no rule for is planned by Calcite as usual, and rows cross between the two untouched.

**One plan, opened either way, read either way.** A plan of this convention is compiled to a `ClrCursorFactory`, with two members, `Open(DataContext)` and `OpenAsync(DataContext, CancellationToken)`. Both hand back the same kind of object: an `IClrCursor`, a forward-only cursor with `Read()` and `ReadAsync(CancellationToken)` over one position. A consumer chooses how to open, and then chooses again on every advance how to read, and a row read with one member and the next with the other are consecutive rows of one result.

That is the shape `DbDataReader` has. A sequence cannot have it: an `IEnumerable` or an `IAsyncEnumerable` states once, at `GetEnumerator` or `GetAsyncEnumerator`, whether it will be pulled or awaited, and takes its cancellation at the same moment. A cursor takes the token per advance and hands it down to the leaf. So the same prepared statement can be read synchronously by one caller and awaited by another, and an `EXPLAIN` cannot tell you which will happen.

## Running a plan yourself

Put this convention's rules on the planner, run `Programs.standard()`, then build the root with `ClrCursorRelImplementor`. This example is executed by a test in the repository, so it cannot go stale silently:

```csharp
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using org.apache.calcite;
using org.apache.calcite.tools;

var calcRules = new java.util.ArrayList();
foreach (var rule in ClrCursorRules.CalcRules())
    calcRules.add(rule);

// Programs.standard(), with this convention's rules put on the planner in front of it -- a
// Frameworks planner carries Calcite's alone -- and its calc rules run afterwards, which is
// Programs.calc once more over this convention's list. standard's own calc pass still runs
var config = Frameworks.newConfigBuilder()
    .defaultSchema(rootSchema)
    .programs(
        Programs.sequence(
            new AddRulesProgram(ClrCursorRules.Rules()),
            Programs.standard(),
            Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)))
    .build();

var planner = Frameworks.getPlanner(config);
var logical = planner.rel(planner.validate(planner.parse(sql))).project();

// one sequence, so one transform, exactly as Programs.standard is driven.
// the logical root's own traits, not an empty set: they carry the collation the ORDER BY produced,
// and SortRemoveRule takes the sort away as unwanted if the required traits do not ask for it
var traits = logical.getTraitSet().replace(ClrCursorConvention.Instance).simplify();
var physical = planner.transform(0, traits, logical);

var implementor = new ClrCursorRelImplementor(
    physical.getCluster().getRexBuilder(), new java.util.HashMap());
var factory = implementor.ImplementRoot((ClrCursorRel)physical, ClrEnumerablePrefer.Array);

// opening runs the plan's acquisition -- a sort drains, a leaf executes -- and reading reads rows
await using var cursor = await factory.OpenAsync(dataContext, cancellationToken);
while (await cursor.ReadAsync(cancellationToken))
    Console.WriteLine(cursor.Current);

// or synchronously, over the same factory
using var pulled = factory.Open(dataContext);
while (pulled.Read())
    Console.WriteLine(pulled.Current);
```

A one-column result is the value itself, not a row of one.

`ImplementRoot` walks the tree twice, through each node's `Implement` and its `ImplementAsync`, and puts both opens on one factory; each is compiled the first time it is called. A node's two bodies differ only in what is acquired at open — one drains a sort by blocking, the other by awaiting — and produce the same cursor class, whose two advances step the same fields. An operator that acquires a source later than at its own open, as linq4j's `concat` does inside `moveNext`, takes both opens of that source and calls the one matching the advance it is in.

Three things about this program are deliberate and worth knowing before you substitute your own:

- **The calc rules are a separate pass.** `VolcanoCost.isLt` compares row counts and nothing else, so a project and a calc are never cheaper than one another and the planner keeps whichever it saw first. Rewriting unconditionally afterwards as a hep pass is what makes a project's refusal to implement itself safe. `Programs.standard()` does the same thing for the same reason.
- **The planner pass registers Calcite's rules, then this convention's.** `Programs.standard()` installs none and plans with whatever is on the planner, which works because `RelOptUtil.registerDefaultRules` has already put Calcite's there. Nothing has heard of this convention, so `Rules()` registers — but it registers Calcite's set *as well as* ours, not instead of it. Dropping Calcite's takes with it the logical rewrites that belong to no convention, and `AVG`, every `DISTINCT` aggregate and every `OVER` window each need one of those before any planner sees them. It is also what lets a node this convention has no rule for be planned in `EnumerableConvention` and carried across a converter.
- **The decorrelation is Calcite's and is run.** A scalar sub-query and an `EXISTS` become joins, and an `UNNEST` over a correlation variable cannot be decorrelated and keeps its correlate — which is how Calcite reaches its own `EnumerableCorrelate` under `Programs.standard()` as well.

## Key public types

| Type | Purpose |
|------|---------|
| `ClrCursorConvention` | The calling convention itself. `ClrCursorConvention.Instance` is the singleton trait. |
| `ClrCursorRules` | The convention's rules: `Rules()` and `CalcRules()`. Add these to a planner you built yourself. |
| `ClrCursorRelImplementor` | Builds both opens of a plan and hands back a `ClrCursorFactory`. Two parallel hierarchies over one instance: `VisitChild` composes opens that acquire synchronously and `VisitChildAsync` opens that await. It carries no mode. `Pulled` and `Awaited` cross between them, and `Opener` and `OpenerAsync` defer an input's open for an operator that acquires it later. |
| `ClrCursorResult` / `ClrCursorAsyncResult` | What a node's two bodies answer, one type per kind, built by `Result` and `ResultAsync`. |
| `IClrCursorFactory` / `ClrCursorFactory` | A compiled plan: `Open(DataContext)` and `OpenAsync(DataContext, CancellationToken)` each hand back an `IClrCursor`, and `ElementType` says what one row is. `ClrCursorFactory` is the one the implementor builds. |
| `IClrCursor` / `IClrCursor<T>` | A forward-only cursor with `Read()` and `ReadAsync(CancellationToken)` over one position, and `Current`. `ClrCursor` and `ClrCursor<T>` are the abstract bases every cursor of this project derives from; a source that is a cursor already implements the interface directly. |
| `ClrEnumerablePrefer` | How a caller wants rows represented — `Array` is what a prepared statement asks for. It is in the `Adapter.Enumerable` namespace with the rest of the row machinery, which mirrors Calcite's `adapter.enumerable` package. |
| `ClrCursorRelFactories` | `RelBuilder` factories producing nodes of this convention. |
| `IClrScannableTable` / `IClrQueryableTable` / `IClrCursorTable` | The table SPI: a table hands back .NET sequences rather than linq4j ones, or a cursor. One interface per table kind, carrying both halves. `Scan`, `GetExpression` and `Open` are required; `ScanAsync`, `GetAsyncExpression` and `OpenAsync` default to reading them across. A table whose rows only ever arrive asynchronously overrides those and drains its own sequence for the required half. A cursor table is for a source that is a forward-only cursor already, a `DbDataReader` say: its cursor is the plan's leaf and the token of each `ReadAsync` reaches it. |
| `ClrCursorRel` | The interface every node of this convention implements. Two bodies: `Implement` composes opens that acquire synchronously, required, and `ImplementAsync` opens that await, optional and defaulting to `Implement`. That default is safe exactly when a body does not visit a child, which is not the same as having no input: a body that asks for its input and takes the default composes a synchronously opened input into an awaiting operator, which `Expression.Call` refuses. |
| `CalciteConnectionProperties` | Typed .NET properties over Calcite's `java.util.Properties`. |
| `CalciteConnectionPropertiesSchemaMap` | The `schema.*` sub-properties, as a dictionary. |

The nodes (`ClrCursorCalc`, `ClrCursorHashJoin`, `ClrCursorWindow`, and the rest) and their rules are public too, so you can subclass or re-register them.

**The operator set is not public.** `ClrCursorDefaults` and the `ClrCursorBuiltInMethod` table that names its members are internal to this package. A node you write outside it builds calls to its own methods with `Expression.Call`, and an awaiting one appends the token the awaiting root takes, which is the implementor's `CancellationToken` parameter.

**The SQL-text prepare pipeline is public, and it is what `Apache.Calcite.Data` prepares through.** `IClrPrepare` and its implementation `ClrPrepareImpl`, in `Apache.Calcite.Extensions.Prepare`, take a statement to an `IClrPrepare.Signature`, whose `Open` and `OpenAsync` are the factory's. The contexts it runs against are internal, so to run SQL text use `Apache.Calcite.Data`; to drive the planner directly, use the public types above.

## `CalciteConnectionProperties`

Strongly-typed .NET properties over a Calcite `java.util.Properties` map. Instead of reading and writing raw string keys, you get compile-time-checked access to Calcite's connection options:

```csharp
using Apache.Calcite.Extensions.Config;
using java.util;
using org.apache.calcite.avatica.util;

var props = new CalciteConnectionProperties();

// Typed setters — no magic strings needed.
props.Lex                  = Lex.MYSQL_ANSI;
props.CaseSensitive        = false;
props.DefaultNullCollation = NullCollation.LOW;
props.Fun                  = "oracle,spatial";
props.TimeZone             = "UTC";
props.ForceDecorrelate     = true;
props.MaterializationsEnabled = false;
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Model` | `string` | — | URI or inline JSON model. |
| `Schema` | `string` | — | Default schema name. |
| `CaseSensitive` | `bool` | from `Lex` (`true` under `ORACLE`) | Case-sensitive identifier matching. |
| `Lex` | `Lex` | `ORACLE` | Lexical policy (`ORACLE`, `MYSQL`, `MYSQL_ANSI`, `SQL_SERVER`, `JAVA`, `BIG_QUERY`). |
| `Quoting` | `Quoting` | from `Lex` | Identifier quote character. |
| `QuotedCasing` | `Casing?` | from `Lex` | Storage of quoted identifiers. |
| `UnquotedCasing` | `Casing?` | from `Lex` | Storage of unquoted identifiers. |
| `Fun` | `string` | `standard` | Function libraries, e.g. `oracle,spatial`. |
| `Conformance` | `SqlConformanceEnum` | `DEFAULT` | SQL conformance level. |
| `DefaultNullCollation` | `NullCollation` | `HIGH` | NULL sort order when `NULLS FIRST`/`LAST` is omitted. |
| `TimeZone` | `string` | JVM default | Session time zone. |
| `Locale` | `string` | `Locale.ROOT` | Session locale. |
| `ForceDecorrelate` | `bool` | `true` | Aggressive subquery de-correlation. |
| `TopDownGeneralDecorrelationEnabled` | `bool` | `false` | De-correlate with `TopDownGeneralDecorrelator` rather than `RelDecorrelator`. |
| `MaterializationsEnabled` | `bool` | `true` | Use materializations in the planner. |
| `CreateMaterializations` | `bool` | `true` | Create materializations on the fly. |
| `TypeCoercion` | `bool` | `true` | Implicit type coercion during validation. |
| `ApproximateDecimal` | `bool` | `false` | Allow approximate DECIMAL aggregate results. |
| `ApproximateDistinctCount` | `bool` | `false` | Allow approximate `COUNT(DISTINCT ...)`. |
| `ApproximateTopN` | `bool` | `false` | Allow approximate Top-N results. |
| `AutoTemp` | `bool` | `false` | Store query results in a temporary table. |
| `NullEqualToEmpty` | `bool` | `true` | Treat empty strings as null, for the Druid adapter. |
| `Spark` | `bool` | `false` | Use Spark as the in-process execution engine. |
| `TopdownOpt` | `bool` | `calcite.planner.topdown.opt` | Enable top-down optimization in the Volcano planner. |
| `LenientOperatorLookup` | `bool` | `false` | Silently create unknown functions during parsing. |
| `DruidFetch` | `int` | `16384` | Rows to fetch per Druid query. |
| `SchemaFactory` | `string` | — | Schema factory class name (when not using a model). |
| `SchemaType` | `string` | — | Schema type: `MAP`, `JDBC`, or `CUSTOM`. |
| `ParserFactory` | `string` | — | Custom SQL parser factory. |
| `MetaTableFactory` / `MetaColumnFactory` | `string` | — | Avatica metadata factories. |
| `TypeSystem` | `string` | — | Type system class name. |

Defaults are Calcite's own, read from `CalciteConnectionProperty` in the version this package references (1.43).

## `CalciteConnectionPropertiesSchemaMap`

Exposes the `schema.*`-prefixed sub-properties of a `CalciteConnectionProperties` instance as a typed dictionary, so operand values can be passed to a custom schema factory:

```csharp
var props = new CalciteConnectionProperties();
props.SchemaProperties["directory"] = "data/csv";
props.SchemaProperties["flavor"]    = "scannable";
```

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | The ADO.NET provider. Executes SQL text through this convention. |
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Exposes any ADO.NET data source to Calcite as a federated schema. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
