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
- **A .NET method can be a SQL function.** Janino cannot resolve the `cli.`-prefixed name IKVM gives a CLR class, so a .NET user-defined function does not work under Calcite's own engine. It works here.

`ClrEnumerableConvention` mirrors Calcite's `EnumerableConvention` node for node and uses the same row types, and converter rules exist in both directions. A plan may hold nodes of both conventions: anything this convention has no rule for is planned by Calcite as usual, and rows cross between the two untouched.

## Running a plan yourself

Plan into the convention with `ClrEnumerablePrograms`, then compile the root with `ClrEnumerableRelImplementor`. This example is executed by a test in the repository, so it cannot go stale silently:

```csharp
using Apache.Calcite.Extensions.Adapter.Enumerable;
using org.apache.calcite;
using org.apache.calcite.tools;

var config = Frameworks.newConfigBuilder()
    .defaultSchema(rootSchema)
    .programs(ClrEnumerablePrograms.Standard())
    .build();

var planner = Frameworks.getPlanner(config);
var logical = planner.rel(planner.validate(planner.parse(sql))).project();

// Standard() is one program, as Programs.standard() is, so this is one transform.
// the logical root's own traits, not an empty set: they carry the collation the ORDER BY produced,
// and SortRemoveRule takes the sort away as unwanted if the required traits do not ask for it
var traits = ClrEnumerablePrograms.DesiredRootTraitSet(logical.getTraitSet());
var physical = (ClrEnumerableRel)planner.transform(0, traits, logical);

// the root is a node of this convention; build its plan and compile it
var implementor = new ClrEnumerableRelImplementor(
    physical.getCluster().getRexBuilder(), new java.util.HashMap());
var lambda = implementor.ImplementRoot(physical, ClrEnumerablePrefer.Array);
var plan = (Func<DataContext, System.Collections.IEnumerable>)lambda.Compile();

foreach (var current in plan(dataContext))
{
    // a one-column result is the value itself, not a row of one
    var row = current as object[] ?? [current];
    Console.WriteLine(string.Join('\t', row));
}
```

`ClrEnumerableInterpretable.ToBindable(...)` is the alternative ending: it does the same work and hands back an `IClrBindable`, which you bind to a `DataContext` and enumerate. Use the implementor when you want the `LambdaExpression` itself.

Three things about `ClrEnumerablePrograms.Standard()` are deliberate and worth knowing before you substitute your own program:

- **The calc rules are a separate pass.** `VolcanoCost.isLt` compares row counts and nothing else, so a project and a calc are never cheaper than one another and the planner keeps whichever it saw first. Rewriting unconditionally afterwards as a hep pass is what makes a project's refusal to implement itself safe. `Programs.standard()` does the same thing for the same reason.
- **The planner pass registers Calcite's rules, then this convention's.** `Programs.standard()` installs none and plans with whatever is on the planner, which works because `RelOptUtil.registerDefaultRules` has already put Calcite's there. Nothing has heard of this convention, so `Rules()` registers — but it registers Calcite's set *as well as* ours, not instead of it. Dropping Calcite's takes with it the logical rewrites that belong to no convention, and `AVG`, every `DISTINCT` aggregate and every `OVER` window each need one of those before any planner sees them. It is also what lets a node this convention has no rule for be planned in `EnumerableConvention` and carried across a converter.
- **There is no decorrelation.** `Programs.standard()` runs one, and it rewrites a correlated sub-query into a join before the planner sees it — which is the node `ClrEnumerableCorrelate` exists to implement. Leaving it out is what puts a correlate on a plan at all.

A Spark handler is not supported: `ToBindable` throws `UnsupportedOperationException` if one is enabled, because a Spark handler compiles generated Java source and a plan of this convention is an expression tree.

## Key public types

| Type | Purpose |
|------|---------|
| `ClrEnumerableConvention` | The calling convention itself. `ClrEnumerableConvention.Instance` is the singleton trait. |
| `ClrEnumerablePrograms` | The program a query is planned with: `Standard()`, and the individual `SubQuery()` / `Rules()` / `CalcRules()` passes it sequences. Also `DesiredRootTraitSet`. |
| `ClrEnumerableRules` | The convention's rules: `Rules()` and `CalcRules()`. Add these to a planner you built yourself. |
| `ClrEnumerableRelImplementor` | Builds the expression tree for a plan. `ImplementRoot` returns a `LambdaExpression`. |
| `ClrEnumerableInterpretable` | `ToBindable` — implement, compile, and return an `IClrBindable`. |
| `IClrBindable` | A compiled plan. `Bind(DataContext)` returns the rows; `ElementType` says what one row is. |
| `ClrEnumerablePrefer` | How a caller wants rows represented — `Array` is what a prepared statement asks for. |
| `ClrEnumerableRelFactories` | `RelBuilder` factories producing nodes of this convention. |
| `ClrEnumerableRel` | The interface every node of this convention implements. |
| `CalciteConnectionProperties` | Typed .NET properties over Calcite's `java.util.Properties`. |
| `CalciteConnectionPropertiesSchemaMap` | The `schema.*` sub-properties, as a dictionary. |

The nodes (`ClrEnumerableCalc`, `ClrEnumerableHashJoin`, `ClrEnumerableWindow`, and the rest) and their rules are public too, so you can subclass or re-register them.

**The SQL-text prepare pipeline is internal to these packages.** `ClrPrepareImpl`, `ClrSignature` and the rest of `Apache.Calcite.Extensions.Prepare` are not part of the public API surface — `Apache.Calcite.Data` reaches them through `InternalsVisibleTo`. To run SQL text, use `Apache.Calcite.Data`; to drive the planner directly, use the public types above.

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

Defaults are Calcite's own, read from `CalciteConnectionProperty` in the version this package references (1.42).

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
