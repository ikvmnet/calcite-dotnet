# Apache.Calcite.Extensions

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Extensions)](https://www.nuget.org/packages/Apache.Calcite.Extensions)

**Apache.Calcite.Extensions** runs [Apache Calcite](https://calcite.apache.org/) query plans as compiled .NET code. Calcite runs on .NET through [IKVM](https://github.com/ikvmnet/ikvm); this package adds:

- **`ClrCursorConvention`**, a Calcite calling convention that compiles a query plan to a `System.Linq.Expressions` tree instead of generating Java source for the Janino compiler.
- **A prepare pipeline** (`ClrPrepareImpl`) that takes a SQL statement to such a plan.
- **A table SPI for .NET**: tables that return `IEnumerable`, `IAsyncEnumerable`, an expression tree, or a cursor such as a `DbDataReader`.
- **`CalciteConnectionProperties`**, typed access to Calcite's connection options.

Most applications get this package as a dependency of [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) (the ADO.NET provider) or [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet), and never call it directly: every statement run through a `CalciteConnection` is already planned and executed this way. Reference it directly when you want to:

- drive Calcite's planner yourself, without an ADO.NET connection;
- implement tables in .NET;
- build Calcite connection properties in code.

Targets .NET 8; tested on .NET 8 and .NET 10. It references Calcite 1.43 and IKVM 8.16.1, and requires IKVM 8.16.0 or later.

## Install

```sh
dotnet add package Apache.Calcite.Extensions
```

## What the convention gives you

- **No Java compiler at prepare time.** Preparing a statement builds an expression tree; each compiled plan is JIT-compiled the first time it is opened.
- **.NET methods as SQL functions.** A static .NET method registered with Calcite's usual function API is called directly from the compiled plan:

  ```csharp
  rootSchema.add("MY_FUNC", org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
      (java.lang.Class)typeof(MyFunctions), "myFunc"));
  ```

- **Synchronous and asynchronous execution from one plan.** A compiled plan opens a forward-only cursor with `Open()` or `OpenAsync(CancellationToken)`, and the cursor advances with `Read()` or `ReadAsync(CancellationToken)`. You can choose on every row: a row read with `Read` and the next with `ReadAsync` are consecutive rows of the same result, and each `ReadAsync` token is passed down to the table the row comes from. This is the shape of `DbDataReader`.
- **Works alongside Calcite's own engine.** `ClrCursorConvention` mirrors Calcite's `EnumerableConvention` node for node and uses the same row types. Calcite's own rules stay registered, and converters connect the two conventions, so anything this convention does not implement is planned by Calcite as usual and its rows pass between the two unchanged.

The results are Calcite's: the conventions are tested against each other by running the same SQL through both and comparing the rows.

## Running a plan yourself

To plan and run SQL without an ADO.NET connection, use a Calcite `Frameworks` planner with this convention's rules added, then compile the physical plan with `ClrCursorRelImplementor`.

A `Frameworks` planner registers only Calcite's rules, so add this convention's in a program that runs first. This one does nothing else:

```csharp
using System.Collections.Generic;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

public sealed class AddRulesProgram(IReadOnlyList<RelOptRule> rules) : Program
{
    public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
    {
        foreach (var rule in rules)
            planner.addRule(rule);

        return rel;
    }
}
```

Then plan the statement. `rootSchema` is your Calcite root schema, `sql` the statement, and `dataContext` a `DataContext` over the same schema:

```csharp
using Apache.Calcite.Extensions.Adapter.Cursor;
using org.apache.calcite.tools;

var calcRules = new java.util.ArrayList();
foreach (var rule in Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.CalcRules())
    calcRules.add(rule);

var config = Frameworks.newConfigBuilder()
    .defaultSchema(rootSchema)
    .programs(
        Programs.sequence(
            new AddRulesProgram(Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.Rules()),
            Programs.standard(Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider),
            Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider)))
    .build();

var planner = Frameworks.getPlanner(config);
var logical = planner.rel(planner.validate(planner.parse(sql))).project();

// keep the root's own traits, which carry any ORDER BY collation, and ask for this convention
var traits = logical.getTraitSet().replace(Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorConvention.Instance).simplify();
var physical = planner.transform(0, traits, logical);
```

Finally compile the plan and read it, asynchronously or synchronously:

```csharp
var implementor = new Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRelImplementor(
    physical.getCluster().getRexBuilder(), new java.util.HashMap());
var factory = implementor.ImplementRoot((Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRel)physical, ClrCursorPrefer.Array);

// opening does the plan's up-front work (a sort drains its input, a table runs its query); reading returns rows
await using var cursor = await factory.OpenAsync(dataContext, cancellationToken);
while (await cursor.ReadAsync(cancellationToken))
    Console.WriteLine(cursor.Current);

// or synchronously, over the same factory
using var pulled = factory.Open(dataContext);
while (pulled.Read())
    Console.WriteLine(pulled.Current);
```

This code is run by a test in the repository (`ReadmeExampleTests`).

Things to know about the result:

- **A row** is an `object[]` when you pass `ClrCursorPrefer.Array`. A result with one column is the column's value itself, not a one-element array.
- **Values are Java values**: `java.lang.Integer`, `java.lang.String`, `java.math.BigDecimal` and so on, as Calcite's type factory declares them.
- **A factory can be opened any number of times.** Each open returns an independent cursor, positioned before the first row. Dispose the cursor when you are done; disposing it releases the whole plan.
- **Each open is compiled on first use.** `ImplementRoot` builds both the synchronous and the asynchronous open; each is JIT-compiled the first time it is called.

Why the program is shaped this way, if you want to write your own:

- **`Rules()` goes on the planner; `CalcRules()` runs afterwards as a separate `Programs.hep` pass.** The calc rules cannot be planner rules: Calcite's Volcano planner compares plans by row count only, so a calc is never cheaper than the project it replaces, and it does not match transformation rules against physical nodes.
- **Keep Calcite's rules registered.** `Rules()` adds to Calcite's rules rather than replacing them. Calcite's logical rewrites are needed for `AVG`, `DISTINCT` aggregates and `OVER` windows, and a node this convention cannot implement is planned by Calcite and connected with a converter.
- **Decorrelation runs as in Calcite.** `Programs.standard` turns a scalar sub-query or `EXISTS` into a join; an `UNNEST` over a correlation variable keeps its correlate.
- **Use `ClrCursorRelMetadata.Provider`** for both passes. It is Calcite's default metadata provider with handlers added for this convention's nodes; without it, some row-count bounds, collations and costs are computed as if for a generic node, and the planner can choose different plans from Calcite's.
- **`ClrRelOptUtil.RegisterDefaultRules`** registers Calcite's default rules followed by `Rules()`, for a planner you create yourself rather than through `Frameworks`.

## Writing tables in .NET

Calcite's own `ScannableTable` and `QueryableTable` work, but require a linq4j `Enumerable`. This package adds three table interfaces, in `Apache.Calcite.Extensions.Schema`, that a table can implement instead. Each has a required synchronous member and an asynchronous member that defaults to it:

| Interface | Required | Asynchronous (optional) | Use for |
|---|---|---|---|
| `IClrScannableTable` | `Scan(DataContext)` returning `IEnumerable<object?[]>` | `ScanAsync(DataContext)` returning `IAsyncEnumerable<object?[]>` | rows as arrays |
| `IClrQueryableTable` | `ElementType`, `GetExpression(schema, name)` returning an expression of `IEnumerable<ElementType>` | `GetAsyncExpression(schema, name)` returning an expression of `IAsyncEnumerable<ElementType>` | a typed element; the expression is compiled into the plan |
| `IClrCursorTable` | `Open(DataContext)` returning `IClrCursor<object?[]>` | `OpenAsync(DataContext, CancellationToken)` | a source that is already a cursor, such as a `DbDataReader`; each `ReadAsync` token reaches it |

- **Row values must be the Java values** Calcite's type factory declares for each column (`java.lang.Integer` for `INTEGER`, `java.lang.String` for `VARCHAR`, and so on). They are not converted; a value of the wrong type fails when the column is read.
- **If your rows arrive over I/O, implement the asynchronous member too.** The default reads the synchronous member without suspending, so it would block an asynchronous caller.
- **If your source is only asynchronous,** implement the asynchronous member and write the synchronous one by blocking on it. Clear `SynchronizationContext.Current` before starting the asynchronous call, not only around the wait, or it can deadlock under a UI or ASP.NET-style context.
- **Cancellation**: an `IClrScannableTable` or `IClrQueryableTable` receives the token of the asynchronous open through `GetAsyncEnumerator`; an `IClrCursorTable` receives a token with every `ReadAsync`. Calcite-implemented parts of a plan observe cancellation through the `DataContext`'s cancel flag instead.

## Key public types

| Type | Purpose |
|------|---------|
| `ClrCursorConvention` | The calling convention. `ClrCursorConvention.Instance` is the trait. |
| `ClrCursorRules` | `Rules()` for the planner and `CalcRules()` for the pass after it. |
| `ClrRelOptUtil` | `RegisterDefaultRules(planner, enableMaterializations)`: Calcite's default rules plus this convention's. |
| `ClrCursorRelMetadata` | `Provider`, the metadata provider to plan with. |
| `ClrCursorRelImplementor` | Compiles a physical plan: `ImplementRoot` returns a `ClrCursorFactory`. |
| `IClrCursorFactory` / `ClrCursorFactory` | A compiled plan: `Open(DataContext)`, `OpenAsync(DataContext, CancellationToken)` and `ElementType`. |
| `IClrCursor` / `IClrCursor<T>` | A forward-only cursor: `Read()`, `ReadAsync(CancellationToken)`, `Current`, and both `Dispose` forms. `ClrCursor` and `ClrCursor<T>` are abstract bases for implementing one. |
| `ClrCursorPrefer` | The row representation requested; `Array` gives `object[]` rows. |
| `IClrScannableTable` / `IClrQueryableTable` / `IClrCursorTable` | The .NET table interfaces described above. |
| `IClrPrepare` / `ClrPrepareImpl` | The SQL prepare pipeline `Apache.Calcite.Data` uses: `PrepareSql` returns an `IClrPrepare.Signature`, whose `Open` and `OpenAsync` run the statement. |
| `ClrCursorRel` | The interface every node of the convention implements, for writing a node of your own. `Implement` is required; `ImplementAsync` defaults to it, which is correct only for a node whose implementation does not visit a child node. |
| `CalciteConnectionProperties` | Typed properties over Calcite's connection options. |
| `CalciteConnectionPropertiesSchemaMap` | The `schema.*` operands passed to a schema factory. |

The convention's nodes (`ClrCursorCalc`, `ClrCursorHashJoin`, `ClrCursorWindow` and the rest) and their rules are public, so you can subclass or re-register them. The runtime operators the nodes compile calls to are internal; a node of your own calls its own methods.

## `CalciteConnectionProperties`

Typed .NET properties over a Calcite `java.util.Properties` map, so you do not have to spell property names or enum values as strings:

```csharp
using Apache.Calcite.Extensions.Config;
using org.apache.calcite.avatica.util;
using org.apache.calcite.config;

var props = new CalciteConnectionProperties();

props.Lex                     = Lex.MYSQL_ANSI;
props.CaseSensitive           = false;
props.DefaultNullCollation    = NullCollation.LOW;
props.Fun                     = "oracle,spatial";
props.TimeZone                = "UTC";
props.ForceDecorrelate        = true;
props.MaterializationsEnabled = false;
```

Pass an existing `Properties` to the constructor to read or modify it in place.

| Property | Type | Calcite's default | Description |
|----------|------|---------|-------------|
| `Model` | `string` | — | Model URI, or an inline JSON model prefixed with `inline:`. |
| `Schema` | `string` | — | Default schema name. |
| `CaseSensitive` | `bool` | from `Lex` (`true` for `ORACLE`) | Case-sensitive identifier matching. |
| `Lex` | `Lex` | `ORACLE` | Lexical policy (`ORACLE`, `MYSQL`, `MYSQL_ANSI`, `SQL_SERVER`, `JAVA`, `BIG_QUERY`). |
| `Quoting` | `Quoting` | from `Lex` | Identifier quote character. |
| `QuotedCasing` | `Casing?` | from `Lex` | How quoted identifiers are stored. |
| `UnquotedCasing` | `Casing?` | from `Lex` | How unquoted identifiers are stored. |
| `Fun` | `string` | `standard` | Function libraries, e.g. `oracle,spatial`. |
| `Conformance` | `SqlConformanceEnum` | `DEFAULT` | SQL conformance level. |
| `DefaultNullCollation` | `NullCollation` | `HIGH` | Where nulls sort when `NULLS FIRST`/`LAST` is omitted. |
| `TimeZone` | `string` | JVM default | Session time zone. |
| `Locale` | `string` | `Locale.ROOT` | Session locale. |
| `ForceDecorrelate` | `bool` | `true` | Decorrelate sub-queries as far as possible. |
| `TopDownGeneralDecorrelationEnabled` | `bool` | `false` | Decorrelate with `TopDownGeneralDecorrelator` rather than `RelDecorrelator`. |
| `MaterializationsEnabled` | `bool` | `true` | Use materializations in the planner. |
| `CreateMaterializations` | `bool` | `true` | Create materializations on the fly. |
| `TypeCoercion` | `bool` | `true` | Implicit type coercion during validation. |
| `ApproximateDecimal` | `bool` | `false` | Allow approximate `DECIMAL` aggregate results. |
| `ApproximateDistinctCount` | `bool` | `false` | Allow approximate `COUNT(DISTINCT ...)`. |
| `ApproximateTopN` | `bool` | `false` | Allow approximate Top-N results. |
| `AutoTemp` | `bool` | `false` | Store query results in temporary tables. |
| `NullEqualToEmpty` | `bool` | `true` | Treat empty strings as null, for the Druid adapter. |
| `Spark` | `bool` | `false` | Use Spark for processing that cannot be pushed to the source. |
| `TopdownOpt` | `bool` | `calcite.planner.topdown.opt` | Top-down optimization in the Volcano planner. |
| `LenientOperatorLookup` | `bool` | `false` | Accept calls to functions not in the operator table. |
| `DruidFetch` | `int` | `16384` | Rows to fetch per Druid query. |
| `SchemaFactory` | `string` | — | Schema factory class (when not using a model). |
| `SchemaType` | `string` | — | Schema type: `MAP`, `JDBC`, or `CUSTOM`. |
| `ParserFactory` | `string` | — | Custom SQL parser factory. |
| `MetaTableFactory` / `MetaColumnFactory` | `string` | — | Avatica metadata factories. |
| `TypeSystem` | `string` | — | Type system class. |

A property that has not been set reads as Calcite's declared default. The four whose default comes from `Lex` read as `null` when unset (`false` for `CaseSensitive`), and Calcite applies the `Lex` value when the connection is made.

## `CalciteConnectionPropertiesSchemaMap`

`CalciteConnectionProperties.SchemaProperties` exposes the `schema.*` entries as a dictionary keyed without the prefix. Calcite passes them as operands to the schema factory:

```csharp
var props = new CalciteConnectionProperties();
props.SchemaProperties["directory"] = "data/csv";
props.SchemaProperties["flavor"]    = "scannable";
```

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | The ADO.NET provider. Executes SQL through this convention. |
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Exposes any ADO.NET data source to Calcite as a schema. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
