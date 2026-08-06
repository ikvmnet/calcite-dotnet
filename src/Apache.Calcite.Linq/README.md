# Apache.Calcite.Linq

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Linq)](https://www.nuget.org/packages/Apache.Calcite.Linq)

Runs [Apache Calcite](https://calcite.apache.org/) query plans as .NET code.

Calcite executes a query by generating Java source and compiling it at runtime with Janino.
`ClrEnumerableConvention` compiles the same plan into a `System.Linq.Expressions` tree instead, so the
query runs as .NET with no Java compiler in the path.

## Install

```sh
dotnet add package Apache.Calcite.Linq
```

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

## You may already have it

If you use [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data), this package comes
with it and every connection uses it by default. Nothing to install and nothing to configure — plans are
already compiled as expression trees.

Install this package directly when you drive Calcite's planner yourself, or want to point some other
Calcite connection at the convention.

## What you get

- **No runtime Java compilation.** Preparing a statement builds an expression tree and compiles it.
- **.NET user-defined functions.** A plan holds the method itself, so a UDF written in C# can be called
  from SQL. Calcite's own engine cannot do this: IKVM names a CLR class `cli.Namespace.Type`, and Janino
  cannot resolve such a name.
- **Rows stay as they are.** Rows are built by Calcite's `JavaTypeFactory` and `PhysType`, in Calcite's
  `JavaRowFormat`s, and pass from one step to the next unmodified.
- **Nothing stops working.** The convention mirrors Calcite's `EnumerableConvention` node for node, and
  converters exist in both directions, so a query it has no node for is planned by Calcite as usual and
  the rows cross untouched.

## Pointing a connection at it

`ClrEnumerablePrepare` is a `CalcitePrepare` that plans into this convention. Set it before the
connection is opened:

```csharp
using Apache.Calcite.Data;
using Apache.Calcite.Linq;

using var c = new CalciteConnection(connectionString);
c.PrepareFactory = () => new ClrEnumerablePrepare();
c.Open();
```

Calcite's own rules stay on the planner, so a statement this convention has no node for — a table
modification, a `MATCH_RECOGNIZE` — is still planned and still runs.

## Driving the planner yourself

If you build plans directly rather than through a connection, ask the planner for this convention on the
root, then compile what comes out:

```csharp
using System.Collections;

using Apache.Calcite.Linq;

using org.apache.calcite;
using org.apache.calcite.tools;

var config = Frameworks.newConfigBuilder()
    .defaultSchema(rootSchema)
    .programs(ClrEnumerablePrograms.Standard())
    .build();

var planner = Frameworks.getPlanner(config);
var logical = planner.rel(planner.validate(planner.parse(sql))).project();

// Standard() is three passes, in this order: sub-query expansion, the planner, then the calc rules.
var expanded = planner.transform(0, logical.getTraitSet(), logical);
var traits = ClrEnumerablePrograms.DesiredRootTraitSet(planner.getEmptyTraitSet());
var chosen = planner.transform(1, traits, expanded);
var physical = (ClrEnumerableRel)planner.transform(2, chosen.getTraitSet(), chosen);

// the root is a node of this convention; build its plan and compile it
var implementor = new ClrEnumerableRelImplementor(
    physical.getCluster().getRexBuilder(), new java.util.HashMap());
var plan = (Func<DataContext, IEnumerable>)implementor
    .ImplementRoot(physical, ClrEnumerablePrefer.Array)
    .Compile();

foreach (var current in plan(dataContext))
{
    // a one-column result is the value itself, not a row of one
    var row = current as object[] ?? [current];
    Console.WriteLine(string.Join('\t', row));
}
```

The plan is a `Func<DataContext, IEnumerable>` and nothing else — a .NET delegate over .NET sequences.
Bind it to a `DataContext` as often as you like.

## Key types

| | |
|---|---|
| `ClrEnumerableConvention` | the calling convention |
| `ClrEnumerablePrepare` | plans a connection's statements into it |
| `ClrEnumerableRules` | the rules that plan a query into it |
| `ClrEnumerablePrograms` | the planner passes those rules need |
| `ClrEnumerableRel` | the node interface; the planned root is one |
| `ClrEnumerableRelImplementor` | builds the plan's expression tree |
| `ClrEnumerablePrefer` | how rows are represented |

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | ADO.NET provider for Calcite. Uses this convention by default. |
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Expose any ADO.NET data source as a federated Calcite schema. |

## License

Apache License 2.0.
