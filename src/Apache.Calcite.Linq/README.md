# Apache.Calcite.Linq

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Linq)](https://www.nuget.org/packages/Apache.Calcite.Linq)

Executes Apache Calcite plans as .NET expression trees.

Calcite's built-in `EnumerableConvention` runs a query by generating Java source and compiling it with
Janino at planning time. `ClrEnumerableConvention` instead builds the plan as a
`System.Linq.Expressions` tree and compiles that, so the query runs as .NET code with no Java compiler
in the path.

It mirrors `EnumerableConvention` node for node and uses the same row types: rows are built by Calcite's
`JavaTypeFactory` and `PhysType`, in Calcite's `JavaRowFormat`s, and pass from one step to the next
unmodified. Converters exist in both directions with Calcite's own convention, so a single plan can mix
them — which is how a query this convention has no node for still runs.

A user-defined function written in .NET runs here and in no plan Janino compiles. IKVM names a CLR class
`cli.Namespace.Type`; Janino cannot resolve such a name, but an expression tree holds the method itself.

## Install

```sh
dotnet add package Apache.Calcite.Linq
```

Targets **.NET 10**.

## Use

Give the planner this convention's programs, plan into it, then compile the plan:

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
var lambda = implementor.ImplementRoot(physical, ClrEnumerablePrefer.Array);
var plan = (Func<DataContext, IEnumerable>)lambda.Compile();

foreach (var current in plan(dataContext))
{
    // a one-column result is the value itself, not a row of one
    var row = current as object[] ?? [current];
    Console.WriteLine(string.Join('\t', row));
}
```

The plan is a `Func<DataContext, IEnumerable>` and nothing else — a .NET delegate over .NET sequences,
which is the point of the package. Bind it to a `DataContext` as often as you like.

## Key types

| | |
|---|---|
| `ClrEnumerableConvention` | the calling convention |
| `ClrEnumerableRules` | the rules that plan a query into it |
| `ClrEnumerablePrograms` | the planner passes those rules need |
| `ClrEnumerableRel` | the node interface; the planned root is one |
| `ClrEnumerableRelImplementor` | builds the plan's expression tree |
| `ClrEnumerablePrefer` | how rows are represented |

## License

Apache License 2.0.
