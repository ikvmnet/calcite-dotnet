# Apache.Calcite.Linq

Executes Apache Calcite plans as .NET sequences.

Calcite's built-in `EnumerableConvention` runs a query by generating Java source and compiling it with
Janino at planning time. This package provides two calling conventions that instead compile a
relational plan into a `System.Linq.Expressions` tree.

| | Produces |
|---|---|
| `ClrEnumerableConvention` | `IEnumerable<TRow>` |
| `ClrAsyncEnumerableConvention` | `IAsyncEnumerable<TRow>` |

Both mirror `EnumerableConvention` node for node, and use the same row types: rows are built by
Calcite's `JavaTypeFactory` and `PhysType`, in Calcite's `JavaRowFormat`s, and pass from one step to
the next unmodified. Converters exist between the two conventions and in both directions with
Calcite's own, so a plan can mix them.

```sh
dotnet add package Apache.Calcite.Linq
```

## License

Apache License 2.0.
