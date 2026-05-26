# Apache.Calcite.Extensions

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Extensions)](https://www.nuget.org/packages/Apache.Calcite.Extensions)

**Apache.Calcite.Extensions** provides .NET-friendly helper and interop types for working with Apache Calcite via [IKVM](https://github.com/ikvmnet/ikvm). It bridges the gap between the Java-oriented Calcite configuration API and idiomatic .NET code.

This package is used internally by [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) and [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet). Reference it directly when you need typed access to Calcite connection properties from your own code.

## Install

```sh
dotnet add package Apache.Calcite.Extensions
```

## Key types

### `CalciteConnectionProperties`

Provides strongly-typed .NET properties over a Calcite `java.util.Properties` map. Instead of reading and writing raw string keys, you get compile-time-checked access to every Calcite connection option:

```csharp
using Apache.Calcite.Data;
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

Available properties mirror every `CalciteConnectionProperty` enum value:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Model` | `string` | — | URI or inline JSON model. |
| `Schema` | `string` | — | Default schema name. |
| `CaseSensitive` | `bool` | `true` | Case-sensitive identifier matching. |
| `Lex` | `Lex` | `ORACLE` | Lexical policy (`ORACLE`, `MYSQL`, `MYSQL_ANSI`, `SQL_SERVER`, `JAVA`, `BIG_QUERY`). |
| `Quoting` | `Quoting` | from `Lex` | Identifier quote character. |
| `QuotedCasing` | `Casing?` | from `Lex` | Storage of quoted identifiers. |
| `UnquotedCasing` | `Casing?` | from `Lex` | Storage of unquoted identifiers. |
| `Fun` | `string` | `standard` | Function libraries, e.g. `oracle,spatial`. |
| `Conformance` | `SqlConformanceEnum` | `DEFAULT` | SQL conformance level. |
| `DefaultNullCollation` | `NullCollation` | `HIGH` | NULL sort order when `NULLS FIRST`/`LAST` is omitted. |
| `TimeZone` | `string` | JVM default | Session time zone. |
| `Locale` | `string` | JVM default | Session locale. |
| `ForceDecorrelate` | `bool` | `true` | Aggressive subquery de-correlation. |
| `MaterializationsEnabled` | `bool` | `false` | Use materializations in the planner. |
| `TypeCoercion` | `bool` | `true` | Implicit type coercion during validation. |
| `ApproximateDecimal` | `bool` | `false` | Allow approximate DECIMAL aggregate results. |
| `ApproximateDistinctCount` | `bool` | `false` | Allow approximate `COUNT(DISTINCT ...)`. |
| `ApproximateTopN` | `bool` | `false` | Allow approximate Top-N results. |
| `Spark` | `bool` | `false` | Use Spark as the in-process execution engine. |
| `TopdownOpt` | `bool` | `false` | Enable top-down optimization in the Volcano planner. |
| `LenientOperatorLookup` | `bool` | `false` | Silently create unknown functions during parsing. |
| `SchemaFactory` | `string` | — | Schema factory class name (when not using a model). |
| `SchemaType` | `string` | `MAP` | Schema type: `MAP`, `JDBC`, or `CUSTOM`. |
| `ParserFactory` | `string` | — | Custom SQL parser factory. |
| `TypeSystem` | `string` | — | Type system class name. |

### `CalciteConnectionPropertiesSchemaMap`

Exposes the `schema.*`-prefixed sub-properties of a `CalciteConnectionProperties` instance as a typed dictionary, making it easy to pass operand values to a custom schema factory:

```csharp
var props = new CalciteConnectionProperties();
props.SchemaProperties["directory"] = "data/csv";
props.SchemaProperties["flavor"]    = "scannable";
```

## Related packages

| Package | Purpose |
|---------|---------|
| [`Apache.Calcite.Data`](https://www.nuget.org/packages/Apache.Calcite.Data) | Core ADO.NET provider for Apache Calcite. |
| [`Apache.Calcite.Adapter.AdoNet`](https://www.nuget.org/packages/Apache.Calcite.Adapter.AdoNet) | Federated ADO.NET adapter for Calcite. |

## Further reading

- [Apache Calcite documentation](https://calcite.apache.org/docs/)
- [Source repository](https://github.com/ikvmnet/calcite-dotnet)

## License

Apache License 2.0.
