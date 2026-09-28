# Apache.Calcite.Data.Common

The CLR type mapping shared by the Apache Calcite ADO.NET provider (`Apache.Calcite.Data`) and the ADO.NET
adapter (`Apache.Calcite.Adapter.AdoNet`): which .NET type each Calcite SQL type is presented as, and how
values are converted between the two in both directions.

Calcite holds values in Java classes: a `DATE` is a count of days in a `java.lang.Integer`, a `DECIMAL` a
`java.math.BigDecimal`, a `UTINYINT` an `org.joou.UByte`. .NET code wants a `DateTime`, a `decimal`, a
`byte`. This package decides that mapping for reading result columns, binding command parameters, reading an
ADO.NET provider's rows into a plan, and typing tables.

You normally get this package as a dependency of `Apache.Calcite.Data`. Reference it directly only to write
your own type resolver.

## The built-in mappings

`DefaultClrTypeResolver` maps every standard Calcite type. Among others:

| Calcite type | .NET type read | notes |
|---|---|---|
| `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `bool`, `sbyte`, `short`, `int`, `long` | |
| `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` | `byte`, `ushort`, `uint`, `ulong` | |
| `REAL`, `DOUBLE`, `FLOAT` | `float`, `double`, `double` | |
| `DECIMAL` | `decimal` | a `BigInteger` parameter is written as `DECIMAL` |
| `CHAR`, `VARCHAR` | `string` | a `char` parameter is written as `CHAR(1)` |
| `BINARY`, `VARBINARY` | `byte[]` | |
| `DATE`, `TIMESTAMP` | `DateTime` | a `DateTime` parameter is written as `TIMESTAMP`, a `DateOnly` as `DATE` |
| `TIME` | `TimeSpan` | a `TimeOnly` parameter is written as `TIME` |
| zoned `TIME` and `TIMESTAMP` types | `DateTimeOffset` | |
| `UUID` | `Guid` | |
| `GEOMETRY` | `string` | well-known text |
| year-month intervals | `int` | a count of months |
| day-time intervals | `TimeSpan` | millisecond precision |
| `ARRAY`, `MULTISET` | an array of the element type | e.g. `int?[]` for a nullable `INTEGER` element |
| `MAP` | `Dictionary<TKey, TValue>` | an array of `KeyValuePair` where the key type is nullable |
| `ROW` | `object[]` | |
| `ANY`, `OTHER`, `VARIANT` | `object` | each value mapped by its own type |

A caller asking for a specific type can also read `TIMESTAMP` as `DateOnly`, `TimeOnly` or
`DateTimeOffset`, `TIMESTAMP WITH TIME ZONE` as `DateTime`, and an array's elements as any type their element
type maps to (for example a `DATE ARRAY` as `DateOnly[]`). A type not listed has no mapping unless a resolver
of your own provides one.

## Adding your own mapping

Mappings come from a chain of `IClrTypeResolver`s; the first non-null answer wins. To present a type
differently, put a resolver in front of the built-in one. `ClrTypeMappingCollection` is a ready-made
resolver built from a table:

```csharp
using Apache.Calcite.Data.Common;
using org.apache.calcite.sql.type;

var mappings = new ClrTypeMappingCollection();

// An EmailAddress parameter is written as VARCHAR, and a VARCHAR column can be read as an EmailAddress
// when asked for by name (GetFieldValue<EmailAddress>). VARCHAR columns still read as string by default.
mappings.Add(
    typeof(EmailAddress),
    SqlTypeName.VARCHAR,
    toCalcite: v => ((EmailAddress)v).Value,
    fromCalcite: v => new EmailAddress((string)v),
    match: ClrTypeMatch.ClrDefault);
```

Register it for every connection of a data source:

```csharp
var dataSource = new CalciteDataSourceBuilder(connectionString)
    .AddTypeResolver(mappings)
    .Build();
```

or for one connection, before it is opened:

```csharp
connection.TypeMapper.Prepend(mappings);
connection.Open();
```

`ClrTypeMatch` says which lookups an entry answers when only one of the two types is known:
`RelDefault` makes the CLR type what the Calcite type reads as by default, `ClrDefault` makes the Calcite
type what a bare value of the CLR type is written as, and `Named` answers only when both are asked for.

Things to know:

- A conversion is given a non-null value, and `toCalcite` must return an instance of the Java class Calcite
  holds the type in (as decided by the session's `JavaTypeFactory.getJavaClass`); the first value a mapping
  converts is checked, and a mismatch throws `ClrTypeMappingException`.
- `Prepend` and `Append` replace any resolver of the same runtime type already in the chain, so two
  `ClrTypeMappingCollection` instances cannot both be registered; put all the entries in one, or implement
  `IClrTypeResolver` on a class of your own.
- Add all entries to a `ClrTypeMappingCollection` before registering it.
- A connection reads its chain when it first opens; `TypeMapper` throws after that.
- For a mapping that needs more than two delegates, derive from `ClrTypeMapping` and add the entry with a
  `ClrTypeMappingFactory`, or implement `IClrTypeResolver.GetMapping` directly.
