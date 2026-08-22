# Apache.Calcite.Data.Common

What the Apache Calcite ADO.NET provider and the ADO.NET adapter both need: the CLR type mapping.

Calcite holds a value of a SQL type in a particular runtime class — a `DATE` is a count of days in a
`java.lang.Integer`, a `DECIMAL` is a `java.math.BigDecimal`, a `UTINYINT` is an `org.joou.UByte` — and
.NET wants a `DateTime`, a `decimal`, a `byte`. Deciding which .NET type a column is seen as, and
converting values across that boundary in both directions, is one question asked in four places: reading a
provider's `DbDataReader` into a plan, binding a command parameter, reading a result column, and typing a
table. This project answers it once.

The answer comes from a chain of `IClrTypeResolver`, first non-null winning, which a caller extends by
putting its own resolver in front:

```csharp
connection.TypeMapper.Prepend(new MyResolver());
```

A mapping states the CLR type it presents, the Calcite type it presents it as, and the two conversions.
What it may not state is the runtime class Calcite holds the value in: that comes from
`JavaTypeFactory.getJavaClass`, and the first conversion a mapping performs is checked against it. The
type factory is the authority because it is not fixed — a schema that types a column with
`createJavaType` carries its own class through the whole plan, ahead of every `SqlTypeName` the switch in
`JavaTypeFactoryImpl` knows.
