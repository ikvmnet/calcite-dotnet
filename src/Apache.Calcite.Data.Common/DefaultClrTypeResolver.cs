using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The mappings that hold without anyone registering anything.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The order of the table is its priority, so the first entry written for a Calcite type is what that
    /// type reads back as and the first written for a CLR type is what that type is written as. The two
    /// catch-alls at the end are what a type nobody has claimed falls to: they dispatch on the runtime
    /// class of the value rather than on either type, which is what the ADO.NET surface did for a column
    /// whose SQL type it had no case for.
    /// </para>
    /// <para>
    /// Every entry is reachable and every one is a fact about Calcite rather than a preference. <c>FLOAT</c>
    /// is eight bytes here as it is in SQL and shares <c>DOUBLE</c>'s representation, <c>REAL</c> being the
    /// four-byte one — <c>JavaTypeFactoryImpl.getJavaClass</c> says so and marks it "sic". <c>TINYINT</c> is
    /// signed and the unsigned types are joou wrappers rather than wider <c>java.lang</c> ones. A
    /// <c>DATE</c> is a count of days and a <c>TIME</c> a count of milliseconds, both in an
    /// <c>Integer</c>; a <c>TIMESTAMP</c> is a count of milliseconds in a <c>Long</c>.
    /// </para>
    /// </remarks>
    public sealed class DefaultClrTypeResolver : IClrTypeResolver
    {

        /// <summary>
        /// Gets the singleton instance.
        /// </summary>
        public static DefaultClrTypeResolver Instance { get; } = new DefaultClrTypeResolver();

        readonly ClrTypeMappingCollection _mappings = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        DefaultClrTypeResolver()
        {
            var m = _mappings;

            // the natural pairs, each the default in both directions
            m.Add(typeof(bool), SqlTypeName.BOOLEAN, CalciteValues.ToBoolean, CalciteValues.FromBoolean);
            m.Add(typeof(sbyte), SqlTypeName.TINYINT, CalciteValues.ToTinyInt, CalciteValues.FromTinyInt);
            m.Add(typeof(short), SqlTypeName.SMALLINT, CalciteValues.ToSmallInt, CalciteValues.FromSmallInt);
            m.Add(typeof(int), SqlTypeName.INTEGER, CalciteValues.ToInteger, CalciteValues.FromInteger);
            m.Add(typeof(long), SqlTypeName.BIGINT, CalciteValues.ToBigInt, CalciteValues.FromBigInt);
            m.Add(typeof(byte), SqlTypeName.UTINYINT, CalciteValues.ToUTinyInt, CalciteValues.FromUTinyInt);
            m.Add(typeof(ushort), SqlTypeName.USMALLINT, CalciteValues.ToUSmallInt, CalciteValues.FromUSmallInt);
            m.Add(typeof(uint), SqlTypeName.UINTEGER, CalciteValues.ToUInteger, CalciteValues.FromUInteger);
            m.Add(typeof(ulong), SqlTypeName.UBIGINT, CalciteValues.ToUBigInt, CalciteValues.FromUBigInt);
            m.Add(typeof(float), SqlTypeName.REAL, CalciteValues.ToReal, CalciteValues.FromReal);
            m.Add(typeof(double), SqlTypeName.DOUBLE, CalciteValues.ToDouble, CalciteValues.FromDouble);
            m.Add(typeof(decimal), SqlTypeName.DECIMAL, CalciteValues.ToDecimal, CalciteValues.FromDecimal);
            m.Add(typeof(string), SqlTypeName.VARCHAR, CalciteValues.ToChar, CalciteValues.FromChar);
            m.Add(typeof(byte[]), SqlTypeName.VARBINARY, CalciteValues.ToBinary, CalciteValues.FromBinary);
            m.Add(typeof(DateTime), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, CalciteValues.FromTimestamp);
            m.Add(typeof(TimeSpan), SqlTypeName.TIME, CalciteValues.ToTime, CalciteValues.FromTime);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP_TZ, CalciteValues.ToTimestampTz, CalciteValues.FromTimestampTz);
            // Calcite has had a UUID type since 1.43 and holds one in a UuidValue, so a Guid pairs with it
            // in both directions. Before that there was no such type and a Guid had to be written as text,
            // which is what the CHAR(36) entry below still accepts by name.
            m.Add(typeof(Guid), SqlTypeName.UUID, CalciteValues.ToUuid, CalciteValues.FromUuid);

            // what a Calcite type reads back as where the CLR type it pairs with is spoken for above
            m.Add(typeof(string), SqlTypeName.CHAR, CalciteValues.ToChar, CalciteValues.FromChar, ClrTypeMatch.RelDefault);
            m.Add(typeof(byte[]), SqlTypeName.BINARY, CalciteValues.ToBinary, CalciteValues.FromBinary, ClrTypeMatch.RelDefault);
            m.Add(typeof(double), SqlTypeName.FLOAT, CalciteValues.ToDouble, CalciteValues.FromDouble, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTime), SqlTypeName.DATE, CalciteValues.ToDate, CalciteValues.FromDate, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, CalciteValues.ToTimestampTz, CalciteValues.FromTimestampTz, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_TZ, CalciteValues.ToTimeTz, CalciteValues.FromTimeTz, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, CalciteValues.ToTimeTz, CalciteValues.FromTimeTz, ClrTypeMatch.RelDefault);

            // what a CLR type is written as where the Calcite type it pairs with is spoken for above. A
            // Guid is a CHAR(36) on the way in and is never what a CHAR(36) column answers with, there
            // being nothing about such a column that says it holds one
            m.Add(typeof(Guid), SqlTypeName.CHAR, CalciteValues.ToGuid, CalciteValues.FromGuid, ClrTypeMatch.Named, precision: 36);
            m.Add(typeof(DateOnly), SqlTypeName.DATE, CalciteValues.ToDate, CalciteValues.FromDateOnly, ClrTypeMatch.ClrDefault);
            m.Add(typeof(TimeOnly), SqlTypeName.TIME, CalciteValues.ToTime, CalciteValues.FromTimeOnly, ClrTypeMatch.ClrDefault);

            // legal when asked for by name, and nobody's default
            m.Add(typeof(DateOnly), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => DateOnly.FromDateTime((DateTime)CalciteValues.FromTimestamp(v)), ClrTypeMatch.Named);
            m.Add(typeof(TimeOnly), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => TimeOnly.FromDateTime((DateTime)CalciteValues.FromTimestamp(v)), ClrTypeMatch.Named);
            m.Add(typeof(DateTime), SqlTypeName.TIMESTAMP_TZ, CalciteValues.ToTimestampTz, v => ((DateTimeOffset)CalciteValues.FromTimestampTz(v)).UtcDateTime, ClrTypeMatch.Named);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => new DateTimeOffset((DateTime)CalciteValues.FromTimestamp(v), TimeSpan.Zero), ClrTypeMatch.Named);
            m.Add(typeof(Guid), SqlTypeName.VARCHAR, CalciteValues.ToGuid, CalciteValues.FromGuid, ClrTypeMatch.Named);

            // the collections, which map by mapping what they hold and wrapping the result. Each asks the
            // registry for its element's, key's, value's or field's mapping, so one entry per kind covers
            // every depth and every element type, a caller's own included. They go in front of the two
            // catch-alls below, which claim every Calcite type and would otherwise take these first.
            m.Add(
                typeof(System.Array),
                SqlTypeName.ARRAY,
                static (context, relType, _) => new CollectionClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t.IsArray,
                relTypePredicate: static t => t.getSqlTypeName() == SqlTypeName.ARRAY);

            m.Add(
                typeof(System.Array),
                SqlTypeName.MULTISET,
                static (context, relType, _) => new CollectionClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t.IsArray,
                relTypePredicate: static t => t.getSqlTypeName() == SqlTypeName.MULTISET);

            m.Add(
                typeof(System.Collections.IDictionary),
                SqlTypeName.MAP,
                static (context, relType, _) => new MapClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || typeof(System.Collections.IEnumerable).IsAssignableFrom(t),
                relTypePredicate: static t => t.getSqlTypeName() == SqlTypeName.MAP);

            m.Add(
                typeof(object[]),
                SqlTypeName.ROW,
                static (context, relType, _) => new RowClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t == typeof(object[]),
                relTypePredicate: static t => t.isStruct());

            // the type whose only value is null. Reading one is null whatever a provider handed over, and
            // writing one is null whatever a caller wrote: java.lang.Void is what holds it and it has no
            // instances
            m.Add(typeof(object), SqlTypeName.NULL, static _ => null, static _ => null, ClrTypeMatch.RelDefault);

            // a Calcite type nothing above claimed: read the value on the strength of its runtime class.
            // OTHER and ANY arrive here, and so does a column a schema typed with createJavaType, whose
            // value is already whatever class that schema chose and wants no conversion at all
            m.Add(
                typeof(object),
                SqlTypeName.ANY,
                CalciteValues.ToShape,
                CalciteValues.FromShape,
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t == typeof(object),
                relTypePredicate: static _ => true);

            // a CLR type nothing above claimed, which is what a caller binding a value of their own gets
            m.Add(
                typeof(object),
                SqlTypeName.ANY,
                CalciteValues.ToShape,
                CalciteValues.FromShape,
                ClrTypeMatch.ClrDefault,
                clrTypePredicate: static _ => true);
        }

        /// <inheritdoc />
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
        {
            return _mappings.GetMapping(clrType, relType, context);
        }

        /// <inheritdoc />
        public System.Collections.Generic.IEnumerable<Type> GetClrTypes(RelDataType relType, ClrTypeContext context)
        {
            return _mappings.GetClrTypes(relType, context);
        }

    }

}
