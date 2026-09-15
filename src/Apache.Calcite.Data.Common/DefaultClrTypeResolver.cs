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
            // and pairs with nothing else. There is deliberately no entry carrying a Guid to or from a
            // character type: reading text as a Guid is a conversion and a typed getter is a cast, and
            // before 1.43 the pairing existed only because there was no UUID to pair with. The ADO adapter
            // typed a provider uniqueidentifier as CHAR(36) for the same reason and stopped.
            m.Add(typeof(Guid), SqlTypeName.UUID, CalciteValues.ToUuid, CalciteValues.FromUuid);

            // what a Calcite type reads back as where the CLR type it pairs with is spoken for above
            m.Add(typeof(string), SqlTypeName.CHAR, CalciteValues.ToChar, CalciteValues.FromChar, ClrTypeMatch.RelDefault);
            m.Add(typeof(byte[]), SqlTypeName.BINARY, CalciteValues.ToBinary, CalciteValues.FromBinary, ClrTypeMatch.RelDefault);
            m.Add(typeof(double), SqlTypeName.FLOAT, CalciteValues.ToDouble, CalciteValues.FromDouble, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTime), SqlTypeName.DATE, CalciteValues.ToDate, CalciteValues.FromDate, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, CalciteValues.ToTimestampTz, CalciteValues.FromTimestampTz, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_TZ, CalciteValues.ToTimeTz, CalciteValues.FromTimeTz, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, CalciteValues.ToTimeTz, CalciteValues.FromTimeTz, ClrTypeMatch.RelDefault);

            // a GEOMETRY is well-known text here, as it is over Calcite's own JDBC: there is no .NET geometry
            // this package can hand out, and the JTS one is a Java object. RelDefault only, a bare string
            // being a VARCHAR.
            m.Add(typeof(string), SqlTypeName.GEOMETRY, CalciteValues.ToGeometry, CalciteValues.FromGeometry, ClrTypeMatch.RelDefault);

            // the intervals. A year-month one is a count of months whichever of the three it is, and .NET
            // has no interval that counts months, so the count is the value; a day-time one is a fixed
            // length of time and a TimeSpan is exactly that. Both are RelDefault only: an int is written as
            // an INTEGER and a TimeSpan as a TIME, which the entries above already say.
            foreach (var months in new[] { SqlTypeName.INTERVAL_YEAR, SqlTypeName.INTERVAL_YEAR_MONTH, SqlTypeName.INTERVAL_MONTH })
                m.Add(typeof(int), months, CalciteValues.ToIntervalMonths, CalciteValues.FromIntervalMonths, ClrTypeMatch.RelDefault);

            foreach (var time in new[]
            {
                SqlTypeName.INTERVAL_DAY, SqlTypeName.INTERVAL_DAY_HOUR, SqlTypeName.INTERVAL_DAY_MINUTE, SqlTypeName.INTERVAL_DAY_SECOND,
                SqlTypeName.INTERVAL_HOUR, SqlTypeName.INTERVAL_HOUR_MINUTE, SqlTypeName.INTERVAL_HOUR_SECOND,
                SqlTypeName.INTERVAL_MINUTE, SqlTypeName.INTERVAL_MINUTE_SECOND, SqlTypeName.INTERVAL_SECOND,
            })
                m.Add(typeof(TimeSpan), time, CalciteValues.ToIntervalTime, CalciteValues.FromIntervalTime, ClrTypeMatch.RelDefault);

            // what a CLR type is written as where the Calcite type it pairs with is spoken for above.
            // a CHAR is a string in Calcite's runtime, so a char is a string of one. Written as a CHAR(1)
            // and never what a CHAR(1) column answers with, a one-character column being a string like any
            // other.
            m.Add(typeof(char), SqlTypeName.CHAR, CalciteValues.ToCharacter, CalciteValues.FromCharacter, ClrTypeMatch.ClrDefault, precision: 1);
            // Calcite has no unbounded integer type, and a DECIMAL of scale zero is what an integer of any
            // width is
            m.Add(typeof(System.Numerics.BigInteger), SqlTypeName.DECIMAL, CalciteValues.ToBigInteger, CalciteValues.FromBigInteger, ClrTypeMatch.ClrDefault);
            m.Add(typeof(DateOnly), SqlTypeName.DATE, CalciteValues.ToDate, CalciteValues.FromDateOnly, ClrTypeMatch.ClrDefault);
            m.Add(typeof(TimeOnly), SqlTypeName.TIME, CalciteValues.ToTime, CalciteValues.FromTimeOnly, ClrTypeMatch.ClrDefault);

            // legal when asked for by name, and nobody's default
            m.Add(typeof(DateOnly), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => DateOnly.FromDateTime((DateTime)CalciteValues.FromTimestamp(v)), ClrTypeMatch.Named);
            m.Add(typeof(TimeOnly), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => TimeOnly.FromDateTime((DateTime)CalciteValues.FromTimestamp(v)), ClrTypeMatch.Named);
            m.Add(typeof(DateTime), SqlTypeName.TIMESTAMP_TZ, CalciteValues.ToTimestampTz, v => ((DateTimeOffset)CalciteValues.FromTimestampTz(v)).UtcDateTime, ClrTypeMatch.Named);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => new DateTimeOffset((DateTime)CalciteValues.FromTimestamp(v), TimeSpan.Zero), ClrTypeMatch.Named);

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
            // a bare collection names no Calcite type, and the one it wants is built from its element's
            // rather than looked up: the table can only build a type from a SqlTypeName and a precision,
            // which cannot say INTEGER ARRAY
            if (relType is null && clrType is not null && Collection(clrType, context) is ClrTypeMapping collection)
                return collection;

            return _mappings.GetMapping(clrType, relType, context);
        }

        /// <summary>
        /// Returns the mapping a bare .NET collection is written through, or <see langword="null"/> where
        /// the type is not one.
        /// </summary>
        /// <param name="clrType">The CLR type a value is being written as.</param>
        /// <param name="context"></param>
        /// <returns>The mapping, or <see langword="null"/>.</returns>
        /// <remarks>
        /// <b>The element decides, so this recurses too.</b> An <c>int[]</c> is an <c>INTEGER ARRAY</c>
        /// because an <see cref="int"/> is an <c>INTEGER</c>, and an <c>int[][]</c> is an
        /// <c>INTEGER ARRAY ARRAY</c> because an <c>int[]</c> is an <c>INTEGER ARRAY</c>. Asking the
        /// registry for the element's mapping is what makes the second sentence follow from the first
        /// without a second rule.
        ///
        /// <para><see cref="T:byte[]"/> is excluded deliberately: it is an array in .NET and a
        /// <c>VARBINARY</c> in SQL, and the table above already pairs them. A <see cref="string"/> is not
        /// excluded because it is not an array, though it does enumerate.</para>
        /// </remarks>
        static ClrTypeMapping? Collection(Type clrType, ClrTypeContext context)
        {
            var typeFactory = context.TypeFactory;

            if (clrType.IsArray && clrType.GetArrayRank() == 1 && clrType != typeof(byte[]))
            {
                var element = clrType.GetElementType()!;
                var mapping = context.Registry.GetMapping(Nullable.GetUnderlyingType(element) ?? element, null);
                if (mapping is null)
                    return null;

                // nullable where the .NET element type admits a null, which is what Nullable<T> says and
                // what a reference type says by being one
                var nullable = Nullable.GetUnderlyingType(element) is not null || element.IsValueType == false;

                return new CollectionClrTypeMapping(context,
                    typeFactory.createArrayType(typeFactory.createTypeWithNullability(mapping.RelType, nullable), -1));
            }

            if (Dictionary(clrType) is not (Type key, Type value))
                return null;

            var keyMapping = context.Registry.GetMapping(Nullable.GetUnderlyingType(key) ?? key, null);
            var valueMapping = context.Registry.GetMapping(Nullable.GetUnderlyingType(value) ?? value, null);
            if (keyMapping is null || valueMapping is null)
                return null;

            // a dictionary's keys are never null, so the map's key type is not nullable and the mapping
            // answers a dictionary rather than pairs, which is the shape that went in
            return new MapClrTypeMapping(context,
                typeFactory.createMapType(
                    typeFactory.createTypeWithNullability(keyMapping.RelType, false),
                    typeFactory.createTypeWithNullability(valueMapping.RelType, Nullable.GetUnderlyingType(value) is not null || value.IsValueType == false)));
        }

        /// <summary>
        /// Returns the key and value types of a .NET dictionary, or <see langword="null"/> where the type
        /// is not one.
        /// </summary>
        /// <param name="clrType"></param>
        /// <returns>The key and value types, or <see langword="null"/>.</returns>
        static (Type Key, Type Value)? Dictionary(Type clrType)
        {
            foreach (var i in clrType.IsInterface ? [clrType, .. clrType.GetInterfaces()] : clrType.GetInterfaces())
            {
                if (i.IsGenericType == false)
                    continue;

                var definition = i.GetGenericTypeDefinition();
                if (definition == typeof(System.Collections.Generic.IDictionary<,>) || definition == typeof(System.Collections.Generic.IReadOnlyDictionary<,>))
                {
                    var arguments = i.GetGenericArguments();
                    return (arguments[0], arguments[1]);
                }
            }

            return null;
        }

        /// <inheritdoc />
        public System.Collections.Generic.IEnumerable<Type> GetClrTypes(RelDataType relType, ClrTypeContext context)
        {
            return _mappings.GetClrTypes(relType, context);
        }

    }

}
