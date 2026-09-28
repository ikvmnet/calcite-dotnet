using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The built-in mappings, which every chain starts with.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each Calcite type reads back as the first CLR type listed for it, and each CLR type is written as the
    /// first Calcite type listed for it:
    /// </para>
    /// <list type="bullet">
    /// <item><c>BOOLEAN</c>, <c>TINYINT</c>, <c>SMALLINT</c>, <c>INTEGER</c>, <c>BIGINT</c>, <c>REAL</c>,
    /// <c>DOUBLE</c> and <c>DECIMAL</c> are <see cref="bool"/>, <see cref="sbyte"/>, <see cref="short"/>,
    /// <see cref="int"/>, <see cref="long"/>, <see cref="float"/>, <see cref="double"/> and
    /// <see cref="decimal"/>; <c>FLOAT</c> reads as <see cref="double"/>, since Calcite holds it as a Java
    /// <c>double</c>.</item>
    /// <item><c>UTINYINT</c>, <c>USMALLINT</c>, <c>UINTEGER</c> and <c>UBIGINT</c> are <see cref="byte"/>,
    /// <see cref="ushort"/>, <see cref="uint"/> and <see cref="ulong"/>.</item>
    /// <item><c>VARCHAR</c> and <c>CHAR</c> read as <see cref="string"/>, which is written as <c>VARCHAR</c>; a
    /// <see cref="char"/> is written as <c>CHAR(1)</c>.</item>
    /// <item><c>VARBINARY</c> and <c>BINARY</c> read as <c>byte[]</c>, which is written as <c>VARBINARY</c>.</item>
    /// <item><c>TIMESTAMP</c> and <c>DATE</c> read as <see cref="DateTime"/>, which is written as
    /// <c>TIMESTAMP</c>; <see cref="DateOnly"/> is written as <c>DATE</c>.</item>
    /// <item><c>TIME</c> reads as <see cref="TimeSpan"/>, which is written as <c>TIME</c>;
    /// <see cref="TimeOnly"/> is written as <c>TIME</c>.</item>
    /// <item><c>TIMESTAMP WITH TIME ZONE</c>, <c>TIMESTAMP WITH LOCAL TIME ZONE</c>, <c>TIME WITH TIME
    /// ZONE</c> and <c>TIME WITH LOCAL TIME ZONE</c> read as <see cref="DateTimeOffset"/>, which is written as
    /// <c>TIMESTAMP WITH TIME ZONE</c>.</item>
    /// <item><c>UUID</c> is <see cref="Guid"/>. No character type converts to or from a
    /// <see cref="Guid"/>.</item>
    /// <item>A <see cref="System.Numerics.BigInteger"/> is written as <c>DECIMAL</c>.</item>
    /// <item><c>GEOMETRY</c> reads as well-known text in a <see cref="string"/>.</item>
    /// <item>A year-month interval reads as an <see cref="int"/> count of months; a day-time interval as a
    /// <see cref="TimeSpan"/>.</item>
    /// <item><c>ARRAY</c> and <c>MULTISET</c> read as arrays, <c>MAP</c> as a dictionary and a row as an
    /// <c>object[]</c>, each element mapped through the chain. A one-dimensional array other than
    /// <c>byte[]</c>, or a generic dictionary, is written as an <c>ARRAY</c> or <c>MAP</c> of whatever its
    /// element, key and value types are written as.</item>
    /// <item><c>VARIANT</c>, <c>ANY</c> and <c>OTHER</c> read as <see cref="object"/>, mapped by the runtime
    /// class of each value; <c>NULL</c> reads and writes as <see langword="null"/>.</item>
    /// </list>
    /// <para>
    /// A <see cref="DateTime"/> can also be read from <c>TIMESTAMP WITH TIME ZONE</c>, a
    /// <see cref="DateTimeOffset"/>, <see cref="DateOnly"/> or <see cref="TimeOnly"/> from <c>TIMESTAMP</c>,
    /// when that CLR type is asked for by name.
    /// </para>
    /// <para>
    /// A type not listed has no mapping, and a lookup for it answers <see langword="null"/> rather than
    /// guessing from the value's runtime class. To read such a type, put a resolver for it in front of this
    /// one.
    /// </para>
    /// </remarks>
    public sealed class DefaultClrTypeResolver : IClrTypeResolver
    {

        /// <summary>
        /// Gets the single instance.
        /// </summary>
        public static DefaultClrTypeResolver Instance { get; } = new DefaultClrTypeResolver();

        readonly ClrTypeMappingCollection _mappings = new();

        /// <summary>
        /// Initializes the instance and fills its table.
        /// </summary>
        DefaultClrTypeResolver()
        {
            var m = _mappings;

            // pairs that are the default in both directions
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
            // Calcite holds a UUID in a UuidValue. There is deliberately no entry between Guid and a
            // character type: reading text as a Guid is a conversion, and a typed getter is a cast.
            m.Add(typeof(Guid), SqlTypeName.UUID, CalciteValues.ToUuid, CalciteValues.FromUuid);

            // Calcite types that read back as a CLR type already written as something else above
            m.Add(typeof(string), SqlTypeName.CHAR, CalciteValues.ToChar, CalciteValues.FromChar, ClrTypeMatch.RelDefault);
            m.Add(typeof(byte[]), SqlTypeName.BINARY, CalciteValues.ToBinary, CalciteValues.FromBinary, ClrTypeMatch.RelDefault);
            m.Add(typeof(double), SqlTypeName.FLOAT, CalciteValues.ToDouble, CalciteValues.FromDouble, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTime), SqlTypeName.DATE, CalciteValues.ToDate, CalciteValues.FromDate, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, CalciteValues.ToTimestampTz, CalciteValues.FromTimestampTz, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_TZ, CalciteValues.ToTimeTz, CalciteValues.FromTimeTz, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, CalciteValues.ToTimeTz, CalciteValues.FromTimeTz, ClrTypeMatch.RelDefault);

            // GEOMETRY reads as well-known text: there is no .NET geometry type to hand out, and the JTS
            // geometry Calcite holds is a Java object. A bare string is written as VARCHAR.
            m.Add(typeof(string), SqlTypeName.GEOMETRY, CalciteValues.ToGeometry, CalciteValues.FromGeometry, ClrTypeMatch.RelDefault);

            // intervals. .NET has no type counting months, so a year-month interval reads as its count of
            // months; a day-time interval is a fixed length of time, which is a TimeSpan. Neither is a
            // CLR default: an int is written as INTEGER and a TimeSpan as TIME.
            foreach (var months in new[] { SqlTypeName.INTERVAL_YEAR, SqlTypeName.INTERVAL_YEAR_MONTH, SqlTypeName.INTERVAL_MONTH })
                m.Add(typeof(int), months, CalciteValues.ToIntervalMonths, CalciteValues.FromIntervalMonths, ClrTypeMatch.RelDefault);

            foreach (var time in new[]
            {
                SqlTypeName.INTERVAL_DAY, SqlTypeName.INTERVAL_DAY_HOUR, SqlTypeName.INTERVAL_DAY_MINUTE, SqlTypeName.INTERVAL_DAY_SECOND,
                SqlTypeName.INTERVAL_HOUR, SqlTypeName.INTERVAL_HOUR_MINUTE, SqlTypeName.INTERVAL_HOUR_SECOND,
                SqlTypeName.INTERVAL_MINUTE, SqlTypeName.INTERVAL_MINUTE_SECOND, SqlTypeName.INTERVAL_SECOND,
            })
                m.Add(typeof(TimeSpan), time, CalciteValues.ToIntervalTime, CalciteValues.FromIntervalTime, ClrTypeMatch.RelDefault);

            // CLR types written as a Calcite type that already reads back as something else above.
            // A char is written as a one-character string in a CHAR(1); a CHAR(1) column still reads as a
            // string.
            m.Add(typeof(char), SqlTypeName.CHAR, CalciteValues.ToCharacter, CalciteValues.FromCharacter, ClrTypeMatch.ClrDefault, precision: 1);
            // Calcite has no unbounded integer type, so a BigInteger is written as a DECIMAL
            m.Add(typeof(System.Numerics.BigInteger), SqlTypeName.DECIMAL, CalciteValues.ToBigInteger, CalciteValues.FromBigInteger, ClrTypeMatch.ClrDefault);
            m.Add(typeof(DateOnly), SqlTypeName.DATE, CalciteValues.ToDate, CalciteValues.FromDateOnly, ClrTypeMatch.ClrDefault);
            m.Add(typeof(TimeOnly), SqlTypeName.TIME, CalciteValues.ToTime, CalciteValues.FromTimeOnly, ClrTypeMatch.ClrDefault);

            // allowed when asked for by name, and not a default in either direction
            m.Add(typeof(DateOnly), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => DateOnly.FromDateTime((DateTime)CalciteValues.FromTimestamp(v)), ClrTypeMatch.Named);
            m.Add(typeof(TimeOnly), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => TimeOnly.FromDateTime((DateTime)CalciteValues.FromTimestamp(v)), ClrTypeMatch.Named);
            m.Add(typeof(DateTime), SqlTypeName.TIMESTAMP_TZ, CalciteValues.ToTimestampTz, v => ((DateTimeOffset)CalciteValues.FromTimestampTz(v)).UtcDateTime, ClrTypeMatch.Named);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP, CalciteValues.ToTimestamp, v => new DateTimeOffset((DateTime)CalciteValues.FromTimestamp(v), TimeSpan.Zero), ClrTypeMatch.Named);

            // collections. Each asks the registry for its element's, key's, value's or field's mapping, so
            // one entry per kind covers every nesting depth and every element type, including those a
            // caller's resolver adds.
            m.Add(
                typeof(System.Array),
                SqlTypeName.ARRAY,
                static (context, relType, clrType) => CollectionClrTypeMapping.Create(context, relType, clrType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t.IsArray,
                relTypePredicate: static t => t.getSqlTypeName() == SqlTypeName.ARRAY);

            m.Add(
                typeof(System.Array),
                SqlTypeName.MULTISET,
                static (context, relType, clrType) => CollectionClrTypeMapping.Create(context, relType, clrType),
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

            // a VARIANT carries its type with each value, so each value is mapped by that type through the
            // registry
            m.Add(
                typeof(object),
                SqlTypeName.VARIANT,
                static (context, relType, _) => new VariantClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t == typeof(object),
                relTypePredicate: static t => t.getSqlTypeName() == SqlTypeName.VARIANT);

            // NULL, whose only value is null: Calcite holds it in java.lang.Void, which has no instances, so
            // both conversions answer null
            m.Add(typeof(object), SqlTypeName.NULL, static _ => null, static _ => null, ClrTypeMatch.RelDefault);

            // ANY says nothing about what it holds, so each value is mapped by its own class. This entry
            // matches ANY only; it is not a catch-all for types the table does not name.
            m.Add(
                typeof(object),
                SqlTypeName.ANY,
                static (context, relType, _) => new AnyClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t == typeof(object));

            // OTHER is the type name of a createJavaType type whose class has no SQL name of its own (for
            // example java.lang.Object), so it says as little about the value as ANY and is read the same way
            m.Add(
                typeof(object),
                SqlTypeName.OTHER,
                static (context, relType, _) => new AnyClrTypeMapping(context, relType),
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t == typeof(object));
        }

        /// <inheritdoc />
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
        {
            // a bare collection's Calcite type is built from its element's mapping: the table builds a type
            // from a SqlTypeName and a precision, which cannot express INTEGER ARRAY
            if (relType is null && clrType is not null && Collection(clrType, context) is ClrTypeMapping collection)
                return collection;

            return _mappings.GetMapping(clrType, relType, context);
        }

        /// <summary>
        /// Returns the mapping a bare one-dimensional array or generic dictionary is written through, or
        /// <see langword="null"/> where the type is neither or its element, key or value type has no mapping.
        /// </summary>
        /// <param name="clrType">The CLR type a value is being written as.</param>
        /// <param name="context">The context of the lookup.</param>
        /// <returns>The mapping, or <see langword="null"/>.</returns>
        /// <remarks>
        /// The element type's mapping comes from the registry, so nesting follows: an <c>int[]</c> is an
        /// <c>INTEGER ARRAY</c> and an <c>int[][]</c> an <c>INTEGER ARRAY ARRAY</c>. <c>byte[]</c> is excluded
        /// because the table maps it to <c>VARBINARY</c>.
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

                // the element is nullable where the .NET element type admits null
                var nullable = Nullable.GetUnderlyingType(element) is not null || element.IsValueType == false;

                return CollectionClrTypeMapping.Create(context,
                    typeFactory.createArrayType(typeFactory.createTypeWithNullability(mapping.RelType, nullable), -1),
                    clrType);
            }

            if (Dictionary(clrType) is not (Type key, Type value))
                return null;

            var keyMapping = context.Registry.GetMapping(Nullable.GetUnderlyingType(key) ?? key, null);
            var valueMapping = context.Registry.GetMapping(Nullable.GetUnderlyingType(value) ?? value, null);
            if (keyMapping is null || valueMapping is null)
                return null;

            // a dictionary's keys are never null, so the map's key type is not nullable
            return new MapClrTypeMapping(context,
                typeFactory.createMapType(
                    typeFactory.createTypeWithNullability(keyMapping.RelType, false),
                    typeFactory.createTypeWithNullability(valueMapping.RelType, Nullable.GetUnderlyingType(value) is not null || value.IsValueType == false)));
        }

        /// <summary>
        /// Returns the key and value types of a .NET dictionary, or <see langword="null"/> where the type
        /// is not one.
        /// </summary>
        /// <param name="clrType">The type to inspect.</param>
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
