using System;
using System.Globalization;

using org.apache.calcite.avatica.util;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The conversions the built-in mappings are made of, between a CLR value and the representation
    /// Calcite holds one in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every one of these converts rather than casts. A value arriving from an ADO.NET provider has the
    /// width that provider chose and not the one Calcite did — a column Calcite types <c>SMALLINT</c> may
    /// have been decoded as a <see cref="byte"/> — and a value arriving from a caller has whatever they
    /// wrote. Casting is what the two tables this replaces disagreed about: the reader converted, the
    /// parameter binder cast, and a <see cref="long"/> bound to an <c>INTEGER</c> parameter threw where the
    /// same value read from a column did not.
    /// </para>
    /// <para>
    /// The cost is one boxed value per cell, which every route was going to pay anyway: the result is a
    /// <c>java.lang</c> wrapper, allocated per cell, whatever it came from.
    /// </para>
    /// </remarks>
    public static class CalciteValues
    {

        /// <summary>
        /// The instant a <c>TIMESTAMP</c> counts from, and the day a <c>DATE</c> counts from.
        /// </summary>
        public static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// The day <c>DATE</c> counts from.
        /// </summary>
        static readonly DateOnly UnixEpochDay = new(1970, 1, 1);

        /// <summary>
        /// Reads a value as a CLR primitive, converting where the runtime type is not already it.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T As<T>(object value)
            where T : struct
        {
            return value is T typed ? typed : (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Counts the milliseconds from the epoch to a <see cref="DateTime"/> already in the terms it is to
        /// be counted in.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        static long ToUnixTimeMilliseconds(DateTime value)
        {
            return (long)(value - UnixEpoch).TotalMilliseconds;
        }

        #region To Calcite

        /// <summary>
        /// Converts to the <c>java.lang.Boolean</c> a <c>BOOLEAN</c> is held in.
        /// </summary>
        public static object ToBoolean(object value) => java.lang.Boolean.valueOf(As<bool>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Byte</c> a <c>TINYINT</c> is held in.
        /// </summary>
        /// <remarks>
        /// Calcite's <c>TINYINT</c> is signed and Java's <c>byte</c> is IKVM's unsigned <see cref="byte"/>,
        /// so the sign travels in the bits.
        /// </remarks>
        public static object ToTinyInt(object value) => java.lang.Byte.valueOf(unchecked((byte)As<sbyte>(value)));

        /// <summary>
        /// Converts to the <c>java.lang.Short</c> a <c>SMALLINT</c> is held in.
        /// </summary>
        public static object ToSmallInt(object value) => java.lang.Short.valueOf(As<short>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> an <c>INTEGER</c> is held in.
        /// </summary>
        public static object ToInteger(object value) => java.lang.Integer.valueOf(As<int>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Long</c> a <c>BIGINT</c> is held in.
        /// </summary>
        public static object ToBigInt(object value) => java.lang.Long.valueOf(As<long>(value));

        /// <summary>
        /// Converts to the <c>org.joou.UByte</c> a <c>UTINYINT</c> is held in.
        /// </summary>
        /// <remarks>
        /// The unsigned types are not a variation on the signed ones: <c>getJavaClass</c> answers a joou
        /// wrapper rather than a <c>java.lang</c> one, and the widening overload is taken in each case so
        /// that the sign is never in question.
        /// </remarks>
        public static object ToUTinyInt(object value) => org.joou.UByte.valueOf((int)As<byte>(value));

        /// <summary>
        /// Converts to the <c>org.joou.UShort</c> a <c>USMALLINT</c> is held in.
        /// </summary>
        public static object ToUSmallInt(object value) => org.joou.UShort.valueOf((int)As<ushort>(value));

        /// <summary>
        /// Converts to the <c>org.joou.UInteger</c> a <c>UINTEGER</c> is held in.
        /// </summary>
        public static object ToUInteger(object value) => org.joou.UInteger.valueOf((long)As<uint>(value));

        /// <summary>
        /// Converts to the <c>org.joou.ULong</c> a <c>UBIGINT</c> is held in.
        /// </summary>
        /// <remarks>
        /// Through the decimal string, because the whole of the range is the point: <c>valueOf(long)</c>
        /// refuses anything above <see cref="long.MaxValue"/>, which is half of what the type holds.
        /// </remarks>
        public static object ToUBigInt(object value) => org.joou.ULong.valueOf(As<ulong>(value).ToString(CultureInfo.InvariantCulture));

        /// <summary>
        /// Converts to the <c>java.lang.Float</c> a <c>REAL</c> is held in.
        /// </summary>
        public static object ToReal(object value) => java.lang.Float.valueOf(As<float>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Double</c> a <c>DOUBLE</c> is held in.
        /// </summary>
        public static object ToDouble(object value) => java.lang.Double.valueOf(As<double>(value));

        /// <summary>
        /// Converts to the <c>java.math.BigDecimal</c> a <c>DECIMAL</c> is held in.
        /// </summary>
        public static object ToDecimal(object value) => BigDecimalConverter.ToBigDecimal(As<decimal>(value));

        /// <summary>
        /// Converts to the <see cref="string"/> a <c>CHAR</c> or <c>VARCHAR</c> is held in.
        /// </summary>
        /// <remarks>
        /// A column Calcite holds as a character type need not be a string to the provider that produced it:
        /// SQL Server hands back a <see cref="Guid"/> for a <c>uniqueidentifier</c>, which the adapter types
        /// as <c>CHAR(36)</c>. Formatting the value is what the type says it is.
        /// </remarks>
        public static object ToChar(object value) => value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

        /// <summary>
        /// Converts a <see cref="Guid"/> to the string a <c>CHAR(36)</c> holds it as.
        /// </summary>
        public static object ToGuid(object value) => (value is Guid guid ? guid : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? "")).ToString();

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> count of days a <c>DATE</c> is held in.
        /// </summary>
        /// <remarks>
        /// Only the date component is read, and no time zone enters into it. Converting through
        /// <see cref="DateTimeOffset"/> would apply the machine's offset to a value whose
        /// <see cref="DateTime.Kind"/> is typically <see cref="DateTimeKind.Unspecified"/>, which for a date
        /// at midnight can land on the day before.
        /// </remarks>
        public static object ToDate(object value)
        {
            var day = value switch
            {
                DateOnly d => d,
                DateTime d => DateOnly.FromDateTime(d),
                DateTimeOffset d => DateOnly.FromDateTime(d.UtcDateTime),
                _ => DateOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture)),
            };

            return java.lang.Integer.valueOf(day.DayNumber - UnixEpochDay.DayNumber);
        }

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> count of milliseconds since midnight a <c>TIME</c> is
        /// held in.
        /// </summary>
        public static object ToTime(object value)
        {
            var span = value switch
            {
                TimeSpan t => t,
                TimeOnly t => t.ToTimeSpan(),
                DateTime d => d.TimeOfDay,
                DateTimeOffset d => d.TimeOfDay,
                string s => TimeSpan.Parse(s, CultureInfo.InvariantCulture),
                _ => TimeSpan.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? "0", CultureInfo.InvariantCulture),
            };

            return java.lang.Integer.valueOf((int)span.TotalMilliseconds);
        }

        /// <summary>
        /// Converts to the <c>java.lang.Long</c> count of milliseconds a <c>TIMESTAMP</c> is held in.
        /// </summary>
        /// <remarks>
        /// A timestamp carries no zone: the count is of milliseconds from the epoch to the wall clock read
        /// as though it were UTC. Casting an unspecified <see cref="DateTime.Kind"/> to a
        /// <see cref="DateTimeOffset"/> instead reads it as local time and shifts the value by the machine's
        /// offset.
        /// </remarks>
        public static object ToTimestamp(object value)
        {
            var instant = value switch
            {
                DateTime d => d.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(d, DateTimeKind.Utc) : d.ToUniversalTime(),
                DateTimeOffset d => d.UtcDateTime,
                DateOnly d => d.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
                _ => DateTime.SpecifyKind(Convert.ToDateTime(value, CultureInfo.InvariantCulture), DateTimeKind.Utc),
            };

            return java.lang.Long.valueOf(ToUnixTimeMilliseconds(instant));
        }

        /// <summary>
        /// Converts to the <c>java.lang.Long</c> count of milliseconds a <c>TIMESTAMP WITH TIME ZONE</c> is
        /// held in.
        /// </summary>
        /// <remarks>
        /// A zoned timestamp is an instant, so the count is to it. A value that carries no offset is read as
        /// UTC, the offset being the thing it had no way to state.
        /// </remarks>
        public static object ToTimestampTz(object value)
        {
            return java.lang.Long.valueOf(value switch
            {
                DateTimeOffset d => d.ToUnixTimeMilliseconds(),
                DateTime d => ToUnixTimeMilliseconds(d.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(d, DateTimeKind.Utc) : d.ToUniversalTime()),
                string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture).ToUnixTimeMilliseconds(),
                _ => ToUnixTimeMilliseconds(DateTime.SpecifyKind(Convert.ToDateTime(value, CultureInfo.InvariantCulture), DateTimeKind.Utc)),
            });
        }

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> count of milliseconds since midnight a
        /// <c>TIME WITH TIME ZONE</c> is held in.
        /// </summary>
        public static object ToTimeTz(object value)
        {
            return value is DateTimeOffset offset ? java.lang.Integer.valueOf((int)offset.TimeOfDay.TotalMilliseconds) : ToTime(value);
        }

        /// <summary>
        /// Converts to the <c>ByteString</c> a <c>BINARY</c> or <c>VARBINARY</c> is held in.
        /// </summary>
        public static object ToBinary(object value)
        {
            return value switch
            {
                byte[] bytes => new ByteString(bytes),
                ByteString bs => bs,
                string s => new ByteString(System.Text.Encoding.UTF8.GetBytes(s)),
                _ => throw new ClrTypeMappingException($"Cannot carry a {value.GetType()} across as binary."),
            };
        }

        #endregion

        #region From Calcite

        /// <summary>
        /// Reads the <c>java.lang.Boolean</c> a <c>BOOLEAN</c> is held in.
        /// </summary>
        public static object FromBoolean(object value) => ((java.lang.Boolean)value).booleanValue();

        /// <summary>
        /// Reads the <c>java.lang.Byte</c> a <c>TINYINT</c> is held in.
        /// </summary>
        public static object FromTinyInt(object value) => unchecked((sbyte)((java.lang.Number)value).byteValue());

        /// <summary>
        /// Reads the <c>java.lang.Short</c> a <c>SMALLINT</c> is held in.
        /// </summary>
        public static object FromSmallInt(object value) => ((java.lang.Number)value).shortValue();

        /// <summary>
        /// Reads the <c>java.lang.Integer</c> an <c>INTEGER</c> is held in.
        /// </summary>
        public static object FromInteger(object value) => ((java.lang.Number)value).intValue();

        /// <summary>
        /// Reads the <c>java.lang.Long</c> a <c>BIGINT</c> is held in.
        /// </summary>
        public static object FromBigInt(object value) => ((java.lang.Number)value).longValue();

        /// <summary>
        /// Reads the <c>org.joou.UByte</c> a <c>UTINYINT</c> is held in.
        /// </summary>
        public static object FromUTinyInt(object value) => unchecked((byte)((java.lang.Number)value).byteValue());

        /// <summary>
        /// Reads the <c>org.joou.UShort</c> a <c>USMALLINT</c> is held in.
        /// </summary>
        public static object FromUSmallInt(object value) => unchecked((ushort)((java.lang.Number)value).shortValue());

        /// <summary>
        /// Reads the <c>org.joou.UInteger</c> a <c>UINTEGER</c> is held in.
        /// </summary>
        public static object FromUInteger(object value) => unchecked((uint)((java.lang.Number)value).intValue());

        /// <summary>
        /// Reads the <c>org.joou.ULong</c> a <c>UBIGINT</c> is held in.
        /// </summary>
        public static object FromUBigInt(object value) => unchecked((ulong)((java.lang.Number)value).longValue());

        /// <summary>
        /// Reads the <c>java.lang.Float</c> a <c>REAL</c> is held in.
        /// </summary>
        public static object FromReal(object value) => ((java.lang.Number)value).floatValue();

        /// <summary>
        /// Reads the <c>java.lang.Double</c> a <c>DOUBLE</c> is held in.
        /// </summary>
        public static object FromDouble(object value) => ((java.lang.Number)value).doubleValue();

        /// <summary>
        /// Reads the <c>java.math.BigDecimal</c> a <c>DECIMAL</c> is held in.
        /// </summary>
        public static object FromDecimal(object value) => BigDecimalConverter.ToDecimal((java.math.BigDecimal)value);

        /// <summary>
        /// Reads the <see cref="string"/> a <c>CHAR</c> or <c>VARCHAR</c> is held in.
        /// </summary>
        public static object FromChar(object value) => value as string ?? value.ToString() ?? string.Empty;

        /// <summary>
        /// Reads a <c>CHAR(36)</c> as the <see cref="Guid"/> it spells.
        /// </summary>
        public static object FromGuid(object value) => Guid.Parse((string)FromChar(value));

        /// <summary>
        /// Reads the count of days a <c>DATE</c> is held in.
        /// </summary>
        public static object FromDate(object value)
        {
            return value switch
            {
                java.lang.Number n => UnixEpoch.AddDays(n.longValue()),
                java.sql.Date d => UnixEpoch.AddMilliseconds(d.getTime()),
                _ => throw Unexpected(value, "DATE"),
            };
        }

        /// <summary>
        /// Reads the count of days a <c>DATE</c> is held in, as a <see cref="DateOnly"/>.
        /// </summary>
        public static object FromDateOnly(object value) => DateOnly.FromDateTime((DateTime)FromDate(value));

        /// <summary>
        /// Reads the count of milliseconds since midnight a <c>TIME</c> is held in.
        /// </summary>
        public static object FromTime(object value)
        {
            return value switch
            {
                java.lang.Number n => TimeSpan.FromMilliseconds(n.longValue()),
                java.sql.Time t => TimeSpan.FromMilliseconds(t.getTime()),
                _ => throw Unexpected(value, "TIME"),
            };
        }

        /// <summary>
        /// Reads the count of milliseconds since midnight a <c>TIME</c> is held in, as a
        /// <see cref="TimeOnly"/>.
        /// </summary>
        public static object FromTimeOnly(object value) => TimeOnly.FromTimeSpan((TimeSpan)FromTime(value));

        /// <summary>
        /// Reads the count of milliseconds a <c>TIMESTAMP</c> is held in.
        /// </summary>
        public static object FromTimestamp(object value)
        {
            return value switch
            {
                java.lang.Number n => UnixEpoch.AddMilliseconds(n.longValue()),
                java.sql.Timestamp t => UnixEpoch.AddMilliseconds(t.getTime()),
                _ => throw Unexpected(value, "TIMESTAMP"),
            };
        }

        /// <summary>
        /// Reads the count of milliseconds a <c>TIMESTAMP WITH TIME ZONE</c> is held in, as an instant.
        /// </summary>
        public static object FromTimestampTz(object value) => new DateTimeOffset((DateTime)FromTimestamp(value), TimeSpan.Zero);

        /// <summary>
        /// Reads the count of milliseconds since midnight a <c>TIME WITH TIME ZONE</c> is held in.
        /// </summary>
        /// <remarks>
        /// The offset is not carried per row, so the value is anchored at the start of the calendar the way
        /// IKVM's JDBC <c>OffsetTime</c> path anchors one.
        /// </remarks>
        public static object FromTimeTz(object value) => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add((TimeSpan)FromTime(value));

        /// <summary>
        /// Reads the <c>ByteString</c> a <c>BINARY</c> or <c>VARBINARY</c> is held in.
        /// </summary>
        public static object FromBinary(object value)
        {
            return value switch
            {
                ByteString bs => bs.getBytes(),
                byte[] bytes => bytes,
                _ => throw Unexpected(value, "VARBINARY"),
            };
        }

        /// <summary>
        /// Describes a representation that is not one the type is held in.
        /// </summary>
        static ClrTypeMappingException Unexpected(object value, string typeName)
        {
            return new ClrTypeMappingException($"A {typeName} is not held in a {value.GetType()}.");
        }

        #endregion

        #region Shapes

        /// <summary>
        /// Carries a CLR value across on the strength of its runtime type alone, for a Calcite type that
        /// says nothing about what holds it.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static object? ToShape(object value)
        {
            return value switch
            {
                bool v => ToBoolean(v),
                sbyte v => ToTinyInt(v),
                byte v => ToUTinyInt(v),
                short v => ToSmallInt(v),
                ushort v => ToUSmallInt(v),
                int v => ToInteger(v),
                uint v => ToUInteger(v),
                long v => ToBigInt(v),
                ulong v => ToUBigInt(v),
                float v => ToReal(v),
                double v => ToDouble(v),
                decimal v => ToDecimal(v),
                string v => v,
                Guid v => ToGuid(v),
                DateTime v => ToTimestamp(v),
                DateTimeOffset v => ToTimestampTz(v),
                DateOnly v => ToDate(v),
                TimeOnly v => ToTime(v),
                TimeSpan v => ToTime(v),
                byte[] v => ToBinary(v),
                _ => value,
            };
        }

        /// <summary>
        /// Reads a representation back on the strength of its runtime class alone, for a Calcite type that
        /// says nothing about what holds it.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static object? FromShape(object value)
        {
            return value switch
            {
                string v => v,
                java.math.BigDecimal v => FromDecimal(v),
                java.lang.Boolean v => v.booleanValue(),
                java.lang.Byte v => unchecked((sbyte)v.byteValue()),
                java.lang.Short v => v.shortValue(),
                java.lang.Integer v => v.intValue(),
                java.lang.Long v => v.longValue(),
                java.lang.Float v => v.floatValue(),
                java.lang.Double v => v.doubleValue(),
                java.lang.Character v => v.charValue(),
                java.sql.Timestamp v => UnixEpoch.AddMilliseconds(v.getTime()),
                java.sql.Date v => UnixEpoch.AddMilliseconds(v.getTime()),
                java.sql.Time v => TimeSpan.FromMilliseconds(v.getTime()),
                ByteString v => v.getBytes(),
                org.joou.UByte v => unchecked((byte)v.byteValue()),
                org.joou.UShort v => unchecked((ushort)v.shortValue()),
                org.joou.UInteger v => unchecked((uint)v.intValue()),
                org.joou.ULong v => unchecked((ulong)v.longValue()),
                _ => value,
            };
        }

        #endregion

    }

}
