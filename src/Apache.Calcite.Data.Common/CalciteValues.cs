using System;

using Apache.Calcite.Extensions.Interop;
using System.Globalization;

using org.apache.calcite.avatica.util;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The conversions the built-in mappings use, between CLR values and the Java classes Calcite holds
    /// values in.
    /// </summary>
    /// <remarks>
    /// Each <c>To…</c> method takes a non-null CLR value and each <c>From…</c> method a non-null value of the
    /// class Calcite holds the type in. The <c>To…</c> methods convert rather than cast, so a value of another
    /// width or type is accepted where <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/> can
    /// convert it: an ADO.NET provider may decode a <c>SMALLINT</c> column as a <see cref="byte"/>, and a
    /// caller may bind a <see cref="long"/> to an <c>INTEGER</c> parameter. The <c>From…</c> methods throw
    /// <see cref="InvalidCastException"/> or <see cref="ClrTypeMappingException"/> for a value of the wrong class.
    /// </remarks>
    public static class CalciteValues
    {

        /// <summary>
        /// 1970-01-01T00:00:00Z, from which Calcite counts a <c>TIMESTAMP</c> in milliseconds and a
        /// <c>DATE</c> in days.
        /// </summary>
        public static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// The day from which Calcite counts a <c>DATE</c>.
        /// </summary>
        static readonly DateOnly UnixEpochDay = new(1970, 1, 1);

        /// <summary>
        /// Returns a value as <typeparamref name="T"/>, converting with the invariant culture where it is not
        /// already of that type.
        /// </summary>
        /// <typeparam name="T">The value type wanted.</typeparam>
        /// <param name="value">The value.</param>
        /// <returns>The value as <typeparamref name="T"/>.</returns>
        /// <exception cref="InvalidCastException">The value cannot be converted.</exception>
        /// <exception cref="OverflowException">The value is out of range for <typeparamref name="T"/>.</exception>
        public static T As<T>(object value)
            where T : struct
        {
            return value is T typed ? typed : (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns the milliseconds from <see cref="UnixEpoch"/> to a <see cref="DateTime"/>, whose kind is
        /// ignored.
        /// </summary>
        /// <param name="value">The date and time, read as if it were UTC.</param>
        /// <returns>The whole milliseconds since the epoch, truncated toward zero; negative before 1970.</returns>
        static long ToUnixTimeMilliseconds(DateTime value)
        {
            return (long)(value - UnixEpoch).TotalMilliseconds;
        }

        #region To Calcite

        /// <summary>
        /// Converts to the <c>java.lang.Boolean</c> a <c>BOOLEAN</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="bool"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Boolean</c>.</returns>
        public static object ToBoolean(object value) => java.lang.Boolean.valueOf(As<bool>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Byte</c> a <c>TINYINT</c> is held in.
        /// </summary>
        /// <remarks>
        /// Calcite's <c>TINYINT</c> is signed, and IKVM exposes Java's <c>byte</c> as the unsigned
        /// <see cref="byte"/>, so the <see cref="sbyte"/> is passed bit for bit.
        /// </remarks>
        /// <param name="value">A <see cref="sbyte"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Byte</c> with the same bits.</returns>
        public static object ToTinyInt(object value) => java.lang.Byte.valueOf(unchecked((byte)As<sbyte>(value)));

        /// <summary>
        /// Converts to the <c>java.lang.Short</c> a <c>SMALLINT</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="short"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Short</c>.</returns>
        public static object ToSmallInt(object value) => java.lang.Short.valueOf(As<short>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> an <c>INTEGER</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="int"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Integer</c>.</returns>
        public static object ToInteger(object value) => java.lang.Integer.valueOf(As<int>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Long</c> a <c>BIGINT</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="long"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Long</c>.</returns>
        public static object ToBigInt(object value) => java.lang.Long.valueOf(As<long>(value));

        /// <summary>
        /// Converts to the <c>org.joou.UByte</c> a <c>UTINYINT</c> is held in.
        /// </summary>
        /// <remarks>
        /// Calcite holds the unsigned types in joou wrappers. Each conversion calls the <c>valueOf</c>
        /// overload taking a wider signed type, so the value is never read as negative.
        /// </remarks>
        /// <param name="value">A <see cref="byte"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>An <c>org.joou.UByte</c>.</returns>
        public static object ToUTinyInt(object value) => org.joou.UByte.valueOf((int)As<byte>(value));

        /// <summary>
        /// Converts to the <c>org.joou.UShort</c> a <c>USMALLINT</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="ushort"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>An <c>org.joou.UShort</c>.</returns>
        public static object ToUSmallInt(object value) => org.joou.UShort.valueOf((int)As<ushort>(value));

        /// <summary>
        /// Converts to the <c>org.joou.UInteger</c> a <c>UINTEGER</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="uint"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>An <c>org.joou.UInteger</c>.</returns>
        public static object ToUInteger(object value) => org.joou.UInteger.valueOf((long)As<uint>(value));

        /// <summary>
        /// Converts to the <c>org.joou.ULong</c> a <c>UBIGINT</c> is held in.
        /// </summary>
        /// <remarks>
        /// Goes through the decimal string, because <c>ULong.valueOf(long)</c> cannot represent a value
        /// above <see cref="long.MaxValue"/>.
        /// </remarks>
        /// <param name="value">A <see cref="ulong"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>An <c>org.joou.ULong</c>.</returns>
        public static object ToUBigInt(object value) => org.joou.ULong.valueOf(As<ulong>(value).ToString(CultureInfo.InvariantCulture));

        /// <summary>
        /// Converts to the <c>java.lang.Float</c> a <c>REAL</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="float"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Float</c>.</returns>
        public static object ToReal(object value) => java.lang.Float.valueOf(As<float>(value));

        /// <summary>
        /// Converts to the <c>java.lang.Double</c> a <c>DOUBLE</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="double"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.lang.Double</c>.</returns>
        public static object ToDouble(object value) => java.lang.Double.valueOf(As<double>(value));

        /// <summary>
        /// Converts to the <c>java.math.BigDecimal</c> a <c>DECIMAL</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="decimal"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>java.math.BigDecimal</c> of the same value.</returns>
        public static object ToDecimal(object value) => JavaDecimals.ToBigDecimal(As<decimal>(value));

        /// <summary>
        /// Converts to the <see cref="string"/> a <c>CHAR</c> or <c>VARCHAR</c> is held in.
        /// </summary>
        /// <remarks>
        /// A value that is not a string is formatted with the invariant culture, since a provider may return
        /// a non-string value for a column Calcite types as character.
        /// </remarks>
        /// <param name="value">A string, or any value to format with the invariant culture.</param>
        /// <returns>The <see cref="string"/>; empty where formatting gives <see langword="null"/>.</returns>
        public static object ToChar(object value) => value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> count of days a <c>DATE</c> is held in.
        /// </summary>
        /// <remarks>
        /// A <see cref="DateTime"/> contributes its date component whatever its <see cref="DateTime.Kind"/>;
        /// no time zone is applied, since doing so could move a date at midnight to the day before. A
        /// <see cref="DateTimeOffset"/> contributes the date of its UTC instant.
        /// </remarks>
        /// <param name="value">A <see cref="DateOnly"/>, <see cref="DateTime"/> or
        /// <see cref="DateTimeOffset"/>, or any value <see cref="Convert.ToDateTime(object, IFormatProvider)"/>
        /// accepts.</param>
        /// <returns>A <c>java.lang.Integer</c> counting days from 1970-01-01.</returns>
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
        /// <param name="value">A <see cref="TimeSpan"/>, <see cref="TimeOnly"/>, <see cref="DateTime"/> or
        /// <see cref="DateTimeOffset"/> (its time of day), or text <see cref="TimeSpan"/> parses.</param>
        /// <returns>A <c>java.lang.Integer</c> counting whole milliseconds.</returns>
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
        /// A <c>TIMESTAMP</c> has no zone: the count is of milliseconds from the epoch to the wall-clock time
        /// read as UTC. A <see cref="DateTime"/> of <see cref="DateTimeKind.Unspecified"/> kind is taken as
        /// that wall-clock time without shifting; a local or UTC one is converted to UTC first; a
        /// <see cref="DateTimeOffset"/> contributes its UTC instant.
        /// </remarks>
        /// <param name="value">A <see cref="DateTime"/>, <see cref="DateTimeOffset"/> or
        /// <see cref="DateOnly"/>, or any value <see cref="Convert.ToDateTime(object, IFormatProvider)"/>
        /// accepts.</param>
        /// <returns>A <c>java.lang.Long</c> counting milliseconds from the epoch.</returns>
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
        /// The count is to the instant the value names. A <see cref="DateTime"/> of
        /// <see cref="DateTimeKind.Unspecified"/> kind is read as UTC.
        /// </remarks>
        /// <param name="value">A <see cref="DateTimeOffset"/> or <see cref="DateTime"/>, text
        /// <see cref="DateTimeOffset"/> parses, or any value <see cref="Convert.ToDateTime(object, IFormatProvider)"/>
        /// accepts.</param>
        /// <returns>A <c>java.lang.Long</c> counting milliseconds from the epoch to the instant.</returns>
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
        /// <param name="value">A <see cref="DateTimeOffset"/>, whose local time of day is taken, or any value
        /// <see cref="ToTime"/> accepts.</param>
        /// <returns>A <c>java.lang.Integer</c> counting whole milliseconds.</returns>
        public static object ToTimeTz(object value)
        {
            return value is DateTimeOffset offset ? java.lang.Integer.valueOf((int)offset.TimeOfDay.TotalMilliseconds) : ToTime(value);
        }

        /// <summary>
        /// Converts to the <c>ByteString</c> a <c>BINARY</c> or <c>VARBINARY</c> is held in.
        /// </summary>
        /// <param name="value">A <see cref="byte"/> array, a <c>ByteString</c>, or a string, which is encoded
        /// as UTF-8.</param>
        /// <returns>A <c>ByteString</c>; one passed in is returned as it is.</returns>
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
        /// <param name="value">A <c>java.lang.Boolean</c>.</param>
        /// <returns>A boxed <see cref="bool"/>.</returns>
        public static object FromBoolean(object value) => ((java.lang.Boolean)value).booleanValue();

        /// <summary>
        /// Reads the <c>java.lang.Byte</c> a <c>TINYINT</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally a <c>java.lang.Byte</c>.</param>
        /// <returns>A boxed <see cref="sbyte"/>, sign kept.</returns>
        public static object FromTinyInt(object value) => unchecked((sbyte)((java.lang.Number)value).byteValue());

        /// <summary>
        /// Reads the <c>java.lang.Short</c> a <c>SMALLINT</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally a <c>java.lang.Short</c>.</param>
        /// <returns>A boxed <see cref="short"/>.</returns>
        public static object FromSmallInt(object value) => ((java.lang.Number)value).shortValue();

        /// <summary>
        /// Reads the <c>java.lang.Integer</c> an <c>INTEGER</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally a <c>java.lang.Integer</c>.</param>
        /// <returns>A boxed <see cref="int"/>.</returns>
        public static object FromInteger(object value) => ((java.lang.Number)value).intValue();

        /// <summary>
        /// Reads the <c>java.lang.Long</c> a <c>BIGINT</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally a <c>java.lang.Long</c>.</param>
        /// <returns>A boxed <see cref="long"/>.</returns>
        public static object FromBigInt(object value) => ((java.lang.Number)value).longValue();

        /// <summary>
        /// Reads the <c>org.joou.UByte</c> a <c>UTINYINT</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally an <c>org.joou.UByte</c>.</param>
        /// <returns>A boxed <see cref="byte"/>.</returns>
        public static object FromUTinyInt(object value) => unchecked((byte)((java.lang.Number)value).byteValue());

        /// <summary>
        /// Reads the <c>org.joou.UShort</c> a <c>USMALLINT</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally an <c>org.joou.UShort</c>.</param>
        /// <returns>A boxed <see cref="ushort"/>.</returns>
        public static object FromUSmallInt(object value) => unchecked((ushort)((java.lang.Number)value).shortValue());

        /// <summary>
        /// Reads the <c>org.joou.UInteger</c> a <c>UINTEGER</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally an <c>org.joou.UInteger</c>.</param>
        /// <returns>A boxed <see cref="uint"/>.</returns>
        public static object FromUInteger(object value) => unchecked((uint)((java.lang.Number)value).intValue());

        /// <summary>
        /// Reads the <c>org.joou.ULong</c> a <c>UBIGINT</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally an <c>org.joou.ULong</c>.</param>
        /// <returns>A boxed <see cref="ulong"/>.</returns>
        public static object FromUBigInt(object value) => unchecked((ulong)((java.lang.Number)value).longValue());

        /// <summary>
        /// Reads the <c>java.lang.Float</c> a <c>REAL</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally a <c>java.lang.Float</c>.</param>
        /// <returns>A boxed <see cref="float"/>.</returns>
        public static object FromReal(object value) => ((java.lang.Number)value).floatValue();

        /// <summary>
        /// Reads the <c>java.lang.Double</c> a <c>DOUBLE</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c>, normally a <c>java.lang.Double</c>.</param>
        /// <returns>A boxed <see cref="double"/>.</returns>
        public static object FromDouble(object value) => ((java.lang.Number)value).doubleValue();

        /// <summary>
        /// Reads the <c>java.math.BigDecimal</c> a <c>DECIMAL</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.math.BigDecimal</c>.</param>
        /// <returns>A boxed <see cref="decimal"/>.</returns>
        public static object FromDecimal(object value) => JavaDecimals.ToDecimal((java.math.BigDecimal)value);

        /// <summary>
        /// Reads the <see cref="string"/> a <c>CHAR</c> or <c>VARCHAR</c> is held in.
        /// </summary>
        /// <param name="value">A string, or any value whose <see cref="object.ToString"/> is wanted.</param>
        /// <returns>The <see cref="string"/>.</returns>
        public static object FromChar(object value) => value as string ?? value.ToString() ?? string.Empty;

        /// <summary>
        /// Reads the count of days a <c>DATE</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c> counting days from the epoch, or a
        /// <c>java.sql.Date</c>.</param>
        /// <returns>A boxed UTC <see cref="DateTime"/> at midnight of that date.</returns>
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
        /// <param name="value">A value <see cref="FromDate"/> accepts.</param>
        /// <returns>A boxed <see cref="DateOnly"/>.</returns>
        public static object FromDateOnly(object value) => DateOnly.FromDateTime((DateTime)FromDate(value));

        /// <summary>
        /// Reads the count of milliseconds since midnight a <c>TIME</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c> counting milliseconds since midnight, or a
        /// <c>java.sql.Time</c>.</param>
        /// <returns>A boxed <see cref="TimeSpan"/>.</returns>
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
        /// <param name="value">A value <see cref="FromTime"/> accepts.</param>
        /// <returns>A boxed <see cref="TimeOnly"/>.</returns>
        public static object FromTimeOnly(object value) => TimeOnly.FromTimeSpan((TimeSpan)FromTime(value));

        /// <summary>
        /// Reads the count of milliseconds a <c>TIMESTAMP</c> is held in.
        /// </summary>
        /// <param name="value">A <c>java.lang.Number</c> counting milliseconds from the epoch, or a
        /// <c>java.sql.Timestamp</c>.</param>
        /// <returns>A boxed UTC <see cref="DateTime"/>.</returns>
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
        /// <param name="value">A value <see cref="FromTimestamp"/> accepts.</param>
        /// <returns>A boxed <see cref="DateTimeOffset"/> at a zero offset.</returns>
        public static object FromTimestampTz(object value) => new DateTimeOffset((DateTime)FromTimestamp(value), TimeSpan.Zero);

        /// <summary>
        /// Reads the count of milliseconds since midnight a <c>TIME WITH TIME ZONE</c> is held in.
        /// </summary>
        /// <remarks>
        /// Calcite holds no offset with the value, so the result is that time of day on 0001-01-01 at a
        /// zero offset.
        /// </remarks>
        /// <param name="value">A value <see cref="FromTime"/> accepts.</param>
        /// <returns>A boxed <see cref="DateTimeOffset"/> on 0001-01-01 at a zero offset.</returns>
        public static object FromTimeTz(object value) => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add((TimeSpan)FromTime(value));

        /// <summary>
        /// Reads the <c>ByteString</c> a <c>BINARY</c> or <c>VARBINARY</c> is held in.
        /// </summary>
        /// <param name="value">A <c>ByteString</c> or a <see cref="byte"/> array.</param>
        /// <returns>The bytes, as a <see cref="byte"/> array.</returns>
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
        /// Returns the exception for a value of a class the named type is not held in.
        /// </summary>
        /// <param name="value">The value whose class was not expected.</param>
        /// <param name="typeName">The SQL type name being read, such as <c>DATE</c>.</param>
        /// <returns>An exception naming both; the caller throws it.</returns>
        static ClrTypeMappingException Unexpected(object value, string typeName)
        {
            return new ClrTypeMappingException($"A {typeName} is not held in a {value.GetType()}.");
        }

        #endregion

        /// <summary>
        /// Converts well-known text to the geometry Calcite holds a <c>GEOMETRY</c> in.
        /// </summary>
        /// <param name="value">Well-known text, as <c>POINT (1.5 2.5)</c>.</param>
        /// <returns>The geometry.</returns>
        /// <exception cref="ClrTypeMappingException">The text is not well-known text.</exception>
        /// <remarks>
        /// A <c>GEOMETRY</c> crosses this boundary as well-known text. Calcite holds it in a JTS
        /// <c>Geometry</c>, a Java object, and there is no .NET geometry type to use instead; Calcite's own
        /// JDBC driver likewise reports <c>GEOMETRY</c> as <c>VARCHAR</c>. The conversions use
        /// <c>ST_GeomFromWKT</c> and <c>ST_AsWKT</c>, which define the format, rather than JTS's
        /// <c>toString</c>.
        /// </remarks>
        public static object ToGeometry(object value)
        {
            var text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture)
                ?? throw new ClrTypeMappingException("A GEOMETRY is written from well-known text, and this value has none.");

            return org.apache.calcite.runtime.SpatialTypeFunctions.ST_GeomFromWKT(text)
                ?? throw new ClrTypeMappingException($"'{text}' is not well-known text.");
        }

        /// <summary>
        /// Converts the geometry Calcite holds a <c>GEOMETRY</c> in to well-known text.
        /// </summary>
        /// <param name="value">A JTS <c>Geometry</c>.</param>
        /// <returns>The well-known text.</returns>
        /// <inheritdoc cref="ToGeometry" path="/remarks" />
        public static object FromGeometry(object value)
        {
            if (value is not org.locationtech.jts.geom.Geometry geometry)
                throw new ClrTypeMappingException($"A GEOMETRY is held in a JTS Geometry, and a {value.GetType()} is not one.");

            return org.apache.calcite.runtime.SpatialTypeFunctions.ST_AsWKT(geometry)
                ?? throw new ClrTypeMappingException("A GEOMETRY answered no well-known text.");
        }

        /// <summary>
        /// Converts a <see cref="char"/> to the one-character string Calcite holds a <c>CHAR(1)</c> in.
        /// </summary>
        /// <param name="value">A <see cref="char"/>, or any value <see cref="ToChar"/> accepts.</param>
        /// <returns>The string.</returns>
        /// <remarks>
        /// Calcite holds a <c>CHAR</c> of any length in a <see cref="string"/>, so a character is written as
        /// a string of length one.
        /// </remarks>
        public static object ToCharacter(object value) => value is char c ? c.ToString() : ToChar(value);

        /// <summary>
        /// Converts what Calcite holds a <c>CHAR</c> in to a <see cref="char"/>.
        /// </summary>
        /// <param name="value">The string.</param>
        /// <returns>The character.</returns>
        /// <exception cref="ClrTypeMappingException">The value is not exactly one character long.</exception>
        /// <remarks>
        /// A longer value is refused rather than truncated to its first character.
        /// </remarks>
        public static object FromCharacter(object value)
        {
            var text = (string)FromChar(value);

            return text.Length == 1 ? text[0] : throw new ClrTypeMappingException($"A character value of length {text.Length} cannot be read as a char.");
        }

        /// <summary>
        /// Converts to the <c>java.lang.Integer</c> count of months a year-month interval is held in.
        /// </summary>
        /// <param name="value">A count of months.</param>
        /// <returns>The count, as a <c>java.lang.Integer</c>.</returns>
        /// <remarks>
        /// Calcite holds <c>INTERVAL YEAR</c>, <c>INTERVAL YEAR TO MONTH</c> and <c>INTERVAL MONTH</c> alike as
        /// a count of months, so an <c>INTERVAL YEAR</c> of 2 is 24. .NET has no type for a number of months
        /// (a month is not a fixed <see cref="TimeSpan"/>), so the count itself is the CLR value.
        /// </remarks>
        public static object ToIntervalMonths(object value) => java.lang.Integer.valueOf(As<int>(value));

        /// <summary>
        /// Converts the count of months a year-month interval is held in to an <see cref="int"/>.
        /// </summary>
        /// <param name="value">The <c>java.lang.Integer</c> count of months.</param>
        /// <returns>The count of months.</returns>
        /// <inheritdoc cref="ToIntervalMonths" path="/remarks" />
        public static object FromIntervalMonths(object value) => ((java.lang.Number)value).intValue();

        /// <summary>
        /// Converts to the <c>java.lang.Long</c> count of milliseconds a day-time interval is held in.
        /// </summary>
        /// <param name="value">A <see cref="TimeSpan"/>, a <see cref="TimeOnly"/>, a string
        /// <see cref="TimeSpan"/> can parse, or a number of milliseconds.</param>
        /// <returns>The count, as a <c>java.lang.Long</c>.</returns>
        /// <remarks>
        /// Calcite counts a day-time interval in milliseconds, so anything finer than a millisecond is
        /// truncated.
        /// </remarks>
        public static object ToIntervalTime(object value)
        {
            var span = value switch
            {
                TimeSpan t => t,
                TimeOnly t => t.ToTimeSpan(),
                string s => TimeSpan.Parse(s, CultureInfo.InvariantCulture),
                _ => TimeSpan.FromMilliseconds(As<double>(value)),
            };

            return java.lang.Long.valueOf((long)span.TotalMilliseconds);
        }

        /// <summary>
        /// Converts the count of milliseconds a day-time interval is held in to a <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="value">The <c>java.lang.Long</c> count of milliseconds.</param>
        /// <returns>The length of time.</returns>
        /// <inheritdoc cref="ToIntervalTime" path="/remarks" />
        public static object FromIntervalTime(object value) => TimeSpan.FromMilliseconds(((java.lang.Number)value).longValue());

        /// <summary>
        /// Converts a <see cref="System.Numerics.BigInteger"/> to the <c>java.math.BigDecimal</c> Calcite
        /// holds a <c>DECIMAL</c> in.
        /// </summary>
        /// <param name="value">A <see cref="System.Numerics.BigInteger"/>, or any value convertible to
        /// <see cref="long"/>.</param>
        /// <returns>A <c>BigDecimal</c> of scale zero.</returns>
        /// <remarks>
        /// Calcite has no unbounded integer type, so an integer is written as a <c>DECIMAL</c> of scale zero.
        /// The conversion is exact.
        /// </remarks>
        public static object ToBigInteger(object value)
        {
            var big = value is System.Numerics.BigInteger b ? b : new System.Numerics.BigInteger(As<long>(value));

            return new java.math.BigDecimal(new java.math.BigInteger(big.ToByteArray(isUnsigned: false, isBigEndian: true)));
        }

        /// <summary>
        /// Converts what Calcite holds a <c>DECIMAL</c> in to a
        /// <see cref="System.Numerics.BigInteger"/>.
        /// </summary>
        /// <param name="value">The <c>java.math.BigDecimal</c>.</param>
        /// <returns>The whole-number value.</returns>
        /// <remarks>
        /// Any fractional part is discarded.
        /// </remarks>
        public static object FromBigInteger(object value)
        {
            var big = ((java.math.BigDecimal)value).toBigInteger();

            return new System.Numerics.BigInteger(big.toByteArray(), isUnsigned: false, isBigEndian: true);
        }

        /// <summary>
        /// Converts a <see cref="Guid"/> to the <c>UuidValue</c> Calcite holds a <c>UUID</c> in.
        /// </summary>
        /// <param name="value">A <see cref="Guid"/>, or any value <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
        /// converts to one.</param>
        /// <returns>A <c>UuidValue</c> holding the same 128 bits.</returns>
        public static object ToUuid(object value) => JavaUuids.ToUuidValue(As<Guid>(value));

        /// <summary>
        /// Converts a <c>UuidValue</c> or a <c>java.util.UUID</c> to a <see cref="Guid"/>.
        /// </summary>
        /// <remarks>
        /// A <c>UUID</c> column holds a <c>UuidValue</c>; a bare <c>java.util.UUID</c> is accepted as well.
        /// </remarks>
        /// <param name="value">A <c>UuidValue</c> or a <c>java.util.UUID</c>.</param>
        /// <returns>A boxed <see cref="Guid"/>.</returns>
        public static object FromUuid(object value) => value switch
        {
            org.apache.calcite.util.UuidValue v => JavaUuids.ToGuid(v),
            java.util.UUID v => JavaUuids.ToGuid(v),
            _ => throw new ClrTypeMappingException($"A UUID is held in a UuidValue, and a {value.GetType()} is not one."),
        };

        /// <summary>
        /// The number of nanoseconds in one <see cref="TimeSpan"/> tick.
        /// </summary>
        const long NanosecondsPerTick = 100;

        /// <summary>
        /// Returns a <c>java.time.LocalDateTime</c> as a <see cref="DateTime"/> of
        /// <see cref="DateTimeKind.Unspecified"/> kind with the same fields.
        /// </summary>
        /// <param name="value">The Java local date and time.</param>
        /// <returns>The same date and time, truncated to the 100-nanosecond tick.</returns>
        static DateTime FromLocalDateTime(java.time.LocalDateTime value)
        {
            return new DateTime(value.getYear(), value.getMonthValue(), value.getDayOfMonth(), value.getHour(), value.getMinute(), value.getSecond())
                .AddTicks(value.getNano() / NanosecondsPerTick);
        }

        /// <summary>
        /// Returns an instant as a <see cref="DateTimeOffset"/> at the given offset, to millisecond precision.
        /// </summary>
        /// <param name="instant">The instant; anything below a millisecond is dropped.</param>
        /// <param name="offset">The offset the result is expressed at.</param>
        /// <returns>The same instant, expressed at <paramref name="offset"/>.</returns>
        static DateTimeOffset FromInstant(java.time.Instant instant, java.time.ZoneOffset offset)
        {
            return new DateTimeOffset(UnixEpoch.AddMilliseconds(instant.toEpochMilli()), TimeSpan.Zero).ToOffset(TimeSpan.FromSeconds(offset.getTotalSeconds()));
        }

        #region Shapes

        /// <summary>
        /// Converts a CLR value to a Calcite representation chosen by the value's runtime type, for a Calcite
        /// type such as <c>ANY</c> that does not say what class holds it.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>The converted value, or <paramref name="value"/> unchanged where its type is not a CLR
        /// primitive, <see cref="string"/>, <see cref="decimal"/>, <see cref="Guid"/>, date or time type, or
        /// <c>byte[]</c>.</returns>
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
                Guid v => ToUuid(v),
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
        /// Converts a Java value to a CLR value chosen by the value's runtime class, for a Calcite type such as
        /// <c>ANY</c> that does not say what class holds it.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>The converted value, or <paramref name="value"/> unchanged where its class is not one of
        /// the Java boxed primitives, <c>BigDecimal</c>, <c>BigInteger</c>, date and time classes,
        /// <c>ByteString</c>, UUID classes or joou unsigned types.</returns>
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
                // after the three java.sql classes, which derive from java.util.Date
                java.util.Date v => UnixEpoch.AddMilliseconds(v.getTime()),
                java.time.LocalDate v => new DateOnly(v.getYear(), v.getMonthValue(), v.getDayOfMonth()),
                java.time.LocalTime v => new TimeOnly(v.toNanoOfDay() / NanosecondsPerTick),
                java.time.LocalDateTime v => FromLocalDateTime(v),
                java.time.Instant v => new DateTimeOffset(UnixEpoch.AddMilliseconds(v.toEpochMilli()), TimeSpan.Zero),
                java.time.OffsetDateTime v => FromInstant(v.toInstant(), v.getOffset()),
                java.time.ZonedDateTime v => FromInstant(v.toInstant(), v.getOffset()),
                java.time.Duration v => TimeSpan.FromTicks(v.getSeconds() * TimeSpan.TicksPerSecond + v.getNano() / NanosecondsPerTick),
                ByteString v => v.getBytes(),
                org.apache.calcite.util.UuidValue v => JavaUuids.ToGuid(v),
                java.util.UUID v => JavaUuids.ToGuid(v),
                java.math.BigInteger v => new System.Numerics.BigInteger(v.toByteArray(), isUnsigned: false, isBigEndian: true),
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
