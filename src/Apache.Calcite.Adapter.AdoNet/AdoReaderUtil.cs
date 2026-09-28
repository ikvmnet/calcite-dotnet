using System;
using System.Data.Common;
using System.Globalization;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Reads a column of a <see cref="DbDataReader"/>'s current row into the representation Calcite's runtime holds
    /// for the column's SQL type. The code the converters generate calls these.
    /// </summary>
    /// <remarks>
    /// The column's declared SQL type decides the representation, whatever CLR type the provider returns: an
    /// integer is a <c>java.lang</c> wrapper, an unsigned integer a joou value, a <c>DECIMAL</c> a
    /// <see cref="java.math.BigDecimal"/>, a date, time or timestamp a number, and a binary value a
    /// <c>ByteString</c>. Each accessor returns <see langword="null"/> for a database null.
    /// </remarks>
    public static class AdoReaderUtil
    {

        /// <summary>
        /// Reads a column as the representation of <paramref name="type"/>'s SQL type.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <param name="type">The column's type.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <exception cref="AdoCalciteException">The SQL type has no mapping.</exception>
        public static object? GetDbReaderValue(DbDataReader reader, int index, RelDataType type)
        {
            return GetDbReaderValue(reader, index, type.getSqlTypeName());
        }

        /// <summary>
        /// Reads a column as the representation of a SQL type.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <param name="typeName">The column's SQL type.</param>
        /// <returns>The value, or <see langword="null"/>. Always <see langword="null"/> for <c>NULL</c>, and the
        /// provider's own value for <c>OTHER</c>.</returns>
        /// <exception cref="AdoCalciteException">The SQL type has no mapping.</exception>
        public static object? GetDbReaderValue(DbDataReader reader, int index, SqlTypeName typeName)
        {
            switch (typeName.name())
            {
                case nameof(SqlTypeName.NULL):
                    return null;
                case nameof(SqlTypeName.BOOLEAN):
                    return GetBoolean(reader, index);
                case nameof(SqlTypeName.TINYINT):
                    return GetByte(reader, index);
                case nameof(SqlTypeName.CHAR):
                    return GetString(reader, index);
                case nameof(SqlTypeName.SMALLINT):
                    return GetShort(reader, index);
                case nameof(SqlTypeName.INTEGER):
                    return GetInt(reader, index);
                case nameof(SqlTypeName.BIGINT):
                    return GetLong(reader, index);
                // JavaTypeFactoryImpl.getJavaClass maps the unsigned types to joou classes
                case nameof(SqlTypeName.UTINYINT):
                    return GetUByte(reader, index);
                case nameof(SqlTypeName.USMALLINT):
                    return GetUShort(reader, index);
                case nameof(SqlTypeName.UINTEGER):
                    return GetUInt(reader, index);
                case nameof(SqlTypeName.UBIGINT):
                    return GetULong(reader, index);
                case nameof(SqlTypeName.TIMESTAMP):
                    return GetTimestamp(reader, index);
                case nameof(SqlTypeName.DATE):
                    return GetDate(reader, index);
                // Calcite's FLOAT is eight bytes, like DOUBLE (JavaTypeFactoryImpl.getJavaClass); REAL is four
                case nameof(SqlTypeName.FLOAT):
                case nameof(SqlTypeName.DOUBLE):
                    return GetDouble(reader, index);
                case nameof(SqlTypeName.REAL):
                    return GetFloat(reader, index);
                case nameof(SqlTypeName.DECIMAL):
                    return GetDecimal(reader, index);
                case nameof(SqlTypeName.BINARY):
                case nameof(SqlTypeName.VARBINARY):
                    return GetBinary(reader, index);
                case nameof(SqlTypeName.TIME):
                    return GetTime(reader, index);
                case nameof(SqlTypeName.TIMESTAMP_TZ):
                    return GetTimestampTz(reader, index);
                case nameof(SqlTypeName.VARCHAR):
                    return GetString(reader, index);
                case nameof(SqlTypeName.UUID):
                    return GetUuid(reader, index);
                case nameof(SqlTypeName.OTHER):
                    return GetValue(reader, index);
                default:
                    break;
            }

            throw new AdoCalciteException($"Unsupported SQL type mapping: {typeName.name()}");
        }

        /// <summary>
        /// Reads a column and converts it to <typeparamref name="T"/>, or returns <see langword="null"/> for a
        /// database null.
        /// </summary>
        /// <typeparam name="T">The CLR type to convert to.</typeparam>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// The provider's width for a column need not be the one Calcite chose, and the typed getters on
        /// <see cref="DbDataReader"/> cast rather than convert (<see cref="DbDataReader.GetInt16"/> throws on a column
        /// the provider returns as a <see cref="byte"/>). So the value is read with
        /// <see cref="DbDataReader.GetValue"/> and converted with <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>.
        /// </remarks>
        static T? GetValueAs<T>(DbDataReader reader, int index)
            where T : struct
        {
            if (reader.IsDBNull(index))
                return null;

            var value = reader.GetValue(index);
            return value is T typed ? typed : (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Boolean"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetBoolean(DbDataReader reader, int index)
        {
            return GetValueAs<bool>(reader, index) is bool value ? java.lang.Boolean.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Byte"/>, for a <c>TINYINT</c>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Calcite's <c>TINYINT</c> is signed, so the column is read as an <see cref="sbyte"/>. IKVM exposes Java's
        /// <c>byte</c> as the unsigned <see cref="byte"/>, so the value is passed as its two's complement bits. An
        /// unsigned tiny integer, such as SQL Server's, is a <c>UTINYINT</c> and is read by <see cref="GetUByte"/>.
        /// </remarks>
        public static object? GetByte(DbDataReader reader, int index)
        {
            return GetValueAs<sbyte>(reader, index) is sbyte value ? java.lang.Byte.valueOf(unchecked((byte)value)) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Short"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetShort(DbDataReader reader, int index)
        {
            return GetValueAs<short>(reader, index) is short value ? java.lang.Short.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Integer"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetInt(DbDataReader reader, int index)
        {
            return GetValueAs<int>(reader, index) is int value ? java.lang.Integer.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Long"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetLong(DbDataReader reader, int index)
        {
            return GetValueAs<long>(reader, index) is long value ? java.lang.Long.valueOf(value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.UByte"/>, which is what Calcite holds a <c>UTINYINT</c> in.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// <c>JavaTypeFactoryImpl.getJavaClass</c> maps each unsigned type to a joou class, so a row holding a
        /// <c>java.lang</c> wrapper for one would hold a value of the wrong class. Each of these accessors calls the
        /// <c>valueOf</c> overload that takes a wider type, so the value is never read as signed.
        /// </remarks>
        public static object? GetUByte(DbDataReader reader, int index)
        {
            return GetValueAs<byte>(reader, index) is byte value ? org.joou.UByte.valueOf((int)value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.UShort"/>, which is what Calcite holds a <c>USMALLINT</c> in.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetUShort(DbDataReader reader, int index)
        {
            return GetValueAs<ushort>(reader, index) is ushort value ? org.joou.UShort.valueOf((int)value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.UInteger"/>, which is what Calcite holds a <c>UINTEGER</c> in.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetUInt(DbDataReader reader, int index)
        {
            return GetValueAs<uint>(reader, index) is uint value ? org.joou.UInteger.valueOf((long)value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.ULong"/>, which is what Calcite holds a <c>UBIGINT</c> in.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// <c>ULong.valueOf(long)</c> reads its argument's bits as unsigned (<c>valueOf(-1L)</c> is
        /// 18446744073709551615), so the <see cref="ulong"/> is passed reinterpreted as a <see cref="long"/>.
        /// </remarks>
        public static object? GetULong(DbDataReader reader, int index)
        {
            return GetValueAs<ulong>(reader, index) is ulong value ? org.joou.ULong.valueOf(unchecked((long)value)) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Double"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetDouble(DbDataReader reader, int index)
        {
            return GetValueAs<double>(reader, index) is double value ? java.lang.Double.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Float"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetFloat(DbDataReader reader, int index)
        {
            return GetValueAs<float>(reader, index) is float value ? java.lang.Float.valueOf(value) : null;
        }

        /// <summary>
        /// The day <see cref="SqlTypeName.DATE"/> counts from.
        /// </summary>
        static readonly DateOnly UnixEpochDay = new(1970, 1, 1);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.DATE"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Calcite holds a date as the number of days since 1 January 1970 in an <see cref="java.lang.Integer"/>
        /// (<c>SqlFunctions.internalToDate</c> decodes it with <c>LocalDate.ofEpochDay</c>). Only the date part of
        /// the provider's <see cref="DateTime"/> is read, with no time zone applied, so a date at midnight cannot
        /// shift to the day before.
        /// </remarks>
        public static object? GetDate(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return java.lang.Integer.valueOf(DateOnly.FromDateTime(reader.GetDateTime(index)).DayNumber - UnixEpochDay.DayNumber);
        }

        /// <summary>
        /// The instant a <see cref="SqlTypeName.TIMESTAMP"/> counts from.
        /// </summary>
        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIMESTAMP"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Calcite holds a timestamp without time zone as milliseconds since the epoch in a
        /// <see cref="java.lang.Long"/>, counting the wall-clock value as though it were UTC. The provider's
        /// <see cref="DateTime"/> is read that way whatever its <see cref="DateTime.Kind"/>, so the machine's
        /// time zone never shifts it.
        /// </remarks>
        public static object? GetTimestamp(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return java.lang.Long.valueOf(ToUnixTimeMilliseconds(DateTime.SpecifyKind(reader.GetDateTime(index), DateTimeKind.Utc)));
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIMESTAMP_TZ"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// The value is milliseconds from the epoch to the instant, in a <see cref="java.lang.Long"/>. A
        /// <see cref="DateTimeOffset"/> (SQL Server's <c>datetimeoffset</c>, which refuses
        /// <see cref="DbDataReader.GetDateTime"/>) is converted by its offset; a <see cref="DateTime"/> of
        /// unspecified kind is taken as UTC; a string is parsed.
        /// </remarks>
        public static object? GetTimestampTz(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return java.lang.Long.valueOf(reader.GetValue(index) switch
            {
                DateTimeOffset o => o.ToUnixTimeMilliseconds(),
                DateTime d => ToUnixTimeMilliseconds(d.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(d, DateTimeKind.Utc) : d.ToUniversalTime()),
                string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture).ToUnixTimeMilliseconds(),
                _ => ToUnixTimeMilliseconds(DateTime.SpecifyKind(reader.GetDateTime(index), DateTimeKind.Utc)),
            });
        }

        /// <summary>
        /// Returns the milliseconds from the epoch to <paramref name="value"/>, treating it as UTC.
        /// </summary>
        /// <param name="value">The value, already in UTC or wall-clock terms.</param>
        /// <returns>The millisecond count.</returns>
        static long ToUnixTimeMilliseconds(DateTime value)
        {
            return (long)(value - UnixEpoch).TotalMilliseconds;
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.DECIMAL"/> as the <see cref="java.math.BigDecimal"/> Calcite holds
        /// one in.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Converted through the decimal's string form, which is exact, rather than through a double.
        /// </remarks>
        public static object? GetDecimal(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return new java.math.BigDecimal(reader.GetDecimal(index).ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.BINARY"/> or <see cref="SqlTypeName.VARBINARY"/> as the <c>ByteString</c>
        /// Calcite holds one in. The provider must return a <c>byte[]</c>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetBinary(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return new org.apache.calcite.avatica.util.ByteString((byte[])reader.GetValue(index));
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIME"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Calcite holds a time as milliseconds since midnight in an <see cref="java.lang.Integer"/>. The provider's
        /// value may be a <see cref="TimeSpan"/>, a <see cref="DateTime"/> whose date part is ignored, or a string.
        /// </remarks>
        public static object? GetTime(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            var value = reader.GetValue(index);
            var span = value switch
            {
                TimeSpan t => t,
                DateTime d => d.TimeOfDay,
                string s => TimeSpan.Parse(s, System.Globalization.CultureInfo.InvariantCulture),
                _ => reader.GetDateTime(index).TimeOfDay,
            };

            return java.lang.Integer.valueOf((int)span.TotalMilliseconds);
        }

        /// <summary>
        /// Gets a <see cref="string"/>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// For <see cref="SqlTypeName.CHAR"/> and <see cref="SqlTypeName.VARCHAR"/>. Reads with
        /// <see cref="DbDataReader.GetString"/>, so a column the provider does not return as a string fails rather
        /// than being formatted.
        /// </remarks>
        public static object? GetString(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return reader.GetString(index);
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.UUID"/> as the <see cref="org.apache.calcite.util.UuidValue"/> Calcite holds
        /// one in.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Reads with <see cref="DbDataReader.GetGuid"/> and converts the sixteen bytes. A character column holding
        /// GUID text is a <c>VARCHAR</c> and is read by <see cref="GetString"/>; a query casts it to <c>UUID</c> to
        /// treat it as one.
        /// </remarks>
        public static object? GetUuid(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return JavaUuids.ToUuidValue(reader.GetGuid(index));
        }

        /// <summary>
        /// Gets the provider's own value unchanged, for <c>OTHER</c>.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="index">The column's ordinal.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static object? GetValue(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            var value = reader.GetValue(index);
            return value == DBNull.Value ? null : value;
        }

    }

}
