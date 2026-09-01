using System;
using System.Data.Common;
using System.Globalization;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Various utilities for working with an ADO data reader.
    /// </summary>
    public static class AdoReaderUtil
    {

        /// <summary>
        /// Gets a value from the reader according to the specified representation and database type.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object? GetDbReaderValue(DbDataReader reader, int index, RelDataType type)
        {
            return GetDbReaderValue(reader, index, type.getSqlTypeName());
        }

        /// <summary>
        /// Gets a object value from the reader according to the specified representation and database type.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="typeName"></param>
        /// <returns></returns>
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
                // the unsigned types travel as joou values, which is what JavaTypeFactoryImpl.getJavaClass
                // answers for them and what CalciteResultValue decodes at the other end
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
                // FLOAT is eight bytes in Calcite, as it is in SQL, and shares DOUBLE's representation;
                // REAL is the four byte one. JavaTypeFactoryImpl.getJavaClass says so, and marks it "sic".
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
                case nameof(SqlTypeName.OTHER):
                    return GetValue(reader, index);
                default:
                    break;
            }

            throw new AdoCalciteException($"Unsupported SQL type mapping: {typeName.name()}");
        }

        /// <summary>
        /// Gets the value at an index converted to a CLR type, or <see langword="null"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// <para>
        /// The width a provider declares a column in is not the width Calcite chose for it, and the typed
        /// accessors on <see cref="DbDataReader"/> cast rather than convert: <see cref="DbDataReader.GetInt16"/>
        /// on a column the driver decoded as a <see cref="byte"/> throws rather than widening. Every one of
        /// these is a lossless widening of an integral or approximate value the provider already decoded, so
        /// converting is what the mapping meant.
        /// </para>
        /// <para>
        /// The cost is one boxed value per cell, which is what every one of these accessors was going to pay
        /// anyway: the result is a <c>java.lang</c> wrapper, allocated per cell, whatever route it took.
        /// </para>
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
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBoolean(DbDataReader reader, int index)
        {
            return GetValueAs<bool>(reader, index) is bool value ? java.lang.Boolean.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Byte"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite's <c>TINYINT</c> is signed, so this is an <see cref="sbyte"/> and not the <see cref="byte"/>
        /// the <see cref="DbDataReader.GetByte"/> accessor answers with. A provider whose own tiny integer is
        /// unsigned — SQL Server's is — maps to <c>UTINYINT</c> and comes through <see cref="GetUByte"/>
        /// instead. Java's <c>byte</c> is IKVM's <see cref="byte"/> and is unsigned, so the sign travels in
        /// the bits.
        /// </remarks>
        public static object? GetByte(DbDataReader reader, int index)
        {
            return GetValueAs<sbyte>(reader, index) is sbyte value ? java.lang.Byte.valueOf(unchecked((byte)value)) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Short"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetShort(DbDataReader reader, int index)
        {
            return GetValueAs<short>(reader, index) is short value ? java.lang.Short.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Integer"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetInt(DbDataReader reader, int index)
        {
            return GetValueAs<int>(reader, index) is int value ? java.lang.Integer.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Long"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetLong(DbDataReader reader, int index)
        {
            return GetValueAs<long>(reader, index) is long value ? java.lang.Long.valueOf(value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.UByte"/>, which is what Calcite holds a <c>UTINYINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// The unsigned types are not a variation on the signed ones: <c>getJavaClass</c> answers a joou
        /// <c>UByte</c>, <c>UShort</c>, <c>UInteger</c> or <c>ULong</c> rather than a <c>java.lang</c>
        /// wrapper, and <c>CalciteResultValue</c> is written to decode exactly those. Handing over a
        /// <see cref="java.lang.Short"/> instead would be a value of the wrong class for the type the row
        /// declares. The widening overload is taken in each case so that the sign is never in question.
        /// </remarks>
        public static object? GetUByte(DbDataReader reader, int index)
        {
            return GetValueAs<byte>(reader, index) is byte value ? org.joou.UByte.valueOf((int)value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.UShort"/>, which is what Calcite holds a <c>USMALLINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetUShort(DbDataReader reader, int index)
        {
            return GetValueAs<ushort>(reader, index) is ushort value ? org.joou.UShort.valueOf((int)value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.UInteger"/>, which is what Calcite holds a <c>UINTEGER</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetUInt(DbDataReader reader, int index)
        {
            return GetValueAs<uint>(reader, index) is uint value ? org.joou.UInteger.valueOf((long)value) : null;
        }

        /// <summary>
        /// Gets an <see cref="org.joou.ULong"/>, which is what Calcite holds a <c>UBIGINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// The bits, read unsigned. This went through the decimal string on the grounds that
        /// <c>valueOf(long)</c> refuses anything above <see cref="long.MaxValue"/> and so could not carry
        /// half of what the type holds; that is not what it does. Measured: <c>valueOf(-1L)</c> is
        /// 18446744073709551615, the overload taking the argument's bits rather than its value, which is
        /// the whole range and the same representation joou stores.
        /// </remarks>
        public static object? GetULong(DbDataReader reader, int index)
        {
            return GetValueAs<ulong>(reader, index) is ulong value ? org.joou.ULong.valueOf(unchecked((long)value)) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Double"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetDouble(DbDataReader reader, int index)
        {
            return GetValueAs<double>(reader, index) is double value ? java.lang.Double.valueOf(value) : null;
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Float"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
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
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// <para>
        /// A date is a count of whole days since 1 January 1970, held in an <see cref="java.lang.Integer"/>:
        /// <c>SqlFunctions.internalToDate</c> decodes one with <c>LocalDate.ofEpochDay</c>, and
        /// <c>JavaTypeFactory.getJavaClass</c> reports <c>int</c> for the type. It is not a millisecond count,
        /// which is what a <see cref="SqlTypeName.TIMESTAMP"/> is.
        /// </para>
        /// <para>
        /// Only the date component is read, and no time zone enters into it. Converting through
        /// <see cref="DateTimeOffset"/> would apply the machine's offset to a value whose
        /// <see cref="DateTime.Kind"/> is typically <see cref="DateTimeKind.Unspecified"/>, which for a date
        /// at midnight can land on the day before.
        /// </para>
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
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A timestamp carries no zone: the count is of milliseconds from the epoch to the wall clock read
        /// as though it were UTC, which is what <c>ParameterBinder.ConvertTimestamp</c> writes and what
        /// <c>CalciteResultValue</c> decodes with <c>UnixEpoch.AddMilliseconds</c>. Casting the provider's
        /// <see cref="DateTime"/> to a <see cref="DateTimeOffset"/> instead reads an unspecified
        /// <see cref="DateTime.Kind"/> as local time and shifts the value by the machine's offset — the same
        /// hazard <see cref="GetDate"/> avoids, and for the same reason.
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
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A zoned timestamp is an instant, so the count is of milliseconds from the epoch to it. A provider
        /// that has a type for one hands back a <see cref="DateTimeOffset"/> and refuses
        /// <see cref="DbDataReader.GetDateTime"/> outright — SQL Server's <c>datetimeoffset</c> does; one
        /// that does not is read as UTC, the offset being the thing it had no way to tell us.
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
        /// Counts the milliseconds from the epoch to a <see cref="DateTime"/> already in the terms it is to
        /// be counted in.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        static long ToUnixTimeMilliseconds(DateTime value)
        {
            return (long)(value - UnixEpoch).TotalMilliseconds;
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.DECIMAL"/> as the <see cref="java.math.BigDecimal"/> Calcite holds
        /// one in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// Via the decimal string rather than a double: a decimal is exact, and routing it through binary
        /// floating point would not be.
        /// </remarks>
        public static object? GetDecimal(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return new java.math.BigDecimal(reader.GetDecimal(index).ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.VARBINARY"/> as the <c>ByteString</c> Calcite holds one in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBinary(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            return new org.apache.calcite.avatica.util.ByteString((byte[])reader.GetValue(index));
        }

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIME"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A time is a count of milliseconds since midnight held in an <see cref="java.lang.Integer"/>, the
        /// same shape a <see cref="SqlTypeName.DATE"/> uses for days. Providers surface one either as a span
        /// or as a whole timestamp whose date part is to be ignored.
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
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A column Calcite holds as <see cref="SqlTypeName.CHAR"/> or <see cref="SqlTypeName.VARCHAR"/> need
        /// not be a string to the provider: SQL Server hands back a <see cref="Guid"/> for a
        /// <c>uniqueidentifier</c>, which <c>AdoTable</c> types as <c>CHAR(36)</c>, and
        /// <see cref="DbDataReader.GetString"/> casts rather than converts and refuses it. Formatting the
        /// value is what the type says it is.
        /// </remarks>
        public static object? GetString(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            var value = reader.GetValue(index);
            return value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the native provider value for types with no dedicated Calcite mapping (e.g. <c>OTHER</c>).
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetValue(DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            var value = reader.GetValue(index);
            return value == DBNull.Value ? null : value;
        }

    }

}
