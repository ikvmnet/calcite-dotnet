using System;
using System.Data.Common;

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
                case nameof(SqlTypeName.TIMESTAMP):
                    return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(((DateTimeOffset)reader.GetDateTime(index)).ToUnixTimeMilliseconds());
                case nameof(SqlTypeName.DATE):
                    return GetDate(reader, index);
                // FLOAT is eight bytes in Calcite, as it is in SQL, and shares DOUBLE's representation;
                // REAL is the four byte one. JavaTypeFactoryImpl.getJavaClass says so, and marks it "sic".
                case nameof(SqlTypeName.FLOAT):
                case nameof(SqlTypeName.DOUBLE):
                    return reader.IsDBNull(index) ? null : java.lang.Double.valueOf(reader.GetDouble(index));
                case nameof(SqlTypeName.REAL):
                    return reader.IsDBNull(index) ? null : java.lang.Float.valueOf(reader.GetFloat(index));
                case nameof(SqlTypeName.DECIMAL):
                    return GetDecimal(reader, index);
                case nameof(SqlTypeName.BINARY):
                case nameof(SqlTypeName.VARBINARY):
                    return GetBinary(reader, index);
                case nameof(SqlTypeName.TIME):
                    return GetTime(reader, index);
                case nameof(SqlTypeName.TIMESTAMP_TZ):
                    return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(new DateTimeOffset(reader.GetDateTime(index), TimeSpan.Zero).ToUnixTimeMilliseconds());
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
        /// Gets a <see cref="java.lang.Boolean"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBoolean(DbDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : java.lang.Boolean.valueOf(reader.GetBoolean(index));
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Byte"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetByte(DbDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : java.lang.Byte.valueOf(reader.GetByte(index));
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Short"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetShort(DbDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : java.lang.Short.valueOf(reader.GetInt16(index));
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Integer"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetInt(DbDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : java.lang.Integer.valueOf(reader.GetInt32(index));
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Long"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetLong(DbDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(reader.GetInt64(index));
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
        public static object? GetString(DbDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : reader.GetString(index);
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
