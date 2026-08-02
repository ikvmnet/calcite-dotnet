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
                    return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(((DateTimeOffset)reader.GetDateTime(index)).ToUnixTimeMilliseconds());
                case nameof(SqlTypeName.FLOAT):
                    return reader.IsDBNull(index) ? null : java.lang.Float.valueOf(reader.GetFloat(index));
                case nameof(SqlTypeName.DOUBLE):
                    return reader.IsDBNull(index) ? null : java.lang.Double.valueOf(reader.GetDouble(index));
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
