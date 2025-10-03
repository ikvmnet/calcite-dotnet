using System;
using System.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.Ado
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
            switch ((SqlTypeName.__Enum)typeName.ordinal())
            {
                case SqlTypeName.__Enum.NULL:
                    return null;
                case SqlTypeName.__Enum.BOOLEAN:
                    return reader.IsDBNull(index) ? null : java.lang.Boolean.valueOf(reader.GetBoolean(index));
                case SqlTypeName.__Enum.TINYINT:
                    return reader.IsDBNull(index) ? null : java.lang.Byte.valueOf(reader.GetByte(index));
                case SqlTypeName.__Enum.CHAR:
                    return reader.IsDBNull(index) ? null : java.lang.String.valueOf(reader.GetString(index));
                case SqlTypeName.__Enum.SMALLINT:
                    return GetShort(reader, index);
                case SqlTypeName.__Enum.INTEGER:
                    return GetInt(reader, index);
                case SqlTypeName.__Enum.BIGINT:
                    return GetLong(reader, index);
                case SqlTypeName.__Enum.TIMESTAMP:
                    return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(((DateTimeOffset)reader.GetDateTime(index)).ToUnixTimeMilliseconds());
                case SqlTypeName.__Enum.DATE:
                    return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(((DateTimeOffset)reader.GetDateTime(index)).ToUnixTimeMilliseconds());
                case SqlTypeName.__Enum.FLOAT:
                    return reader.IsDBNull(index) ? null : java.lang.Float.valueOf(reader.GetFloat(index));
                case SqlTypeName.__Enum.DOUBLE:
                    return reader.IsDBNull(index) ? null : java.lang.Double.valueOf(reader.GetDouble(index));
                case SqlTypeName.__Enum.VARCHAR:
                    return reader.IsDBNull(index) ? null : reader.GetString(index);
                default:
                    break;
            }

            throw new AdoCalciteException($"Unsupported SQL type mapping: {(SqlTypeName.__Enum)typeName.ordinal()}");
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

    }

}
