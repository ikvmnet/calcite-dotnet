using System;
using System.Data.Common;

using Apache.Calcite.Adapter.Ado.Extensions;
using Apache.Calcite.Adapter.Ado.Utils;

using java.util;

using org.apache.calcite.linq4j.function;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Various utilities for working with the ADO Calcite adapter.
    /// </summary>
    public static class AdoUtils
    {

        /// <summary>
        /// Creates a row builder factory for converting to the set of <see cref="RelDataTypeField"/>s
        /// </summary>
        /// <param name="fields"></param>
        /// <returns></returns>
        internal static Function1 CreateObjectArrayRowBuilderFactory(List fields)
        {
            return new FuncFunction1<DbDataReader, object>(reader => new ObjectArrayRowBuilder(reader, fields));
        }

        /// <summary>
        /// Gets a value from the reader according to the specified representation and database type.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="index"></param>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static object? GetDbReaderValue(RelDataType type, int index, DbDataReader reader)
        {
            return GetDbReaderValue(type.getSqlTypeName(), index, reader);
        }

        /// <summary>
        /// Gets a object value from the reader according to the specified representation and database type.
        /// </summary>
        /// <param name="typeName"></param>
        /// <param name="index"></param>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static object? GetDbReaderValue(SqlTypeName typeName, int index, DbDataReader reader)
        {
            switch ((SqlTypeName.__Enum)typeName.ordinal())
            {
                case SqlTypeName.__Enum.BOOLEAN:
                    return reader.IsDBNull(index) ? null : java.lang.Boolean.valueOf(reader.GetBoolean(index));
                case SqlTypeName.__Enum.TINYINT:
                    return reader.IsDBNull(index) ? null : java.lang.Byte.valueOf(reader.GetByte(index));
                case SqlTypeName.__Enum.CHAR:
                    return reader.IsDBNull(index) ? null : java.lang.String.valueOf(reader.GetString(index));
                case SqlTypeName.__Enum.SMALLINT:
                    return reader.IsDBNull(index) ? null : java.lang.Short.valueOf(reader.GetInt16(index));
                case SqlTypeName.__Enum.INTEGER:
                    return reader.IsDBNull(index) ? null : java.lang.Integer.valueOf(reader.GetInt32(index));
                case SqlTypeName.__Enum.BIGINT:
                    return reader.IsDBNull(index) ? null : java.lang.Long.valueOf(reader.GetInt64(index));
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

    }

}
