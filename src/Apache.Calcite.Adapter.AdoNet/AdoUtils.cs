using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Linq;

using Apache.Calcite.Adapter.AdoNet.Extensions;
using Apache.Calcite.Adapter.AdoNet.Utils;

using org.apache.calcite.avatica;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    internal static class AdoUtils
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="types"></param>
        /// <returns></returns>
        internal static Function1 RowBuilderFactory2(ImmutableArray<(ColumnMetaData.Rep, DbType)> types)
        {
            var r = types.Select(i => i.Item1).ToArray();
            var t = types.Select(i => i.Item2).ToArray();
            return new FuncFunction1<DbDataReader, object>(reader => new ObjectArrayRowBuilder2(reader, r, t));
        }

        /// <summary>
        /// Gets a value from the reader according to the specified representation and database type.
        /// </summary>
        /// <param name="rep"></param>
        /// <param name="dbType"></param>
        /// <param name="reader"></param>
        /// <param name="i"></param>
        /// <returns></returns>
        public static object? GetDbReaderValue(ColumnMetaData.Rep rep, DbType dbType, DbDataReader reader, int i)
        {
            switch ((ColumnMetaData.Rep.__Enum)rep.ordinal())
            {
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_BOOLEAN when dbType == DbType.Boolean:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_BYTE when dbType == DbType.Byte:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_CHAR when dbType == DbType.AnsiStringFixedLength:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_SHORT when dbType == DbType.Int16:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_INT when dbType == DbType.Int32:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_LONG when dbType == DbType.Int64:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_FLOAT when dbType == DbType.Single:
                    break;
                case ColumnMetaData.Rep.__Enum.PRIMITIVE_DOUBLE when dbType == DbType.Double:
                    break;
                case ColumnMetaData.Rep.__Enum.BOOLEAN when dbType == DbType.Boolean:
                    return reader.IsDBNull(i) ? null : java.lang.Boolean.valueOf(reader.GetBoolean(i));
                case ColumnMetaData.Rep.__Enum.BYTE when dbType == DbType.Byte:
                    return reader.IsDBNull(i) ? null : java.lang.Byte.valueOf(reader.GetByte(i));
                case ColumnMetaData.Rep.__Enum.CHARACTER when dbType == DbType.String:
                    return reader.IsDBNull(i) ? null : java.lang.Character.valueOf(reader.GetChar(i));
                case ColumnMetaData.Rep.__Enum.SHORT when dbType == DbType.Int16:
                    return reader.IsDBNull(i) ? null : java.lang.Short.valueOf(reader.GetInt16(i));
                case ColumnMetaData.Rep.__Enum.INTEGER when dbType == DbType.Int32:
                    return reader.IsDBNull(i) ? null : java.lang.Integer.valueOf(reader.GetInt32(i));
                case ColumnMetaData.Rep.__Enum.LONG when dbType == DbType.Int64:
                    return reader.IsDBNull(i) ? null : java.lang.Long.valueOf(reader.GetInt64(i));
                case ColumnMetaData.Rep.__Enum.LONG when dbType == DbType.DateTime:
                    return reader.IsDBNull(i) ? null : java.lang.Long.valueOf(((DateTimeOffset)reader.GetDateTime(i)).ToUnixTimeMilliseconds());
                case ColumnMetaData.Rep.__Enum.LONG when dbType == DbType.DateTime2:
                    return reader.IsDBNull(i) ? null : java.lang.Long.valueOf(((DateTimeOffset)reader.GetDateTime(i)).ToUnixTimeMilliseconds());
                case ColumnMetaData.Rep.__Enum.FLOAT:
                    return reader.IsDBNull(i) ? null : java.lang.Float.valueOf(reader.GetFloat(i));
                case ColumnMetaData.Rep.__Enum.DOUBLE:
                    return reader.IsDBNull(i) ? null : java.lang.Double.valueOf(reader.GetDouble(i));
                case ColumnMetaData.Rep.__Enum.JAVA_SQL_TIME:
                    break;
                case ColumnMetaData.Rep.__Enum.JAVA_SQL_TIMESTAMP:
                    break;
                case ColumnMetaData.Rep.__Enum.JAVA_SQL_DATE:
                    break;
                case ColumnMetaData.Rep.__Enum.JAVA_UTIL_DATE:
                    break;
                case ColumnMetaData.Rep.__Enum.BYTE_STRING:
                    break;
                case ColumnMetaData.Rep.__Enum.STRING when dbType == DbType.String:
                    return reader.IsDBNull(i) ? null : reader.GetString(i);
                case ColumnMetaData.Rep.__Enum.STRING when dbType == DbType.StringFixedLength:
                    return reader.IsDBNull(i) ? null : reader.GetString(i);
                case ColumnMetaData.Rep.__Enum.STRING when dbType == DbType.AnsiString:
                    return reader.IsDBNull(i) ? null : reader.GetString(i);
                case ColumnMetaData.Rep.__Enum.STRING when dbType == DbType.AnsiStringFixedLength && reader.GetFieldType(i) == typeof(Guid):
                    return reader.IsDBNull(i) ? null : reader.GetGuid(i).ToString("d").ToUpperInvariant();
                case ColumnMetaData.Rep.__Enum.STRING when dbType == DbType.AnsiStringFixedLength:
                    return reader.IsDBNull(i) ? null : reader.GetString(i);
                case ColumnMetaData.Rep.__Enum.NUMBER:
                case ColumnMetaData.Rep.__Enum.ARRAY:
                case ColumnMetaData.Rep.__Enum.MULTISET:
                case ColumnMetaData.Rep.__Enum.STRUCT:
                case ColumnMetaData.Rep.__Enum.OBJECT:
                    break;
            }

            throw new AdoSchemaException($"Unsupported representative and database type mapping: {rep} -> {dbType}");
        }

    }

}
