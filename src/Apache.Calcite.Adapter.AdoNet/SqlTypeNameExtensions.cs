using System.Data;

using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    static class SqlTypeNameExtensions
    {

        /// <summary>
        /// Attempts to derive the <see cref="DbType"/> for the given <see cref="SqlTypeName"/>.
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        public static DbType ToDbType(this SqlTypeName typeName)
        {
            switch ((SqlTypeName.__Enum)typeName.ordinal())
            {
                case SqlTypeName.__Enum.BOOLEAN:
                    return DbType.Boolean;
                case SqlTypeName.__Enum.TINYINT:
                    return DbType.SByte;
                case SqlTypeName.__Enum.SMALLINT:
                    return DbType.Int16;
                case SqlTypeName.__Enum.INTEGER:
                    return DbType.Int32;
                case SqlTypeName.__Enum.BIGINT:
                    return DbType.Int64;
                case SqlTypeName.__Enum.DECIMAL:
                    return DbType.Decimal;
                case SqlTypeName.__Enum.FLOAT:
                    break;
                case SqlTypeName.__Enum.REAL:
                    break;
                case SqlTypeName.__Enum.DOUBLE:
                    return DbType.Double;
                case SqlTypeName.__Enum.DATE:
                    return DbType.Date;
                case SqlTypeName.__Enum.TIME:
                    return DbType.Time;
                case SqlTypeName.__Enum.TIME_WITH_LOCAL_TIME_ZONE:
                    return DbType.DateTimeOffset;
                case SqlTypeName.__Enum.TIME_TZ:
                    break;
                case SqlTypeName.__Enum.TIMESTAMP:
                    break;
                case SqlTypeName.__Enum.TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    break;
                case SqlTypeName.__Enum.TIMESTAMP_TZ:
                    break;
                case SqlTypeName.__Enum.INTERVAL_YEAR:
                    break;
                case SqlTypeName.__Enum.INTERVAL_YEAR_MONTH:
                    break;
                case SqlTypeName.__Enum.INTERVAL_MONTH:
                    break;
                case SqlTypeName.__Enum.INTERVAL_DAY:
                    break;
                case SqlTypeName.__Enum.INTERVAL_DAY_HOUR:
                    break;
                case SqlTypeName.__Enum.INTERVAL_DAY_MINUTE:
                    break;
                case SqlTypeName.__Enum.INTERVAL_DAY_SECOND:
                    break;
                case SqlTypeName.__Enum.INTERVAL_HOUR:
                    break;
                case SqlTypeName.__Enum.INTERVAL_HOUR_MINUTE:
                    break;
                case SqlTypeName.__Enum.INTERVAL_HOUR_SECOND:
                    break;
                case SqlTypeName.__Enum.INTERVAL_MINUTE:
                    break;
                case SqlTypeName.__Enum.INTERVAL_MINUTE_SECOND:
                    break;
                case SqlTypeName.__Enum.INTERVAL_SECOND:
                    break;
                case SqlTypeName.__Enum.CHAR:
                    return DbType.AnsiStringFixedLength;
                case SqlTypeName.__Enum.VARCHAR:
                    return DbType.AnsiString;
                case SqlTypeName.__Enum.BINARY:
                    return DbType.Binary;
                case SqlTypeName.__Enum.VARBINARY:
                    return DbType.Binary;
                case SqlTypeName.__Enum.NULL:
                    break;
                case SqlTypeName.__Enum.UNKNOWN:
                    break;
                case SqlTypeName.__Enum.ANY:
                    break;
                case SqlTypeName.__Enum.SYMBOL:
                    break;
                case SqlTypeName.__Enum.MULTISET:
                    break;
                case SqlTypeName.__Enum.ARRAY:
                    break;
                case SqlTypeName.__Enum.MAP:
                    break;
                case SqlTypeName.__Enum.DISTINCT:
                    break;
                case SqlTypeName.__Enum.STRUCTURED:
                    break;
                case SqlTypeName.__Enum.ROW:
                    break;
                case SqlTypeName.__Enum.OTHER:
                    break;
                case SqlTypeName.__Enum.CURSOR:
                    break;
                case SqlTypeName.__Enum.COLUMN_LIST:
                    break;
                case SqlTypeName.__Enum.DYNAMIC_STAR:
                    break;
                case SqlTypeName.__Enum.GEOMETRY:
                    break;
                case SqlTypeName.__Enum.MEASURE:
                    break;
                case SqlTypeName.__Enum.FUNCTION:
                    break;
                case SqlTypeName.__Enum.SARG:
                    break;
                case SqlTypeName.__Enum.UUID:
                    break;
                case SqlTypeName.__Enum.VARIANT:
                    break;
            }

            throw new AdoSchemaException($"Unsupported SqlTypeName: {typeName.getName()}");
        }

    }

}
