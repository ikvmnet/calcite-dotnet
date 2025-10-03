using System;
using System.Data;

using java.sql;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Static methods for working with <see cref="DbType"/>.
    /// </summary>
    static class CalciteDbType
    {

        /// <summary>
        /// Gets the Java <see cref="SQLType"/> that corresponds to the given .NET <see cref="DbType"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static JDBCType ToJdbcType(DbType type) => type switch
        {
            DbType.AnsiString => JDBCType.VARCHAR,
            DbType.Binary => JDBCType.BINARY,
            DbType.Byte => JDBCType.TINYINT,
            DbType.Boolean => JDBCType.BOOLEAN,
            DbType.Currency => JDBCType.DECIMAL,
            DbType.Date => JDBCType.DATE,
            DbType.DateTime => JDBCType.TIMESTAMP,
            DbType.Decimal => JDBCType.DECIMAL,
            DbType.Double => JDBCType.DOUBLE,
            DbType.Guid => JDBCType.VARCHAR,
            DbType.Int16 => JDBCType.SMALLINT,
            DbType.Int32 => JDBCType.INTEGER,
            DbType.Int64 => JDBCType.BIGINT,
            DbType.Object => JDBCType.JAVA_OBJECT,
            DbType.SByte => JDBCType.TINYINT,
            DbType.Single => JDBCType.FLOAT,
            DbType.String => JDBCType.NVARCHAR,
            DbType.Time => JDBCType.TIME,
            DbType.UInt16 => JDBCType.SMALLINT,
            DbType.UInt32 => JDBCType.INTEGER,
            DbType.UInt64 => JDBCType.BIGINT,
            DbType.VarNumeric => JDBCType.NUMERIC,
            DbType.AnsiStringFixedLength => JDBCType.CHAR,
            DbType.StringFixedLength => JDBCType.NCHAR,
            DbType.Xml => JDBCType.SQLXML,
            DbType.DateTime2 => JDBCType.TIMESTAMP,
            DbType.DateTimeOffset => JDBCType.TIMESTAMP_WITH_TIMEZONE,
            _ => throw new NotSupportedException(),
        };

    }

}