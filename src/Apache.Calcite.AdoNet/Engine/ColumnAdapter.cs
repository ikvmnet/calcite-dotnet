using System;
using System.Collections.Generic;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;

using Apache.Calcite.AdoNet.Protocol;

namespace Apache.Calcite.AdoNet.Engine
{

    /// <summary>
    /// Maps Calcite/Avatica column metadata into <see cref="CalciteColumn"/> descriptors used by the
    /// ADO.NET layer.
    /// </summary>
    internal static class ColumnAdapter
    {

        public static IReadOnlyList<CalciteColumn> MapColumns(CalcitePrepare.CalciteSignature signature)
        {
            var src = signature.columns;
            var count = src.size();
            var list = new List<CalciteColumn>(count);

            for (var i = 0; i < count; i++)
            {
                var c = (ColumnMetaData)src.get(i);
                list.Add(new CalciteColumn(
                    c.columnName,
                    MapClrType(c.type.rep),
                    c.type.name,
                    c.nullable != 0 /* java.sql.ResultSetMetaData.columnNoNulls */));
            }

            return list;
        }

        public static Type MapClrType(ColumnMetaData.Rep rep)
        {
            if (rep == ColumnMetaData.Rep.PRIMITIVE_BOOLEAN || rep == ColumnMetaData.Rep.BOOLEAN) return typeof(bool);
            if (rep == ColumnMetaData.Rep.PRIMITIVE_BYTE || rep == ColumnMetaData.Rep.BYTE) return typeof(sbyte);
            if (rep == ColumnMetaData.Rep.PRIMITIVE_SHORT || rep == ColumnMetaData.Rep.SHORT) return typeof(short);
            if (rep == ColumnMetaData.Rep.PRIMITIVE_INT || rep == ColumnMetaData.Rep.INTEGER) return typeof(int);
            if (rep == ColumnMetaData.Rep.PRIMITIVE_LONG || rep == ColumnMetaData.Rep.LONG) return typeof(long);
            if (rep == ColumnMetaData.Rep.PRIMITIVE_FLOAT || rep == ColumnMetaData.Rep.FLOAT) return typeof(float);
            if (rep == ColumnMetaData.Rep.PRIMITIVE_DOUBLE || rep == ColumnMetaData.Rep.DOUBLE) return typeof(double);
            if (rep == ColumnMetaData.Rep.NUMBER) return typeof(decimal);
            if (rep == ColumnMetaData.Rep.STRING || rep == ColumnMetaData.Rep.CHARACTER) return typeof(string);
            if (rep == ColumnMetaData.Rep.JAVA_SQL_DATE || rep == ColumnMetaData.Rep.JAVA_UTIL_DATE || rep == ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP) return typeof(DateTime);
            if (rep == ColumnMetaData.Rep.JAVA_SQL_TIME) return typeof(TimeSpan);
            if (rep == ColumnMetaData.Rep.BYTE_STRING) return typeof(byte[]);

            return typeof(object);
        }

    }

}
