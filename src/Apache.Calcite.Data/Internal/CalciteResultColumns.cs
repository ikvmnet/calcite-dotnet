using System;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads a prepared <see cref="IClrPrepare.Signature"/>'s columns as an ADO.NET caller expects them.
    /// </summary>
    /// <remarks>
    /// The columns are Avatica's <see cref="ColumnMetaData"/>, which is what the metadata port produces;
    /// <see cref="MapClrType(ColumnMetaData.AvaticaType)"/> is where that becomes a CLR type.
    /// </remarks>
    internal readonly struct CalciteResultColumns
    {

        /// <summary>
        /// Maps a SQL type described by an AvaticaType to the corresponding Common Language Runtime (CLR) type used by DO.NET consumers.
        /// This is not necessarily the final and only type that can be retrieved from any connection, only the primary type advertised by
        /// the connection.
        /// </summary>
        /// <returns>A Type object representing the CLR type that corresponds to the specified SQL type.</returns>
        public static Type MapClrType(ColumnMetaData.AvaticaType type)
        {
            // The SQL type name takes precedence for date/time/binary because Calcite's runtime
            // representation (rep) is the internal storage form (int days, long ms, ByteString),
            // not the public CLR type expected by ADO.NET consumers.

            // an ARRAY, a MULTISET and a ROW are not scalars, and their rep does not describe them:
            // Avatica puts the *component's* rep on an array type, so an INTEGER ARRAY reports
            // PRIMITIVE_INT, and a struct reports OBJECT. The JDBC ordinal is what names the shape.
            if (type.id == java.sql.Types.ARRAY)
                // the component array, which is what the value materializes as; a component that holds a
                // null materializes as Nullable<T>[] instead, there being no other way to carry one, and
                // a component the metadata cannot name falls back to object[]
                return type is ColumnMetaData.ArrayType array && array.getComponent() != null
                    ? MapClrType(array.getComponent()).MakeArrayType()
                    : typeof(object[]);

            // a row's fields are heterogeneous, so the array that carries them is of object
            if (type.id == java.sql.Types.STRUCT)
                return typeof(object[]);

            switch (type.name)
            {
                case "DATE":
                case "TIMESTAMP":
                    return typeof(DateTime);
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                case "TIMESTAMP WITH TIME ZONE":
                case "TIME WITH LOCAL TIME ZONE":
                case "TIME WITH TIME ZONE":
                    return typeof(DateTimeOffset);
                case "TIME":
                    return typeof(TimeSpan);
                case "BINARY":
                case "VARBINARY":
                    return typeof(byte[]);
                // Calcite's runtime representation of a UUID is a java.util.UUID, whose rep is OBJECT
                // like every class Avatica has no name of its own for, so the rep cannot say what this
                // is and the SQL type name has to
                case "UUID":
                    return typeof(Guid);
                case "TINYINT UNSIGNED":
                case "UTINYINT":
                    return typeof(byte);
                case "SMALLINT UNSIGNED":
                case "USMALLINT":
                    return typeof(ushort);
                case "INTEGER UNSIGNED":
                case "UINTEGER":
                    return typeof(uint);
                case "BIGINT UNSIGNED":
                case "UBIGINT":
                    return typeof(ulong);
            }

            return MapClrType(type.rep);
        }

        /// <summary>
        /// Maps a column representation to its corresponding Common Language Runtime (CLR) type.
        /// </summary>
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

        readonly IClrPrepare.Signature _signature;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        public CalciteResultColumns(IClrPrepare.Signature signature)
        {
            _signature = signature ?? throw new ArgumentNullException(nameof(signature));
        }

        /// <summary>
        /// Gets the count of columns in the result set.
        /// </summary>
        public int Count => _signature.Columns.size();

        /// <summary>
        /// Gets the column at the specified index.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        ColumnMetaData GetColumn(int index)
        {
            return (ColumnMetaData)_signature.Columns.get(index);
        }

        /// <summary>
        /// Gets the name of the column: the label — what an <c>AS</c> alias names the result column.
        /// </summary>
        /// <remarks>
        /// This is JDBC's <c>getColumnLabel</c> and it is what ADO.NET's <c>GetName</c> means. The
        /// origin <c>columnName</c> is the wrong answer: two projections of one table column under
        /// different aliases share it, so the result schema reports a duplicate name and a consumer
        /// keying by it fails. The label is always set — the metadata is built one column per field
        /// of the validated row type, and the label is that field's name.
        /// </remarks>
        /// <param name="index"></param>
        /// <returns></returns>
        public string GetName(int index)
        {
            return GetColumn(index).label;
        }

        /// <summary>
        /// Gets the <see cref="Type"/> of the column.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public Type GetClrType(int index)
        {
            return MapClrType(GetColumn(index).type);
        }

        /// <summary>
        /// Gets the type name of the column.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public string GetProviderTypeName(int index)
        {
            return GetColumn(index).type.name;
        }

        /// <summary>
        /// Gets the Calcite <see cref="RelDataType"/> of the column.
        /// </summary>
        /// <remarks>
        /// The whole type rather than its <see cref="SqlTypeName"/>, because reading a value needs more
        /// than the name: a <c>DATE</c> is a count of days and an <c>ARRAY</c> of them is a list of
        /// counts, so the component, key, value and field types are what say how to read one. Avatica's
        /// <see cref="ColumnMetaData"/> does not carry them.
        /// </remarks>
        /// <param name="index"></param>
        /// <returns></returns>
        public RelDataType GetRelType(int index)
        {
            var rowType = _signature.RowType ?? throw new InvalidOperationException($"{_signature.Sql ?? "The statement"} has no row type.");
            var field = (RelDataTypeField)rowType.getFieldList().get(index);
            return field.getType();
        }

        /// <summary>
        /// Gets whether or not the column is nullable.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public bool GetIsNullable(int index)
        {
            return GetColumn(index).nullable != 0;
        }

    }

}
