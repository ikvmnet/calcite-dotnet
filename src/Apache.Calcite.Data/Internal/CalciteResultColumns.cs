using System;

using Apache.Calcite.Data.Common;
using Apache.Calcite.Extensions.Prepare;

using org.apache.calcite.avatica;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads a prepared <see cref="IClrPrepare.Signature"/>'s columns as an ADO.NET caller expects them.
    /// </summary>
    /// <remarks>
    /// Which CLR type a column is seen as comes from the connection's <see cref="ClrTypeRegistry"/>, so that
    /// the answer here and the conversion a value goes through are the same decision. The signature's row
    /// type is what the registry is asked about rather than the Avatica <see cref="ColumnMetaData"/>: a
    /// <c>ColumnMetaData.Rep</c> is Avatica's summary of the storage form and drops the facets, and a
    /// registry entry may be written for a type the summary cannot express. A signature carries a row type
    /// wherever it carries columns — the one that does not is DDL, whose column list is empty.
    /// </remarks>
    internal readonly struct CalciteResultColumns
    {

        readonly IClrPrepare.Signature _signature;
        readonly ClrTypeRegistry _registry;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="registry"></param>
        /// <param name="signature"></param>
        public CalciteResultColumns(ClrTypeRegistry registry, IClrPrepare.Signature signature)
        {
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
            _signature = signature ?? throw new ArgumentNullException(nameof(signature));
        }

        /// <summary>
        /// Gets the type mapping this result is read through.
        /// </summary>
        public ClrTypeRegistry Registry => _registry;

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
        /// Gets the Calcite type of the column.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public RelDataType GetRelType(int index)
        {
            var rowType = _signature.RowType ?? throw new InvalidOperationException($"{_signature.Sql ?? "The statement"} has no row type.");
            return ((RelDataTypeField)rowType.getFieldList().get(index)).getType();
        }

        /// <summary>
        /// Gets the <see cref="Type"/> of the column.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public Type GetClrType(int index)
        {
            return _registry.GetClrType(GetRelType(index));
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
        /// Gets the Calcite <see cref="SqlTypeName"/> enum value for the column.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public SqlTypeName GetSqlType(int index)
        {
            return GetRelType(index).getSqlTypeName();
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
