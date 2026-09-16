using System;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Apache.Calcite.Data.Common;
using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads a prepared <see cref="IClrPrepare.Signature"/>'s columns as an ADO.NET caller expects them.
    /// </summary>
    /// <remarks>
    /// The columns are Avatica's <see cref="ColumnMetaData"/>, which is what the metadata port produces, and
    /// it answers the naming questions — the label, the SQL type name, the precision. What a column
    /// <em>is</em> comes from its <see cref="RelDataType"/> through the registry instead, because Avatica's
    /// type does not carry a component's nullability and the value accessors read the <see cref="RelDataType"/>:
    /// a table that answered one and a reader that answered the other is two answers to one question.
    /// </remarks>
    internal readonly struct CalciteResultColumns
    {

        readonly IClrPrepare.Signature _signature;
        readonly ClrTypeRegistry _registry;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="registry">The mappings values are read through.</param>
        public CalciteResultColumns(IClrPrepare.Signature signature, ClrTypeRegistry registry)
        {
            _signature = signature ?? throw new ArgumentNullException(nameof(signature));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        }

        /// <summary>
        /// Gets the mappings values of these columns are read through.
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
        /// Gets the <see cref="Type"/> of the column.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// The registry, and the column's own <see cref="RelDataType"/> rather than Avatica's
        /// <see cref="ColumnMetaData"/>, because those two do not agree and the value accessors read the
        /// first: Avatica's array type carries the component's <em>rep</em> and not its nullability, so an
        /// <c>INTEGER ARRAY</c> whose elements may be null reported <c>int[]</c> while the value came back
        /// as <c>int?[]</c>. A caller doing <c>GetFieldValue&lt;T&gt;</c> with what this answers is the
        /// documented way to read a column, so the two have to be one answer.
        /// </remarks>
        public Type GetClrType(int index)
        {
            // and it refuses rather than guessing. object is an answer three Calcite types genuinely give
            // — ANY carries no type, OTHER is a class with no SQL name, a VARIANT is any type at all, and
            // each of their mappings says object — so answering object for a type nothing maps says the
            // same thing about a column the provider cannot read at all, and a caller reading GetFieldType
            // to decide what to ask for would be told to ask for object and then refused
            return _registry.RequireMapping(null, GetRelType(index)).ClrType;
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
