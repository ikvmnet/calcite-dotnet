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
    /// The columns of a prepared <see cref="IClrPrepare.Signature"/>, described as an ADO.NET caller sees them.
    /// </summary>
    /// <remarks>
    /// Names, SQL type names and nullability come from Avatica's <see cref="ColumnMetaData"/>. The .NET type
    /// comes from the column's <see cref="RelDataType"/> through the registry instead, because that is what
    /// the value accessors read, and Avatica's type does not carry a collection element's nullability: an
    /// <c>INTEGER ARRAY</c> with nullable elements reads back as <c>int?[]</c>, which only the
    /// <see cref="RelDataType"/> says.
    /// </remarks>
    internal readonly struct CalciteResultColumns
    {

        readonly IClrPrepare.Signature _signature;
        readonly ClrTypeRegistry _registry;

        /// <summary>
        /// Each column's Calcite type and mapping, resolved on first use and cached for the life of the result.
        /// </summary>
        /// <remarks>
        /// Arrays, because this struct is copied wherever it is passed and the copies must share the cache.
        /// </remarks>
        readonly RelDataType?[] _relTypes;
        readonly ClrTypeMapping?[] _mappings;
        readonly bool[] _resolved;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature">The prepared statement whose columns these are.</param>
        /// <param name="registry">The mappings values are read through.</param>
        public CalciteResultColumns(IClrPrepare.Signature signature, ClrTypeRegistry registry)
        {
            _signature = signature ?? throw new ArgumentNullException(nameof(signature));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));

            var count = signature.Columns.size();
            _relTypes = new RelDataType?[count];
            _mappings = new ClrTypeMapping?[count];
            _resolved = new bool[count];
        }

        /// <summary>
        /// Gets the mappings values of these columns are read through.
        /// </summary>
        public ClrTypeRegistry Registry => _registry;

        /// <summary>
        /// Gets the number of columns.
        /// </summary>
        public int Count => _signature.Columns.size();

        /// <summary>
        /// Gets the Avatica metadata of a column.
        /// </summary>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The column's entry in the statement signature.</returns>
        ColumnMetaData GetColumn(int index)
        {
            return (ColumnMetaData)_signature.Columns.get(index);
        }

        /// <summary>
        /// Gets the name of a column: its label, which is the <c>AS</c> alias where there is one.
        /// </summary>
        /// <remarks>
        /// The label is JDBC's <c>getColumnLabel</c> and is what ADO.NET's <c>GetName</c> means. The origin
        /// column name would give two aliased projections of one table column the same name. The label is
        /// always set, from the field name of the validated row type.
        /// </remarks>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The column name.</returns>
        public string GetName(int index)
        {
            return GetColumn(index).label;
        }

        /// <summary>
        /// Gets the .NET type a column's values read back as by default.
        /// </summary>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The type.</returns>
        /// <exception cref="ClrTypeMappingException">No mapping covers the column's type.</exception>
        /// <remarks>
        /// Answers from the column's <see cref="RelDataType"/>, as the value accessors do, so the type
        /// reported here is the type <c>GetValue</c> returns and <c>GetFieldValue&lt;T&gt;</c> accepts.
        /// </remarks>
        public Type GetClrType(int index)
        {
            // refuses rather than answering object: object is the real answer for ANY, OTHER and VARIANT,
            // and giving it for a type nothing maps would tell a caller to ask for object and then refuse it
            return _registry.RequireMapping(null, GetRelType(index)).ClrType;
        }

        /// <summary>
        /// Gets the SQL type name of a column.
        /// </summary>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The type name.</returns>
        public string GetProviderTypeName(int index)
        {
            return GetColumn(index).type.name;
        }

        /// <summary>
        /// Gets the Calcite <see cref="RelDataType"/> of a column.
        /// </summary>
        /// <remarks>
        /// The full type rather than its <see cref="SqlTypeName"/>, because reading a value needs the
        /// component, key, value and field types, which Avatica's <see cref="ColumnMetaData"/> does not carry.
        /// </remarks>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The type.</returns>
        /// <exception cref="InvalidOperationException">The statement has no row type.</exception>
        public RelDataType GetRelType(int index)
        {
            return _relTypes[index] ??= ResolveRelType(index);
        }

        /// <summary>
        /// Reads a column's Calcite type off the signature's row type.
        /// </summary>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The type of the row type's field at that ordinal.</returns>
        RelDataType ResolveRelType(int index)
        {
            var rowType = _signature.RowType ?? throw new InvalidOperationException($"{_signature.Sql ?? "The statement"} has no row type.");
            var field = (RelDataTypeField)rowType.getFieldList().get(index);
            return field.getType();
        }

        /// <summary>
        /// Gets the mapping a column's values are read back through, or <see langword="null"/> where the
        /// registry has none for its type.
        /// </summary>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns>The mapping, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Resolved once per column and cached, including a <see langword="null"/> answer, because a registry
        /// lookup costs several times the conversion it selects.
        /// </remarks>
        public ClrTypeMapping? GetMapping(int index)
        {
            if (_resolved[index])
                return _mappings[index];

            _mappings[index] = _registry.GetMapping(null, GetRelType(index));
            _resolved[index] = true;
            return _mappings[index];
        }

        /// <summary>
        /// Gets whether a column may hold nulls. A column whose nullability is unknown counts as nullable.
        /// </summary>
        /// <param name="index">The zero-based column ordinal.</param>
        /// <returns><see langword="true"/> where the column may hold nulls.</returns>
        public bool GetIsNullable(int index)
        {
            return GetColumn(index).nullable != 0;
        }

    }

}
