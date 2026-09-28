using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The mapping for a <c>ROW</c> (a struct type), which presents it as an <c>object[]</c> with one element
    /// per field, each converted with its field type's mapping.
    /// </summary>
    /// <remarks>
    /// A row is an <c>object[]</c> even where its fields share a type: <c>ROW(1, 2)</c> is two fields, not an
    /// array of two integers. Field mappings are resolved through <see cref="ClrTypeContext.Registry"/>, so a
    /// field that is itself a row, a collection or a type added by a caller's resolver is handled the same way.
    /// </remarks>
    public sealed class RowClrTypeMapping : ClrTypeMapping
    {

        /// <summary>
        /// Resolves each field type's default mapping, in field order.
        /// </summary>
        /// <exception cref="ClrTypeMappingException">A field type has no mapping.</exception>
        /// <param name="context">The context whose registry resolves the field mappings.</param>
        /// <param name="relType">The <c>ROW</c> type.</param>
        /// <returns>One mapping per field, in field order.</returns>
        static ClrTypeMapping[] Fields(ClrTypeContext context, RelDataType relType)
        {
            var list = relType.getFieldList();
            var fields = new ClrTypeMapping[list.size()];
            for (var i = 0; i < fields.Length; i++)
                fields[i] = context.Registry.RequireMapping(null, ((RelDataTypeField)list.get(i)).getType());

            return fields;
        }

        readonly ClrTypeMapping[] _fields;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>ROW</c> type.</param>
        /// <exception cref="ClrTypeMappingException">A field type has no mapping.</exception>
        public RowClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            base(context, relType, typeof(object[]))
        {
            _fields = Fields(context, relType);
        }

        /// <summary>
        /// Gets the mapping each field is converted with, in field order.
        /// </summary>
        public IReadOnlyList<ClrTypeMapping> FieldMappings => _fields;

        /// <inheritdoc />
        /// <remarks>
        /// Always <c>object[]</c>. For a <c>ROW</c>, <c>getJavaClass</c> answers the synthetic record class
        /// Calcite uses inside a plan, but a row value at this boundary, whether a column or inside a
        /// collection, is an <c>Object[]</c>.
        /// </remarks>
        public override Type RepresentationType => typeof(object[]);

        /// <inheritdoc />
        public override object? ToCalcite(object value)
        {
            if (value is not object[] source)
                throw new ClrTypeMappingException($"A {RelType} is written from an object[], and a {value.GetType()} is not one.");

            var row = new object?[source.Length];
            for (var i = 0; i < source.Length; i++)
                row[i] = source[i] is null ? null : Mapping(i).ToCalcite(source[i]!);

            return row;
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            if (value is not object[] source)
                throw new ClrTypeMappingException($"A {RelType} is held in an Object[], and a {value.GetType()} is not one.");

            var row = new object?[source.Length];
            for (var i = 0; i < source.Length; i++)
                row[i] = source[i] is null ? null : Mapping(i).FromCalcite(source[i]!);

            return row;
        }

        /// <summary>
        /// Returns the mapping for a field position, throwing where the value has more fields than the type
        /// declares.
        /// </summary>
        /// <param name="i">The zero-based field position.</param>
        /// <returns>The mapping for that field.</returns>
        ClrTypeMapping Mapping(int i)
        {
            return i < _fields.Length ? _fields[i] : throw new ClrTypeMappingException($"A {RelType} declares {_fields.Length} fields and a value carried more.");
        }

    }

}
