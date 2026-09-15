using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A <c>ROW</c>, mapped by mapping each field and wrapping the result in an <c>object[]</c>.
    /// </summary>
    /// <remarks>
    /// A row stays <c>object[]</c> however alike its fields happen to be: <c>ROW(1, 2)</c> is two fields and
    /// not an array of two, and unifying its element type would say otherwise. Each field is resolved
    /// through the registry, so a field that is itself a row, a collection or a caller's own type needs
    /// nothing here.
    /// </remarks>
    public sealed class RowClrTypeMapping : ClrTypeMapping
    {

        /// <summary>
        /// Resolves one mapping per field through the registry, which is the recursion.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>ROW</c> type.</param>
        /// <returns>The mappings, in the order the row carries its fields.</returns>
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
        /// <param name="context"></param>
        /// <param name="relType">The <c>ROW</c> type.</param>
        public RowClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            base(context, relType, typeof(object[]))
        {
            _fields = Fields(context, relType);
        }

        /// <summary>
        /// Gets the mapping each field is carried across by, in the order the row carries them.
        /// </summary>
        public IReadOnlyList<ClrTypeMapping> FieldMappings => _fields;

        /// <inheritdoc />
        /// <remarks>
        /// <b>The one place <c>getJavaClass</c> describes a different form from the one that arrives here.</b>
        /// Asked about a <c>ROW</c> it answers <c>createSyntheticType</c>'s generated class — a
        /// <c>Record2_0_1</c> with a field per column — because that is what a row is <em>inside</em> a plan,
        /// where a physical type has been chosen and the fields are read by name. What crosses this boundary
        /// is an <c>Object[]</c>: measured, for a <c>ROW</c> column and for a row inside a collection alike.
        /// Taking the factory's answer here would refuse every row that is actually produced.
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
        /// Returns the mapping for a position, or throws where the value has more of them than the type
        /// declares.
        /// </summary>
        ClrTypeMapping Mapping(int i)
        {
            return i < _fields.Length ? _fields[i] : throw new ClrTypeMappingException($"A {RelType} declares {_fields.Length} fields and a value carried more.");
        }

    }

}
