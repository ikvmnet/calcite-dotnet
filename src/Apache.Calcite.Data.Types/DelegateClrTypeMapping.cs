using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Types
{

    /// <summary>
    /// A mapping whose two conversions are delegates.
    /// </summary>
    /// <remarks>
    /// What the built-in table is made of, and enough for most of what a caller adds. Each conversion is
    /// handed the Calcite type as well as the value, because the table is written against a
    /// <c>SqlTypeName</c> and a conversion may need the facets — and because that is what lets one pair of
    /// functions serve every entry. A mapping that needs to hold state — a converter that has resolved
    /// something once and keeps it — derives from <see cref="ClrTypeMapping"/> instead.
    /// </remarks>
    public sealed class DelegateClrTypeMapping : ClrTypeMapping
    {

        readonly Func<object, RelDataType, object?> _toCalcite;
        readonly Func<object, RelDataType, object?> _fromCalcite;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType"></param>
        /// <param name="clrType"></param>
        /// <param name="toCalcite"></param>
        /// <param name="fromCalcite"></param>
        public DelegateClrTypeMapping(ClrTypeContext context, RelDataType relType, Type clrType, Func<object, RelDataType, object?> toCalcite, Func<object, RelDataType, object?> fromCalcite) :
            base(context, relType, clrType)
        {
            _toCalcite = toCalcite ?? throw new ArgumentNullException(nameof(toCalcite));
            _fromCalcite = fromCalcite ?? throw new ArgumentNullException(nameof(fromCalcite));
        }

        /// <inheritdoc />
        public override object? ToCalcite(object value) => _toCalcite(value, RelType);

        /// <inheritdoc />
        public override object? FromCalcite(object value) => _fromCalcite(value, RelType);

    }

}
