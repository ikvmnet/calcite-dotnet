using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A mapping whose two conversions are delegates.
    /// </summary>
    /// <remarks>
    /// What the built-in table is made of, and enough for most of what a caller adds. A mapping that needs
    /// to hold state — a converter that has resolved something once and keeps it — derives from
    /// <see cref="ClrTypeMapping"/> instead.
    /// </remarks>
    public sealed class DelegateClrTypeMapping : ClrTypeMapping
    {

        readonly Func<object, object?> _toCalcite;
        readonly Func<object, object?> _fromCalcite;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType"></param>
        /// <param name="clrType"></param>
        /// <param name="toCalcite"></param>
        /// <param name="fromCalcite"></param>
        public DelegateClrTypeMapping(ClrTypeContext context, RelDataType relType, Type clrType, Func<object, object?> toCalcite, Func<object, object?> fromCalcite) :
            base(context, relType, clrType)
        {
            _toCalcite = toCalcite ?? throw new ArgumentNullException(nameof(toCalcite));
            _fromCalcite = fromCalcite ?? throw new ArgumentNullException(nameof(fromCalcite));
        }

        /// <inheritdoc />
        public override object? ToCalcite(object value) => _toCalcite(value);

        /// <inheritdoc />
        public override object? FromCalcite(object value) => _fromCalcite(value);

    }

}
