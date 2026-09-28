using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A mapping whose two conversions are delegates.
    /// </summary>
    /// <remarks>
    /// Most of the built-in mappings are of this kind. A mapping that needs state of its own, such as a
    /// resolved element mapping, derives from <see cref="ClrTypeMapping"/> instead.
    /// </remarks>
    public sealed class DelegateClrTypeMapping : ClrTypeMapping
    {

        readonly Func<object, object?> _toCalcite;
        readonly Func<object, object?> _fromCalcite;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The Calcite type the mapping is for.</param>
        /// <param name="clrType">The CLR type the mapping presents it as.</param>
        /// <param name="toCalcite">Converts a non-null CLR value to the class Calcite holds the type in.</param>
        /// <param name="fromCalcite">Converts a non-null value of that class to <paramref name="clrType"/>.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
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
