using System;

using org.apache.calcite.plan;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// A <see cref="RelRule.OperandTransform"/> backed by a delegate.
    /// </summary>
    /// <param name="transform"></param>
    class DelegateOperandTransform(Func<RelRule.OperandBuilder, RelRule.Done> transform) : RelRule.OperandTransform
    {

        readonly Func<RelRule.OperandBuilder, RelRule.Done> transform = transform ?? throw new ArgumentNullException(nameof(transform));

        /// <inheritdoc />
        public object apply(object builder)
        {
            return transform((RelRule.OperandBuilder)builder);
        }

        /// <inheritdoc />
        /// <remarks>
        /// C# does not inherit the defaults of an interface IKVM compiled, so composition is forwarded rather
        /// than left to <see cref="java.util.function.Function"/>.
        /// </remarks>
        public java.util.function.Function andThen(java.util.function.Function after)
        {
            return java.util.function.Function.__DefaultMethods.andThen(this, after);
        }

        /// <inheritdoc cref="andThen" />
        public java.util.function.Function compose(java.util.function.Function before)
        {
            return java.util.function.Function.__DefaultMethods.compose(this, before);
        }

    }

}
