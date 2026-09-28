using System;

using org.apache.calcite.plan;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A <see cref="RelRule.OperandTransform"/> backed by a delegate.
    /// </summary>
    /// <param name="transform">Implements <c>apply</c>.</param>
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
        /// C# does not inherit the default methods of an interface IKVM compiled, so this forwards to
        /// <see cref="java.util.function.Function"/>'s default.
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
