using System;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A <see cref="java.util.function.Predicate"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="predicate"></param>
    /// <remarks>
    /// What a MATCH_RECOGNIZE pattern definition becomes. <c>Matcher.Builder.add</c> takes one of these per
    /// symbol, so the condition is compiled as a delegate and handed over as the interface Calcite's matcher
    /// asks for.
    /// </remarks>
    sealed class DelegatePredicate<T>(Func<T, bool> predicate) : java.util.function.Predicate
    {

        readonly Func<T, bool> predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

        /// <inheritdoc />
        public bool test(object value) => predicate(JavaValues.As<T>(value));

        // C# does not inherit the defaults of an interface IKVM compiled, so each is forwarded

        /// <inheritdoc />
        public java.util.function.Predicate and(java.util.function.Predicate other) => java.util.function.Predicate.__DefaultMethods.and(this, other);

        /// <inheritdoc />
        public java.util.function.Predicate negate() => java.util.function.Predicate.__DefaultMethods.negate(this);

        /// <inheritdoc />
        public java.util.function.Predicate or(java.util.function.Predicate other) => java.util.function.Predicate.__DefaultMethods.or(this, other);

    }

}
