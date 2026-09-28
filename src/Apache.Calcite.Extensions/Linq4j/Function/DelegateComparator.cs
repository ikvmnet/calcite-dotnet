using System;

using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A <see cref="java.util.Comparator"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T">The type compared.</typeparam>
    /// <param name="comparison">The comparison.</param>
    /// <remarks>
    /// For a multi-field collation <c>PhysType</c> generates an anonymous <c>Comparator</c> class, which is
    /// translated to a lambda and wrapped in this so that operators receive a <c>Comparator</c> either way.
    /// Arguments are converted with <see cref="JavaValues.As{T}"/>.
    /// </remarks>
    sealed class DelegateComparator<T>(Func<T, T, int> comparison) : java.util.Comparator
    {

        readonly Func<T, T, int> comparison = comparison ?? throw new ArgumentNullException(nameof(comparison));

        /// <inheritdoc />
        public int compare(object x, object y)
        {
            return comparison(JavaValues.As<T>(x), JavaValues.As<T>(y));
        }

        /// <inheritdoc />
        public bool equals(object obj)
        {
            return ReferenceEquals(this, obj);
        }

        // C# does not inherit the default methods of an interface IKVM compiled, so each is forwarded

        /// <inheritdoc />
        public java.util.Comparator reversed() => java.util.Comparator.__DefaultMethods.reversed(this);

        /// <inheritdoc />
        public java.util.Comparator thenComparing(java.util.Comparator other) => java.util.Comparator.__DefaultMethods.thenComparing(this, other);

        /// <inheritdoc />
        public java.util.Comparator thenComparing(java.util.function.Function keyExtractor) => java.util.Comparator.__DefaultMethods.thenComparing(this, keyExtractor);

        /// <inheritdoc />
        public java.util.Comparator thenComparing(java.util.function.Function keyExtractor, java.util.Comparator keyComparator) => java.util.Comparator.__DefaultMethods.thenComparing(this, keyExtractor, keyComparator);

        /// <inheritdoc />
        public java.util.Comparator thenComparingDouble(java.util.function.ToDoubleFunction keyExtractor) => java.util.Comparator.__DefaultMethods.thenComparingDouble(this, keyExtractor);

        /// <inheritdoc />
        public java.util.Comparator thenComparingInt(java.util.function.ToIntFunction keyExtractor) => java.util.Comparator.__DefaultMethods.thenComparingInt(this, keyExtractor);

        /// <inheritdoc />
        public java.util.Comparator thenComparingLong(java.util.function.ToLongFunction keyExtractor) => java.util.Comparator.__DefaultMethods.thenComparingLong(this, keyExtractor);

    }

}
