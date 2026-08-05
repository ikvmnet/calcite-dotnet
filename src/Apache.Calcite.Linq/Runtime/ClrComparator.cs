using System;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// A <see cref="java.util.Comparator"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="comparison"></param>
    /// <remarks>
    /// <c>PhysType</c> yields a comparator by two routes. For one collation it calls a method that returns
    /// one; for several it declares an anonymous class, which an expression tree cannot do and which becomes
    /// the lambda its compare method already was. Both have to arrive at an operator as the same thing, so the
    /// lambda is wrapped rather than the operator overloaded.
    /// </remarks>
    sealed class ClrComparator<T>(Func<T, T, int> comparison) : java.util.Comparator
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

        // C# does not inherit the defaults of an interface IKVM compiled, so each is forwarded

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
