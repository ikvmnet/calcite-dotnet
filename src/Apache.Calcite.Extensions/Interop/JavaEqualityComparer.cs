using System;
using System.Collections.Generic;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// An <see cref="IEqualityComparer{T}"/> over a linq4j <see cref="EqualityComparer"/>.
    /// </summary>
    /// <typeparam name="T">The type compared.</typeparam>
    /// <param name="comparer">The linq4j comparer.</param>
    /// <remarks>
    /// Used where an operator needs the comparer <c>PhysType.comparer</c> returns, which is how rows of
    /// <c>JavaRowFormat.ARRAY</c>, whose own equality is by reference, are compared by value. Two nulls are
    /// equal and a null is unequal to any other value, without consulting the comparer.
    /// </remarks>
    sealed class JavaEqualityComparer<T>(EqualityComparer comparer) : IEqualityComparer<T>
    {

        readonly EqualityComparer comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));

        /// <inheritdoc />
        public bool Equals(T? x, T? y)
        {
            if (x == null || y == null)
                return ReferenceEquals(x, y);

            return comparer.equal(x, y);
        }

        /// <inheritdoc />
        public int GetHashCode(T value)
        {
            return value == null ? 0 : comparer.hashCode(value);
        }

        /// <summary>
        /// Returns a comparer over <paramref name="comparer"/>, or <see cref="EqualityComparer{T}.Default"/>
        /// when it is <see langword="null"/>.
        /// </summary>
        /// <param name="comparer">The linq4j comparer, or <see langword="null"/> for default equality.</param>
        /// <returns>A CLR comparer that answers equality and hash codes through <paramref name="comparer"/>.</returns>
        public static IEqualityComparer<T> Of(EqualityComparer? comparer)
        {
            return comparer == null ? EqualityComparer<T>.Default : new JavaEqualityComparer<T>(comparer);
        }

    }

}
