using System;
using System.Collections.Generic;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// An <see cref="IEqualityComparer{T}"/> over a linq4j <see cref="EqualityComparer"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="comparer"></param>
    /// <remarks>
    /// A row of <c>JavaRowFormat.ARRAY</c> is an array, whose own equality is by reference, so a set operation
    /// over one needs to be told how to compare. <c>PhysType.comparer</c> is what says so, and what it returns
    /// is Calcite's own comparer for that format.
    /// </remarks>
    public sealed class JavaEqualityComparer<T>(EqualityComparer comparer) : IEqualityComparer<T>
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
        /// Returns a comparer over <paramref name="comparer"/>, or the default one when there is none.
        /// </summary>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static IEqualityComparer<T> Of(EqualityComparer? comparer)
        {
            return comparer == null ? EqualityComparer<T>.Default : new JavaEqualityComparer<T>(comparer);
        }

    }

}
