using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// A value whose Java <c>equals</c> and <c>hashCode</c> are those of an <see cref="EqualityComparer"/>,
    /// so that it can be a key of a Java collection.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's package-private <c>EnumerableDefaults.Wrapped</c>. The set operators
    /// hold their rows in Java collections so that rows come out in the order Calcite's do, and a custom
    /// comparer reaches such a collection only through the keys' own <c>equals</c> and <c>hashCode</c>.
    /// </remarks>
    /// <param name="comparer">The comparer that defines equality.</param>
    /// <param name="element">The value wrapped.</param>
    sealed class JavaWrapped(EqualityComparer comparer, object? element) : java.lang.Object
    {

        /// <summary>
        /// Returns <paramref name="element"/> wrapped, or unwrapped when <paramref name="comparer"/> is
        /// <see langword="null"/>.
        /// </summary>
        /// <param name="comparer">The comparer that decides the wrapper's equality and hash code, or <see langword="null"/>.</param>
        /// <param name="element">The value to wrap.</param>
        /// <returns>A <see cref="JavaWrapped"/> over <paramref name="element"/>, or <paramref name="element"/> itself.</returns>
        public static object? Of(EqualityComparer? comparer, object? element)
        {
            return comparer == null ? element : new JavaWrapped(comparer, element);
        }

        /// <summary>
        /// Returns the value a wrapper holds, or <paramref name="element"/> itself if it is not a wrapper.
        /// </summary>
        /// <param name="element">A value that may be a <see cref="JavaWrapped"/>.</param>
        /// <returns>The wrapped value, or <paramref name="element"/> if it is not a wrapper.</returns>
        public static object? Unwrap(object? element)
        {
            return element is JavaWrapped wrapped ? wrapped.Element : element;
        }

        /// <summary>
        /// Gets the value wrapped.
        /// </summary>
        public object? Element => element;

        /// <inheritdoc />
        public override int hashCode()
        {
            return comparer.hashCode(element);
        }

        /// <inheritdoc />
        public override bool equals(object? obj)
        {
            return ReferenceEquals(this, obj) || (obj is JavaWrapped other && comparer.equal(element, other.Element));
        }

    }

}
