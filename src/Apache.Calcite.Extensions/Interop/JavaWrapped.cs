using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// A value whose equality and hash are an <see cref="EqualityComparer"/>'s, so that it can be a key of a
    /// Java collection that knows nothing about the comparer.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableDefaults.Wrapped</c>, which is package private. A set operator holds
    /// its rows in a Java collection, because the order it yields them in is that collection's and has to be
    /// the one Calcite yields; a comparer therefore has to reach the collection as the rows' own
    /// <c>hashCode</c> and <c>equals</c>, which is what this is for.
    /// </remarks>
    /// <param name="comparer"></param>
    /// <param name="element"></param>
    sealed class JavaWrapped(EqualityComparer comparer, object? element) : java.lang.Object
    {

        /// <summary>
        /// Returns the value wrapped, or the value itself where there is no comparer to wrap it for.
        /// </summary>
        /// <param name="comparer"></param>
        /// <param name="element"></param>
        /// <returns></returns>
        public static object? Of(EqualityComparer? comparer, object? element)
        {
            return comparer == null ? element : new JavaWrapped(comparer, element);
        }

        /// <summary>
        /// Returns the value a wrapper holds, or the value itself where it is not one.
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
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
