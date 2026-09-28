using System;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Reads elements of a <c>java.util.List</c> of boxed primitives as CLR values.
    /// </summary>
    /// <remarks>
    /// Calcite returns lists of field ordinals, such as <c>ImmutableBitSet.asList</c> and
    /// <c>JoinInfo.leftKeys</c>, and Java code unboxes their elements implicitly. Through IKVM the element
    /// type is <see cref="object"/>, so each read needs a cast and an unboxing call.
    /// </remarks>
    static class JavaLists
    {

        /// <summary>
        /// Returns the value of one element of a list of <c>java.lang.Integer</c>.
        /// </summary>
        /// <param name="list">A list whose elements are <c>java.lang.Integer</c>.</param>
        /// <param name="index">The element's index.</param>
        /// <returns>The element's <see cref="int"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="list"/> is <see langword="null"/>.</exception>
        public static int Int(java.util.List list, int index)
        {
            ArgumentNullException.ThrowIfNull(list);

            return ((java.lang.Integer)list.get(index)).intValue();
        }

        /// <summary>
        /// Returns whether a list of <c>java.lang.Integer</c> holds the given value.
        /// </summary>
        /// <remarks>
        /// Boxes <paramref name="value"/>, because <c>List.contains</c> compares by <c>equals</c>.
        /// </remarks>
        /// <param name="list">A list whose elements are <c>java.lang.Integer</c>.</param>
        /// <param name="value">The value to look for.</param>
        /// <returns><see langword="true"/> if an element equals <paramref name="value"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="list"/> is <see langword="null"/>.</exception>
        public static bool ContainsInt(java.util.List list, int value)
        {
            ArgumentNullException.ThrowIfNull(list);

            return list.contains(java.lang.Integer.valueOf(value));
        }

        /// <summary>
        /// Returns the value of one element of a list of <c>java.lang.Boolean</c>.
        /// </summary>
        /// <param name="list">A list whose elements are <c>java.lang.Boolean</c>.</param>
        /// <param name="index">The element's index.</param>
        /// <returns>The element's <see cref="bool"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="list"/> is <see langword="null"/>.</exception>
        public static bool Bool(java.util.List list, int index)
        {
            ArgumentNullException.ThrowIfNull(list);

            return ((java.lang.Boolean)list.get(index)).booleanValue();
        }

    }

}
