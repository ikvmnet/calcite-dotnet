using System;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Reading a <c>java.util.List</c> of boxed values from C#.
    /// </summary>
    /// <remarks>
    /// Calcite hands out lists of field ordinals — <c>ImmutableBitSet.asList</c>, <c>JoinInfo.leftKeys</c>,
    /// <c>RelCollation.getFieldCollations</c> — and writes <c>for (int index : integers)</c> over them, because
    /// Java unboxes on the way out and its generics survive to the source Janino compiles. Neither is true
    /// here: IKVM erases the element type to <see cref="object"/> and nothing unboxes, so every read is a cast
    /// and a call. That is a difference of language rather than of what the code means, and it belongs with
    /// the rest of the interop.
    /// </remarks>
    static class JavaLists
    {

        /// <summary>
        /// Returns the value of one element of a list of <c>java.lang.Integer</c>.
        /// </summary>
        /// <param name="list"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static int Int(java.util.List list, int index)
        {
            ArgumentNullException.ThrowIfNull(list);

            return ((java.lang.Integer)list.get(index)).intValue();
        }

        /// <summary>
        /// Returns whether a list of <c>java.lang.Integer</c> holds the given value.
        /// </summary>
        /// <param name="list"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// Boxed to ask, because <c>List.contains</c> takes an object and compares by <c>equals</c>.
        /// </remarks>
        public static bool ContainsInt(java.util.List list, int value)
        {
            ArgumentNullException.ThrowIfNull(list);

            return list.contains(java.lang.Integer.valueOf(value));
        }

        /// <summary>
        /// Returns the value of one element of a list of <c>java.lang.Boolean</c>.
        /// </summary>
        /// <param name="list"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static bool Bool(java.util.List list, int index)
        {
            ArgumentNullException.ThrowIfNull(list);

            return ((java.lang.Boolean)list.get(index)).booleanValue();
        }

    }

}
