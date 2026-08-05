using System;

using Apache.Calcite.Linq.Tree;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// Reads a value arriving as an object at the type an adapter was made for.
    /// </summary>
    /// <remarks>
    /// An adapter implements one of Calcite's functional interfaces, whose arguments are erased to
    /// <see cref="object"/>, and calls a delegate that is typed. Where that type is a primitive the value
    /// arriving is a <c>java.lang.Integer</c> rather than a boxed CLR int, and casting one to the other fails.
    /// The same unboxing every conversion in this port does is what is wanted.
    /// </remarks>
    static class JavaValues
    {

        /// <summary>
        /// Returns a value as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T As<T>(object value)
        {
            if (value is T typed)
                return typed;

            if (value != null && typeof(T).IsValueType)
                return (T)JavaCast.Unwrap(value, typeof(T));

            return (T)value!;
        }

        /// <summary>
        /// Returns a value as the object Calcite expects to receive.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// The other direction, and the one that matters more: handing back a boxed CLR int where the type
        /// factory says java.lang.Integer leaves two representations of one value loose in a plan, and whatever
        /// compares them fails.
        /// </remarks>
        public static object From<T>(T value)
        {
            if (value == null)
                return null!;

            return typeof(T).IsValueType ? JavaCast.Box(value, typeof(T)) : value;
        }

    }

}
