using System;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Interop
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
                return (T)JavaValues.Unwrap(value, typeof(T));

            // the other way round, and the case a table of this runtime makes: its rows hold a CLR int where
            // the plan's row type is java.lang.Integer, and a cast between those two is not a conversion any
            // more than the one above is
            if (value != null && value.GetType().IsValueType && ClrPrimitive.PrimitiveClass(typeof(T)) is Type primitive)
                return (T)JavaValues.Box(value, primitive);

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

            // the value's own type, not typeof(T). A boundary is crossed by a value, and the static type
            // parameter at these sites is nearly always object or a PhysType.RowType -- and a RowType is
            // ClrPrimitive.Box(...), a Java class -- so testing typeof(T) compiled the guard away at exactly
            // the sites that had something to guard. Box returns what it was given where the type is not one
            // of linq4j's primitives, so a struct of our own still passes through untouched.
            var type = value.GetType();

            return type.IsValueType ? JavaValues.Box(value, type) : value;
        }



        /// <summary>
        /// Returns <paramref name="value"/> as the CLR primitive <paramref name="type"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// A linq4j constant holds its value boxed even where its type is a primitive, because Java has
        /// nowhere else to put it. The unboxing that a tree would do at runtime is done here instead, once.
        /// </remarks>
        public static object Unwrap(object value, Type type)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(type);

            if (J.Primitive.of((java.lang.Class)type) is not J.Primitive primitive)
                throw new NotSupportedException($"'{type}' is not a Java primitive.");

            var name = primitive.primitiveName + "Value";
            var method = value.GetType().GetMethod(name, BindingFlags.Public | BindingFlags.Instance, null, [], null)
                ?? throw new NotSupportedException($"'{value.GetType()}' has no {name}().");

            return method.Invoke(value, [])
                ?? throw new NotSupportedException($"{name}() on '{value.GetType()}' gave nothing.");
        }

        /// <summary>
        /// Returns <paramref name="value"/> boxed as Java boxes it.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// The counterpart of <see cref="Unwrap"/>, and needed for the same reason: a value handed back to
        /// Calcite as an object has to be a java.lang.Integer rather than a boxed CLR int, because that is what
        /// the type factory says the value is and what everything reading it expects.
        /// </remarks>
        public static object Box(object value, Type type)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(type);

            if (J.Primitive.of((java.lang.Class)type) is not J.Primitive primitive)
                return value;

            var box = ClrTypes.FromClass(primitive.boxClass);
            var valueOf = box.GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [type], null)
                ?? throw new NotSupportedException($"'{box}' has no valueOf for '{type}'.");

            return valueOf.Invoke(null, [value])
                ?? throw new NotSupportedException($"valueOf on '{box}' gave nothing.");
        }
    }

}
