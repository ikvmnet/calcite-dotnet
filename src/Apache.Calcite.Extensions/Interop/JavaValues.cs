using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
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
        public static T As<T>(object? value)
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
        ///
        /// <para>Null is the one value it passes through, so a caller that has already ruled null out keeps
        /// that knowledge across the call.</para>
        /// </remarks>
        [return: NotNullIfNotNull(nameof(value))]
        public static object? From<T>(T value)
        {
            if (value == null)
                return null;

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
        /// nowhere else to put it, and an adapter's argument arrives as Java boxed it. What reads it is the
        /// primitive's own accessor — <c>intValue()</c> for an <c>int</c> — called on the value, as javac's
        /// unboxing calls it: on any <c>java.lang.Number</c> for the six numeric primitives, and on the
        /// <c>Character</c> or <c>Boolean</c> for the other two.
        ///
        /// <para>A value that is none of those is asked by name for the accessor, which is what this did for
        /// every value once; the method found is kept per type, because this is on the path of every value
        /// an adapter reads.</para>
        /// </remarks>
        public static object Unwrap(object value, Type type)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(type);

            if (ClrPrimitive.Of(type) is not J.Primitive primitive)
                throw new NotSupportedException($"'{type}' is not a Java primitive.");

            if (value is java.lang.Number number)
            {
                if (type == typeof(int))
                    return number.intValue();
                if (type == typeof(long))
                    return number.longValue();
                if (type == typeof(double))
                    return number.doubleValue();
                if (type == typeof(float))
                    return number.floatValue();
                if (type == typeof(short))
                    return number.shortValue();
                if (type == typeof(byte))
                    return number.byteValue();
            }
            else if (value is java.lang.Character character && type == typeof(char))
            {
                return character.charValue();
            }
            else if (value is java.lang.Boolean boolean && type == typeof(bool))
            {
                return boolean.booleanValue();
            }

            var name = primitive.primitiveName + "Value";
            var method = accessors.GetOrAdd((value.GetType(), name), static key => key.Type.GetMethod(key.Name, BindingFlags.Public | BindingFlags.Instance, null, [], null))
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
        ///
        /// <para>A value of the primitive it is boxed as — every call <see cref="From"/> makes, and nearly
        /// every one <see cref="As"/> does — is boxed by that primitive's own <c>valueOf</c>, as javac's
        /// autoboxing boxes it. A value type that is not one of Java's primitives, a struct of our own, is
        /// returned as it is.</para>
        ///
        /// <para>A value of another type than <paramref name="type"/> — a CLR <c>int</c> read where the row
        /// type says <c>java.lang.Long</c> — goes through <c>valueOf</c> by reflection, because what gives
        /// the answer there is <see cref="MethodBase.Invoke(object, object[])"/>'s widening of the argument,
        /// and a direct call cannot reproduce it without restating it. The method found is kept per type.</para>
        /// </remarks>
        public static object Box(object value, Type type)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(type);

            if (value.GetType() == type)
            {
                switch (value)
                {
                    case int v:
                        return java.lang.Integer.valueOf(v);
                    case long v:
                        return java.lang.Long.valueOf(v);
                    case double v:
                        return java.lang.Double.valueOf(v);
                    case bool v:
                        return java.lang.Boolean.valueOf(v);
                    case short v:
                        return java.lang.Short.valueOf(v);
                    case byte v:
                        return java.lang.Byte.valueOf(v);
                    case char v:
                        return java.lang.Character.valueOf(v);
                    case float v:
                        return java.lang.Float.valueOf(v);
                }
            }

            if (ClrPrimitive.Of(type) is not J.Primitive primitive)
                return value;

            var valueOf = valueOfs.GetOrAdd(type, static (type, primitive) => ClrTypes.FromClass(primitive.boxClass).GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [type], null), primitive)
                ?? throw new NotSupportedException($"'{ClrTypes.FromClass(primitive.boxClass)}' has no valueOf for '{type}'.");

            return valueOf.Invoke(null, [value])
                ?? throw new NotSupportedException($"valueOf on '{ClrTypes.FromClass(primitive.boxClass)}' gave nothing.");
        }

        /// <summary>
        /// The <c>valueOf</c> of each primitive's box, by the primitive's CLR type, for <see cref="Box"/>'s
        /// reflective case.
        /// </summary>
        static readonly ConcurrentDictionary<Type, MethodInfo?> valueOfs = new();

        /// <summary>
        /// The accessor a type answers for each primitive's name, for <see cref="Unwrap"/>'s reflective case.
        /// </summary>
        static readonly ConcurrentDictionary<(Type Type, string Name), MethodInfo?> accessors = new();

    }

}
