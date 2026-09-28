using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Linq4j.Tree;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Converts values between Java's boxed primitives and the CLR's where they cross between the two
    /// runtimes.
    /// </summary>
    /// <remarks>
    /// A Java primitive boxed as an object is a <c>java.lang.Integer</c> (and so on), not a CLR-boxed
    /// <see cref="int"/>, and a cast between the two fails. Calcite's comparators and accessors expect the
    /// Java box, so a value handed to Calcite is boxed the Java way and a value read from Calcite is unboxed
    /// through the box's accessor.
    /// </remarks>
    static class JavaValues
    {

        /// <summary>
        /// Returns a value as <typeparamref name="T"/>, converting between Java and CLR boxing where needed.
        /// </summary>
        /// <remarks>
        /// A Java box read as a CLR primitive is unboxed with <see cref="Unwrap"/>; a CLR value type read as
        /// a Java box type is boxed with <see cref="Box"/>. Anything else is cast.
        /// </remarks>
        /// <typeparam name="T">The type the caller reads the value as.</typeparam>
        /// <param name="value">The value, boxed either the Java or the CLR way, or <see langword="null"/>.</param>
        /// <returns><paramref name="value"/> as <typeparamref name="T"/>.</returns>
        public static T As<T>(object? value)
        {
            if (value is T typed)
                return typed;

            if (value != null && typeof(T).IsValueType)
                return (T)JavaValues.Unwrap(value, typeof(T));

            // a CLR value where the type is a Java box, such as an int where java.lang.Integer is expected
            if (value != null && value.GetType().IsValueType && ClrPrimitive.PrimitiveClass(typeof(T)) is Type primitive)
                return (T)JavaValues.Box(value, primitive);

            return (T)value!;
        }

        /// <summary>
        /// Returns a value as the object Calcite expects: a CLR primitive boxed as Java boxes it, and anything
        /// else unchanged.
        /// </summary>
        /// <remarks>
        /// Handing Calcite a CLR-boxed <see cref="int"/> where the type factory declares
        /// <c>java.lang.Integer</c> leaves two representations of one value in a plan, and comparisons between
        /// them fail. Returns <see langword="null"/> only for <see langword="null"/>.
        /// </remarks>
        /// <typeparam name="T">The static type of the value at the call site.</typeparam>
        /// <param name="value">The value to hand to Calcite.</param>
        /// <returns><paramref name="value"/>, boxed the Java way if it is a CLR primitive.</returns>
        [return: NotNullIfNotNull(nameof(value))]
        public static object? From<T>(T value)
        {
            if (value == null)
                return null;

            // tests the runtime type rather than typeof(T), which at most call sites is object or a Java box
            // type and would say nothing. Box returns a value type that is not a Java primitive unchanged.
            var type = value.GetType();

            return type.IsValueType ? JavaValues.Box(value, type) : value;
        }



        /// <summary>
        /// Returns <paramref name="value"/> as the CLR primitive <paramref name="type"/>, as Java unboxing
        /// would.
        /// </summary>
        /// <param name="value">The boxed value.</param>
        /// <param name="type">A CLR type that corresponds to a Java primitive.</param>
        /// <returns>The primitive value, boxed by the CLR.</returns>
        /// <exception cref="NotSupportedException"><paramref name="type"/> is not a Java primitive, or the
        /// value has no accessor for it.</exception>
        /// <remarks>
        /// Calls the primitive's accessor, such as <c>intValue()</c>, on the value: on any
        /// <c>java.lang.Number</c> for the numeric primitives, and on a <c>Character</c> or <c>Boolean</c> for
        /// the other two. Any other value is asked for the accessor by reflection, cached per type.
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
        /// Returns <paramref name="value"/> boxed as Java boxes the primitive <paramref name="type"/>.
        /// </summary>
        /// <param name="value">The CLR value.</param>
        /// <param name="type">The CLR type of the Java primitive to box as.</param>
        /// <returns>The Java box, or <paramref name="value"/> itself if <paramref name="type"/> is not a Java
        /// primitive.</returns>
        /// <exception cref="NotSupportedException">The box type has no <c>valueOf</c> for
        /// <paramref name="type"/>.</exception>
        /// <remarks>
        /// A value whose type is <paramref name="type"/> is boxed by that box's <c>valueOf</c>, as Java
        /// autoboxing does. A value of another type, such as an <see cref="int"/> where the type is
        /// <see cref="long"/>, is passed to <c>valueOf</c> by reflection, which widens it; the method is cached
        /// per type.
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
        /// The <c>valueOf</c> of each primitive's box, keyed by the primitive's CLR type.
        /// </summary>
        static readonly ConcurrentDictionary<Type, MethodInfo?> valueOfs = new();

        /// <summary>
        /// The primitive accessor methods found by reflection, keyed by value type and method name.
        /// </summary>
        static readonly ConcurrentDictionary<(Type Type, string Name), MethodInfo?> accessors = new();

    }

}
