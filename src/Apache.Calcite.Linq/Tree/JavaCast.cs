using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// Builds the <see cref="Expression"/> that a Java cast means.
    /// </summary>
    /// <remarks>
    /// A linq4j <c>Convert</c> node says what Java would write as a cast, and Java's cast is not the CLR's.
    /// Java boxes and unboxes on its own: <c>(Integer) i</c> is <c>Integer.valueOf(i)</c> and <c>(int) o</c>
    /// is <c>((Integer) o).intValue()</c>. Neither is a conversion the CLR has, because a boxed Java value is
    /// an instance of a class rather than a boxed CLR value, so <see cref="Expression.Convert(Expression,
    /// Type)"/> would emit an <c>unbox.any</c> demanding a runtime type that is never there.
    ///
    /// <para>The rule is that this is for what Java the language does implicitly and an expression tree will
    /// not: boxing where a primitive is passed as an Object, unboxing where a box is used as a number,
    /// widening one operand of a binary operator to meet the other, promoting a byte. It is not a way to make
    /// one type into another where they ought already to agree; converting a value that already has the type
    /// wanted does nothing except absorb the case where it does not, and the code that produced the wrong type
    /// is where that should be seen.</para>
    ///
    /// <para>Measured against every plan the tests run, including arithmetic over a nullable column, what is
    /// actually asked for is four reference conversions and one boxing. No unboxing, no widening and no byte
    /// promotion is reached, because <c>RexImpTable</c> emits <c>Integer.valueOf</c> and <c>intValue()</c> as
    /// calls in the tree rather than leaning on autoboxing -- Calcite writes Java source that has to compile,
    /// and needs the null handling exact. Those paths are what Java means, but they are unproven here.</para>
    /// </remarks>
    public static class JavaCast
    {

        /// <summary>
        /// CLR type of each Java primitive, against the <see cref="J.Primitive"/> that describes it.
        /// </summary>
        static readonly Dictionary<Type, J.Primitive> Primitives = [];

        /// <summary>
        /// CLR type of each Java box class, against the <see cref="J.Primitive"/> it boxes.
        /// </summary>
        static readonly Dictionary<Type, J.Primitive> Boxes = [];

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static JavaCast()
        {
            // taken from linq4j rather than written out, so the pairing of a primitive with its box is the
            // one Calcite itself uses, and the CLR types are whichever ones IKVM actually chose
            foreach (J.Primitive primitive in J.Primitive.values())
            {
                if (primitive.primitiveClass == null || primitive.boxClass == null)
                    continue;
                if (primitive.primitiveName == "void")
                    continue;

                Primitives[TypeResolver.FromClass(primitive.primitiveClass)] = primitive;
                Boxes[TypeResolver.FromClass(primitive.boxClass)] = primitive;
            }
        }

        /// <summary>
        /// Returns an expression yielding <paramref name="expression"/> as <paramref name="type"/>.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Expression To(Expression expression, Type type)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(type);

            if (expression.Type == type)
                return expression;

            if (type == typeof(void))
                return expression;

            var fromPrimitive = Primitives.GetValueOrDefault(expression.Type);
            var toPrimitive = Primitives.GetValueOrDefault(type);
            var fromBox = Boxes.GetValueOrDefault(expression.Type);

            // int to Integer, and int to Long by way of long, exactly as Java widens before it boxes
            if (fromPrimitive != null && Boxes.TryGetValue(type, out var toBox))
                return Box(Number(expression, toBox), toBox);

            // Integer to int, and Integer to long by way of int
            if (fromBox != null && toPrimitive != null)
                return Number(Unbox(expression, fromBox), toPrimitive);

            // int to Object, Number, Comparable: box first, then the reference conversion is ordinary
            if (fromPrimitive != null && type.IsValueType == false)
                return Expression.Convert(Box(expression, fromPrimitive), type);

            // Object to int, which Java reads as a cast to the box class followed by an unboxing call. A
            // straight Convert would emit unbox.any and demand a boxed CLR int that is never the value here.
            if (expression.Type.IsValueType == false && toPrimitive != null)
                return Unbox(Expression.Convert(expression, TypeResolver.FromClass(toPrimitive.boxClass)), toPrimitive);

            if (fromPrimitive != null && toPrimitive != null)
                return Widen(expression, type);

            return Expression.Convert(expression, type);
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

            if (Primitives.TryGetValue(type, out var primitive) == false)
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

            if (Primitives.TryGetValue(type, out var primitive) == false)
                return value;

            var box = TypeResolver.FromClass(primitive.boxClass);
            var valueOf = box.GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [type], null)
                ?? throw new NotSupportedException($"'{box}' has no valueOf for '{type}'.");

            return valueOf.Invoke(null, [value])
                ?? throw new NotSupportedException($"valueOf on '{box}' gave nothing.");
        }

        /// <summary>
        /// Converts between two primitives, and returns <paramref name="expression"/> when they agree.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="primitive"></param>
        /// <returns></returns>
        static Expression Number(Expression expression, J.Primitive primitive)
        {
            return Widen(expression, TypeResolver.FromClass(primitive.primitiveClass));
        }

        /// <summary>
        /// Converts between two primitives.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// Java's byte is signed and IKVM stores it in a CLR byte, which is not. Widening one therefore has to
        /// go by way of an sbyte, or -1 becomes 255 the moment it is promoted to an int.
        /// </remarks>
        static Expression Widen(Expression expression, Type type)
        {
            if (expression.Type == type)
                return expression;

            if (expression.Type == typeof(byte))
                expression = Expression.Convert(expression, typeof(sbyte));

            return expression.Type == type ? expression : Expression.Convert(expression, type);
        }

        /// <summary>
        /// Boxes a primitive into its Java box class.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="primitive"></param>
        /// <returns></returns>
        static Expression Box(Expression expression, J.Primitive primitive)
        {
            var box = TypeResolver.FromClass(primitive.boxClass);
            var valueOf = box.GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [expression.Type], null)
                ?? throw new NotSupportedException($"'{box}' has no valueOf for '{expression.Type}'.");

            return Expression.Call(null, valueOf, expression);
        }

        /// <summary>
        /// Unboxes a Java box class into its primitive.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="primitive"></param>
        /// <returns></returns>
        static Expression Unbox(Expression expression, J.Primitive primitive)
        {
            // intValue, booleanValue, charValue, and so on for each of the eight
            var name = primitive.primitiveName + "Value";
            var value = expression.Type.GetMethod(name, BindingFlags.Public | BindingFlags.Instance, null, [], null)
                ?? throw new NotSupportedException($"'{expression.Type}' has no {name}().");

            return Expression.Call(expression, value);
        }

    }

}
