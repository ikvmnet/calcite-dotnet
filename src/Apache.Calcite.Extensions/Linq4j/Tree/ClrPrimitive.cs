using System;
using System.Collections.Generic;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Answers linq4j <c>Primitive</c>'s questions about the eight Java primitives and their box classes for
    /// CLR types.
    /// </summary>
    /// <remarks>
    /// The pairs are read from linq4j's <c>Primitive</c> values and mapped to the CLR types IKVM gives them, so
    /// the CLR type of a Java <c>int</c> is <see cref="int"/> and of <c>java.lang.Integer</c> is IKVM's
    /// <c>java.lang.Integer</c>.
    /// </remarks>
    public static class ClrPrimitive
    {

        /// <summary>
        /// The <c>Primitive</c> for each Java primitive's CLR type.
        /// </summary>
        static readonly Dictionary<Type, J.Primitive> primitives = [];

        /// <summary>
        /// The <c>Primitive</c> for each Java box class's CLR type.
        /// </summary>
        static readonly Dictionary<Type, J.Primitive> boxes = [];

        /// <summary>
        /// Fills the two maps from <c>Primitive.values()</c>, skipping <c>void</c> and the entries with no
        /// primitive or box class.
        /// </summary>
        static ClrPrimitive()
        {
            foreach (J.Primitive primitive in J.Primitive.values())
            {
                if (primitive.primitiveClass == null || primitive.boxClass == null)
                    continue;
                if (primitive.primitiveName == "void")
                    continue;

                primitives[ClrTypes.FromClass(primitive.primitiveClass)] = primitive;
                boxes[ClrTypes.FromClass(primitive.boxClass)] = primitive;
            }
        }

        /// <summary>
        /// Returns the primitive <paramref name="type"/> is, or <see langword="null"/> if it is not one.
        /// </summary>
        /// <param name="type">The CLR type.</param>
        /// <returns>The primitive, or <see langword="null"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The counterpart of <c>Primitive.of</c>.
        /// </remarks>
        public static J.Primitive? Of(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return primitives.GetValueOrDefault(type);
        }

        /// <summary>
        /// Returns the primitive <paramref name="type"/> boxes, or <see langword="null"/> if it is not a Java
        /// box class.
        /// </summary>
        /// <param name="type">The CLR type.</param>
        /// <returns>The primitive, or <see langword="null"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The counterpart of <c>Primitive.ofBox</c>.
        /// </remarks>
        public static J.Primitive? OfBox(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return boxes.GetValueOrDefault(type);
        }

        /// <summary>
        /// Returns whether <paramref name="type"/> is a Java primitive.
        /// </summary>
        /// <param name="type">The CLR type.</param>
        /// <returns><see langword="true"/> if it is a primitive.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The counterpart of <c>Primitive.is</c>.
        /// </remarks>
        public static bool Is(Type type) => Of(type) != null;

        /// <summary>
        /// Returns the Java box class of a primitive, or <paramref name="type"/> itself if it is not one.
        /// </summary>
        /// <param name="type">The CLR type.</param>
        /// <returns>The CLR type of the box class, or <paramref name="type"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The counterpart of <c>Primitive.box</c>.
        /// </remarks>
        public static Type Box(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Of(type) is J.Primitive primitive ? ClrTypes.FromClass(primitive.boxClass) : type;
        }

        /// <summary>
        /// Returns the primitive a Java box class holds, or <see langword="null"/> if
        /// <paramref name="type"/> is not a box class.
        /// </summary>
        /// <param name="type">The CLR type.</param>
        /// <returns>The CLR type of the primitive, or <see langword="null"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The counterpart of <c>Primitive.ofBox(type).primitiveClass</c>, the inverse of <see cref="Box"/>.
        /// </remarks>
        public static Type? PrimitiveClass(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return OfBox(type) is J.Primitive primitive ? ClrTypes.FromClass(primitive.primitiveClass) : null;
        }

        /// <summary>
        /// Returns whether <paramref name="type"/> is neither a Java primitive nor a Java box class.
        /// </summary>
        /// <param name="type">The CLR type.</param>
        /// <returns><see langword="true"/> if it is neither.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The counterpart of <c>Primitive.flavor(type) == Flavor.OBJECT</c>.
        /// </remarks>
        public static bool IsObject(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Of(type) == null && OfBox(type) == null;
        }

    }

}
