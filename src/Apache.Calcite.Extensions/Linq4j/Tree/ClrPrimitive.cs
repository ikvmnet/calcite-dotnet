using System;
using System.Collections.Generic;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// The eight Java primitives and their boxes, answered for the CLR.
    /// </summary>
    /// <remarks>
    /// <c>Primitive</c>, for types that have been through IKVM. Calcite asks it whether a type is a primitive,
    /// which primitive a box holds, and what a primitive boxes to; the questions are the same and the runtime
    /// is the difference.
    ///
    /// <para>The pairing is taken from linq4j rather than written out, so a primitive is matched with the box
    /// Calcite itself matches it with, and the CLR types are whichever ones IKVM actually chose. Every answer
    /// is a <c>Primitive</c> of Calcite's or a CLR <see cref="Type"/>; nothing here builds an expression.</para>
    /// </remarks>
    public static class ClrPrimitive
    {

        /// <summary>
        /// CLR type of each Java primitive, against the <c>Primitive</c> that describes it.
        /// </summary>
        static readonly Dictionary<Type, J.Primitive> primitives = [];

        /// <summary>
        /// CLR type of each Java box class, against the <c>Primitive</c> it boxes.
        /// </summary>
        static readonly Dictionary<Type, J.Primitive> boxes = [];

        /// <summary>
        /// Initializes the static instance.
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
        /// Returns the primitive a type is, or <see langword="null"/> where it is not one.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Primitive.of</c>.
        /// </remarks>
        public static J.Primitive? Of(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return primitives.GetValueOrDefault(type);
        }

        /// <summary>
        /// Returns the primitive a type boxes, or <see langword="null"/> where it is not a box.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Primitive.ofBox</c>.
        /// </remarks>
        public static J.Primitive? OfBox(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return boxes.GetValueOrDefault(type);
        }

        /// <summary>
        /// Returns whether a type is a primitive.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Primitive.is</c>.
        /// </remarks>
        public static bool Is(Type type) => Of(type) != null;

        /// <summary>
        /// Returns the box class of a primitive, and the type itself where it is not one.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Primitive.box</c>. A row that is a primitive is boxed wherever it has to be a reference — inside
        /// a sequence, as an argument of an outer join's selector, as the key of a map.
        /// </remarks>
        public static Type Box(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Of(type) is J.Primitive primitive ? ClrTypes.FromClass(primitive.boxClass) : type;
        }

        /// <summary>
        /// Returns the primitive class a box holds, or <see langword="null"/> where the type is not a box.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Primitive.ofBox(type).primitiveClass</c>, which is how Calcite spells the other direction of
        /// <see cref="Box"/>.
        /// </remarks>
        public static Type? PrimitiveClass(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return OfBox(type) is J.Primitive primitive ? ClrTypes.FromClass(primitive.primitiveClass) : null;
        }

        /// <summary>
        /// Returns whether a type is neither a primitive nor one of their boxes.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Primitive.flavor(type) == Flavor.OBJECT</c>. Calcite's <c>Flavor</c> has three values and every
        /// caller here asks for this one, which is what selects the <c>Comparable</c> overload of a comparison.
        /// </remarks>
        public static bool IsObject(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Of(type) == null && OfBox(type) == null;
        }

    }

}
