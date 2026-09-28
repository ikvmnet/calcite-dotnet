using System;
using System.Collections;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The mapping for an <c>ARRAY</c> or <c>MULTISET</c>, which presents it as a one-dimensional .NET array
    /// and converts each element with the element type's mapping.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The element mapping is resolved through <see cref="ClrTypeContext.Registry"/>, so nested collections
    /// and element types added by a caller's resolver are handled the same way.
    /// </para>
    /// <para>
    /// By default a nullable element of a value type makes the array's element type <see cref="Nullable{T}"/>:
    /// a nullable <c>INTEGER</c> element gives <c>int?[]</c>. Calcite holds the collection in a
    /// <c>java.util.Collection</c>, and a value written to Calcite may be any <see cref="IEnumerable"/>.
    /// </para>
    /// </remarks>
    public sealed class CollectionClrTypeMapping : ClrTypeMapping
    {

        /// <summary>
        /// Resolves the element type's default mapping.
        /// </summary>
        /// <exception cref="ClrTypeMappingException">The type has no element type, or the element type has
        /// no mapping.</exception>
        /// <param name="context">The context whose registry resolves the element mapping.</param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <returns>The element type's default mapping.</returns>
        static ClrTypeMapping Element(ClrTypeContext context, RelDataType relType)
        {
            var component = relType.getComponentType()
                ?? throw new ClrTypeMappingException($"{relType} is a collection with no element type.");

            return context.Registry.RequireMapping(null, component);
        }

        /// <summary>
        /// Returns the mapping for a collection read as <paramref name="clrType"/>, or <see langword="null"/>
        /// where no mapping presents the element type as the requested element type.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <param name="clrType">The array type requested, or <see langword="null"/> for the default.</param>
        /// <returns>The mapping, or <see langword="null"/> where there is none or <paramref name="relType"/>
        /// has no element type.</returns>
        /// <remarks>
        /// <para>
        /// The requested element type selects the element mapping, so a <c>DATE ARRAY</c> read as
        /// <c>DateOnly[]</c> uses the <c>DATE</c>-to-<see cref="DateOnly"/> mapping rather than converting
        /// the default <see cref="DateTime"/> elements.
        /// </para>
        /// <para>
        /// An element type of <see cref="object"/>, a <paramref name="clrType"/> that is not a one-dimensional
        /// array, or no <paramref name="clrType"/> uses the element type's default mapping. An element type of
        /// <see cref="object"/> still gives an <c>object[]</c>. A <see cref="Nullable{T}"/> element type is
        /// looked up as its underlying type and kept as the array's element type.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="context"/> or <paramref name="relType"/>
        /// is <see langword="null"/>.</exception>
        public static CollectionClrTypeMapping? Create(ClrTypeContext context, RelDataType relType, Type? clrType)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(relType);

            if (relType.getComponentType() is not RelDataType component)
                return null;

            // System.Array (the table entry's CLR type) and anything but a one-dimensional array name no
            // element type
            var named = clrType is { IsArray: true } && clrType.GetArrayRank() == 1 ? clrType.GetElementType() : null;
            var wanted = named is null || named == typeof(object) ? null : Nullable.GetUnderlyingType(named) ?? named;

            if (context.Registry.GetMapping(wanted, component) is not ClrTypeMapping element)
                return null;

            return new CollectionClrTypeMapping(context, relType, element, named ?? ElementClrType(element));
        }

        /// <summary>
        /// Returns the default .NET element type for an element mapping: its CLR type, made
        /// <see cref="Nullable{T}"/> where the element is nullable and the type is a value type.
        /// </summary>
        /// <param name="element">The element mapping.</param>
        /// <returns>The element mapping's CLR type, or its <see cref="Nullable{T}"/> form.</returns>
        static Type ElementClrType(ClrTypeMapping element)
        {
            var clrType = element.ClrType;

            return element.RelType.isNullable() && clrType.IsValueType ? typeof(Nullable<>).MakeGenericType(clrType) : clrType;
        }

        readonly ClrTypeMapping _element;
        readonly Type _elementClrType;

        /// <summary>
        /// Initializes a new instance that uses the element type's default mapping.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <exception cref="ClrTypeMappingException"><paramref name="relType"/> has no element type, or the
        /// element type has no mapping.</exception>
        public CollectionClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            this(context, relType, Element(context, relType))
        {

        }

        /// <summary>
        /// Initializes a new instance from a resolved element mapping and its default .NET element type.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <param name="element">The mapping each element is converted with.</param>
        CollectionClrTypeMapping(ClrTypeContext context, RelDataType relType, ClrTypeMapping element) :
            this(context, relType, element, ElementClrType(element))
        {

        }

        /// <summary>
        /// Initializes a new instance from an element mapping and the .NET type of the array's elements.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <param name="element">The mapping each element is converted with.</param>
        /// <param name="elementClrType">The element type of the array the collection is read as.</param>
        CollectionClrTypeMapping(ClrTypeContext context, RelDataType relType, ClrTypeMapping element, Type elementClrType) :
            base(context, relType, elementClrType.MakeArrayType())
        {
            _element = element;
            _elementClrType = elementClrType;
        }

        /// <summary>
        /// Gets the mapping each element is converted with.
        /// </summary>
        public ClrTypeMapping ElementMapping => _element;

        /// <inheritdoc />
        public override object? ToCalcite(object value)
        {
            if (value is not IEnumerable source)
                throw new ClrTypeMappingException($"A {RelType} is written from a sequence, and a {value.GetType()} is not one.");

            var list = new java.util.ArrayList();
            foreach (var item in source)
                list.add(item is null ? null : _element.ToCalcite(item));

            return list;
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            if (value is not java.util.Collection source)
                throw new ClrTypeMappingException($"A {RelType} is held in a java.util.Collection, and a {value.GetType()} is not one.");

            var array = Array.CreateInstance(_elementClrType, source.size());

            var i = 0;
            for (var e = source.iterator(); e.hasNext(); i++)
            {
                var item = Unwrap(e.next(), _element);
                if (item is null)
                {
                    // Array.SetValue stores default(T) for a null in an array of a non-nullable value type,
                    // which would make a null indistinguishable from zero; refuse instead. This happens only
                    // where the caller asked for such an element type
                    if (_elementClrType.IsValueType && Nullable.GetUnderlyingType(_elementClrType) is null)
                        throw new ClrTypeMappingException($"An element of {RelType} is null and a {_elementClrType} does not hold one.");

                    continue;
                }

                array.SetValue(_element.FromCalcite(item), i);
            }

            return array;
        }

        /// <summary>
        /// Returns a collection element with Calcite's one-field record wrapper removed, where it has one.
        /// </summary>
        /// <param name="value">The element as the collection holds it.</param>
        /// <param name="element">The mapping the element is converted with.</param>
        /// <returns>The single field of a one-element <c>object[]</c>, or <paramref name="value"/>
        /// unchanged.</returns>
        /// <remarks>
        /// <para>
        /// Depending on the plan, Calcite can hold the elements of a nested <c>MULTISET</c> each wrapped in a
        /// one-field <c>Object[]</c>, and the declared type does not say whether it has. An element whose
        /// mapping's representation is neither <c>object[]</c> nor <see cref="object"/> is never legitimately
        /// an <c>object[]</c>, so for such an element a one-field <c>object[]</c> is taken to be the wrapper.
        /// An element whose representation is <c>object[]</c> or <see cref="object"/> (a row, <c>ANY</c>) is
        /// returned unchanged.
        /// </para>
        /// <para>
        /// Use this when walking a Calcite collection directly, so that the same rule applies.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
        public static object? Unwrap(object? value, ClrTypeMapping element)
        {
            ArgumentNullException.ThrowIfNull(element);

            if (element.RepresentationType == typeof(object[]) || element.RepresentationType == typeof(object))
                return value;

            // the exact type: by array covariance, an array of any reference type matches object[], and such
            // an array is a value rather than a wrapper
            return value is object[] wrapper && wrapper.Length == 1 && value.GetType() == typeof(object[]) ? wrapper[0] : value;
        }

    }

}
