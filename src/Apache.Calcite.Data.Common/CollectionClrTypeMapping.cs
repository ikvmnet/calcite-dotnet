using System;
using System.Collections;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// An <c>ARRAY</c> or a <c>MULTISET</c>, mapped by mapping its element and wrapping the result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is why a mapping may ask the registry for another.</b> The element's mapping is resolved
    /// through <see cref="ClrTypeContext.Registry"/> rather than by knowing every element type here, so one
    /// entry covers <c>INTEGER ARRAY</c>, <c>INTEGER ARRAY ARRAY</c> and every depth after it, and a
    /// collection of a type a caller registered itself is mapped by the caller's own mapping without this
    /// class knowing it exists. Resolution terminates because each step strips one level.
    /// </para>
    /// <para>
    /// <b>The element's nullability is part of the .NET type.</b> A nullable <c>INTEGER</c> element makes
    /// the array <c>int?[]</c>, because inside an array there is no <see cref="DBNull"/> and
    /// <see cref="Nullable{T}"/> is the only way a null can be carried. That is the one place nullability
    /// changes a type here; a column's own nullability is stated separately by ADO.NET and does not.
    /// </para>
    /// </remarks>
    public sealed class CollectionClrTypeMapping : ClrTypeMapping
    {

        /// <summary>
        /// Resolves the element's mapping, which is the whole of the recursion.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <returns>The mapping one element is carried across by.</returns>
        /// <exception cref="ClrTypeMappingException">Where the element's type has no mapping.</exception>
        static ClrTypeMapping Element(ClrTypeContext context, RelDataType relType)
        {
            var component = relType.getComponentType()
                ?? throw new ClrTypeMappingException($"{relType} is a collection with no element type.");

            return context.Registry.RequireMapping(null, component);
        }

        /// <summary>
        /// Returns the mapping for a collection read as <paramref name="clrType"/>, or
        /// <see langword="null"/> where nothing carries the elements to what that names.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <param name="clrType">The array type a caller named, or <see langword="null"/> for whichever
        /// the collection reads back as.</param>
        /// <returns>The mapping, or <see langword="null"/>.</returns>
        /// <remarks>
        /// <b>Naming the element type selects the mapping the elements cross by; it is not a cast of what
        /// the collection read back as.</b> A <c>DATE</c> is a <see cref="DateTime"/> by default and a
        /// <see cref="DateOnly"/> when asked, because the chain carries both, and asking is the only way to
        /// reach the second — narrowing the default reading could never get there. So the element type the
        /// caller spelled goes into the element lookup rather than being applied to its result, which is
        /// what makes <c>GetFieldValue&lt;DateOnly[]&gt;</c> the same ask as <c>GetArray&lt;DateOnly&gt;</c>.
        ///
        /// <para>Two spellings mean "no preference": no CLR type at all, and an element of
        /// <see cref="object"/>. Both are a null to the registry, which is what <c>GetFieldValue{object}</c>
        /// means everywhere else — the array is of <see cref="object"/> and the elements are whatever the
        /// column reads back as, boxed into it.</para>
        ///
        /// <para><see cref="Nullable{T}"/> on the element is a statement about the array and not about which
        /// mapping carries a value, so it is stripped for the lookup and kept for the array.</para>
        /// </remarks>
        public static CollectionClrTypeMapping? Create(ClrTypeContext context, RelDataType relType, Type? clrType)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(relType);

            if (relType.getComponentType() is not RelDataType component)
                return null;

            // System.Array is the table entry's own CLR type and names no element; anything that is not a
            // one-dimensional array names none either
            var named = clrType is { IsArray: true } && clrType.GetArrayRank() == 1 ? clrType.GetElementType() : null;
            var wanted = named is null || named == typeof(object) ? null : Nullable.GetUnderlyingType(named) ?? named;

            if (context.Registry.GetMapping(wanted, component) is not ClrTypeMapping element)
                return null;

            return new CollectionClrTypeMapping(context, relType, element, named ?? ElementClrType(element));
        }

        /// <summary>
        /// Returns the .NET type an element materializes as, which carries the element's nullability
        /// because an array has no other way to hold a null.
        /// </summary>
        /// <param name="element">The mapping one element is carried across by.</param>
        /// <returns>The type, made <see cref="Nullable{T}"/> where the element admits a null and the type
        /// is a value type.</returns>
        static Type ElementClrType(ClrTypeMapping element)
        {
            var clrType = element.ClrType;

            return element.RelType.isNullable() && clrType.IsValueType ? typeof(Nullable<>).MakeGenericType(clrType) : clrType;
        }

        readonly ClrTypeMapping _element;
        readonly Type _elementClrType;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        public CollectionClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            this(context, relType, Element(context, relType))
        {

        }

        /// <summary>
        /// Initializes a new instance from an element mapping already resolved.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <param name="element">The mapping one element is carried across by.</param>
        /// <remarks>
        /// Private because the base constructor needs the .NET type up front and that type is the element's
        /// array: the element has to be resolved before the chain to <c>base</c> can be written, and
        /// resolving it in both places would build it twice.
        /// </remarks>
        CollectionClrTypeMapping(ClrTypeContext context, RelDataType relType, ClrTypeMapping element) :
            this(context, relType, element, ElementClrType(element))
        {

        }

        /// <summary>
        /// Initializes a new instance from an element mapping and the .NET type the elements are held as,
        /// which is the caller's spelling where it named one.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>ARRAY</c> or <c>MULTISET</c> type.</param>
        /// <param name="element">The mapping one element is carried across by.</param>
        /// <param name="elementClrType">The .NET type an element materializes as.</param>
        CollectionClrTypeMapping(ClrTypeContext context, RelDataType relType, ClrTypeMapping element, Type elementClrType) :
            base(context, relType, elementClrType.MakeArrayType())
        {
            _element = element;
            _elementClrType = elementClrType;
        }

        /// <summary>
        /// Gets the mapping one element is carried across by.
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
                    // Array.SetValue writes default(T) for a null into an array of a value type rather than
                    // refusing it, so a null element and a zero would be the same array afterwards. The
                    // element type follows the column's nullability unless a caller named one, and a caller
                    // that named a type with no room for a null is told so
                    if (_elementClrType.IsValueType && Nullable.GetUnderlyingType(_elementClrType) is null)
                        throw new ClrTypeMappingException($"An element of {RelType} is null and a {_elementClrType} does not hold one.");

                    continue;
                }

                array.SetValue(_element.FromCalcite(item), i);
            }

            return array;
        }

        /// <summary>
        /// Returns an element with Calcite's one-field record wrapper taken off, where it has one.
        /// </summary>
        /// <remarks>
        /// <b>The plan decides this and the type cannot.</b> Calcite's internal representation of a multiset
        /// holds every element as a record, and a <c>MULTISET</c> whose elements are themselves multisets
        /// arrives with the inner collection's values each wrapped in a one-field <c>Object[]</c>. The same
        /// declared type written as a query constructor does not:
        /// <c>MULTISET[MULTISET[1, 2]]</c> wraps and <c>MULTISET(SELECT MULTISET[1, 2])</c> does not, and
        /// both are <c>INTEGER MULTISET MULTISET</c>. So nothing in the type tells the two apart and the
        /// wrapper has to be recognised where it is found.
        ///
        /// <para>What the type does settle is that recognising it is safe. An element that is not a row is
        /// never legitimately an <c>Object[]</c>: a collection is held in a <c>java.util.List</c>, a map in
        /// a <c>java.util.Map</c>, a binary in a <c>ByteString</c>, and a scalar in its own boxed class. An
        /// element whose own representation is <c>Object[]</c> — a row, or a type that says nothing — is
        /// left alone.</para>
        /// </remarks>
        /// <param name="value">The element as the collection held it.</param>
        /// <param name="element">The mapping one element is carried across by.</param>
        /// <returns>The element, unwrapped where it was wrapped.</returns>
        /// <remarks>
        /// Public and static because a caller naming its own element type walks a collection itself and
        /// needs the same rule; two copies of it would be two answers to when a value is a wrapper.
        /// </remarks>
        public static object? Unwrap(object? value, ClrTypeMapping element)
        {
            ArgumentNullException.ThrowIfNull(element);

            if (element.RepresentationType == typeof(object[]) || element.RepresentationType == typeof(object))
                return value;

            // the exact type, because an array of a reference type matches object[] by covariance and is a
            // value in its own right rather than a wrapper
            return value is object[] wrapper && wrapper.Length == 1 && value.GetType() == typeof(object[]) ? wrapper[0] : value;
        }

    }

}
