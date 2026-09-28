using System;
using System.Collections;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.avatica.util;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The mapping for <c>ANY</c> and <c>OTHER</c>, types that do not say what they hold, which converts each
    /// value according to its own runtime class.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Reading converts a Java value with <see cref="CalciteValues.FromShape"/>. A <c>java.util.Map</c> becomes
    /// a <see cref="Dictionary{TKey, TValue}"/>, a <c>java.util.Collection</c> an array, and an
    /// <c>Object[]</c> an <c>object[]</c>, with their contents converted the same way; the element, key and
    /// value types of the result are the single runtime type all the values share, or <see cref="object"/>
    /// where they share none. A <c>VariantValue</c> is read through the chain's <c>VARIANT</c> mapping. A value
    /// of a class with no .NET counterpart is returned unchanged.
    /// </para>
    /// <para>
    /// Writing is the reverse: a dictionary becomes a <c>java.util.LinkedHashMap</c>, any other sequence a
    /// <c>java.util.ArrayList</c>, and a scalar is converted with <see cref="CalciteValues.ToShape"/>.
    /// </para>
    /// </remarks>
    public sealed class AnyClrTypeMapping : ClrTypeMapping
    {

        readonly ClrTypeContext _context;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The type, typically <c>ANY</c> or <c>OTHER</c>.</param>
        public AnyClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            base(context, relType, typeof(object))
        {
            _context = context;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Always <see langword="false"/>: the value's own class stands in for the column's type.
        /// </remarks>
        public override bool DescribesValue => false;

        /// <inheritdoc />
        public override object? ToCalcite(object value)
        {
            return Write(value);
        }

        /// <summary>
        /// Converts a value by its runtime type, converting the contents of a dictionary or sequence as well.
        /// </summary>
        /// <param name="value">The .NET value.</param>
        /// <returns>The value as Calcite's runtime holds it.</returns>
        /// <remarks>
        /// A <see cref="string"/> is matched before the sequence case, so it is written as a string rather
        /// than as a list of characters.
        /// </remarks>
        object? Write(object? value)
        {
            switch (value)
            {
                case null:
                    return null;

                case string:
                    return CalciteValues.ToShape(value);

                case IDictionary dictionary:
                    {
                        // insertion-ordered, as the map Calcite's SqlFunctions.map builds is
                        var map = new java.util.LinkedHashMap();
                        for (var i = dictionary.GetEnumerator(); i.MoveNext();)
                            map.put(Write(i.Key), Write(i.Value));

                        return map;
                    }

                case IEnumerable sequence:
                    {
                        var list = new java.util.ArrayList();
                        foreach (var item in sequence)
                            list.add(Write(item));

                        return list;
                    }

                default:
                    return CalciteValues.ToShape(value);
            }
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            return Read(value);
        }

        /// <summary>
        /// Converts a value by its runtime class, converting the contents of a map, collection or row as well.
        /// </summary>
        /// <param name="value">The value as Calcite produced it.</param>
        /// <returns>The .NET value.</returns>
        object? Read(object? value)
        {
            switch (value)
            {
                case null:
                    return null;

                // read through the chain, so that a caller's mappings apply to the variant's payload
                case org.apache.calcite.runtime.variant.VariantValue variant:
                    return _context.Registry.RequireMapping(null,
                        _context.TypeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARIANT)).FromCalcite(variant);

                case java.util.Map map:
                    return Entries(map);

                case java.util.Collection collection:
                    return Elements(collection);

                // a row: its fields stay an object[] rather than being unified to a common element type
                case object[] row when value.GetType() == typeof(object[]):
                    return Fields(row);

                default:
                    return CalciteValues.FromShape(value);
            }
        }

        /// <summary>
        /// Returns a collection's elements, converted, in an array of the type they share.
        /// </summary>
        /// <param name="source">The Java collection Calcite produced.</param>
        /// <returns>An array of the elements in iteration order, each converted as <see cref="Read"/>
        /// converts a value.</returns>
        Array Elements(java.util.Collection source)
        {
            var items = new List<object?>(source.size());
            for (var i = source.iterator(); i.hasNext();)
                items.Add(Read(i.next()));

            return Pack(items);
        }

        /// <summary>
        /// Returns a row's fields, converted, in an <c>object[]</c>.
        /// </summary>
        /// <remarks>
        /// A row stays <c>object[]</c> even where its fields share a type: <c>ROW(1, 2)</c> is two fields, not
        /// an array of two integers.
        /// </remarks>
        /// <param name="row">The row as Calcite holds it.</param>
        /// <returns>A new array holding each field converted, in field order.</returns>
        object?[] Fields(object[] row)
        {
            var fields = new object?[row.Length];
            for (var i = 0; i < row.Length; i++)
                fields[i] = Read(row[i]);

            return fields;
        }

        /// <summary>
        /// Returns a map's entries, converted, in a dictionary whose key and value types are those the keys
        /// and values share.
        /// </summary>
        /// <returns>A dictionary, or an array of <see cref="KeyValuePair{TKey, TValue}"/> where a key is
        /// null.</returns>
        /// <remarks>
        /// <see cref="Dictionary{TKey, TValue}"/> does not accept a null key, and Calcite can produce one (for
        /// example <c>MAP[CAST(NULL AS VARCHAR), 1]</c>), so such a map is returned as an array of pairs rather
        /// than losing the entry.
        /// </remarks>
        /// <param name="source">The Java map Calcite produced.</param>
        object Entries(java.util.Map source)
        {
            var count = source.size();
            var keys = new List<object?>(count);
            var values = new List<object?>(count);

            for (var i = source.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                keys.Add(Read(entry.getKey()));
                values.Add(Read(entry.getValue()));
            }

            if (keys.Contains(null))
            {
                var pairType = typeof(KeyValuePair<,>).MakeGenericType(Unify(keys), Unify(values));
                var pairs = Array.CreateInstance(pairType, count);
                for (var i = 0; i < count; i++)
                    pairs.SetValue(Activator.CreateInstance(pairType, keys[i], values[i]), i);

                return pairs;
            }

            var dictionary = (IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(Unify(keys), Unify(values)), count)!;

            for (var i = 0; i < count; i++)
                dictionary[keys[i]!] = values[i];

            return dictionary;
        }

        /// <summary>
        /// Returns values in an array of the type they share.
        /// </summary>
        /// <param name="items">The converted values, in order; any may be <see langword="null"/>.</param>
        /// <returns>A new array whose element type is what <see cref="Unify"/> answers for the values.</returns>
        static Array Pack(List<object?> items)
        {
            var element = Unify(items);
            var array = Array.CreateInstance(element, items.Count);
            for (var i = 0; i < items.Count; i++)
                array.SetValue(items[i], i);

            return array;
        }

        /// <summary>
        /// Returns the runtime type every non-null value has, or <see cref="object"/> where they differ or
        /// there are none.
        /// </summary>
        /// <remarks>
        /// Where the shared type is a value type and a value is null, the result is the nullable form of that
        /// type.
        /// </remarks>
        /// <param name="items">The converted values; any may be <see langword="null"/>.</param>
        /// <returns>The shared runtime type, its nullable form, or <see cref="object"/>.</returns>
        static Type Unify(List<object?> items)
        {
            Type? common = null;
            var nulls = false;

            foreach (var item in items)
            {
                if (item is null)
                {
                    nulls = true;
                    continue;
                }

                var type = item.GetType();
                if (common is null)
                    common = type;
                else if (common != type)
                    return typeof(object);
            }

            if (common is null)
                return typeof(object);

            return nulls && common.IsValueType ? typeof(Nullable<>).MakeGenericType(common) : common;
        }

    }

}
