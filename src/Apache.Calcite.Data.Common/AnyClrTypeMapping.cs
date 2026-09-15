using System;
using System.Collections;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.avatica.util;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A type that says nothing about what it holds, read on the strength of the value's own class.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>ANY</c> is <c>java.lang.Object</c> and carries nothing, so the value is whatever a table, a
    /// user-defined function or a schema put there. <c>OTHER</c> and a type a schema supplied itself arrive
    /// here for the same reason. There is nothing in the type to read, so the value's class decides.
    /// </para>
    /// <para>
    /// <b>It still recurses.</b> A value reaching here may be a collection, a map or a row, and each of
    /// those holds values that also have no declared type — a map in an <c>ANY</c> column has no key or
    /// value type either. So the contents are read the same way, one level at a time, and the element type
    /// of what comes back is measured from the values because nothing declares it. That measurement is the
    /// only thing available here and is exactly what a declared type replaces everywhere else.
    /// </para>
    /// <para>
    /// <b>No Java object leaves.</b> That is the rule this class exists to keep, and the reason it lists
    /// every class Calcite's runtime can produce rather than falling through: a value with no case would be
    /// handed back as the Java object it is. The last arm hands back what it was given, which is the only
    /// answer for a class nothing corresponds to — a user-defined function returning its own type reaches
    /// it — and a value that is already a .NET one falls through it unchanged, which is what a table of
    /// this runtime supplies.
    /// </para>
    /// </remarks>
    public sealed class AnyClrTypeMapping : ClrTypeMapping
    {

        readonly ClrTypeContext _context;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The type that says nothing, which is <c>ANY</c> or one like it.</param>
        public AnyClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            base(context, relType, typeof(object))
        {
            _context = context;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The value's own class decides here too: there is no declared type to write it as, so what goes
        /// in is whatever Calcite holds a value of that .NET type as.
        /// </remarks>
        public override object? ToCalcite(object value)
        {
            return CalciteValues.ToShape(value);
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            return Read(value);
        }

        /// <summary>
        /// Reads a value by its runtime class, descending into anything that holds other values.
        /// </summary>
        /// <param name="value">The value as Calcite produced it.</param>
        /// <returns>The .NET value.</returns>
        object? Read(object? value)
        {
            switch (value)
            {
                case null:
                    return null;

                // a variant carries its payload's type with it, which is the whole of what it is for, and
                // is read through the chain so a caller's mapping applies inside one
                case org.apache.calcite.runtime.variant.VariantValue variant:
                    return _context.Registry.RequireMapping(null,
                        _context.TypeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARIANT)).FromCalcite(variant);

                case java.util.Map map:
                    return Entries(map);

                case java.util.Collection collection:
                    return Elements(collection);

                // an Object[] is heterogeneous by construction, so its elements are read and its shape is
                // not unified the way a collection's is
                case object[] row when value.GetType() == typeof(object[]):
                    return Fields(row);

                default:
                    return CalciteValues.FromShape(value);
            }
        }

        /// <summary>
        /// Returns a collection's elements, read and packed as the type they share.
        /// </summary>
        /// <param name="source"></param>
        /// <returns>The array.</returns>
        Array Elements(java.util.Collection source)
        {
            var items = new List<object?>(source.size());
            for (var i = source.iterator(); i.hasNext();)
                items.Add(Read(i.next()));

            return Pack(items);
        }

        /// <summary>
        /// Returns a row's fields, read one at a time.
        /// </summary>
        /// <param name="row"></param>
        /// <returns>The fields.</returns>
        /// <remarks>
        /// A row stays <c>object[]</c> however alike its fields happen to be: <c>ROW(1, 2)</c> is two fields
        /// and not an array of two, and unifying its element type would say otherwise.
        /// </remarks>
        object?[] Fields(object[] row)
        {
            var fields = new object?[row.Length];
            for (var i = 0; i < row.Length; i++)
                fields[i] = Read(row[i]);

            return fields;
        }

        /// <summary>
        /// Returns a map's entries, read and packed as the types they share.
        /// </summary>
        /// <param name="source"></param>
        /// <returns>A dictionary, or an array of pairs where a key is null.</returns>
        /// <remarks>
        /// A map holding a null key becomes an array of pairs: no dictionary the framework ships accepts
        /// one — <see cref="Dictionary{TKey, TValue}"/> throws for a null key whatever its key type is — and
        /// dropping the entry would lose a row's contents. Calcite reaches the case, as
        /// <c>MAP[CAST(NULL AS VARCHAR), 1]</c> validates and runs.
        /// </remarks>
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
        /// Returns read values as an array of the type they share.
        /// </summary>
        /// <param name="items"></param>
        /// <returns>The array.</returns>
        static Array Pack(List<object?> items)
        {
            var element = Unify(items);
            var array = Array.CreateInstance(element, items.Count);
            for (var i = 0; i < items.Count; i++)
                array.SetValue(items[i], i);

            return array;
        }

        /// <summary>
        /// Returns the type every value has, or <see cref="object"/> where they do not agree on one.
        /// </summary>
        /// <param name="items"></param>
        /// <returns>The shared type.</returns>
        /// <remarks>
        /// A null among values of a value type makes the type nullable rather than <see cref="object"/>, so
        /// a sequence holding a null still names what it holds. An empty sequence has no type to read and
        /// is <see cref="object"/>.
        /// </remarks>
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
