using System;
using System.Collections;
using System.Collections.Generic;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A <c>MAP</c>, mapped by mapping its key and its value and wrapping the result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Recursive for the same reason <see cref="CollectionClrTypeMapping"/> is: the key and the value are
    /// resolved through the registry, so a map of arrays, or of a type a caller registered, needs nothing
    /// here.
    /// </para>
    /// <para>
    /// <b>A nullable key decides the .NET shape.</b> No dictionary the framework ships accepts a null key —
    /// <see cref="Dictionary{TKey, TValue}"/> throws for one whatever its key type is — and Calcite
    /// validates and runs <c>MAP[CAST(NULL AS VARCHAR), 1]</c>. So a map whose key type admits a null
    /// materializes as an array of pairs and one whose key type does not materializes as a dictionary.
    /// Reading the declared type rather than the keys in hand is what makes the .NET type this mapping
    /// promises hold for every row, including the row where no key happened to be null.
    /// </para>
    /// </remarks>
    public sealed class MapClrTypeMapping : ClrTypeMapping
    {

        /// <summary>
        /// Resolves the key's mapping through the registry, which is half of the recursion.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <returns>The mapping a key is carried across by.</returns>
        /// <exception cref="ClrTypeMappingException">Where the map states no key type.</exception>
        static ClrTypeMapping Key(ClrTypeContext context, RelDataType relType)
        {
            return context.Registry.RequireMapping(null, relType.getKeyType()
                ?? throw new ClrTypeMappingException($"{relType} is a map with no key type."));
        }

        /// <summary>
        /// Resolves the value's mapping through the registry, which is the other half.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <returns>The mapping a value is carried across by.</returns>
        /// <exception cref="ClrTypeMappingException">Where the map states no value type.</exception>
        static ClrTypeMapping Value(ClrTypeContext context, RelDataType relType)
        {
            return context.Registry.RequireMapping(null, relType.getValueType()
                ?? throw new ClrTypeMappingException($"{relType} is a map with no value type."));
        }

        /// <summary>
        /// Returns the .NET type a key or a value materializes as, carrying its nullability.
        /// </summary>
        /// <param name="mapping"></param>
        /// <returns>The type, made <see cref="Nullable{T}"/> where the entry admits a null and the type is
        /// a value type.</returns>
        /// <remarks>
        /// A dictionary's own value type is the only place a null value can live, there being no
        /// <see cref="DBNull"/> inside one, so nullability belongs in the type here for the same reason it
        /// does in a collection's element.
        /// </remarks>
        static Type EntryClrType(ClrTypeMapping mapping)
        {
            var clrType = mapping.ClrType;

            return mapping.RelType.isNullable() && clrType.IsValueType ? typeof(Nullable<>).MakeGenericType(clrType) : clrType;
        }

        /// <summary>
        /// Returns the .NET type a map of these keys and values materializes as.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>A <see cref="Dictionary{TKey, TValue}"/>, or an array of
        /// <see cref="KeyValuePair{TKey, TValue}"/> where the key type admits a null.</returns>
        static Type ShapeOf(ClrTypeMapping key, ClrTypeMapping value)
        {
            var k = EntryClrType(key);
            var v = EntryClrType(value);

            return key.RelType.isNullable()
                ? typeof(KeyValuePair<,>).MakeGenericType(k, v).MakeArrayType()
                : typeof(Dictionary<,>).MakeGenericType(k, v);
        }

        readonly ClrTypeMapping _key;
        readonly ClrTypeMapping _value;
        readonly bool _pairs;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        public MapClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            this(context, relType, Key(context, relType), Value(context, relType))
        {

        }

        /// <summary>
        /// Initializes a new instance from mappings already resolved.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <param name="key">The mapping a key is carried across by.</param>
        /// <param name="value">The mapping a value is carried across by.</param>
        /// <remarks>
        /// Private because the base constructor needs the .NET type up front, and that type is computed
        /// from the two mappings: they have to be resolved before the chain to <c>base</c> can be written,
        /// and resolving them twice would build each twice.
        /// </remarks>
        MapClrTypeMapping(ClrTypeContext context, RelDataType relType, ClrTypeMapping key, ClrTypeMapping value) :
            base(context, relType, ShapeOf(key, value))
        {
            _key = key;
            _value = value;
            _pairs = key.RelType.isNullable();
        }

        /// <summary>
        /// Gets the mapping a key is carried across by.
        /// </summary>
        public ClrTypeMapping KeyMapping => _key;

        /// <summary>
        /// Gets the mapping a value is carried across by.
        /// </summary>
        public ClrTypeMapping ValueMapping => _value;

        /// <inheritdoc />
        /// <remarks>
        /// A <c>LinkedHashMap</c> because Calcite's own <c>SqlFunctions.map</c> builds one: the entries of a
        /// map come out in the order they went in, and a <c>HashMap</c> would reorder a value on its way
        /// through a parameter.
        /// </remarks>
        public override object? ToCalcite(object value)
        {
            var map = new java.util.LinkedHashMap();

            switch (value)
            {
                case IDictionary dictionary:
                    for (var i = dictionary.GetEnumerator(); i.MoveNext();)
                        map.put(i.Key is null ? null : _key.ToCalcite(i.Key), i.Value is null ? null : _value.ToCalcite(i.Value));

                    return map;

                case IEnumerable pairs:
                    // the pairs an array of KeyValuePair reads back as, so what came out can go back in
                    foreach (var pair in pairs)
                    {
                        if (pair is null)
                            continue;

                        var type = pair.GetType();
                        var k = type.GetProperty("Key")?.GetValue(pair);
                        var v = type.GetProperty("Value")?.GetValue(pair);
                        map.put(k is null ? null : _key.ToCalcite(k), v is null ? null : _value.ToCalcite(v));
                    }

                    return map;

                default:
                    throw new ClrTypeMappingException($"A {RelType} is written from a dictionary or a sequence of pairs, and a {value.GetType()} is neither.");
            }
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            if (value is not java.util.Map source)
                throw new ClrTypeMappingException($"A {RelType} is held in a java.util.Map, and a {value.GetType()} is not one.");

            var arguments = ClrType.IsArray ? ClrType.GetElementType()!.GetGenericArguments() : ClrType.GetGenericArguments();

            if (_pairs)
            {
                var pairType = ClrType.GetElementType()!;
                var pairs = Array.CreateInstance(pairType, source.size());

                var n = 0;
                for (var i = source.entrySet().iterator(); i.hasNext(); n++)
                {
                    var entry = (java.util.Map.Entry)i.next();
                    pairs.SetValue(Activator.CreateInstance(pairType, Read(_key, entry.getKey()), Read(_value, entry.getValue())), n);
                }

                return pairs;
            }

            var dictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(arguments), source.size())!;
            for (var i = source.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                var key = Read(_key, entry.getKey())
                    ?? throw new ClrTypeMappingException($"A {RelType} declares its keys not null and one was.");

                dictionary[key] = Read(_value, entry.getValue());
            }

            return dictionary;
        }

        /// <summary>
        /// Reads one key or value back, a null staying null rather than reaching a conversion.
        /// </summary>
        /// <param name="mapping"></param>
        /// <param name="value"></param>
        /// <returns>The converted value, or <see langword="null"/>.</returns>
        static object? Read(ClrTypeMapping mapping, object? value)
        {
            return value is null ? null : mapping.FromCalcite(value);
        }

    }

}
