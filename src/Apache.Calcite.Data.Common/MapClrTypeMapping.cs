using System;
using System.Collections;
using System.Collections.Generic;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The mapping for a <c>MAP</c>, which converts each key and value with the key and value types' mappings.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The key and value mappings are resolved through <see cref="ClrTypeContext.Registry"/>, so nested
    /// types and types added by a caller's resolver are handled the same way. A nullable key or value of a
    /// value type is presented as <see cref="Nullable{T}"/>.
    /// </para>
    /// <para>
    /// Where the declared key type is not nullable the map is read as a
    /// <see cref="Dictionary{TKey, TValue}"/>. Where it is nullable the map is read as an array of
    /// <see cref="KeyValuePair{TKey, TValue}"/>, because <see cref="Dictionary{TKey, TValue}"/> does not accept
    /// a null key and Calcite can produce one (<c>MAP[CAST(NULL AS VARCHAR), 1]</c>). The choice follows the
    /// declared type, so every row of a column has the same .NET type.
    /// </para>
    /// <para>
    /// A value written to Calcite may be an <see cref="IDictionary"/> or a sequence of objects with
    /// <c>Key</c> and <c>Value</c> properties, such as <see cref="KeyValuePair{TKey, TValue}"/>. Calcite holds
    /// the map in a <c>java.util.LinkedHashMap</c>, so entries keep the order they were written in.
    /// </para>
    /// </remarks>
    public sealed class MapClrTypeMapping : ClrTypeMapping
    {

        /// <summary>
        /// Resolves the key type's default mapping.
        /// </summary>
        /// <exception cref="ClrTypeMappingException">The map has no key type, or the key type has no
        /// mapping.</exception>
        /// <param name="context">The context whose registry resolves the key mapping.</param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <returns>The key type's default mapping.</returns>
        static ClrTypeMapping Key(ClrTypeContext context, RelDataType relType)
        {
            return context.Registry.RequireMapping(null, relType.getKeyType()
                ?? throw new ClrTypeMappingException($"{relType} is a map with no key type."));
        }

        /// <summary>
        /// Resolves the value type's default mapping.
        /// </summary>
        /// <exception cref="ClrTypeMappingException">The map has no value type, or the value type has no
        /// mapping.</exception>
        /// <param name="context">The context whose registry resolves the value mapping.</param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <returns>The value type's default mapping.</returns>
        static ClrTypeMapping Value(ClrTypeContext context, RelDataType relType)
        {
            return context.Registry.RequireMapping(null, relType.getValueType()
                ?? throw new ClrTypeMappingException($"{relType} is a map with no value type."));
        }

        /// <summary>
        /// Returns the .NET type of a key or value: the mapping's CLR type, made <see cref="Nullable{T}"/>
        /// where the Calcite type is nullable and the CLR type is a value type.
        /// </summary>
        /// <param name="mapping">The key or value mapping.</param>
        /// <returns>The mapping's CLR type, or its <see cref="Nullable{T}"/> form.</returns>
        static Type EntryClrType(ClrTypeMapping mapping)
        {
            var clrType = mapping.ClrType;

            return mapping.RelType.isNullable() && clrType.IsValueType ? typeof(Nullable<>).MakeGenericType(clrType) : clrType;
        }

        /// <summary>
        /// Returns the .NET type a map with these key and value mappings is read as.
        /// </summary>
        /// <returns>A <see cref="Dictionary{TKey, TValue}"/>, or an array of
        /// <see cref="KeyValuePair{TKey, TValue}"/> where the key type admits a null.</returns>
        /// <param name="key">The key mapping; the nullability of its Calcite type decides the shape.</param>
        /// <param name="value">The value mapping.</param>
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
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <exception cref="ClrTypeMappingException">The key or value type is missing or has no
        /// mapping.</exception>
        public MapClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            this(context, relType, Key(context, relType), Value(context, relType))
        {

        }

        /// <summary>
        /// Initializes a new instance from resolved key and value mappings, which the base constructor's CLR
        /// type is computed from.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>MAP</c> type.</param>
        /// <param name="key">The mapping each key is converted with.</param>
        /// <param name="value">The mapping each value is converted with.</param>
        MapClrTypeMapping(ClrTypeContext context, RelDataType relType, ClrTypeMapping key, ClrTypeMapping value) :
            base(context, relType, ShapeOf(key, value))
        {
            _key = key;
            _value = value;
            _pairs = key.RelType.isNullable();
        }

        /// <summary>
        /// Gets the mapping each key is converted with.
        /// </summary>
        public ClrTypeMapping KeyMapping => _key;

        /// <summary>
        /// Gets the mapping each value is converted with.
        /// </summary>
        public ClrTypeMapping ValueMapping => _value;

        /// <inheritdoc />
        /// <remarks>
        /// Builds a <c>LinkedHashMap</c>, as Calcite's <c>SqlFunctions.map</c> does, so that entries keep the
        /// order they were written in.
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
                    // any sequence of Key/Value pairs, so that the pair array a nullable-key map reads as can be
                    // written back
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
        /// Converts one key or value with <paramref name="mapping"/>, leaving a null as null.
        /// </summary>
        /// <param name="mapping">The key or value mapping.</param>
        /// <param name="value">The key or value as Calcite holds it, or <see langword="null"/>.</param>
        /// <returns>The converted value, or <see langword="null"/> for a null.</returns>
        static object? Read(ClrTypeMapping mapping, object? value)
        {
            return value is null ? null : mapping.FromCalcite(value);
        }

    }

}
