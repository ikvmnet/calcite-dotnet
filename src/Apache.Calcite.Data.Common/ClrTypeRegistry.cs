using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A <see cref="ClrTypeMapper"/> bound to a type factory, answering and caching lookups.
    /// </summary>
    /// <remarks>
    /// Resolution walks a chain and builds an object; conversion is a delegate call. Only the first is worth
    /// caching, and it is cached the way it is asked: a parameter carrying a bare CLR value asks by type, a
    /// result column asks by Calcite type, and one Calcite type is read as several CLR types, so the second
    /// cache holds a short list per type rather than a single entry.
    /// </remarks>
    public sealed class ClrTypeRegistry
    {

        readonly JavaTypeFactory _typeFactory;
        readonly IClrTypeResolver[] _resolvers;
        readonly ClrTypeContext _context;

        readonly ConcurrentDictionary<Type, ClrTypeMapping?> _byClrType = new();

        /// <summary>
        /// Mappings by the Calcite type they are for, keyed on the instance.
        /// </summary>
        /// <remarks>
        /// <b>A <see cref="RelDataType"/> is interned, so the instance is the identity.</b>
        /// <c>RelDataTypeFactoryImpl</c> canonizes every type it builds through a cache keyed on the
        /// type's digest, and that cache is static — measured, two separately constructed
        /// <c>JavaTypeFactoryImpl</c>s answer the same instance for <c>INTEGER</c>, and a type taken off a
        /// plan's row type is the same instance a factory hands back for the same description.
        ///
        /// <para>Keying on <c>getFullTypeString()</c> instead would build that digest again on every lookup
        /// and then hash the string. Measured back to back over 10,000,000 lookups, the whole of
        /// <see cref="GetMapping"/> is 9.4 to 18.1 ns by reference against 27.7 to 44.1 by digest, and the
        /// identity hash alone is 1.5 ns where the type's own <c>hashCode</c> is 19.2.</para>
        ///
        /// <para>Interning is what makes it fast and not what makes it correct. Were two instances ever to
        /// describe one type, each would resolve its own entry and both would be right; the cost would be a
        /// second entry, and the digest key it replaces already kept one entry per distinct type for the
        /// life of the registry.</para>
        /// </remarks>
        readonly ConcurrentDictionary<RelDataType, KeyValuePair<Type?, ClrTypeMapping?>[]> _byRelType = new(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory">The factory this registry answers against.</param>
        /// <param name="resolvers">The chain, in the order it is asked.</param>
        /// <remarks>
        /// Public, because a chain is fixed somewhere before this is built — a data source settles one when
        /// it is built and holds it fixed thereafter — and whatever fixed it should be able to bind it
        /// without going back through a <see cref="ClrTypeMapper"/>, which is the thing for assembling a
        /// chain rather than for holding one.
        /// </remarks>
        public ClrTypeRegistry(JavaTypeFactory typeFactory, IReadOnlyList<IClrTypeResolver> resolvers)
        {
            _typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
            _resolvers = [.. resolvers ?? throw new ArgumentNullException(nameof(resolvers))];
            _context = new ClrTypeContext(typeFactory, this);
        }

        /// <summary>
        /// Gets the type factory this registry answers against.
        /// </summary>
        public JavaTypeFactory TypeFactory => _typeFactory;

        /// <summary>
        /// Resolves a mapping, or answers <see langword="null"/> where the chain has none.
        /// </summary>
        /// <param name="clrType">The CLR type wanted, or <see langword="null"/> for whichever the Calcite type answers by default.</param>
        /// <param name="relType">The Calcite type in play, or <see langword="null"/> where it is not yet decided.</param>
        /// <returns></returns>
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType)
        {
            if (clrType is null && relType is null)
                throw new ArgumentException("A lookup carries at least one of a CLR type and a Calcite type.");

            if (clrType is not null)
                clrType = Nullable.GetUnderlyingType(clrType) ?? clrType;

            if (relType is null)
                return _byClrType.TryGetValue(clrType!, out var byType) ? byType : _byClrType[clrType!] = Resolve(clrType, null);

            // one Calcite type is read as several CLR types, so the entry is a list rather than a value; it
            // is short enough that a scan beats a second dictionary
            if (_byRelType.TryGetValue(relType, out var entries))
                foreach (var entry in entries)
                    if (entry.Key == clrType)
                        return entry.Value;

            return Add(clrType, relType, entries);
        }

        /// <summary>
        /// Resolves and records a mapping keyed on the Calcite type.
        /// </summary>
        /// <param name="clrType"></param>
        /// <param name="relType"></param>
        /// <param name="entries"></param>
        /// <returns></returns>
        ClrTypeMapping? Add(Type? clrType, RelDataType relType, KeyValuePair<Type?, ClrTypeMapping?>[]? entries)
        {
            var mapping = Resolve(clrType, relType);

            while (true)
            {
                var existing = _byRelType.TryGetValue(relType, out var current) ? current : null;
                if (existing is not null)
                    foreach (var entry in existing)
                        if (entry.Key == clrType)
                            return entry.Value;

                var updated = new KeyValuePair<Type?, ClrTypeMapping?>[(existing?.Length ?? 0) + 1];
                existing?.CopyTo(updated, 0);
                updated[^1] = new KeyValuePair<Type?, ClrTypeMapping?>(clrType, mapping);

                if (existing is null ? _byRelType.TryAdd(relType, updated) : _byRelType.TryUpdate(relType, updated, existing))
                    return mapping;
            }
        }

        /// <summary>
        /// Walks the chain, first answer winning.
        /// </summary>
        /// <param name="clrType"></param>
        /// <param name="relType"></param>
        /// <returns></returns>
        ClrTypeMapping? Resolve(Type? clrType, RelDataType? relType)
        {
            foreach (var resolver in _resolvers)
                if (resolver.GetMapping(clrType, relType, _context) is ClrTypeMapping mapping)
                    return mapping;

            return null;
        }

        /// <summary>
        /// Resolves a mapping, throwing where the chain has none.
        /// </summary>
        /// <param name="clrType"></param>
        /// <param name="relType"></param>
        /// <returns></returns>
        /// <exception cref="ClrTypeMappingException"></exception>
        public ClrTypeMapping RequireMapping(Type? clrType, RelDataType? relType)
        {
            return GetMapping(clrType, relType) ?? throw new ClrTypeMappingException(Describe(clrType, relType));
        }

        /// <summary>
        /// Describes a lookup that found nothing.
        /// </summary>
        /// <param name="clrType"></param>
        /// <param name="relType"></param>
        /// <returns></returns>
        static string Describe(Type? clrType, RelDataType? relType)
        {
            if (clrType is null)
                return $"No mapping presents {relType} as a CLR type.";
            if (relType is null)
                return $"No mapping carries a {clrType} into Calcite.";

            return $"No mapping presents {relType} as a {clrType}.";
        }

        /// <summary>
        /// Returns the CLR type a Calcite type is seen as by default.
        /// </summary>
        /// <param name="relType"></param>
        /// <returns></returns>
        public Type GetClrType(RelDataType relType)
        {
            ArgumentNullException.ThrowIfNull(relType);

            return GetMapping(null, relType)?.ClrType ?? typeof(object);
        }

        /// <summary>
        /// Converts a CLR value to the representation Calcite holds it in.
        /// </summary>
        /// <param name="clrType">The type the value is being written as, or <see langword="null"/> to take it from the value.</param>
        /// <param name="relType">The Calcite type it is being written to, or <see langword="null"/> where the CLR type decides.</param>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="ClrTypeMappingException"></exception>
        public object? ToCalcite(Type? clrType, RelDataType? relType, object? value)
        {
            if (value is null || value is DBNull)
                return null;

            clrType ??= relType is null ? value.GetType() : null;

            return RequireMapping(clrType, relType).ConvertToCalcite(value);
        }

        /// <summary>
        /// Converts the representation Calcite holds a value in to a CLR value.
        /// </summary>
        /// <param name="clrType">The type wanted, or <see langword="null"/> for whichever the Calcite type answers by default.</param>
        /// <param name="relType">The Calcite type of the value.</param>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="ClrTypeMappingException"></exception>
        public object? FromCalcite(Type? clrType, RelDataType relType, object? value)
        {
            ArgumentNullException.ThrowIfNull(relType);

            if (value is null || value is DBNull)
                return null;

            return RequireMapping(clrType, relType).FromCalcite(value);
        }

    }

}
