using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A chain of resolvers bound to a type factory, which answers type lookups, caches them, and converts
    /// values.
    /// </summary>
    /// <remarks>
    /// Each lookup's answer, including the absence of one, is cached for the life of the registry, so a
    /// resolver is asked about a given pair of types at most about once. Lookups are safe from multiple
    /// threads.
    /// </remarks>
    public sealed class ClrTypeRegistry
    {

        readonly JavaTypeFactory _typeFactory;
        readonly IClrTypeResolver[] _resolvers;
        readonly ClrTypeContext _context;

        readonly ConcurrentDictionary<Type, ClrTypeMapping?> _byClrType = new();

        /// <summary>
        /// Mappings by Calcite type, each with a short list of entries keyed on the CLR type asked for.
        /// </summary>
        /// <remarks>
        /// Keyed on the instance: Calcite's type factories intern every type they build through a static
        /// cache keyed on the type's digest, so equal types are the same instance, and comparing references
        /// avoids rebuilding the digest on every lookup. Were two instances ever to describe one type, each
        /// would get its own correct entry.
        /// </remarks>
        readonly ConcurrentDictionary<RelDataType, KeyValuePair<Type?, ClrTypeMapping?>[]> _byRelType = new(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory">The type factory lookups are answered against.</param>
        /// <param name="resolvers">The chain, in the order it is asked. The list is copied.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
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
        /// Resolves a mapping, or returns <see langword="null"/> where no resolver in the chain answers.
        /// </summary>
        /// <param name="clrType">The CLR type wanted, or <see langword="null"/> for the Calcite type's default.
        /// A <see cref="Nullable{T}"/> is looked up as its underlying type.</param>
        /// <param name="relType">The Calcite type, or <see langword="null"/> to look up the Calcite type
        /// <paramref name="clrType"/> is written as.</param>
        /// <returns>The mapping, or <see langword="null"/>.</returns>
        /// <exception cref="ArgumentException">Both arguments are <see langword="null"/>.</exception>
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType)
        {
            if (clrType is null && relType is null)
                throw new ArgumentException("A lookup carries at least one of a CLR type and a Calcite type.");

            if (clrType is not null)
                clrType = Nullable.GetUnderlyingType(clrType) ?? clrType;

            if (relType is null)
                return _byClrType.TryGetValue(clrType!, out var byType) ? byType : _byClrType[clrType!] = Resolve(clrType, null);

            // a Calcite type can be read as several CLR types; the list is short enough that a linear scan
            // beats a second dictionary
            if (_byRelType.TryGetValue(relType, out var entries))
                foreach (var entry in entries)
                    if (entry.Key == clrType)
                        return entry.Value;

            return Add(clrType, relType, entries);
        }

        /// <summary>
        /// Resolves a mapping and adds it to the Calcite type's entries, returning the entry another thread
        /// added first where there is one.
        /// </summary>
        /// <param name="clrType">The CLR type asked for, or <see langword="null"/> for the Calcite type's
        /// default.</param>
        /// <param name="relType">The Calcite type the entry is cached under.</param>
        /// <param name="entries">The entries the caller found for <paramref name="relType"/>, or
        /// <see langword="null"/> if there were none.</param>
        /// <returns>The cached mapping for the pair, which is <see langword="null"/> where no resolver
        /// answers.</returns>
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
        /// Asks each resolver in turn and returns the first non-null answer.
        /// </summary>
        /// <param name="clrType">The CLR type asked for, or <see langword="null"/> for the Calcite type's
        /// default.</param>
        /// <param name="relType">The Calcite type, or <see langword="null"/> to let a resolver choose one for
        /// <paramref name="clrType"/>.</param>
        /// <returns>The first resolver's mapping, or <see langword="null"/> if every resolver declines.</returns>
        ClrTypeMapping? Resolve(Type? clrType, RelDataType? relType)
        {
            foreach (var resolver in _resolvers)
                if (resolver.GetMapping(clrType, relType, _context) is ClrTypeMapping mapping)
                    return mapping;

            return null;
        }

        /// <summary>
        /// Resolves a mapping, throwing where no resolver in the chain answers.
        /// </summary>
        /// <param name="clrType">The CLR type wanted, or <see langword="null"/> for the Calcite type's
        /// default.</param>
        /// <param name="relType">The Calcite type, or <see langword="null"/> to look up the Calcite type
        /// <paramref name="clrType"/> is written as.</param>
        /// <returns>The mapping.</returns>
        /// <exception cref="ArgumentException">Both arguments are <see langword="null"/>.</exception>
        /// <exception cref="ClrTypeMappingException">No resolver answers.</exception>
        public ClrTypeMapping RequireMapping(Type? clrType, RelDataType? relType)
        {
            return GetMapping(clrType, relType) ?? throw new ClrTypeMappingException(Describe(clrType, relType));
        }

        /// <summary>
        /// Returns the message for a lookup that found nothing.
        /// </summary>
        /// <param name="clrType">The CLR type that was asked for, or <see langword="null"/>.</param>
        /// <param name="relType">The Calcite type that was asked for, or <see langword="null"/>.</param>
        /// <returns>A message naming whichever of the two types was given.</returns>
        static string Describe(Type? clrType, RelDataType? relType)
        {
            if (clrType is null)
                return $"No mapping presents {relType} as a CLR type.";
            if (relType is null)
                return $"No mapping carries a {clrType} into Calcite.";

            return $"No mapping presents {relType} as a {clrType}.";
        }

        /// <summary>
        /// Returns the CLR type a Calcite type is read as by default.
        /// </summary>
        /// <param name="relType">The Calcite type.</param>
        /// <returns>The CLR type, or <see cref="object"/> where no resolver answers.</returns>
        public Type GetClrType(RelDataType relType)
        {
            ArgumentNullException.ThrowIfNull(relType);

            return GetMapping(null, relType)?.ClrType ?? typeof(object);
        }

        /// <summary>
        /// Converts a CLR value to the class Calcite holds it in.
        /// </summary>
        /// <param name="clrType">The CLR type the value is written as, or <see langword="null"/>. Where both this
        /// and <paramref name="relType"/> are <see langword="null"/>, the value's runtime type is used.</param>
        /// <param name="relType">The Calcite type it is written to, or <see langword="null"/> for the Calcite
        /// type the CLR type is written as.</param>
        /// <param name="value">The value; <see langword="null"/> and <see cref="DBNull"/> convert to
        /// <see langword="null"/>.</param>
        /// <returns>The converted value.</returns>
        /// <exception cref="ClrTypeMappingException">No mapping answers, or the mapping produces a value of
        /// the wrong class.</exception>
        public object? ToCalcite(Type? clrType, RelDataType? relType, object? value)
        {
            if (value is null || value is DBNull)
                return null;

            clrType ??= relType is null ? value.GetType() : null;

            return RequireMapping(clrType, relType).ConvertToCalcite(value);
        }

        /// <summary>
        /// Converts a value of the class Calcite holds a type in to a CLR value.
        /// </summary>
        /// <param name="clrType">The CLR type wanted, or <see langword="null"/> for the Calcite type's
        /// default.</param>
        /// <param name="relType">The Calcite type of the value.</param>
        /// <param name="value">The value; <see langword="null"/> and <see cref="DBNull"/> convert to
        /// <see langword="null"/>.</param>
        /// <returns>The converted value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="relType"/> is <see langword="null"/>.</exception>
        /// <exception cref="ClrTypeMappingException">No mapping presents <paramref name="relType"/> as
        /// <paramref name="clrType"/>.</exception>
        public object? FromCalcite(Type? clrType, RelDataType relType, object? value)
        {
            ArgumentNullException.ThrowIfNull(relType);

            if (value is null || value is DBNull)
                return null;

            return RequireMapping(clrType, relType).FromCalcite(value);
        }

    }

}
