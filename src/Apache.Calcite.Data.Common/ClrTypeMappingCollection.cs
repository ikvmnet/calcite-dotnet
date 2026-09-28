using System;
using System.Collections.Frozen;
using System.Collections.Generic;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// An ordered table of mappings that serves as a resolver.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A lookup is answered by the first entry, in the order entries were added, that accepts it and whose
    /// factory does not decline. Where the lookup names both a CLR type and a Calcite type, any entry that
    /// accepts both answers. Where it names only one, an entry answers only if its <see cref="ClrTypeMatch"/>
    /// makes it the default in that direction.
    /// </para>
    /// <para>
    /// Add every entry before the collection is first used as a resolver: <c>Add</c> is not safe to call
    /// concurrently with a lookup.
    /// </para>
    /// </remarks>
    public sealed class ClrTypeMappingCollection : IClrTypeResolver
    {

        /// <summary>
        /// One row of the table.
        /// </summary>
        readonly struct Entry
        {

            public required Type ClrType { get; init; }

            public required SqlTypeName SqlTypeName { get; init; }

            public required ClrTypeMappingFactory Factory { get; init; }

            public ClrTypeMatch Match { get; init; }

            public int Precision { get; init; }

            public int Scale { get; init; }

            public Func<Type?, bool>? ClrTypePredicate { get; init; }

            public Func<RelDataType, bool>? RelTypePredicate { get; init; }

            /// <summary>
            /// Whether the entry accepts the CLR type, an absent one counting as accepted.
            /// </summary>
            public bool AcceptsClrType(Type? clrType)
            {
                return ClrTypePredicate is Func<Type?, bool> predicate ? predicate(clrType) : clrType is null || ClrType == clrType;
            }

            /// <summary>
            /// Whether the entry accepts the Calcite type.
            /// </summary>
            public bool AcceptsRelType(RelDataType relType)
            {
                return RelTypePredicate is Func<RelDataType, bool> predicate ? predicate(relType) : relType.getSqlTypeName() == SqlTypeName;
            }

            /// <summary>
            /// Builds the Calcite type the entry is written for, where the lookup did not carry one.
            /// </summary>
            public RelDataType CreateRelType(ClrTypeContext context)
            {
                var typeFactory = context.TypeFactory;
                var type = Precision < 0 ? typeFactory.createSqlType(SqlTypeName)
                    : Scale < 0 ? typeFactory.createSqlType(SqlTypeName, Precision)
                    : typeFactory.createSqlType(SqlTypeName, Precision, Scale);

                // nullable: a bare CLR type carries no NOT NULL constraint
                return typeFactory.createTypeWithNullability(type, true);
            }

        }

        readonly List<Entry> _entries = [];

        /// <summary>
        /// Entry positions by the SQL type name of entries that match on the type name rather than a predicate.
        /// </summary>
        /// <remarks>
        /// Positions rather than entries, because the order entries were added is their priority: each list
        /// is ascending, as is <see cref="_predicated"/>, so <see cref="Candidates"/> merges the two in order.
        /// </remarks>
        readonly Dictionary<string, List<int>> _byTypeName = [];

        /// <summary>
        /// A frozen copy of <see cref="_byTypeName"/>, built on the first lookup and discarded by
        /// <c>Add</c>.
        /// </summary>
        FrozenDictionary<string, List<int>>? _frozen;

        /// <summary>
        /// Entry positions of the entries that accept a Calcite type by predicate, which are candidates for
        /// every lookup.
        /// </summary>
        readonly List<int> _predicated = [];

        /// <summary>
        /// Entry positions of the <see cref="ClrTypeMatch.ClrDefault"/> entries, which are the candidates for a
        /// lookup naming no Calcite type.
        /// </summary>
        readonly List<int> _clrDefaults = [];

        /// <summary>
        /// Returns the positions a lookup for a Calcite type considers, in the order they were added.
        /// </summary>
        IEnumerable<int> Candidates(RelDataType relType)
        {
            var index = _frozen ??= _byTypeName.ToFrozenDictionary();

            var name = relType.getSqlTypeName();
            var named = name is not null && index.TryGetValue(name.name(), out var list) ? list : null;

            if (named is null)
                return _predicated;
            if (_predicated.Count == 0)
                return named;

            return Merge(named, _predicated);
        }

        /// <summary>
        /// Walks two ascending position lists as one ascending sequence.
        /// </summary>
        static IEnumerable<int> Merge(List<int> left, List<int> right)
        {
            int i = 0, j = 0;

            while (i < left.Count && j < right.Count)
                yield return left[i] <= right[j] ? left[i++] : right[j++];

            while (i < left.Count)
                yield return left[i++];

            while (j < right.Count)
                yield return right[j++];
        }

        /// <summary>
        /// Adds an entry whose mapping converts with two delegates.
        /// </summary>
        /// <param name="clrType">The CLR type the mapping presents the Calcite type as.</param>
        /// <param name="sqlTypeName">The type name of the Calcite type the mapping is for.</param>
        /// <param name="toCalcite">Converts a non-null CLR value to the class Calcite holds the type in.</param>
        /// <param name="fromCalcite">Converts a non-null value of that class to <paramref name="clrType"/>.</param>
        /// <param name="match">Which lookups naming only one type the entry answers. Defaults to
        /// <see cref="ClrTypeMatch.Default"/>.</param>
        /// <param name="precision">The precision of the Calcite type built for a lookup that names only a CLR
        /// type, or -1 for none.</param>
        /// <param name="scale">The scale of that Calcite type, or -1 for none; used only with a precision.</param>
        /// <param name="clrTypePredicate">Decides which CLR types the entry accepts, in place of comparing with
        /// <paramref name="clrType"/>.</param>
        /// <param name="relTypePredicate">Decides which Calcite types the entry accepts, in place of comparing
        /// the type name with <paramref name="sqlTypeName"/>.</param>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public void Add(
            Type clrType,
            SqlTypeName sqlTypeName,
            Func<object, object?> toCalcite,
            Func<object, object?> fromCalcite,
            ClrTypeMatch match = ClrTypeMatch.Default,
            int precision = -1,
            int scale = -1,
            Func<Type?, bool>? clrTypePredicate = null,
            Func<RelDataType, bool>? relTypePredicate = null)
        {
            ArgumentNullException.ThrowIfNull(clrType);
            ArgumentNullException.ThrowIfNull(sqlTypeName);
            ArgumentNullException.ThrowIfNull(toCalcite);
            ArgumentNullException.ThrowIfNull(fromCalcite);

            Add(clrType, sqlTypeName, (context, relType, resolved) => new DelegateClrTypeMapping(context, relType, resolved, toCalcite, fromCalcite), match, precision, scale, clrTypePredicate, relTypePredicate);
        }

        /// <summary>
        /// Adds an entry whose mapping is built by a factory.
        /// </summary>
        /// <param name="clrType">The CLR type the mapping presents the Calcite type as. For a lookup naming
        /// only a Calcite type, this is the CLR type passed to <paramref name="factory"/>.</param>
        /// <param name="sqlTypeName">The type name of the Calcite type the mapping is for.</param>
        /// <param name="factory">Builds the mapping, or declines by returning <see langword="null"/>.</param>
        /// <param name="match">Which lookups naming only one type the entry answers. Defaults to
        /// <see cref="ClrTypeMatch.Default"/>.</param>
        /// <param name="precision">The precision of the Calcite type built for a lookup that names only a CLR
        /// type, or -1 for none.</param>
        /// <param name="scale">The scale of that Calcite type, or -1 for none; used only with a precision.</param>
        /// <param name="clrTypePredicate">Decides which CLR types the entry accepts, in place of comparing with
        /// <paramref name="clrType"/>.</param>
        /// <param name="relTypePredicate">Decides which Calcite types the entry accepts, in place of comparing
        /// the type name with <paramref name="sqlTypeName"/>.</param>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public void Add(
            Type clrType,
            SqlTypeName sqlTypeName,
            ClrTypeMappingFactory factory,
            ClrTypeMatch match = ClrTypeMatch.Default,
            int precision = -1,
            int scale = -1,
            Func<Type?, bool>? clrTypePredicate = null,
            Func<RelDataType, bool>? relTypePredicate = null)
        {
            ArgumentNullException.ThrowIfNull(clrType);
            ArgumentNullException.ThrowIfNull(sqlTypeName);
            ArgumentNullException.ThrowIfNull(factory);

            var position = _entries.Count;

            _entries.Add(new Entry
            {
                ClrType = clrType,
                SqlTypeName = sqlTypeName,
                Factory = factory,
                Match = match,
                Precision = precision,
                Scale = scale,
                ClrTypePredicate = clrTypePredicate,
                RelTypePredicate = relTypePredicate,
            });

            // an entry that accepts by predicate can match any type name, so it cannot be bucketed by one
            if (relTypePredicate is not null)
                _predicated.Add(position);
            else
            {
                if (_byTypeName.TryGetValue(sqlTypeName.name(), out var bucket) == false)
                    _byTypeName[sqlTypeName.name()] = bucket = [];

                bucket.Add(position);
            }

            if (match.HasFlag(ClrTypeMatch.ClrDefault))
                _clrDefaults.Add(position);

            _frozen = null;
        }

        /// <inheritdoc />
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (clrType is null && relType is null)
                throw new ArgumentException("A lookup carries at least one of a CLR type and a Calcite type.");

            // only the CLR type: only an entry that is that type's default answers
            if (relType is null)
            {
                foreach (var position in _clrDefaults)
                {
                    var entry = _entries[position];
                    if (entry.AcceptsClrType(clrType) && entry.Factory(context, entry.CreateRelType(context), clrType!) is { } mapping)
                        return mapping;
                }

                return null;
            }

            foreach (var position in Candidates(relType))
            {
                var entry = _entries[position];

                // both named: any entry accepting both answers, unless its factory declines
                if (clrType is not null)
                {
                    if (entry.AcceptsClrType(clrType) && entry.AcceptsRelType(relType) && entry.Factory(context, relType, clrType) is { } named)
                        return named;

                    continue;
                }

                // only the Calcite type: only an entry that is that type's default answers
                if (entry.Match.HasFlag(ClrTypeMatch.RelDefault) && entry.AcceptsRelType(relType) && entry.Factory(context, relType, entry.ClrType) is { } fallback)
                    return fallback;
            }

            return null;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The types are in the order their entries were added, without duplicates. An entry that accepts CLR
        /// types by predicate is left out, because the types it accepts cannot be listed.
        /// </remarks>
        public IEnumerable<Type> GetClrTypes(RelDataType relType, ClrTypeContext context)
        {
            ArgumentNullException.ThrowIfNull(relType);
            ArgumentNullException.ThrowIfNull(context);

            var seen = new List<Type>();

            foreach (var position in Candidates(relType))
            {
                var entry = _entries[position];

                if (entry.ClrTypePredicate is not null || entry.AcceptsRelType(relType) == false)
                    continue;

                if (seen.Contains(entry.ClrType) == false)
                {
                    seen.Add(entry.ClrType);
                    yield return entry.ClrType;
                }
            }
        }

    }

}
