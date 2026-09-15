using System;
using System.Collections.Frozen;
using System.Collections.Generic;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Builds a mapping once the lookup has settled which Calcite type and which CLR type are in play.
    /// </summary>
    /// <param name="context"></param>
    /// <param name="relType"></param>
    /// <param name="clrType"></param>
    /// <returns></returns>
    public delegate ClrTypeMapping ClrTypeMappingFactory(ClrTypeContext context, RelDataType relType, Type clrType);

    /// <summary>
    /// A table of mappings, and the rule by which a lookup picks one. Serves as a resolver on its own.
    /// </summary>
    /// <remarks>
    /// The rule is the interesting part and it is one method. Where both keys are named, an entry answers
    /// if it accepts both — so a conversion that is legal only when asked for is written once and is
    /// nobody's default. Where one key is missing, the entry answers only if it claims to be the default in
    /// that direction, and the first such entry wins, so order in the table is the priority. Which .NET
    /// type a column reads back as and which .NET types are merely accepted for it are therefore the same
    /// table rather than two that can drift.
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

                // nullable, so that the representation is the box either way: getJavaClass answers int.class
                // for a NOT NULL INTEGER and Integer.class for a nullable one, and a value that has left the
                // plan is a reference regardless
                return typeFactory.createTypeWithNullability(type, true);
            }

        }

        readonly List<Entry> _entries = [];

        /// <summary>
        /// Entry positions by the SQL type name an entry names, for the entries that name one outright.
        /// </summary>
        /// <remarks>
        /// <b>Positions and not entries, because order is priority.</b> The first entry written for a Calcite
        /// type is what that type reads back as, so a lookup has to consider candidates in the order they
        /// were added even though it reaches them through two collections. Every bucket is ascending and
        /// <see cref="_predicated"/> is ascending, so merging them is one walk with two cursors.
        ///
        /// <para>An entry whose Calcite type is decided by a predicate cannot be bucketed: it accepts types
        /// it never names. Those go in <see cref="_predicated"/> and are considered against every lookup,
        /// which is what a catch-all is for and is why there are few of them.</para>
        /// </remarks>
        readonly Dictionary<string, List<int>> _byTypeName = [];

        /// <summary>
        /// <see cref="_byTypeName"/> frozen, built on the first lookup and dropped by the next
        /// <see cref="Add"/>.
        /// </summary>
        /// <remarks>
        /// <b>Built once and read per cache miss, which is what a frozen dictionary is for.</b> The
        /// built-in table is filled in a static constructor and never touched again, so every lookup after
        /// that reads a dictionary nobody is writing; freezing it trades a one-off build for a faster read
        /// of exactly that shape. A caller adding an entry afterwards drops it rather than rebuilding
        /// eagerly, so configuring a table stays cheap and the cost lands on the first lookup after.
        /// </remarks>
        FrozenDictionary<string, List<int>>? _frozen;

        /// <summary>
        /// Entry positions for the entries whose Calcite type is decided by a predicate.
        /// </summary>
        readonly List<int> _predicated = [];

        /// <summary>
        /// Entry positions for the entries that can answer a lookup carrying no Calcite type, which is a
        /// value being written on the strength of its CLR type alone.
        /// </summary>
        readonly List<int> _clrDefaults = [];

        /// <summary>
        /// Returns the positions a lookup for a Calcite type must consider, in the order they were added.
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
        /// Adds a mapping.
        /// </summary>
        /// <param name="clrType">The CLR type the mapping presents the Calcite type as.</param>
        /// <param name="sqlTypeName">The Calcite type the mapping is for.</param>
        /// <param name="toCalcite">Converts a CLR value to the representation Calcite holds it in.</param>
        /// <param name="fromCalcite">Converts that representation back to <paramref name="clrType"/>.</param>
        /// <param name="match">When the entry is willing to answer. Defaults to <see cref="ClrTypeMatch.Default"/>.</param>
        /// <param name="precision">Precision of the Calcite type where the lookup does not carry one.</param>
        /// <param name="scale">Scale of the Calcite type where the lookup does not carry one.</param>
        /// <param name="clrTypePredicate">Accepts a CLR type in place of comparing to <paramref name="clrType"/>.</param>
        /// <param name="relTypePredicate">Accepts a Calcite type in place of comparing its type name.</param>
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
        /// Adds a mapping built by a factory of its own.
        /// </summary>
        /// <param name="clrType"></param>
        /// <param name="sqlTypeName"></param>
        /// <param name="factory"></param>
        /// <param name="match"></param>
        /// <param name="precision"></param>
        /// <param name="scale"></param>
        /// <param name="clrTypePredicate"></param>
        /// <param name="relTypePredicate"></param>
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

            // an entry that decides by predicate accepts types it never names, so it cannot be bucketed
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

            // the frozen copy is of a table that has just changed
            _frozen = null;
        }

        /// <inheritdoc />
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (clrType is null && relType is null)
                throw new ArgumentException("A lookup carries at least one of a CLR type and a Calcite type.");

            // only the CLR type: the entry answers if it is what that type is written as
            if (relType is null)
            {
                foreach (var position in _clrDefaults)
                {
                    var entry = _entries[position];
                    if (entry.AcceptsClrType(clrType))
                        return entry.Factory(context, entry.CreateRelType(context), clrType!);
                }

                return null;
            }

            foreach (var position in Candidates(relType))
            {
                var entry = _entries[position];

                // both named: the entry answers whenever it accepts both, whatever its defaults are
                if (clrType is not null)
                {
                    if (entry.AcceptsClrType(clrType) && entry.AcceptsRelType(relType))
                        return entry.Factory(context, relType, clrType);

                    continue;
                }

                // only the Calcite type: the entry answers if it is what that type reads back as
                if (entry.Match.HasFlag(ClrTypeMatch.RelDefault) && entry.AcceptsRelType(relType))
                    return entry.Factory(context, relType, entry.ClrType);
            }

            return null;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Table order, so the type's default comes first and the conversions that are legal only when
        /// asked for follow. An entry whose CLR type is decided by a predicate rather than named contributes
        /// nothing here: it accepts types it cannot list, and inventing <see cref="object"/> for it would
        /// report the catch-all as though it were a choice.
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
