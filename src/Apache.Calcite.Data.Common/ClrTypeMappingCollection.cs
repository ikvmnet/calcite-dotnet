using System;
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
        }

        /// <inheritdoc />
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (clrType is null && relType is null)
                throw new ArgumentException("A lookup carries at least one of a CLR type and a Calcite type.");

            foreach (var entry in _entries)
            {
                // both named: the entry answers whenever it accepts both, whatever its defaults are
                if (clrType is not null && relType is not null)
                {
                    if (entry.AcceptsClrType(clrType) && entry.AcceptsRelType(relType))
                        return entry.Factory(context, relType, clrType);

                    continue;
                }

                // only the Calcite type: the entry answers if it is what that type reads back as
                if (relType is not null)
                {
                    if (entry.Match.HasFlag(ClrTypeMatch.RelDefault) && entry.AcceptsRelType(relType))
                        return entry.Factory(context, relType, entry.ClrType);

                    continue;
                }

                // only the CLR type: the entry answers if it is what that type is written as
                if (entry.Match.HasFlag(ClrTypeMatch.ClrDefault) && entry.AcceptsClrType(clrType))
                    return entry.Factory(context, entry.CreateRelType(context), clrType!);
            }

            return null;
        }

    }

}
