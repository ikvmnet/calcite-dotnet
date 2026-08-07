using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A planned statement: everything a caller needs to describe the result, plus the compiled plan that
    /// produces its rows.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>CalcitePrepare.CalciteSignature</c>, member for member, with
    /// <c>Bindable</c> swapped for <see cref="IClrBindable"/> and <c>enumerable</c> for <see cref="Bind"/>.
    ///
    /// <para>It does not extend Avatica's <c>Meta.Signature</c> as Calcite's does. That base exists so a
    /// signature can cross Avatica's JDBC wire protocol; nothing here does, and inheriting it would only
    /// bind this type to a constructor it gains nothing from. The members it contributes —
    /// <see cref="Sql"/>, <see cref="Parameters"/>, <see cref="InternalParameters"/>,
    /// <see cref="Columns"/>, <see cref="CursorFactory"/>, <see cref="StatementType"/> — are all here.</para>
    /// </remarks>
    sealed class ClrSignature
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="sql">The statement's text.</param>
        /// <param name="parameters">One <see cref="AvaticaParameter"/> per dynamic parameter.</param>
        /// <param name="internalParameters">Values the query reads through the <see cref="DataContext"/>
        /// rather than from the plan. This must be the map the plan was built against.</param>
        /// <param name="rowType">The result's row type, or <see langword="null"/> for DDL.</param>
        /// <param name="columns">One <see cref="ColumnMetaData"/> per result column.</param>
        /// <param name="cursorFactory">How a row is read back.</param>
        /// <param name="rootSchema">The schema the statement was planned against.</param>
        /// <param name="collations">The collations the result is known to carry.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <param name="bindable">The compiled plan, or <see langword="null"/> where there is nothing to
        /// run — a DDL statement has already taken effect by the time this exists.</param>
        /// <param name="statementType">What kind of statement this is.</param>
        public ClrSignature(
            string sql,
            java.util.List parameters,
            java.util.Map internalParameters,
            RelDataType? rowType,
            java.util.List columns,
            Meta.CursorFactory cursorFactory,
            CalciteSchema? rootSchema,
            java.util.List collations,
            long maxRowCount,
            IClrBindable? bindable,
            Meta.StatementType statementType)
        {
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            InternalParameters = internalParameters ?? throw new ArgumentNullException(nameof(internalParameters));
            RowType = rowType;
            Columns = columns ?? throw new ArgumentNullException(nameof(columns));
            CursorFactory = cursorFactory ?? throw new ArgumentNullException(nameof(cursorFactory));
            RootSchema = rootSchema;
            Collations = collations ?? throw new ArgumentNullException(nameof(collations));
            MaxRowCount = maxRowCount;
            Bindable = bindable;
            StatementType = statementType ?? throw new ArgumentNullException(nameof(statementType));
        }

        /// <summary>
        /// Gets the statement's text.
        /// </summary>
        public string Sql { get; }

        /// <summary>
        /// Gets one <see cref="AvaticaParameter"/> per dynamic parameter.
        /// </summary>
        public java.util.List Parameters { get; }

        /// <summary>
        /// Gets the values the query reads through the <see cref="DataContext"/> rather than from the plan.
        /// </summary>
        /// <remarks>
        /// The map the plan was built against, so a value stashed while planning is readable while running.
        /// Calcite's own signature carries <c>CalcitePreparingStmt</c>'s private field, which a subclass
        /// that overrides <c>implement</c> never writes to.
        /// </remarks>
        public java.util.Map InternalParameters { get; }

        /// <summary>
        /// Gets the result's row type, or <see langword="null"/> for DDL.
        /// </summary>
        public RelDataType? RowType { get; }

        /// <summary>
        /// Gets one <see cref="ColumnMetaData"/> per result column.
        /// </summary>
        public java.util.List Columns { get; }

        /// <summary>
        /// Gets how a row is read back.
        /// </summary>
        public Meta.CursorFactory CursorFactory { get; }

        /// <summary>
        /// Gets the schema the statement was planned against.
        /// </summary>
        public CalciteSchema? RootSchema { get; }

        /// <summary>
        /// Gets the collations the result is known to carry.
        /// </summary>
        public java.util.List Collations { get; }

        /// <summary>
        /// Gets the row limit, or a negative number for none.
        /// </summary>
        public long MaxRowCount { get; }

        /// <summary>
        /// Gets the compiled plan, or <see langword="null"/> where there is nothing to run.
        /// </summary>
        public IClrBindable? Bindable { get; }

        /// <summary>
        /// Gets what kind of statement this is.
        /// </summary>
        public Meta.StatementType StatementType { get; }

        /// <summary>
        /// Runs the plan against a <see cref="DataContext"/> and returns its rows.
        /// </summary>
        /// <param name="root"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
        /// <remarks>
        /// <c>CalciteSignature.enumerable</c>, including the limit: it applies
        /// <c>EnumerableDefaults.take</c> when <c>maxRowCount</c> is not negative, and a caller reaching
        /// past the signature to the bindable would silently lose that. In JDBC zero means no limit; here,
        /// as there, a negative number means no limit and zero is a limit of zero.
        /// </remarks>
        public IEnumerable<object> Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            if (Bindable is null)
                throw new InvalidOperationException($"{Sql} has no plan to run.");

            var rows = Bindable.Bind(root);

            return MaxRowCount < 0 ? rows : Take(rows, MaxRowCount);
        }

        /// <summary>
        /// Yields at most <paramref name="count"/> rows.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        /// <remarks>
        /// Not <see cref="System.Linq.Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/>, because the
        /// limit is a <see cref="long"/> and a statement may legitimately ask for more rows than an
        /// <see cref="int"/> holds.
        /// </remarks>
        static IEnumerable<object> Take(IEnumerable<object> source, long count)
        {
            if (count <= 0)
                yield break;

            var taken = 0L;

            foreach (var row in source)
            {
                yield return row;

                if (++taken >= count)
                    yield break;
            }
        }

    }

}
