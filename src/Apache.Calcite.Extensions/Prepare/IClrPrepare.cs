using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Plans and compiles statements into plans of the <c>ClrCursorConvention</c> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>CalcitePrepare</c>. <see cref="ClrPrepareImpl"/> is the
    /// implementation.
    /// </remarks>
    public interface IClrPrepare
    {

        /// <summary>
        /// Plans and compiles one query.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="query">The statement's text, or a relational expression built rather than parsed. A
        /// relational expression is planned by the planner of its own cluster, which must already have this
        /// convention's rules registered.</param>
        /// <param name="elementType">The type the caller wants a row to be; <c>object[]</c> asks for an
        /// array.</param>
        /// <param name="maxRowCount">The maximum number of rows to return, or a negative number for no
        /// limit.</param>
        /// <returns>The planned statement.</returns>
        Signature PrepareSql(CalcitePrepare.Context context, Query query, System.Type elementType, long maxRowCount);

        /// <summary>
        /// Executes a DDL statement.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to execute against.</param>
        /// <param name="node">The parsed statement.</param>
        void ExecuteDdl(CalcitePrepare.Context context, org.apache.calcite.sql.SqlNode node);

        /// <summary>
        /// What a caller asks to have planned: either a statement's text or a relational expression.
        /// </summary>
        public sealed class Query
        {

            /// <summary>
            /// Returns a query over a statement's text.
            /// </summary>
            /// <param name="sql">The statement's text.</param>
            /// <returns>The query.</returns>
            /// <exception cref="ArgumentNullException"><paramref name="sql"/> is <see langword="null"/>.</exception>
            public static Query Of(string sql)
            {
                return new Query(sql ?? throw new ArgumentNullException(nameof(sql)), null);
            }

            /// <summary>
            /// Returns a query over a relational expression that was built rather than parsed.
            /// </summary>
            /// <param name="rel">The relational expression.</param>
            /// <returns>The query.</returns>
            /// <exception cref="ArgumentNullException"><paramref name="rel"/> is <see langword="null"/>.</exception>
            public static Query Of(RelNode rel)
            {
                return new Query(null, rel ?? throw new ArgumentNullException(nameof(rel)));
            }

            Query(string? sql, RelNode? rel)
            {
                if ((sql is null ? 0 : 1) + (rel is null ? 0 : 1) != 1)
                    throw new ArgumentException("A query is one thing or the other.");

                Sql = sql;
                Rel = rel;
            }

            /// <summary>
            /// Gets the statement's text, or <see langword="null"/> if the query is a relational expression.
            /// </summary>
            public string? Sql { get; }

            /// <summary>
            /// Gets the relational expression, or <see langword="null"/> if the query is text.
            /// </summary>
            public RelNode? Rel { get; }

        }

        /// <summary>
        /// A planned statement: the description of its parameters and result, and the compiled plan that
        /// produces its rows.
        /// </summary>
        public sealed class Signature : Meta.Signature
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="sql">The statement's text, or <see langword="null"/> if it had none.</param>
            /// <param name="parameters">One <see cref="AvaticaParameter"/> per dynamic parameter.</param>
            /// <param name="internalParameters">Values stashed during planning, which the plan reads through
            /// the <see cref="DataContext"/>. This must be the map the plan was built against.</param>
            /// <param name="rowType">The result's row type, or <see langword="null"/> for DDL.</param>
            /// <param name="parameterRowType">One field per dynamic parameter, holding the type the
            /// validator inferred for its placeholder.</param>
            /// <param name="columns">One <see cref="ColumnMetaData"/> per result column.</param>
            /// <param name="cursorFactory">How a row is read back.</param>
            /// <param name="rootSchema">The schema the statement was planned against.</param>
            /// <param name="collations">The collations the result is known to carry.</param>
            /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
            /// <param name="bindable">The compiled plan, or <see langword="null"/> if there is nothing to run,
            /// as for a DDL statement, which has already been executed.</param>
            /// <param name="statementType">What kind of statement this is.</param>
            /// <exception cref="ArgumentNullException"><paramref name="collations"/> is
            /// <see langword="null"/>.</exception>
            public Signature(
                string? sql,
                java.util.List parameters,
                java.util.Map internalParameters,
                RelDataType? rowType,
                RelDataType? parameterRowType,
                java.util.List columns,
                Meta.CursorFactory cursorFactory,
                CalciteSchema? rootSchema,
                java.util.List collations,
                long maxRowCount,
                IClrCursorFactory? bindable,
                Meta.StatementType statementType) :
                base(columns, sql, parameters, internalParameters, cursorFactory, statementType)
            {
                RowType = rowType;
                ParameterRowType = parameterRowType;
                RootSchema = rootSchema;
                Collations = collations ?? throw new ArgumentNullException(nameof(collations));
                this.maxRowCount = maxRowCount;
                this.bindable = bindable;
            }

            /// <summary>
            /// Gets the statement's text.
            /// </summary>
            public string? Sql => sql;

            /// <summary>
            /// Gets one <see cref="AvaticaParameter"/> per dynamic parameter.
            /// </summary>
            public java.util.List Parameters => parameters;

            /// <summary>
            /// Gets the values stashed during planning, which the plan reads through the
            /// <see cref="DataContext"/>.
            /// </summary>
            public java.util.Map InternalParameters => internalParameters;

            /// <summary>
            /// Gets the result's row type, or <see langword="null"/> for DDL.
            /// </summary>
            public RelDataType? RowType { get; }

            /// <summary>
            /// Gets one field per dynamic parameter, holding the type the validator inferred for its
            /// placeholder.
            /// </summary>
            /// <remarks>
            /// The plan reads each parameter value as this type, whatever type the caller binds. Use this
            /// rather than <see cref="Parameters"/> to convert a value, since an <see cref="AvaticaParameter"/>
            /// carries only a type name and precision.
            /// </remarks>
            public RelDataType? ParameterRowType { get; }

            /// <summary>
            /// Gets one <see cref="ColumnMetaData"/> per result column.
            /// </summary>
            public java.util.List Columns => columns;

            /// <summary>
            /// Gets how a row is read back.
            /// </summary>
            public Meta.CursorFactory CursorFactory => cursorFactory;

            /// <summary>
            /// Gets the schema the statement was planned against.
            /// </summary>
            public CalciteSchema? RootSchema { get; }

            /// <summary>
            /// Gets the collations the result is known to carry.
            /// </summary>
            public java.util.List Collations { get; }

            readonly long maxRowCount;

            readonly IClrCursorFactory? bindable;

            /// <summary>
            /// Gets what kind of statement this is.
            /// </summary>
            public Meta.StatementType StatementType => statementType;

            /// <summary>
            /// Opens a cursor over the plan's rows synchronously.
            /// </summary>
            /// <param name="root">The context the statement executes against.</param>
            /// <returns>The cursor, positioned before the first row, limited to the maximum row count if
            /// there is one.</returns>
            /// <exception cref="ArgumentNullException"><paramref name="root"/> is
            /// <see langword="null"/>.</exception>
            /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
            /// <remarks>
            /// Opening does the plan's work before its first row, such as draining a sort, on the calling
            /// thread. The cursor can be advanced synchronously or asynchronously.
            /// </remarks>
            public IClrCursor Open(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");
                var opened = bindable.Open(root);

                // a negative maximum means no limit and zero is a valid limit, unlike JDBC's maxRows
                if (maxRowCount >= 0)
                    opened = ClrCursorDefaults.Take((IClrCursor<object>)Typed(opened), java.math.BigDecimal.valueOf(maxRowCount));

                return opened;
            }

            /// <summary>
            /// Opens a cursor over the plan's rows asynchronously.
            /// </summary>
            /// <param name="root">The context the statement executes against.</param>
            /// <param name="cancellationToken">The token that cancels the open. Each advance of the cursor
            /// takes its own.</param>
            /// <returns>The cursor, positioned before the first row, limited to the maximum row count if
            /// there is one.</returns>
            /// <exception cref="ArgumentNullException"><paramref name="root"/> is
            /// <see langword="null"/>.</exception>
            /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
            public async ValueTask<IClrCursor> OpenAsync(DataContext root, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");
                var opened = await bindable.OpenAsync(root, cancellationToken).ConfigureAwait(false);

                if (maxRowCount >= 0)
                    opened = ClrCursorDefaults.Take((IClrCursor<object>)Typed(opened), java.math.BigDecimal.valueOf(maxRowCount));

                return opened;
            }

            /// <summary>
            /// Returns a plan's cursor as a cursor of objects, which the limit operator takes.
            /// </summary>
            /// <remarks>
            /// A plan's rows are always of a reference type, so a root cursor is usually already an
            /// <c>IClrCursor&lt;object&gt;</c> through covariance; otherwise it is wrapped.
            /// </remarks>
            static IClrCursor<object> Typed(IClrCursor cursor)
            {
                return cursor as IClrCursor<object> ?? new ObjectCursor(cursor);
            }

            /// <summary>
            /// A cursor of objects over one of any row type.
            /// </summary>
            sealed class ObjectCursor(IClrCursor cursor) : ClrCursor<object>
            {

                /// <inheritdoc />
                public override object Current => cursor.Current!;

                /// <inheritdoc />
                public override bool Read() => cursor.Read();

                /// <inheritdoc />
                public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken) => cursor.ReadAsync(cancellationToken);

                /// <inheritdoc />
                public override void Dispose() => cursor.Dispose();

                /// <inheritdoc />
                public override ValueTask DisposeAsync() => cursor.DisposeAsync();

            }

            /// <summary>
            /// Returns the plan's rows as a sequence.
            /// </summary>
            /// <param name="root">The context the statement executes against.</param>
            /// <returns>A sequence that opens the plan with <see cref="Open"/> each time it is enumerated,
            /// when <c>GetEnumerator</c> is called.</returns>
            /// <exception cref="ArgumentNullException"><paramref name="root"/> is
            /// <see langword="null"/>.</exception>
            /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
            public IEnumerable<object> Bind(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");

                return ClrCursorDefaults.AsEnumerable(() => Typed(Open(root)));
            }

            /// <summary>
            /// Returns the plan's rows as an asynchronous sequence.
            /// </summary>
            /// <param name="root">The context the statement executes against.</param>
            /// <returns>A sequence that opens the plan with <see cref="OpenAsync"/> each time it is enumerated.
            /// The open happens on the first <c>MoveNextAsync</c>, since <c>GetAsyncEnumerator</c> cannot
            /// await, and uses the token passed to <c>GetAsyncEnumerator</c>.</returns>
            /// <exception cref="ArgumentNullException"><paramref name="root"/> is
            /// <see langword="null"/>.</exception>
            /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
            public IAsyncEnumerable<object> BindAsync(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");

                return ClrCursorDefaults.AsAsyncEnumerable(async token => Typed(await OpenAsync(root, token).ConfigureAwait(false)), CancellationToken.None);
            }

        }

    }

}
