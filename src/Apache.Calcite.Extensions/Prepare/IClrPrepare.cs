using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Plans and compiles a statement against a schema.
    /// </summary>
    public interface IClrPrepare
    {

        /// <summary>
        /// Plans and compiles one query.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="query">The statement's text, or a plan that was built rather than parsed. A plan keeps
        /// its own cluster, and the planner that chooses is that cluster's — so it must already carry the
        /// rules of the convention asked for.</param>
        /// <param name="elementType">What a caller wants a row to be. <c>object[]</c> asks for an array.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <param name="async">Whether to prepare into the asynchronous convention.</param>
        /// <returns>The planned statement.</returns>
        Signature PrepareSql(CalcitePrepare.Context context, Query query, System.Type elementType, long maxRowCount);

        /// <summary>
        /// Executes a DDL statement.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to execute against.</param>
        /// <param name="node">The parsed statement.</param>
        void ExecuteDdl(CalcitePrepare.Context context, org.apache.calcite.sql.SqlNode node);

        /// <summary>
        /// What a caller asks to have planned: either a statement's text or a relational expression, and never
        /// both.
        /// </summary>
        public sealed class Query
        {

            /// <summary>
            /// Returns a query over a statement's text.
            /// </summary>
            public static Query Of(string sql)
            {
                return new Query(sql ?? throw new ArgumentNullException(nameof(sql)), null);
            }

            /// <summary>
            /// Returns a query over a relational expression that was built rather than parsed.
            /// </summary>
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
            /// Gets the statement's text, or <see langword="null"/> where the query is a plan.
            /// </summary>
            public string? Sql { get; }

            /// <summary>
            /// Gets the plan, or <see langword="null"/> where the query is text.
            /// </summary>
            public RelNode? Rel { get; }

        }

        /// <summary>
        /// A planned statement: everything a caller needs to describe the result, plus the compiled plan that
        /// produces its rows.
        /// </summary>
        public sealed class Signature : Meta.Signature
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="sql">The statement's text, or <see langword="null"/> where it had none.</param>
            /// <param name="parameters">One <see cref="AvaticaParameter"/> per dynamic parameter.</param>
            /// <param name="internalParameters">Values the query reads through the <see cref="DataContext"/>
            /// rather than from the plan. This must be the map the plan was built against.</param>
            /// <param name="rowType">The result's row type, or <see langword="null"/> for DDL.</param>
            /// <param name="parameterRowType">One field per dynamic parameter, holding the type the
            /// validator inferred for its placeholder.</param>
            /// <param name="columns">One <see cref="ColumnMetaData"/> per result column.</param>
            /// <param name="cursorFactory">How a row is read back.</param>
            /// <param name="rootSchema">The schema the statement was planned against.</param>
            /// <param name="collations">The collations the result is known to carry.</param>
            /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
            /// <param name="bindable">The compiled plan, or <see langword="null"/> where there is nothing to
            /// run — a DDL statement has already taken effect by the time this exists.</param>
            /// <param name="statementType">What kind of statement this is.</param>
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
                IClrBindableBase? bindable,
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
            /// Gets the values the query reads through the <see cref="DataContext"/> rather than from the plan.
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
            /// <b>What a placeholder is, is the validator's answer and not the caller's.</b> Calcite refuses
            /// a placeholder it cannot infer a type for, so by the time there is a plan there is a type, and
            /// the plan reads the value as that type whatever a caller said it was binding. The
            /// <see cref="AvaticaParameter"/> list beside this describes the same placeholders for a
            /// consumer, but flatly — it carries a type name and a precision, not the type — so a value
            /// cannot be converted from it.
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

            readonly IClrBindableBase? bindable;

            /// <summary>
            /// Gets what kind of statement this is.
            /// </summary>
            public Meta.StatementType StatementType => statementType;

            /// <summary>
            /// Opens a cursor over the plan's rows, running its acquisition on the calling thread.
            /// </summary>
            /// <param name="root"></param>
            /// <returns>The cursor, positioned before the first row.</returns>
            /// <exception cref="InvalidOperationException">There is no plan to run, or it is not one a
            /// cursor can be opened over.</exception>
            /// <remarks>
            /// What the ADO.NET provider's <c>ExecuteReader</c> is: the statement is prepared into the
            /// cursor convention, and the cursor it opens carries both advances over one position.
            /// </remarks>
            public IClrCursor Open(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");
                if (bindable is not IClrCursorFactory cursor)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no cursor plan.");

                var opened = cursor.Open(root);

                // apply the limit; in JDBC 0 means "no limit", but for us -1 means "no limit" and 0 is a
                // valid limit
                if (maxRowCount >= 0)
                    opened = ClrCursorDefaults.Take((IClrCursor<object>)Typed(opened), java.math.BigDecimal.valueOf(maxRowCount));

                return opened;
            }

            /// <summary>
            /// Opens a cursor over the plan's rows, awaiting its acquisition.
            /// </summary>
            /// <param name="root"></param>
            /// <param name="cancellationToken">The token for the acquisition; each advance takes its own.</param>
            /// <returns>The cursor, positioned before the first row.</returns>
            /// <exception cref="InvalidOperationException">There is no plan to run, or it is not one a
            /// cursor can be opened over.</exception>
            public async ValueTask<IClrCursor> OpenAsync(DataContext root, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");
                if (bindable is not IClrCursorFactory cursor)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no cursor plan.");

                var opened = await cursor.OpenAsync(root, cancellationToken).ConfigureAwait(false);

                if (maxRowCount >= 0)
                    opened = ClrCursorDefaults.Take((IClrCursor<object>)Typed(opened), java.math.BigDecimal.valueOf(maxRowCount));

                return opened;
            }

            /// <summary>
            /// Reads the untyped cursor a plan hands out as a cursor of objects, which the limit operator
            /// takes.
            /// </summary>
            /// <remarks>
            /// The root of a cursor plan is typed by its physical row, and every physical row is a reference
            /// type, so the cursor is a <c>IClrCursor&lt;TRow&gt;</c> for some class and the limit can be
            /// applied over it as objects only through one more cursor. A row is never a value type — the
            /// physical type boxes what the type factory answers — so nothing is boxed here either.
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
            /// Runs the plan against a <see cref="DataContext"/> and returns its rows as a sequence.
            /// </summary>
            /// <param name="root"></param>
            /// <returns></returns>
            /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
            /// <remarks>
            /// A plan of the sequence convention is enumerated as it stands; a plan of the cursor convention
            /// is opened at <c>GetEnumerator</c> and read through its synchronous advance. The provider does
            /// not read this way — it opens a cursor — but a caller driving the pipeline for its rows alone
            /// can.
            /// </remarks>
            public IEnumerable<object> Bind(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");

                if (bindable is IClrCursorFactory)
                    return ClrCursorDefaults.AsEnumerable(() => Typed(Open(root)));

                if (bindable is not IClrBindable sync)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no synchronous plan.");

                var rows = sync.Bind(root);

                // apply the limit; in JDBC 0 means "no limit", but for us -1 means "no limit" and 0 is a
                // valid limit
                if (maxRowCount >= 0)
                    rows = ClrEnumerableDefaults.Take(rows, maxRowCount);

                return rows;
            }

            /// <summary>
            /// Runs the plan against a <see cref="DataContext"/> and returns its rows as an asynchronous
            /// sequence.
            /// </summary>
            /// <param name="root"></param>
            /// <returns></returns>
            /// <exception cref="InvalidOperationException">There is no plan to run, or it is a synchronous
            /// one.</exception>
            /// <remarks>
            /// <see cref="Bind"/> for the awaiting sequence. A cursor plan is opened, with await, on the
            /// first advance, because <c>GetAsyncEnumerator</c> cannot await an open.
            /// </remarks>
            public IAsyncEnumerable<object> BindAsync(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");

                if (bindable is IClrCursorFactory)
                    return ClrCursorDefaults.AsAsyncEnumerable(async token => Typed(await OpenAsync(root, token).ConfigureAwait(false)), CancellationToken.None);

                if (bindable is not IClrAsyncBindable async)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no asynchronous plan.");

                var rows = async.Bind(root);

                // apply the limit; in JDBC 0 means "no limit", but for us -1 means "no limit" and 0 is a
                // valid limit
                if (maxRowCount >= 0)
                    rows = ClrEnumerableDefaults.TakeAsync(rows, maxRowCount);

                return rows;
            }

        }

    }

}
