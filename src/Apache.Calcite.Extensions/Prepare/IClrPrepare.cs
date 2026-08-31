using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
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
        Signature PrepareSql(CalcitePrepare.Context context, Query query, System.Type elementType, long maxRowCount, bool async);

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
            /// <param name="parameterRowType">A record type whose fields are the statement's dynamic
            /// parameters, in placeholder order, or <see langword="null"/> where there are none to describe.</param>
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
            /// Gets a record type whose fields are the statement's dynamic parameters, in placeholder order,
            /// or <see langword="null"/> where there are none to describe.
            /// </summary>
            /// <remarks>
            /// What the validator inferred for each <c>?</c>, which is the type the plan will read that slot
            /// as. It is not necessarily the type the caller named: a caller states a <c>DbType</c> and
            /// Calcite states what the SQL around the placeholder requires, and where those disagree it is
            /// this one the value has to arrive as.
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
            /// Runs the plan against a <see cref="DataContext"/> and returns its rows.
            /// </summary>
            /// <param name="root"></param>
            /// <returns></returns>
            /// <exception cref="InvalidOperationException">There is no plan to run.</exception>
            public IEnumerable<object> Bind(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");
                if (bindable is not IClrBindable sync)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} was prepared into an asynchronous convention; read it with {nameof(BindAsync)}.");

                var rows = sync.Bind(root);

                // apply the limit; in JDBC 0 means "no limit", but for us -1 means "no limit" and 0 is a
                // valid limit
                if (maxRowCount >= 0)
                    rows = ClrEnumerableDefaults.Take(rows, maxRowCount);

                return rows;
            }

            /// <summary>
            /// Runs an asynchronous plan against a <see cref="DataContext"/> and returns its rows.
            /// </summary>
            /// <param name="root"></param>
            /// <returns></returns>
            /// <exception cref="InvalidOperationException">There is no plan to run, or it is a synchronous
            /// one.</exception>
            public IAsyncEnumerable<object> BindAsync(DataContext root)
            {
                ArgumentNullException.ThrowIfNull(root);

                if (bindable is null)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} has no plan to run.");
                if (bindable is not IClrAsyncBindable async)
                    throw new InvalidOperationException($"{Sql ?? "The statement"} was prepared into a synchronous convention; read it with {nameof(Bind)}.");

                var rows = async.Bind(root);

                // apply the limit; in JDBC 0 means "no limit", but for us -1 means "no limit" and 0 is a
                // valid limit
                if (maxRowCount >= 0)
                    rows = ClrAsyncEnumerableDefaults.Take(rows, maxRowCount);

                return rows;
            }

        }

    }

}
