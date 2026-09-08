using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Reads a query's rows as a .NET sequence.
    /// </summary>
    /// <remarks>
    /// What <see cref="AdoEnumerable"/> does for a plan of Calcite's calling convention, this does for one of
    /// <c>ClrEnumerableConvention</c>: the same connection, command and enricher, and the same row builder,
    /// yielding an <see cref="IEnumerable{T}"/> rather than a linq4j <c>Enumerable</c>. A plan that ends here
    /// reads its rows without a linq4j enumerator between the reader and the operator above it.
    ///
    /// <para><see cref="ReadAsync{TRow}"/> is the same again for <c>ClrAsyncEnumerableConvention</c>, over
    /// the <c>Async</c> members of the same three types. It is the only place in a plan of that convention
    /// where there is network I/O to suspend on.</para>
    /// </remarks>
    public static class AdoSequences
    {

        /// <summary>
        /// Executes a query and returns its rows.
        /// </summary>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement to execute.</param>
        /// <param name="rowBuilder">Builds one row from the reader's current position.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has
        /// none.</param>
        /// <returns>The rows, read as the query is enumerated.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
        public static IEnumerable<TRow> Read<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            DbConnection connection;
            DbCommand command;
            DbDataReader reader;

            try
            {
                connection = dataSource.OpenConnection();
                command = connection.CreateCommand();
                command.CommandText = sql;
                enricher?.Enrich(command);
                reader = command.ExecuteReader();
            }
            catch (DbException e)
            {
                throw new AdoCalciteException("Exception while enumerating query.", e);
            }

            return Rows(connection, command, reader, rowBuilder);
        }

        /// <summary>
        /// Executes a query and returns its rows, without blocking a thread on the provider.
        /// </summary>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement to execute.</param>
        /// <param name="rowBuilder">Builds one row from the reader's current position.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has
        /// none.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>The rows, read as the query is enumerated.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
        /// <remarks>
        /// <para><b>The statement is sent on the first <c>MoveNextAsync</c> and not before.</b>
        /// <see cref="Read{TRow}"/> opens the connection and executes the reader where it is called, and
        /// yields from a separate iterator, so acquisition happens when the plan's expression is evaluated.
        /// This cannot do the same: a method returning an <see cref="IAsyncEnumerable{T}"/> cannot await
        /// before it returns, and opening the connection and executing the reader are the two awaits. That
        /// is the CLR limit the asynchronous convention states at every site it reaches, and it is the one
        /// place this sequence differs from its synchronous twin.</para>
        ///
        /// <para>The row builder is the synchronous one and stays so: <c>ReadAsync</c> has fetched the row
        /// before the builder reads a field out of it, so reading a field awaits nothing.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TRow> ReadAsync<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            DbConnection connection;
            DbCommand command;
            DbDataReader reader;

            try
            {
                connection = await dataSource.OpenConnectionAsync(cancellationToken);
                command = connection.CreateCommand();
                command.CommandText = sql;
                enricher?.Enrich(command);
                reader = await command.ExecuteReaderAsync(cancellationToken);
            }
            catch (DbException e)
            {
                throw new AdoCalciteException("Exception while enumerating query.", e);
            }

            await using (connection)
            await using (command)
            await using (reader)
            {
                while (await reader.ReadAsync(cancellationToken))
                    yield return rowBuilder(reader);
            }
        }

        /// <summary>
        /// Yields the reader's rows and closes what was opened for them.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="command"></param>
        /// <param name="reader"></param>
        /// <param name="rowBuilder"></param>
        /// <returns></returns>
        static IEnumerable<TRow> Rows<TRow>(DbConnection connection, DbCommand command, DbDataReader reader, Func<DbDataReader, TRow> rowBuilder)
        {
            using (connection)
            using (command)
            using (reader)
            {
                while (reader.Read())
                    yield return rowBuilder(reader);
            }
        }

    }

}
