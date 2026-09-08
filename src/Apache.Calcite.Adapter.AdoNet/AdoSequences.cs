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
    /// <para><see cref="ReadAsync{TRow}"/> is the same query for a plan of
    /// <c>ClrAsyncEnumerableConvention</c>, over <c>OpenConnectionAsync</c>, <c>ExecuteReaderAsync</c> and
    /// <c>ReadAsync</c>. This is the one leaf in a plan with real network I/O to suspend on, which is why it
    /// has an asynchronous side at all when nothing else in the adapter does.</para>
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

            DbConnection? connection = null;
            DbCommand? command = null;
            DbDataReader? reader = null;

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
                // what was opened before the failure is nobody else's to close: the sequence that would have
                // owned it is never returned
                reader?.Dispose();
                command?.Dispose();
                connection?.Dispose();

                throw new AdoCalciteException("Exception while enumerating query.", e);
            }

            return Rows(connection, command, reader, rowBuilder);
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

        /// <summary>
        /// Executes a query and returns its rows, without blocking a thread on the provider.
        /// </summary>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement to execute.</param>
        /// <param name="rowBuilder">Builds one row from the reader's current position.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has
        /// none.</param>
        /// <param name="cancellationToken">Abandons the query. Every generated call site passes
        /// <c>default</c> for this; the token that reaches the provider is the one the consumer hands
        /// <c>GetAsyncEnumerator</c>.</param>
        /// <returns>The rows, read as the query is enumerated.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
        /// <remarks>
        /// <see cref="Read{TRow}"/> over <c>OpenConnectionAsync</c>, <c>ExecuteReaderAsync</c> and
        /// <c>ReadAsync</c>. The statement, the enricher and the row builder are the same; a row is built
        /// from the reader synchronously in both, there being nothing in reading a materialized row to
        /// await.
        ///
        /// <para><b>Forced by the CLR:</b> <see cref="Read{TRow}"/> opens the connection and executes the
        /// statement when it is called, and the rest of the convention's leaves acquire at
        /// <c>GetAsyncEnumerator</c>. Neither is available here — a method returning an
        /// <see cref="IAsyncEnumerable{T}"/> cannot await before it returns, and neither can
        /// <c>GetAsyncEnumerator</c> — so the connection is opened and the statement executed on the first
        /// <c>MoveNextAsync</c>. Nothing observes the difference but the moment the provider is first
        /// spoken to.</para>
        /// </remarks>
        public static IAsyncEnumerable<TRow> ReadAsync<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            return RowsAsync(dataSource, sql, rowBuilder, enricher, cancellationToken);
        }

        /// <summary>
        /// Opens, executes, yields the reader's rows, and closes what was opened for them.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilder"></param>
        /// <param name="enricher"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        static async IAsyncEnumerable<TRow> RowsAsync<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            DbConnection? connection = null;
            DbCommand? command = null;
            DbDataReader? reader = null;

            try
            {
                connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                command = connection.CreateCommand();
                command.CommandText = sql;
                enricher?.Enrich(command);
                reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (DbException e)
            {
                // as in Read: the loop below owns these once it is entered, and it is not entered
                if (reader is not null)
                    await reader.DisposeAsync().ConfigureAwait(false);
                if (command is not null)
                    await command.DisposeAsync().ConfigureAwait(false);
                if (connection is not null)
                    await connection.DisposeAsync().ConfigureAwait(false);

                throw new AdoCalciteException("Exception while enumerating query.", e);
            }

            await using (connection)
            await using (command)
            await using (reader)
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    yield return rowBuilder(reader);
            }
        }

    }

}
