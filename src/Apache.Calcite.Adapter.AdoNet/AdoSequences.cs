using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

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
    /// <c>ClrAsyncEnumerableConvention</c>, reading its rows through <c>DbDataReader.ReadAsync</c>. This is
    /// the one leaf in a plan with real network I/O to suspend on, which is why it has an asynchronous side
    /// at all when nothing else in the adapter does.</para>
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

            Execute(dataSource, sql, enricher, out var connection, out var command, out var reader);

            return Rows(connection, command, reader, rowBuilder);
        }

        /// <summary>
        /// Executes a query and returns its rows, reading each without blocking a thread.
        /// </summary>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement to execute.</param>
        /// <param name="rowBuilder">Builds one row from the reader's current position.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has
        /// none.</param>
        /// <param name="cancellationToken">Unused. Every generated call site passes <c>default</c>, as it
        /// does for every operator of this convention; the token that reaches the provider is the one the
        /// consumer hands <c>GetAsyncEnumerator</c>, and the parameter is declared so that the operator ends
        /// in one.</param>
        /// <returns>The rows, read as the query is enumerated.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
        /// <remarks>
        /// <see cref="Read{TRow}"/> reading its rows through <c>ReadAsync</c>. The statement, the enricher
        /// and the row builder are the same, and a row is built from the reader synchronously in both, there
        /// being nothing in reading a materialized row to await.
        ///
        /// <para><b>The statement is sent at <c>GetAsyncEnumerator</c>, and sent synchronously.</b> That is
        /// where this convention puts acquisition — linq4j acquires inside <c>enumerator()</c>, and
        /// <c>AcquisitionTimingTests</c> holds that the whole cascade runs there — and
        /// <c>GetAsyncEnumerator</c> cannot await, so acquisition-time work is synchronous work, exactly as
        /// it is for a table whose <c>ScanAsync</c> opens something before returning its sequence.
        /// <c>CalciteSession</c> calls <c>GetAsyncEnumerator</c> inside <c>ExecuteReaderAsync</c>, so a
        /// statement the provider rejects fails from the call that executed it rather than from the first
        /// <c>ReadAsync</c> — which is what an ADO.NET consumer expects, and what a leaf that opened on its
        /// first <c>MoveNextAsync</c> could not give.</para>
        ///
        /// <para><b>So connecting and executing block a thread and only the rows do not.</b> That is the
        /// trade this convention's acquisition model imposes, and it is the whole of what is left: the row
        /// loop is where a query spends its time. Making the connect asynchronous as well would mean
        /// awaiting it somewhere, and the only place earlier than the first row is
        /// <c>ExecuteReaderAsync</c> itself — which <c>ShouldReadNothingUntilTheFirstRead</c> and
        /// <c>AcquisitionTimingTests</c> deliberately hold to reading nothing.</para>
        /// </remarks>
        public static IAsyncEnumerable<TRow> ReadAsync<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            // the factory is where linq4j's enumerator() is, and the owner disposes what it acquired whether
            // or not a row was ever read -- an async iterator that never moved runs none of its finally
            // blocks, so the row loop below cannot be trusted with them
            return new ClrAsyncEnumerable<TRow>(token =>
            {
                Execute(dataSource, sql, enricher, out var connection, out var command, out var reader);

                return new AcquiredAsyncEnumerator<TRow>(RowsAsync(reader, rowBuilder, token), reader, command, connection);
            });
        }

        /// <summary>
        /// Opens a connection, fills the command and executes the statement.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="enricher"></param>
        /// <param name="connection"></param>
        /// <param name="command"></param>
        /// <param name="reader"></param>
        /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
        /// <remarks>
        /// Shared by both sequences, which acquire the same way and differ only in how they read a row.
        /// </remarks>
        static void Execute(AdoDataSource dataSource, string sql, DbCommandEnricher? enricher, out DbConnection connection, out DbCommand command, out DbDataReader reader)
        {
            DbConnection? opened = null;
            DbCommand? created = null;

            try
            {
                opened = dataSource.OpenConnection();
                created = opened.CreateCommand();
                created.CommandText = sql;
                enricher?.Enrich(created);

                connection = opened;
                command = created;
                reader = created.ExecuteReader();
            }
            catch (DbException e)
            {
                // what was opened before the failure is nobody else's to close: the sequence that would have
                // owned it is never returned
                created?.Dispose();
                opened?.Dispose();

                throw new AdoCalciteException("Exception while enumerating query.", e);
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

        /// <summary>
        /// Yields the reader's rows, over an already executed reader the owner will close.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="rowBuilder"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        static async IAsyncEnumerator<TRow> RowsAsync<TRow>(DbDataReader reader, Func<DbDataReader, TRow> rowBuilder, CancellationToken cancellationToken)
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                yield return rowBuilder(reader);
        }

    }

}
