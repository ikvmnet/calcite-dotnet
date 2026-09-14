using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

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
    /// a plan that awaits, reading its rows through <c>DbDataReader.ReadAsync</c>. This is
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
        /// <para><b>The connect and the execute are started at <c>GetAsyncEnumerator</c> and awaited nowhere
        /// in it.</b> That is where this convention acquires, and <c>GetAsyncEnumerator</c> cannot await, so
        /// what it can do is begin the work and hand back an enumerator holding it in flight — never one
        /// holding a send known to have succeeded. Nothing blocks: the request goes through
        /// <c>OpenConnectionAsync</c> and <c>DbCommand.ExecuteReaderAsync</c>, and the factory returns at
        /// their first suspension.</para>
        ///
        /// <para><b>Observing it is <c>IClrStartable</c>.</b> <c>AcquiredAsyncEnumerator</c> forwards that
        /// to everything a factory acquired, so <c>CalciteSession</c>'s Execute awaits every leaf of the
        /// plan and a statement the provider rejects is reported from the call that executed it, with no row
        /// read — which is what an ADO.NET consumer expects, and what <c>DbCommand.ExecuteReaderAsync</c>
        /// does itself, that being where a provider parses the first response. The first
        /// <c>MoveNextAsync</c> awaits the same request, so a caller that never starts it still gets a
        /// reader that behaves.</para>
        ///
        /// <para>Two leaves therefore have their requests in flight together rather than one after the
        /// other, because each was started where it was acquired and only the observing is sequential.</para>
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
                var pending = new PendingRead(dataSource, sql, enricher, token);

                return new AcquiredAsyncEnumerator<TRow>(RowsAsync(pending, rowBuilder, token), pending);
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
        /// The pulled sequence's. <see cref="ReadAsync{TRow}"/> issues the same request through
        /// <see cref="PendingRead"/> instead, which starts it rather than waiting for it.
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
        static async IAsyncEnumerator<TRow> RowsAsync<TRow>(PendingRead pending, Func<DbDataReader, TRow> rowBuilder, CancellationToken cancellationToken)
        {
            var reader = await pending.ReaderAsync().ConfigureAwait(false);

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                yield return rowBuilder(reader);
        }

        /// <summary>
        /// A connect and an execute begun at acquisition, awaited by whoever gets there first.
        /// </summary>
        /// <remarks>
        /// The constructor starts the request and returns at its first suspension, which is what lets
        /// <c>GetAsyncEnumerator</c> issue a statement without blocking. Everything the request opened is
        /// owned here and closed by <see cref="DisposeAsync"/>, including where the request failed partway
        /// and where no row was ever read.
        ///
        /// <para>One request, awaited by <see cref="StartAsync"/> at Execute and by
        /// <see cref="ReaderAsync"/> at the first row, in either order and any number of times — so a
        /// rejected statement is reported to both, and a caller that skips the first still sees it.</para>
        /// </remarks>
        sealed class PendingRead : IClrStartable, IAsyncDisposable
        {

            readonly Task<DbDataReader> _request;

            DbConnection? _connection;
            DbCommand? _command;
            DbDataReader? _reader;

            /// <summary>
            /// Initializes a new instance, beginning the request.
            /// </summary>
            /// <param name="dataSource"></param>
            /// <param name="sql"></param>
            /// <param name="enricher"></param>
            /// <param name="cancellationToken"></param>
            public PendingRead(AdoDataSource dataSource, string sql, DbCommandEnricher? enricher, CancellationToken cancellationToken)
            {
                _request = RequestAsync(dataSource, sql, enricher, cancellationToken);
            }

            /// <summary>
            /// Opens a connection, fills the command and executes the statement.
            /// </summary>
            /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
            async Task<DbDataReader> RequestAsync(AdoDataSource dataSource, string sql, DbCommandEnricher? enricher, CancellationToken cancellationToken)
            {
                try
                {
                    _connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                    _command = _connection.CreateCommand();
                    _command.CommandText = sql;
                    enricher?.Enrich(_command);

                    return _reader = await _command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (DbException e)
                {
                    throw new AdoCalciteException("Exception while enumerating query.", e);
                }
            }

            /// <inheritdoc />
            public ValueTask StartAsync() => new ValueTask(_request);

            /// <summary>
            /// Returns the executed reader, awaiting the request where it has not finished.
            /// </summary>
            /// <returns></returns>
            public Task<DbDataReader> ReaderAsync() => _request;

            /// <inheritdoc />
            /// <remarks>
            /// The request is awaited before anything is closed, because what it opened is assigned as it
            /// runs and a disposal racing it would close nothing. Its failure is swallowed here and nowhere
            /// else: whoever awaited it has already been told, and a disposal is not the place to report it
            /// a second time.
            /// </remarks>
            public async ValueTask DisposeAsync()
            {
                try
                {
                    await _request.ConfigureAwait(false);
                }
                catch
                {
                    // reported to the reader or to the starter; closing is all that is left to do
                }

                if (_reader is not null)
                    await _reader.DisposeAsync().ConfigureAwait(false);

                if (_command is not null)
                    await _command.DisposeAsync().ConfigureAwait(false);

                if (_connection is not null)
                    await _connection.DisposeAsync().ConfigureAwait(false);
            }

        }

    }

}
