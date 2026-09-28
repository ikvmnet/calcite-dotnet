using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Runs a statement and returns its <see cref="DbDataReader"/> as an <see cref="IClrCursor{T}"/>. The code
    /// <see cref="Rel.Convert.AdoToClrCursorConverter"/> generates calls these.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Opening is where the work happens, as Calcite's JDBC adapter executes its statement in
    /// <c>enumerator()</c>: the connection is opened and the statement executed before the cursor is returned,
    /// so a statement the provider rejects fails the open rather than the first read.
    /// </para>
    /// <para>
    /// Each <c>Read</c> or <c>ReadAsync</c> of the cursor advances the provider's reader once and builds one row.
    /// The cursor owns the reader, the command and the connection, and disposes them when it is disposed.
    /// </para>
    /// </remarks>
    public static class AdoCursors
    {

        /// <summary>
        /// Opens a connection, executes the statement and returns its rows as a cursor.
        /// </summary>
        /// <typeparam name="TRow">The row type.</typeparam>
        /// <param name="dataSource">The data source to open a connection from.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilder">Builds the row the reader is positioned on.</param>
        /// <param name="enricher">Adds the command's parameters, or <see langword="null"/> where it has none.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/>, <paramref name="sql"/> or
        /// <paramref name="rowBuilder"/> is <see langword="null"/>.</exception>
        /// <exception cref="AdoCalciteException">The provider raised a <see cref="DbException"/>. Anything opened is
        /// disposed first.</exception>
        public static IClrCursor<TRow> Open<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            Execute(dataSource, sql, enricher, out var connection, out var command, out var reader);

            return new ReaderCursor<TRow>(connection, command, reader, rowBuilder, CancellationToken.None);
        }

        /// <summary>
        /// Opens a connection and executes the statement asynchronously, and returns its rows as a cursor.
        /// </summary>
        /// <typeparam name="TRow">The row type.</typeparam>
        /// <param name="dataSource">The data source to open a connection from.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilder">Builds the row the reader is positioned on.</param>
        /// <param name="enricher">Adds the command's parameters, or <see langword="null"/> where it has none.</param>
        /// <param name="cancellationToken">Cancels opening and executing, and also every later
        /// <c>ReadAsync</c> of the cursor, together with the token that call is given.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/>, <paramref name="sql"/> or
        /// <paramref name="rowBuilder"/> is <see langword="null"/>.</exception>
        /// <exception cref="AdoCalciteException">The provider raised a <see cref="DbException"/>. Anything opened is
        /// disposed first.</exception>
        public static async ValueTask<IClrCursor<TRow>> OpenAsync<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            DbConnection? opened = null;
            DbCommand? created = null;

            try
            {
                opened = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                created = opened.CreateCommand();
                created.CommandText = sql;
                enricher?.Enrich(created);

                var reader = await created.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                return new ReaderCursor<TRow>(opened, created, reader, rowBuilder, cancellationToken);
            }
            catch (DbException e)
            {
                // no cursor is returned to own these
                if (created is not null)
                    await created.DisposeAsync().ConfigureAwait(false);
                if (opened is not null)
                    await opened.DisposeAsync().ConfigureAwait(false);

                throw new AdoCalciteException("Exception while enumerating query.", e);
            }
        }

        /// <summary>
        /// Opens a connection, prepares the command and executes it, disposing what it opened if the provider
        /// raises a <see cref="DbException"/>.
        /// </summary>
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
                // no cursor is returned to own these
                created?.Dispose();
                opened?.Dispose();

                throw new AdoCalciteException("Exception while enumerating query.", e);
            }
        }

        /// <summary>
        /// A reader as a cursor, owning the reader, its command and its connection.
        /// </summary>
        /// <remarks>
        /// <c>ReadAsync</c> passes the provider both the token it is given and the token the cursor was opened
        /// under, so cancelling the statement's token stops the next read. A linked source is created only when
        /// both tokens can be cancelled.
        /// </remarks>
        sealed class ReaderCursor<TRow>(DbConnection connection, DbCommand command, DbDataReader reader, Func<DbDataReader, TRow> rowBuilder, CancellationToken open) : ClrCursor<TRow>
        {

            TRow current = default!;

            /// <inheritdoc />
            public override TRow Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (reader.Read() == false)
                    return false;

                current = rowBuilder(reader);
                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (open.CanBeCanceled == false)
                    return await AdvanceAsync(cancellationToken).ConfigureAwait(false);

                if (cancellationToken.CanBeCanceled == false)
                    return await AdvanceAsync(open).ConfigureAwait(false);

                using var linked = CancellationTokenSource.CreateLinkedTokenSource(open, cancellationToken);

                return await AdvanceAsync(linked.Token).ConfigureAwait(false);
            }

            async ValueTask<bool> AdvanceAsync(CancellationToken cancellationToken)
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    return false;

                current = rowBuilder(reader);
                return true;
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                using (connection)
                using (command)
                using (reader)
                {

                }
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                await using (connection.ConfigureAwait(false))
                await using (command.ConfigureAwait(false))
                await using (reader.ConfigureAwait(false))
                {

                }
            }

        }

    }

}
