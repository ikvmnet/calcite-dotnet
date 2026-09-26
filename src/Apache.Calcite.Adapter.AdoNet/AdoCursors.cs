using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Opens a statement's <see cref="DbDataReader"/> as a <see cref="ClrCursor{T}"/>.
    /// </summary>
    /// <remarks>
    /// The leaf of a plan of the cursor convention over this adapter, and the shape the convention exists
    /// for: a <see cref="DbDataReader"/> is a forward-only cursor with <c>Read</c> and
    /// <c>ReadAsync(token)</c> over one position, so the cursor handed back is the reader itself with one
    /// row built per advance, and the token each advance is given is the token the provider's
    /// <see cref="DbDataReader.ReadAsync(CancellationToken)"/> is given. <see cref="AdoSequences"/> reads the
    /// same reader as a sequence, where a token can only enter once, at the enumerator.
    ///
    /// <para>The open is the acquisition, as it is in <see cref="AdoSequences"/>: the connection is opened
    /// and the statement sent here, so a failing statement fails the open rather than the first advance.
    /// <see cref="OpenAsync"/> opens the connection and executes with await, under the open's token.</para>
    /// </remarks>
    public static class AdoCursors
    {

        /// <summary>
        /// Opens the statement and returns its rows as a cursor.
        /// </summary>
        /// <typeparam name="TRow"></typeparam>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilder">Builds one row from the reader positioned on it.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has none.</param>
        /// <returns></returns>
        public static IClrCursor<TRow> Open<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            AdoSequences.Execute(dataSource, sql, enricher, out var connection, out var command, out var reader);

            return new ReaderCursor<TRow>(connection, command, reader, rowBuilder, CancellationToken.None);
        }

        /// <summary>
        /// Opens the statement with await and returns its rows as a cursor.
        /// </summary>
        /// <typeparam name="TRow"></typeparam>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilder">Builds one row from the reader positioned on it.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has none.</param>
        /// <param name="cancellationToken">The token the open runs under, which is the statement's: each
        /// advance brings its own, and the reader is advanced under both.</param>
        /// <returns></returns>
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
                // what was opened before the failure is nobody else's to close: the cursor that would have
                // owned it is never returned
                if (created is not null)
                    await created.DisposeAsync().ConfigureAwait(false);
                if (opened is not null)
                    await opened.DisposeAsync().ConfigureAwait(false);

                throw new AdoCalciteException("Exception while enumerating query.", e);
            }
        }

        /// <summary>
        /// The reader as a cursor, owning the command and the connection it was opened over.
        /// </summary>
        /// <remarks>
        /// An awaited advance runs the provider's <c>ReadAsync</c> under the advance's token and the open's
        /// together. The open's is the statement's, which <c>DbCommand.Cancel</c> and a read the provider
        /// refused both cancel, so a statement cancelled between two advances stops the reader on the next
        /// one, as it did when the reader was enumerated under that token alone. The two are linked only
        /// where both can be cancelled; where one cannot, the other is passed as it is.
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
