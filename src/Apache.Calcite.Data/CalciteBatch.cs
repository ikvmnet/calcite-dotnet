using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a batch of SQL statements to execute against an Apache Calcite engine. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Add <see cref="CalciteBatchCommand"/> instances to <see cref="BatchCommands"/>, then call one of the
    /// execute methods. The commands are executed one after another, in order, on the batch's
    /// <see cref="Connection"/>; each is parsed and planned separately, and a failure stops the batch at that
    /// command. After execution each command's <see cref="DbBatchCommand.RecordsAffected"/> holds its own count.
    /// </para>
    /// <para>
    /// Hooks registered with <see cref="CalciteConnection.RegisterHook(org.apache.calcite.runtime.Hook, java.util.function.Consumer)"/>
    /// are not attached while a batch executes.
    /// </para>
    /// </remarks>
    public sealed class CalciteBatch : DbBatch
    {

        CalciteConnection? _connection;
        CalciteTransaction? _transaction;
        readonly CalciteBatchCommandCollection _batchCommands = new();
        int _timeout = 30;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatch"/> class with no connection.
        /// </summary>
        /// <remarks>Set <see cref="Connection"/> before executing.</remarks>
        public CalciteBatch()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatch"/> class associated with the specified connection.
        /// </summary>
        /// <param name="connection">The <see cref="CalciteConnection"/> the batch executes on, or <see langword="null"/> to set it later through <see cref="Connection"/>.</param>
        public CalciteBatch(CalciteConnection? connection)
        {
            _connection = connection;
        }

        /// <inheritdoc />
        protected override DbBatchCommandCollection DbBatchCommands => _batchCommands;

        /// <summary>
        /// Gets the commands the batch executes, in order.
        /// </summary>
        public new CalciteBatchCommandCollection BatchCommands => _batchCommands;

        /// <summary>
        /// Gets or sets the query timeout, in seconds, applied to each command. The default is 30; 0 means no timeout.
        /// </summary>
        /// <remarks>
        /// Applied as <see cref="CalciteCommand.CommandTimeout"/> is.
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
        public override int Timeout
        {
            get => _timeout;
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value));

                _timeout = value;
            }
        }

        /// <inheritdoc />
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => _connection = (CalciteConnection?)value;
        }

        /// <summary>
        /// Gets or sets the connection the batch executes on.
        /// </summary>
        /// <remarks>The connection must be open when an execute method is called.</remarks>
        public new CalciteConnection? Connection
        {
            get => _connection;
            set => _connection = value;
        }

        /// <inheritdoc />
        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set => _transaction = (CalciteTransaction?)value;
        }

        /// <summary>
        /// Gets or sets the transaction the batch executes within.
        /// </summary>
        /// <remarks>
        /// Transactions are not supported. The value is stored and not used.
        /// </remarks>
        public new CalciteTransaction? Transaction
        {
            get => _transaction;
            set => _transaction = value;
        }

        /// <inheritdoc />
        protected override DbBatchCommand CreateDbBatchCommand() => new CalciteBatchCommand();

        /// <summary>
        /// Creates a new <see cref="CalciteBatchCommand"/>.
        /// </summary>
        /// <remarks>
        /// The command is not added to <see cref="BatchCommands"/>. Set its
        /// <see cref="CalciteBatchCommand.CommandText"/> and <see cref="CalciteBatchCommand.Parameters"/>, then
        /// add it with <see cref="CalciteBatchCommandCollection.Add(CalciteBatchCommand)"/>.
        /// </remarks>
        /// <returns>A new, empty <see cref="CalciteBatchCommand"/>.</returns>
        public new CalciteBatchCommand CreateBatchCommand() => new();

        /// <summary>
        /// Does nothing.
        /// </summary>
        /// <remarks>
        /// To cancel a batch, pass a <see cref="CancellationToken"/> to an asynchronous execute method.
        /// </remarks>
        public override void Cancel()
        {

        }

        /// <summary>
        /// Does nothing. Statements are not cached; each execution parses and plans every command again.
        /// </summary>
        public override void Prepare()
        {

        }

        /// <summary>
        /// Does nothing. Statements are not cached; each execution parses and plans every command again.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A completed task, or a cancelled one where <paramref name="cancellationToken"/> is cancelled.</returns>
        public override Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            return cancellationToken.IsCancellationRequested ? Task.FromCanceled(cancellationToken) : Task.CompletedTask;
        }

        /// <summary>
        /// Executes every command in the batch and returns the total number of rows affected.
        /// </summary>
        /// <returns>The sum of the positive counts of the commands; a query's -1 and DDL's 0 add nothing. A total
        /// above <see cref="int.MaxValue"/> is returned as <see cref="int.MaxValue"/>.</returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="CalciteException">A command could not be parsed, planned or executed.</exception>
        public override int ExecuteNonQuery()
        {
            return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes every command in the batch and returns the total number of rows affected.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. It is checked before
        /// each command.</param>
        /// <returns>A task whose result is the sum of the positive counts of the commands.</returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
        /// <exception cref="CalciteException">A command could not be parsed, planned or executed.</exception>
        /// <remarks>
        /// Each command's count is set on its <see cref="DbBatchCommand.RecordsAffected"/>: the number of rows
        /// affected for a data modification, 0 for DDL, and -1 for a query, which is planned but not run.
        /// </remarks>
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
        {
            var session = GetOpenSession();
            long total = 0;

            foreach (var command in _batchCommands.Items)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using var result = await ExecuteNonQueryCoreAsync(session, command, cancellationToken).ConfigureAwait(false);
                var n = result.RecordsAffected;
                command.SetRecordsAffected(CalciteExecuteRequest.ClampToInt32(n));
                if (n > 0)
                    total += n;
            }

            return CalciteExecuteRequest.ClampToInt32(total);
        }

        /// <summary>
        /// Executes every command in the batch and returns the first column of the first row of the first
        /// command's result.
        /// </summary>
        /// <returns>The value, <see cref="DBNull.Value"/> where it is SQL null, or <see langword="null"/> where the
        /// batch is empty or the first result has no rows or no columns.</returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="CalciteException">A command could not be parsed, planned or executed.</exception>
        public override object? ExecuteScalar()
        {
            return ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes every command in the batch and returns the first column of the first row of the first
        /// command's result.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task whose result is the value, <see cref="DBNull.Value"/> where it is SQL null, or
        /// <see langword="null"/> where the batch is empty or the first result has no rows or no columns.</returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
        /// <exception cref="CalciteException">A command could not be parsed, planned or executed.</exception>
        /// <remarks>
        /// The first command is executed as a query and the rest as non-queries. The first command's
        /// <see cref="DbBatchCommand.RecordsAffected"/> is 0.
        /// </remarks>
        public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
        {
            if (_batchCommands.Count == 0)
                return null;

            var session = GetOpenSession();
            object? scalar = null;

            for (var i = 0; i < _batchCommands.Items.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var command = _batchCommands.Items[i];
                if (i == 0)
                {
                    using var result = await ExecuteReaderCoreAsync(session, command, cancellationToken).ConfigureAwait(false);
                    command.SetRecordsAffected(CalciteExecuteRequest.ClampToInt32(result.RecordsAffected));
                    if (await result.ReadAsync(cancellationToken).ConfigureAwait(false) && result.Columns.Count > 0)
                        scalar = result.Current.GetValue(0).GetValue();
                }
                else
                {
                    using var result = await ExecuteNonQueryCoreAsync(session, command, cancellationToken).ConfigureAwait(false);
                    command.SetRecordsAffected(CalciteExecuteRequest.ClampToInt32(result.RecordsAffected));
                }
            }

            return scalar;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Runs <see cref="ExecuteDbDataReaderAsync"/> and waits for it.
        /// </remarks>
        protected override DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior)
        {
            return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        /// <remarks>
        /// Every command is planned and its result opened before the reader is returned, in order; the reader
        /// then holds one result set per command, read in turn with <see cref="DbDataReader.NextResult"/>. If a
        /// command fails, the results already opened are released and the exception is thrown. Each command's
        /// <see cref="DbBatchCommand.RecordsAffected"/> is 0.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The batch has no commands, or <see cref="Connection"/> is
        /// not set or not open.</exception>
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(System.Data.CommandBehavior behavior, CancellationToken cancellationToken = default)
        {
            if (_batchCommands.Count == 0)
                throw new InvalidOperationException("Batch contains no commands.");

            var session = GetOpenSession();
            var results = new List<CalciteResult>(_batchCommands.Items.Count);
            try
            {
                foreach (var command in _batchCommands.Items)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var result = await ExecuteReaderCoreAsync(session, command, cancellationToken).ConfigureAwait(false);
                    command.SetRecordsAffected(CalciteExecuteRequest.ClampToInt32(result.RecordsAffected));
                    results.Add(result);
                }
            }
            catch
            {
                foreach (var r in results)
                    r.Dispose();

                throw;
            }

            return new CalciteDataReader(results.ToArray(), behavior);
        }

        /// <summary>
        /// Plans a command and opens its result with await, as <see cref="CalciteCommand"/> does.
        /// </summary>
        /// <param name="session">The connection's session.</param>
        /// <param name="command">The command to execute.</param>
        /// <param name="cancellationToken">The token, linked into the statement's cancellation.</param>
        /// <returns>The command's result.</returns>
        Task<CalciteResult> ExecuteReaderCoreAsync(CalciteSession session, CalciteBatchCommand command, CancellationToken cancellationToken)
        {
            return session.ExecuteReaderAsync(CalciteExecuteRequest.From(command.CommandText, command.Parameters, _timeout), cancellationToken);
        }

        /// <summary>
        /// Plans and executes a command for its affected-row count.
        /// </summary>
        /// <param name="session">The connection's session.</param>
        /// <param name="command">The command to execute.</param>
        /// <param name="cancellationToken">The token for the statement.</param>
        /// <returns>A result with no rows and the count set.</returns>
        Task<CalciteResult> ExecuteNonQueryCoreAsync(CalciteSession session, CalciteBatchCommand command, CancellationToken cancellationToken)
        {
            return session.ExecuteNonQueryAsync(CalciteExecuteRequest.From(command.CommandText, command.Parameters, _timeout), cancellationToken);
        }

        /// <summary>
        /// Gets the session of the batch's connection.
        /// </summary>
        /// <returns>The session.</returns>
        /// <exception cref="InvalidOperationException">No connection is set, or it is not open.</exception>
        CalciteSession GetOpenSession()
        {
            if (_connection is null)
                throw new InvalidOperationException("Batch requires an open connection.");

            return _connection.RequireSession();
        }

        /// <inheritdoc />
        /// <remarks>
        /// Removes every command from <see cref="BatchCommands"/>.
        /// </remarks>
        public override void Dispose()
        {
            _batchCommands.Clear();
            base.Dispose();
        }

    }

}
