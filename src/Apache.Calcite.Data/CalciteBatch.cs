using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a batch of SQL commands to execute against an Apache Calcite engine. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Add one or more <see cref="CalciteBatchCommand"/> instances to <see cref="BatchCommands"/> and
    /// then call <see cref="DbBatch.ExecuteNonQuery"/> or <see cref="DbBatch.ExecuteReader"/> to run
    /// them all. Commands are executed sequentially against the same open <see cref="CalciteConnection"/>.
    /// The cumulative records-affected count is returned by <see cref="DbBatch.ExecuteNonQuery"/> and
    /// the per-command count is available via <see cref="DbBatchCommand.RecordsAffected"/> after
    /// execution completes.
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
        /// <param name="connection">The <see cref="CalciteConnection"/> that the batch will execute against, or <see langword="null"/> to set it later via <see cref="Connection"/>.</param>
        public CalciteBatch(CalciteConnection? connection)
        {
            _connection = connection;
        }

        /// <inheritdoc />
        protected override DbBatchCommandCollection DbBatchCommands => _batchCommands;

        /// <summary>
        /// Gets the strongly typed collection of commands that will be executed when the batch runs.
        /// </summary>
        /// <remarks>Add commands in the order you want them executed.</remarks>
        public new CalciteBatchCommandCollection BatchCommands => _batchCommands;

        /// <inheritdoc />
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
        /// Gets or sets the connection that the batch executes against.
        /// </summary>
        /// <remarks>Must be set to an open <see cref="CalciteConnection"/> before calling any Execute method.</remarks>
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
        /// Gets or sets the transaction within which all commands in this batch execute.
        /// </summary>
        /// <remarks>Calcite does not support transactions; this property is provided for ADO.NET API compatibility only.</remarks>
        public new CalciteTransaction? Transaction
        {
            get => _transaction;
            set => _transaction = value;
        }

        /// <inheritdoc />
        protected override DbBatchCommand CreateDbBatchCommand() => new CalciteBatchCommand();

        /// <summary>
        /// Creates a new <see cref="CalciteBatchCommand"/> for use in this batch.
        /// </summary>
        /// <remarks>
        /// The returned command is not automatically added to <see cref="BatchCommands"/>. Set its
        /// <see cref="CalciteBatchCommand.CommandText"/> and <see cref="CalciteBatchCommand.Parameters"/>,
        /// then add it via <c>BatchCommands.Add(...)</c>.
        /// </remarks>
        /// <returns>A new, empty <see cref="CalciteBatchCommand"/>.</returns>
        public new CalciteBatchCommand CreateBatchCommand() => new();

        /// <inheritdoc />
        public override void Cancel()
        {
            // Phase 1: cancellation hook is exposed via *Async overloads; synchronous cancel is a no-op.
        }

        /// <inheritdoc />
        public override void Prepare()
        {
            // Phase 1: no preparation cache.
        }

        /// <inheritdoc />
        public override Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            return cancellationToken.IsCancellationRequested ? Task.FromCanceled(cancellationToken) : Task.CompletedTask;
        }

        /// <inheritdoc />
        public override int ExecuteNonQuery()
        {
            return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
        public override object? ExecuteScalar()
        {
            return ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
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
        protected override DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior)
        {
            return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
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
        /// Executes the specified <see cref="CalciteBatchCommand"/> using the provided <see cref="CalciteSession"/> and returns the resulting <see cref="CalciteResult"/>.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="command"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// The synchronous plan, though this method is awaited. A batch is a sequence of statements over
        /// whatever tables the caller has, and there is no reason to think they can produce rows
        /// asynchronously — <c>ExecuteReaderAsync</c> would throw for any that cannot, which would make a
        /// batch fail on the strength of how it happened to be executed rather than on what it says.
        ///
        /// <para>An asynchronous batch would want the asynchronous plan and would then need every table it
        /// touches to be an <c>IClrAsyncScannableTable</c>. That is a decision about the batch API and not
        /// one to make here by accident.</para>
        /// </remarks>
        Task<CalciteResult> ExecuteReaderCoreAsync(CalciteSession session, CalciteBatchCommand command, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult<CalciteResult>(session.ExecuteReader(CalciteExecuteRequest.From(command.CommandText, command.Parameters, _timeout)));
        }

        /// <summary>
        /// Executes the specified <see cref="CalciteBatchCommand"/> using the provided <see cref="CalciteSession"/> and returns the resulting <see cref="CalciteResult"/>.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="command"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<CalciteResult> ExecuteNonQueryCoreAsync(CalciteSession session, CalciteBatchCommand command, CancellationToken cancellationToken)
        {
            return session.ExecuteNonQueryAsync(CalciteExecuteRequest.From(command.CommandText, command.Parameters, _timeout), cancellationToken);
        }

        /// <summary>
        /// Gets the open <see cref="CalciteSession"/> from the current <see cref="CalciteConnection"/>.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        CalciteSession GetOpenSession()
        {
            if (_connection is null)
                throw new InvalidOperationException("Batch requires an open connection.");

            return _connection.RequireSession();
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            _batchCommands.Clear();
            base.Dispose();
        }

    }

}
