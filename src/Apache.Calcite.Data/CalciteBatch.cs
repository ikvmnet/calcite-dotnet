using System;
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
    /// Each <see cref="CalciteBatchCommand"/> in <see cref="BatchCommands"/> is executed sequentially
    /// against the same open <see cref="CalciteConnection"/>. Calcite does not currently expose a
    /// dedicated batched execution API to this provider, so the batch is executed as a series of
    /// individual statements; the cumulative records-affected value is reported through
    /// <see cref="DbBatch.ExecuteNonQuery"/> and reflected on each command's
    /// <see cref="DbBatchCommand.RecordsAffected"/>.
    /// </remarks>
    public sealed class CalciteBatch : DbBatch
    {

        CalciteConnection? _connection;
        CalciteTransaction? _transaction;
        readonly CalciteBatchCommandCollection _batchCommands = new();
        int _timeout = 30;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatch"/> class.
        /// </summary>
        public CalciteBatch()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatch"/> class associated with the specified connection.
        /// </summary>
        /// <param name="connection">The <see cref="CalciteConnection"/> against which the batch will execute.</param>
        public CalciteBatch(CalciteConnection? connection)
        {
            _connection = connection;
        }

        /// <inheritdoc />
        protected override DbBatchCommandCollection DbBatchCommands => _batchCommands;

        /// <summary>
        /// Gets the strongly typed collection of <see cref="CalciteBatchCommand"/> instances in this batch.
        /// </summary>
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
        /// Gets or sets the <see cref="CalciteConnection"/> used by this batch.
        /// </summary>
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
        /// Gets or sets the <see cref="CalciteTransaction"/> within which this batch executes.
        /// </summary>
        public new CalciteTransaction? Transaction
        {
            get => _transaction;
            set => _transaction = value;
        }

        /// <inheritdoc />
        protected override DbBatchCommand CreateDbBatchCommand() => new CalciteBatchCommand();

        /// <summary>
        /// Creates and returns a new <see cref="CalciteBatchCommand"/> for use with this batch.
        /// </summary>
        /// <returns>A new <see cref="CalciteBatchCommand"/> instance.</returns>
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

                using var result = await ExecuteCommandAsync(session, command, cancellationToken).ConfigureAwait(false);
                var n = result.RecordsAffected;
                command.SetRecordsAffected(ClampToInt32(n));
                if (n > 0)
                    total += n;
            }

            return ClampToInt32(total);
        }

        /// <inheritdoc />
        public override object? ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

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
                using var result = await ExecuteCommandAsync(session, command, cancellationToken).ConfigureAwait(false);
                command.SetRecordsAffected(ClampToInt32(result.RecordsAffected));

                if (i == 0)
                {
                    if (await result.ReadAsync(cancellationToken).ConfigureAwait(false) && result.Columns.Count > 0)
                        scalar = result.Current.GetValue(0);
                }
            }

            return scalar;
        }

        /// <inheritdoc />
        protected override DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior) =>
            ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(System.Data.CommandBehavior behavior, CancellationToken cancellationToken = default)
        {
            if (_batchCommands.Count == 0)
                throw new InvalidOperationException("Batch contains no commands.");

            var session = GetOpenSession();

            // Execute all but the last command for their side effects, then return the reader for the last command.
            for (var i = 0; i < _batchCommands.Items.Count - 1; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var command = _batchCommands.Items[i];
                using var result = await ExecuteCommandAsync(session, command, cancellationToken).ConfigureAwait(false);
                command.SetRecordsAffected(ClampToInt32(result.RecordsAffected));
            }

            var last = _batchCommands.Items[_batchCommands.Items.Count - 1];
            var lastResult = await ExecuteCommandAsync(session, last, cancellationToken).ConfigureAwait(false);
            last.SetRecordsAffected(ClampToInt32(lastResult.RecordsAffected));
            return new CalciteDataReader(lastResult, behavior);
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            _batchCommands.Clear();
            base.Dispose();
        }

        async Task<CalciteResult> ExecuteCommandAsync(CalciteSession session, CalciteBatchCommand command, CancellationToken cancellationToken)
        {
            var parameters = command.Parameters;
            var values = new CalciteParameterValue[parameters.Items.Count];
            for (var i = 0; i < parameters.Items.Count; i++)
            {
                var p = parameters.Items[i];
                values[i] = new CalciteParameterValue(p.ParameterName, p.DbType, p.Value);
            }

            var request = new CalciteExecuteRequest(command.CommandText, values, _timeout);
            return await session.ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
        }

        CalciteSession GetOpenSession()
        {
            if (_connection is null)
                throw new InvalidOperationException("Batch requires an open connection.");

            return _connection.RequireSession();
        }

        static int ClampToInt32(long value)
        {
            if (value > int.MaxValue) return int.MaxValue;
            if (value < int.MinValue) return int.MinValue;
            return (int)value;
        }

    }

}
