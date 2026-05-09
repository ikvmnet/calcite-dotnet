using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a batch of <see cref="CalciteBatchCommand"/> instances that are executed sequentially
    /// against an Apache Calcite engine over a single round-trip. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite does not provide a native batched-execution protocol. Each command in the batch is
    /// executed in order on the same connection; the cumulative effect is equivalent to invoking
    /// the commands one-by-one through <see cref="CalciteCommand"/>. If any command fails the
    /// remaining commands are not executed.
    /// </para>
    /// <para>
    /// Only <see cref="CommandType.Text"/> is supported. Each <see cref="CalciteBatchCommand"/>
    /// uses positional <c>?</c> parameter markers, bound by ordinal in the order parameters were added.
    /// </para>
    /// </remarks>
    public sealed class CalciteBatch : DbBatch
    {

        readonly Collection _commands = new();
        CalciteConnection? _connection;
        CalciteTransaction? _transaction;
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
        protected override DbBatchCommandCollection DbBatchCommands => _commands;

        /// <summary>
        /// Gets the strongly typed collection of <see cref="CalciteBatchCommand"/> instances contained in this batch.
        /// </summary>
        public new DbBatchCommandCollection BatchCommands => _commands;

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
        /// Creates a new <see cref="CalciteBatchCommand"/> that can be added to <see cref="BatchCommands"/>.
        /// </summary>
        /// <returns>A new <see cref="CalciteBatchCommand"/> instance.</returns>
        public new CalciteBatchCommand CreateBatchCommand() => new();

        /// <inheritdoc />
        public override void Cancel()
        {
            // Synchronous cancel is a no-op; cancellation flows through the *Async overrides.
        }

        /// <inheritdoc />
        public override void Prepare()
        {
            // No statement-cache support.
        }

        /// <inheritdoc />
        public override Task PrepareAsync(CancellationToken cancellationToken = default) =>
            cancellationToken.IsCancellationRequested ? Task.FromCanceled(cancellationToken) : Task.CompletedTask;

        /// <inheritdoc />
        public override int ExecuteNonQuery() => ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
        {
            EnsureConnection();
            var total = 0;
            foreach (CalciteBatchCommand bc in _commands)
            {
                cancellationToken.ThrowIfCancellationRequested();
                using var cmd = CreateCommand(bc);
                var n = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                bc.SetRecordsAffected(n);
                total = AddSaturating(total, n);
            }

            return total;
        }

        /// <inheritdoc />
        public override object? ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
        {
            EnsureConnection();
            object? scalar = null;
            foreach (CalciteBatchCommand bc in _commands)
            {
                cancellationToken.ThrowIfCancellationRequested();
                using var cmd = CreateCommand(bc);
                scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }

            return scalar;
        }

        /// <inheritdoc />
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
            ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            EnsureConnection();
            if (_commands.Count == 0)
                throw new InvalidOperationException("Batch contains no commands.");

            // Execute every command except the last with ExecuteNonQuery so all of them run; return the
            // reader from the final command. This mirrors the documented DbBatch shape that exposes a
            // reader for the trailing command in the batch.
            for (var i = 0; i < _commands.Count - 1; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var bc = (CalciteBatchCommand)_commands[i];
                using var cmd = CreateCommand(bc);
                var n = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                bc.SetRecordsAffected(n);
            }

            var last = (CalciteBatchCommand)_commands[_commands.Count - 1];
            var lastCmd = CreateCommand(last);
            return await lastCmd.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            _commands.Clear();
            base.Dispose();
        }

        CalciteCommand CreateCommand(CalciteBatchCommand bc)
        {
            var cmd = new CalciteCommand(bc.CommandText, _connection!)
            {
                CommandTimeout = _timeout,
                Transaction = _transaction,
            };

            foreach (CalciteParameter p in bc.Parameters.Items)
                cmd.Parameters.Add(p);

            return cmd;
        }

        void EnsureConnection()
        {
            if (_connection is null)
                throw new InvalidOperationException("Batch requires an open connection.");
        }

        static int AddSaturating(int a, int b)
        {
            long sum = (long)a + b;
            if (sum > int.MaxValue) return int.MaxValue;
            if (sum < int.MinValue) return int.MinValue;
            return (int)sum;
        }

        sealed class Collection : DbBatchCommandCollection
        {

            readonly List<DbBatchCommand> _list = new();

            public override int Count => _list.Count;

            public override bool IsReadOnly => false;

            protected override DbBatchCommand GetBatchCommand(int index) => _list[index];

            protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => _list[index] = batchCommand;

            public override void Add(DbBatchCommand item) => _list.Add(item);

            public override void Clear() => _list.Clear();

            public override bool Contains(DbBatchCommand item) => _list.Contains(item);

            public override void CopyTo(DbBatchCommand[] array, int arrayIndex) => _list.CopyTo(array, arrayIndex);

            public override IEnumerator<DbBatchCommand> GetEnumerator() => _list.GetEnumerator();

            public override int IndexOf(DbBatchCommand item) => _list.IndexOf(item);

            public override void Insert(int index, DbBatchCommand item) => _list.Insert(index, item);

            public override bool Remove(DbBatchCommand item) => _list.Remove(item);

            public override void RemoveAt(int index) => _list.RemoveAt(index);

        }

    }

}
