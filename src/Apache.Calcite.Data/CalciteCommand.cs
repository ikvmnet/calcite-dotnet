using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

using java.util.function;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a SQL statement to execute against an Apache Calcite engine. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only <see cref="CommandType.Text"/> is supported. Parameter placeholders are positional <c>?</c>
    /// markers, bound in the order the parameters appear in <see cref="Parameters"/>;
    /// <see cref="DbParameter.ParameterName"/> is not used for binding.
    /// </para>
    /// <para>
    /// Each execution parses and plans the statement again; <see cref="Prepare"/> does nothing. A DDL
    /// statement takes effect while it is planned, before the execute method returns.
    /// </para>
    /// </remarks>
    public sealed class CalciteCommand : DbCommand
    {

        CalciteConnection? _connection;
        CalciteTransaction? _transaction;
        readonly CalciteParameterCollection _parameters = new();
        string _commandText = string.Empty;
        int _commandTimeout = 30;
        CommandType _commandType = CommandType.Text;
        UpdateRowSource _updateRowSource = UpdateRowSource.None;
        List<CalciteHookEntry>? _hooks;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteCommand"/> class.
        /// </summary>
        public CalciteCommand()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteCommand"/> class with the text of the query.
        /// </summary>
        /// <param name="commandText">The SQL text to execute, or <see langword="null"/> for an empty command text.</param>
        public CalciteCommand(string commandText)
        {
            _commandText = commandText ?? string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteCommand"/> class with the text of the query and a <see cref="CalciteConnection"/>.
        /// </summary>
        /// <param name="commandText">The SQL text to execute, or <see langword="null"/> for an empty command text.</param>
        /// <param name="connection">The <see cref="CalciteConnection"/> to execute against.</param>
        public CalciteCommand(string commandText, CalciteConnection connection) :
            this(commandText)
        {
            _connection = connection;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Setting <see langword="null"/> sets the empty string.
        /// </remarks>
        [AllowNull]
        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the wait time, in seconds, passed to Calcite as the statement's query timeout. The default
        /// is 30; 0 means no timeout.
        /// </summary>
        /// <remarks>
        /// The value reaches Calcite's <c>DataContext</c> as its <c>timeout</c> variable, which Calcite applies
        /// to statements its JDBC adapter sends to a database. The provider does not otherwise stop a
        /// statement that runs longer; use a cancellation token for that.
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
        public override int CommandTimeout
        {
            get => _commandTimeout;
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value));
                _commandTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets how <see cref="CommandText"/> is interpreted. Only <see cref="CommandType.Text"/> is supported.
        /// </summary>
        /// <exception cref="NotSupportedException">The value is not <see cref="CommandType.Text"/>.</exception>
        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                if (value != CommandType.Text)
                    throw new NotSupportedException("Only CommandType.Text is supported.");
                _commandType = value;
            }
        }

        /// <inheritdoc />
        public override bool DesignTimeVisible { get; set; }

        /// <inheritdoc />
        /// <remarks>
        /// Stored but not used by the provider.
        /// </remarks>
        public override UpdateRowSource UpdatedRowSource
        {
            get => _updateRowSource;
            set => _updateRowSource = value;
        }

        /// <inheritdoc />
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => _connection = (CalciteConnection?)value;
        }

        /// <inheritdoc />
        protected override DbParameterCollection DbParameterCollection => _parameters;

        /// <inheritdoc />
        /// <remarks>
        /// Transactions are not supported, so this is always <see langword="null"/> unless set.
        /// </remarks>
        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set => _transaction = (CalciteTransaction?)value;
        }

        /// <summary>
        /// Gets the parameters of the statement, bound to its <c>?</c> placeholders in order.
        /// </summary>
        public new CalciteParameterCollection Parameters => _parameters;

        /// <summary>
        /// Attaches a Java <see cref="Consumer"/> to a Calcite hook each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="consumer">The consumer the hook invokes with its argument.</param>
        /// <remarks>
        /// The hook is attached to the executing thread while the statement is planned and its result opened,
        /// and detached before the execute method returns; it is not attached while rows are read. Hooks
        /// registered on the <see cref="CalciteConnection"/> are attached first. A registration cannot be
        /// removed.
        /// </remarks>
        public void RegisterHook(Hook hook, Consumer consumer)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, consumer));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="bool"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook, such as <see cref="Hook.ENABLE_BINDABLE"/>.</param>
        /// <param name="value">The value the hook supplies.</param>
        /// <remarks>
        /// The value is supplied through <c>Hook.propertyJ</c>. When the hook is attached is described on
        /// <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, bool value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Boolean.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to an <see cref="int"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Integer</c>.</param>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, int value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Integer.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="long"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Long</c>.</param>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, long value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Long.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="double"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Double</c>.</param>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, double value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Double.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="float"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Float</c>.</param>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, float value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Float.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="short"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Short</c>.</param>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, short value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Short.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="byte"/> value each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Byte</c>.</param>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, byte value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Byte.valueOf(value))));
        }

        /// <summary>
        /// Attaches a .NET callback to a Calcite hook each time this command executes.
        /// </summary>
        /// <param name="hook">The Calcite hook, such as <see cref="Hook.PLAN_BEFORE_IMPLEMENTATION"/>.</param>
        /// <param name="function">The callback, invoked with the hook's argument.</param>
        /// <remarks>
        /// The callback runs on the thread executing the command, and receives the hook's argument as Calcite
        /// passes it, which is usually a Java object. When the hook is attached is described on
        /// <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, Action<object> function)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, new DelegateConsumer<object>(function)));
        }

        /// <summary>
        /// Gets or sets the <see cref="CalciteConnection"/> used by this command.
        /// </summary>
        public new CalciteConnection? Connection
        {
            get => _connection;
            set => _connection = value;
        }

        /// <summary>
        /// Does nothing.
        /// </summary>
        /// <remarks>
        /// To cancel a statement, pass a <see cref="CancellationToken"/> to an asynchronous execute method or to
        /// <see cref="DbDataReader.ReadAsync(CancellationToken)"/>.
        /// </remarks>
        public override void Cancel()
        {

        }

        /// <summary>
        /// Does nothing. Statements are not cached; each execution parses and plans the statement again.
        /// </summary>
        public override void Prepare()
        {

        }

        /// <inheritdoc />
        protected override DbParameter CreateDbParameter() => new CalciteParameter();

        /// <summary>
        /// Executes the statement and returns the number of rows affected.
        /// </summary>
        /// <returns>
        /// For <c>INSERT</c>, <c>UPDATE</c>, <c>DELETE</c> and <c>MERGE</c>, the number of rows affected; for DDL,
        /// 0; for a query, -1. A count above <see cref="int.MaxValue"/> is returned as <see cref="int.MaxValue"/>.
        /// </returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="CalciteException">The statement could not be parsed, planned or executed.</exception>
        /// <remarks>
        /// A query is planned but not run.
        /// </remarks>
        public override int ExecuteNonQuery()
        {
            return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes the statement and returns the number of rows affected.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. It is checked before the
        /// statement is planned and is tied to Calcite's cancel flag while it runs.</param>
        /// <returns>
        /// A task whose result is, for <c>INSERT</c>, <c>UPDATE</c>, <c>DELETE</c> and <c>MERGE</c>, the number of
        /// rows affected; for DDL, 0; for a query, -1.
        /// </returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
        /// <exception cref="CalciteException">The statement could not be parsed, planned or executed.</exception>
        /// <remarks>
        /// The statement runs synchronously on the calling thread and the task returned is already complete:
        /// a data modification is executed by Calcite's own operators, which have nothing to await.
        /// </remarks>
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            using var result = await ExecuteNonQueryCoreAsync(cancellationToken).ConfigureAwait(false);
            return CalciteExecuteRequest.ClampToInt32(result.RecordsAffected);
        }

        /// <summary>
        /// Executes the statement and returns the first column of the first row.
        /// </summary>
        /// <returns>The value, <see cref="DBNull.Value"/> where it is SQL null, or <see langword="null"/> where
        /// the result has no rows or no columns.</returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="CalciteException">The statement could not be parsed, planned or executed.</exception>
        /// <remarks>
        /// The value is converted as <see cref="CalciteDataReader.GetValue"/> converts it. Only the first row is
        /// read.
        /// </remarks>
        public override object? ExecuteScalar()
        {
            using var result = GetOpenSession().ExecuteReader(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()));
            if (result.Read() == false)
                return null;

            if (result.Columns.Count == 0)
                return null;

            return result.Current.GetValue(0).GetValue();
        }

        /// <summary>
        /// Executes the statement and returns the first column of the first row.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task whose result is the value, <see cref="DBNull.Value"/> where it is SQL null, or
        /// <see langword="null"/> where the result has no rows or no columns.</returns>
        /// <exception cref="InvalidOperationException"><see cref="Connection"/> is not set or not open.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
        /// <exception cref="CalciteException">The statement could not be parsed, planned or executed.</exception>
        /// <remarks>
        /// The value is converted as <see cref="CalciteDataReader.GetValue"/> converts it. Only the first row is
        /// read.
        /// </remarks>
        public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            using var result = await ExecuteReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            if (await result.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                return null;

            if (result.Columns.Count == 0)
                return null;

            return result.Current.GetValue(0).GetValue();
        }

        /// <inheritdoc />
        /// <remarks>
        /// The statement is planned and its result opened on the calling thread, which is when a sort drains its
        /// input and a table adapter sends its query, so errors from starting the query are thrown here rather
        /// than at the first <see cref="DbDataReader.Read"/>. The reader returned supports both
        /// <see cref="DbDataReader.Read"/> and <see cref="DbDataReader.ReadAsync(CancellationToken)"/>. Of the
        /// <see cref="CommandBehavior"/> flags, only <see cref="CommandBehavior.CloseConnection"/> is observed,
        /// and only by the reader's enumerator.
        /// </remarks>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var result = GetOpenSession().ExecuteReader(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()));
            return new CalciteDataReader(result, behavior);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Planning runs synchronously before the first await. Opening the result is awaited, so a table that reads
        /// asynchronously is started without blocking; tables read through Calcite's own interfaces complete
        /// synchronously. <paramref name="cancellationToken"/> is linked into the statement's cancellation, so
        /// cancelling it later also stops the reader. Of the <see cref="CommandBehavior"/> flags, only
        /// <see cref="CommandBehavior.CloseConnection"/> is observed, and only by the reader's enumerator.
        /// </remarks>
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var result = await ExecuteReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            return new CalciteDataReader(result, behavior);
        }

        /// <summary>
        /// Plans the statement and opens its result with await.
        /// </summary>
        /// <param name="cancellationToken">The token for the statement.</param>
        Task<CalciteResult> ExecuteReaderCoreAsync(CancellationToken cancellationToken)
        {
            return GetOpenSession().ExecuteReaderAsync(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()), cancellationToken);
        }

        /// <summary>
        /// Plans and executes the statement for its affected-row count.
        /// </summary>
        /// <param name="cancellationToken">The token for the statement.</param>
        Task<CalciteResult> ExecuteNonQueryCoreAsync(CancellationToken cancellationToken)
        {
            return GetOpenSession().ExecuteNonQueryAsync(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()), cancellationToken);
        }

        /// <summary>
        /// Returns the hooks for one execution: the connection's first, then the command's.
        /// </summary>
        /// <exception cref="InvalidOperationException">No connection is set.</exception>
        IEnumerable<CalciteHookEntry>? ResolveHooks()
        {
            if (_connection is null)
                throw new InvalidOperationException("Command requires an open connection.");

            var connectionHooks = _connection.Hooks;
            if (connectionHooks is null)
                return _hooks;
            if (_hooks is null)
                return connectionHooks;

            return connectionHooks.Concat(_hooks);
        }

        CalciteSession GetOpenSession()
        {
            if (_connection is null)
                throw new InvalidOperationException("Command requires an open connection.");

            return _connection.RequireSession();
        }

    }

}
