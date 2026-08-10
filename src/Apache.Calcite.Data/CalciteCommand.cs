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
    /// Only <see cref="CommandType.Text"/> is supported. Parameter placeholders are positional
    /// <c>?</c> markers, bound by ordinal in the order they were added to <see cref="Parameters"/>;
    /// the value of <see cref="DbParameter.ParameterName"/> is informational.
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
        /// <param name="commandText">The SQL text to execute against the Calcite engine. May be <see langword="null"/>, in which case the command text is set to an empty string.</param>
        public CalciteCommand(string commandText)
        {
            _commandText = commandText ?? string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteCommand"/> class with the text of the query and a <see cref="CalciteConnection"/>.
        /// </summary>
        /// <param name="commandText">The SQL text to execute against the Calcite engine.</param>
        /// <param name="connection">The <see cref="CalciteConnection"/> against which the command will execute.</param>
        public CalciteCommand(string commandText, CalciteConnection connection) :
            this(commandText)
        {
            _connection = connection;
        }

        /// <inheritdoc />
        /// <remarks>
        /// <see cref="DbCommand.CommandText"/> is declared <see cref="AllowNullAttribute"/>, so the override
        /// says so too and turns a null into the empty text rather than refusing it.
        /// </remarks>
        [AllowNull]
        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? string.Empty;
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
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
        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set => _transaction = (CalciteTransaction?)value;
        }

        /// <summary>
        /// Gets the strongly typed <see cref="CalciteParameterCollection"/> associated with this command.
        /// </summary>
        public new CalciteParameterCollection Parameters => _parameters;

        /// <summary>
        /// Registers a Calcite hook with a Java <see cref="Consumer"/> for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="consumer">The Java consumer invoked by the hook.</param>
        public void RegisterHook(Hook hook, Consumer consumer)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, consumer));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="bool"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The boolean value to set on the hook property.</param>
        public void RegisterHook(Hook hook, bool value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Boolean.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with an <see cref="int"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The integer value to set on the hook property.</param>
        public void RegisterHook(Hook hook, int value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Integer.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="long"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The long value to set on the hook property.</param>
        public void RegisterHook(Hook hook, long value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Long.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="double"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The double value to set on the hook property.</param>
        public void RegisterHook(Hook hook, double value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Double.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="float"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The float value to set on the hook property.</param>
        public void RegisterHook(Hook hook, float value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Float.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="short"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The short value to set on the hook property.</param>
        public void RegisterHook(Hook hook, short value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Short.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="byte"/> property value for the duration of each execute request on this command.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The byte value to set on the hook property.</param>
        public void RegisterHook(Hook hook, byte value)
        {
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Byte.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a .NET <see cref="Action{T}"/> callback for the duration of each execute request on this command.
        /// The action is wrapped in a Java consumer and invoked with the hook's argument on each execution.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="function">The .NET delegate invoked by the hook.</param>
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

        /// <inheritdoc />
        public override void Cancel()
        {
            // Phase 1: cancellation hook is exposed via *Async overloads; synchronous cancel is a no-op.
        }

        /// <inheritdoc />
        /// <remarks>
        /// Plans the statement now, so a later execute of the same text hits the connection's plan cache
        /// instead of planning again. Without a plan cache —
        /// <see cref="CalciteConnection.PlanCacheFactory"/> or
        /// <see cref="CalciteConnectionStringBuilder.PlanCacheSize"/> — this does nothing, because there
        /// would be nowhere to keep the plan. A DDL statement is never planned from here: in this engine,
        /// as in Calcite's own prepare, planning DDL executes it, and <c>Prepare</c> must not have
        /// effects.
        /// </remarks>
        public override void Prepare()
        {
            GetOpenSession().Prepare(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()));
        }

        /// <inheritdoc />
        protected override DbParameter CreateDbParameter() => new CalciteParameter();

        /// <inheritdoc />
        public override int ExecuteNonQuery()
        {
            return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            using var result = await ExecuteNonQueryCoreAsync(cancellationToken).ConfigureAwait(false);
            return CalciteExecuteRequest.ClampToInt32(result.RecordsAffected);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The reader path with one row and one column taken off it, through <see cref="CalciteResult.Read"/>.
        /// The plan is the connection's — mode, not entry point — so in the default mode this blocks per row
        /// exactly as <c>Read</c> on the reader would, and in synchronous mode nothing here ever waits.
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

        /// <inheritdoc />
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
        /// The same plan <see cref="ExecuteDbDataReaderAsync"/> prepares — the convention is the
        /// connection's mode, not the entry point's choice. In the default mode the reader answers
        /// <c>Read</c> by blocking wherever the plan genuinely suspends, which is what <c>Read</c> over an
        /// asynchronous source means; a connection whose consumers are synchronous can say
        /// <see cref="CalciteConnectionStringBuilder.Synchronous"/> and get the plan that never waits.
        /// </remarks>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var result = GetOpenSession().ExecuteReader(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()));
            return new CalciteDataReader(result, behavior);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Prepares the connection's plan. By default that is the asynchronous convention, and everything
        /// plans: an <c>IClrAsyncScannableTable</c> is scanned asynchronously, a table Calcite can scan is
        /// read the way Calcite reads it and completes synchronously — a state machine and no thread — and
        /// anything else is implemented in <c>EnumerableConvention</c> with a converter carrying its rows.
        /// In synchronous mode this is the synchronous plan in a completed task, and a query touching a
        /// table that can <em>only</em> produce rows asynchronously fails to plan — visibly, rather than
        /// blocking behind a surface that looks asynchronous.
        /// </remarks>
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var result = await ExecuteReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            return new CalciteDataReader(result, behavior);
        }

        /// <summary>
        /// Executes the command and returns a <see cref="CalciteResult"/> containing the result set.
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        Task<CalciteResult> ExecuteReaderCoreAsync(CancellationToken cancellationToken)
        {
            return GetOpenSession().ExecuteReaderAsync(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()), cancellationToken);
        }

        Task<CalciteResult> ExecuteNonQueryCoreAsync(CancellationToken cancellationToken)
        {
            return GetOpenSession().ExecuteNonQueryAsync(CalciteExecuteRequest.From(_commandText, _parameters, _commandTimeout, ResolveHooks()), cancellationToken);
        }

        /// <summary>
        /// Returns the combined hook entries for this request: connection-level first, then command-level.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when no connection is set.</exception>
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
