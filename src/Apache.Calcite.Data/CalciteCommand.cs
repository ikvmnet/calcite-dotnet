using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

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
        public override void Prepare()
        {
            // Phase 1: no preparation cache.
        }

        /// <inheritdoc />
        protected override DbParameter CreateDbParameter() => new CalciteParameter();

        /// <inheritdoc />
        public override int ExecuteNonQuery() => ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            using var result = await ExecuteCoreAsync(cancellationToken).ConfigureAwait(false);
            var n = result.RecordsAffected;
            if (n > int.MaxValue) return int.MaxValue;
            if (n < int.MinValue) return int.MinValue;
            return (int)n;
        }

        /// <inheritdoc />
        public override object? ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            using var result = await ExecuteCoreAsync(cancellationToken).ConfigureAwait(false);
            if (await result.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                return null;

            if (result.Columns.Count == 0)
                return null;

            return result.Current.GetValue(0);
        }

        /// <inheritdoc />
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
            ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var result = await ExecuteCoreAsync(cancellationToken).ConfigureAwait(false);
            return new CalciteDataReader(result, behavior);
        }

        async Task<CalciteResult> ExecuteCoreAsync(CancellationToken cancellationToken)
        {
            var session = GetOpenSession();
            var values = new CalciteParameterValue[_parameters.Items.Count];
            for (var i = 0; i < _parameters.Items.Count; i++)
            {
                var p = _parameters.Items[i];
                values[i] = new CalciteParameterValue(p.ParameterName, p.DbType, p.Value);
            }

            var request = new CalciteExecuteRequest(_commandText, values, _commandTimeout);
            return await session.ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
        }

        CalciteSession GetOpenSession()
        {
            if (_connection is null)
                throw new InvalidOperationException("Command requires an open connection.");

            return _connection.RequireSession();
        }

    }

}
