using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

using java.sql;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a SQL statement or stored procedure to execute against a <see cref="CalciteConnection"/>.
    /// </summary>
    public abstract partial class CalciteCommandBase : DbCommand
    {

        /// <summary>
        /// Manages locks around execution.
        /// </summary>
        readonly struct ExecutingLock : IDisposable
        {

            readonly CalciteCommandBase _command;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="command"></param>
            /// <exception cref="ArgumentNullException"></exception>
            public ExecutingLock(CalciteCommandBase command)
            {
                _command = command ?? throw new ArgumentNullException(nameof(command));
                Monitor.Enter(_command._syncRoot);

                if (_command._executing != null)
                    throw new CalciteDbException("The command is already executing a statement.");
            }

            /// <summary>
            /// Exits the lock.
            /// </summary>
            public void Exit()
            {
                Monitor.Exit(_command._syncRoot);
            }

            /// <summary>
            /// Enters the lock an additional time.
            /// </summary>
            public void Enter()
            {
                Monitor.Enter(_command._syncRoot);
            }

            /// <summary>
            /// Releases the lock.
            /// </summary>
            public void Dispose()
            {
                Monitor.Exit(_command._syncRoot);
            }

        }

        readonly object _syncRoot = new object();
        readonly CalciteParameterCollection _parameters = new CalciteParameterCollection();
        CalciteConnection? _connection;
        CommandType _commandType = CommandType.Text;
        string? _commandText;
        object? _executing;
        object? _prepared;

        /// <summary>
        /// Creates a new command.
        /// </summary>
        internal CalciteCommandBase()
        {

        }

        /// <summary>
        /// Creates a new command attached to the specified connection.
        /// </summary>
        /// <param name="connection"></param>
        internal CalciteCommandBase(CalciteConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// Gets or sets the database connection.
        /// </summary>
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set
            {
                using (new ExecutingLock(this))
                    _connection = (CalciteConnection?)value;
            }
        }

        /// <summary>
        /// Gets or sets how the <see cref="CommandText"/> property is interpreted.
        /// </summary>
        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                using (new ExecutingLock(this))
                {
                    _commandType = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the text command to run against the data source.
        /// </summary>
#if NET
        [AllowNull]
#endif
        public override string CommandText
        {
            get => _commandText ?? "";
            set
            {
                using (new ExecutingLock(this))
                {
                    _commandText = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the wait time (in seconds) before terminating the attempt to execute the command and generating an error.
        /// </summary>
        public override int CommandTimeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the command object should be visible in a customized interface control.
        /// </summary>
        public override bool DesignTimeVisible { get; set; } = false;

        /// <summary>
        /// Gets or sets how command results are applied to the <see cref="DataRow"/> when used by the Update method of a <see cref="DbDataAdapter"/>.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        /// Gets the collection of <see cref="DbParameter"/> objects.
        /// </summary>
        protected override DbParameterCollection DbParameterCollection => _parameters;

        /// <summary>
        /// Gets or sets the <see cref="DbTransaction"/> within which this <see cref="CalciteCommandBase"/> object executes.
        /// </summary>
        protected override DbTransaction? DbTransaction { get; set; }

        /// <summary>
        /// Indicates that the JDBC <see cref="Statement.RETURN_GENERATED_KEYS" /> value will be used to retrieve an extra result set.
        /// </summary>
        public bool ReturnGeneratedKeys { get; set; }

        /// <summary>
        /// Creates a new instance of a <see cref="DbParameter"/> object.
        /// </summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter()
        {
            if (_prepared != null)
                throw new CalciteDbException("Command is already prepared.");

            return new CalciteParameter();
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
            using (new ExecutingLock(this))
            {
                if (_connection == null)
                    throw new CalciteDbException("Connection must be available.");

                if (_connection.State != ConnectionState.Open)
                    throw new CalciteDbException("Connection must be open.");

                if (_prepared != null)
                    throw new CalciteDbException("Command is already prepared.");

                if (_connection._impl is null)
                    throw new InvalidOperationException();

                try
                {
                    switch (CommandType)
                    {
                        case CommandType.Text:
                            _prepared = null; // TODO prepare the statement
                            break;
                        case CommandType.StoredProcedure:
                            _prepared = null; // TODO prepare the statement
                            break;
                        case CommandType.TableDirect:
                            throw new NotImplementedException();
                    }
                }
                catch (SQLException e)
                {
                    throw new CalciteDbException(e);
                }
            }
        }

        /// <summary>
        /// Sets the value of the parameter on the given statement.
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="parameter"></param>
        /// <param name="offset"></param>
        void ApplyParameterValue(object statement, CalciteParameter parameter, int offset)
        {
            if (statement is null)
                throw new ArgumentNullException(nameof(statement));
            if (parameter is null)
                throw new ArgumentNullException(nameof(parameter));

            if (int.TryParse(parameter.ParameterName, out var index) == false || index < 0)
                throw new CalciteDbException("Parameter names must be postive integers.");

            // if parameter is to be used as input, set the value based on the index
            if (parameter.Direction == ParameterDirection.Input || parameter.Direction == ParameterDirection.InputOutput)
            {
                //statement.SetParameterValue(index + offset, parameter.DbType, parameter.Value);
                throw new NotImplementedException(); // TODO implement setting the parameter
                return;
            }

            // if parameter is to be used as output, set the value based on the index
            if (parameter.Direction == ParameterDirection.Output || parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.ReturnValue)
            {
                if (statement is not CallableStatement callableStatement)
                    throw new CalciteDbException("Cannot add output statement to non-callable statement.");

                callableStatement.registerOutParameter(index + offset, CalciteDbType.ToJdbcType(parameter.DbType), parameter.Scale);
            }
        }

        /// <summary>
        /// Executes the command against its connection object, returning the number of rows affected.
        /// </summary>
        /// <returns></returns>
        public override int ExecuteNonQuery()
        {
            using (var lck = new ExecutingLock(this))
            {
                if (_connection == null)
                    throw new CalciteDbException("Connection must be available.");

                if (_connection.State != ConnectionState.Open)
                    throw new CalciteDbException("Connection must be open.");

                if (_connection._impl is null)
                    throw new InvalidOperationException();

                try
                {
                    var type = CommandType;
                    var text = CommandText;

                    // if we're doing an implicit return value for a stored procedure, it is always index 0, and everything is offset
                    var offset = type == CommandType.StoredProcedure ? 1 : 0;

                    if (_prepared is not null)
                    {
                        foreach (CalciteParameter parameter in _parameters)
                            ApplyParameterValue(_prepared, parameter, offset);

                        try
                        {
                            // mark statement as excuting
                            _executing = _prepared;

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                //_prepared.setQueryTimeout(CommandTimeout);
                                //_prepared.execute();
                                throw new NotImplementedException();
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            //return _prepared.getUpdateCount();
                            throw new NotImplementedException();
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }
                    else
                    {
                        try
                        {
                            _executing = _connection._impl.createStatement();
                            //_executing.setQueryTimeout(CommandTimeout);
                            throw new NotImplementedException();

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                switch (type)
                                {
                                    case CommandType.Text:
                                        //_executing.execute(text);
                                        break;
                                    case CommandType.StoredProcedure:
                                        //_executing.execute(BuildJdbcStoredProcedureCallString(text));
                                        break;
                                    case CommandType.TableDirect:
                                        throw new NotImplementedException();
                                }
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            //return _executing.getUpdateCount();
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }

                    throw new InvalidOperationException();
                }
                catch (CalciteException e)
                {
                    throw new CalciteDbException(e);
                }
            }
        }

        /// <summary>
        /// Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.
        /// </summary>
        /// <returns></returns>
        public override object? ExecuteScalar()
        {
            // execute a reader with a single result
            using var rdr = ExecuteDbDataReader(CommandBehavior.SingleResult);
            while (rdr.Read())
                return rdr[0];

            return null;
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="DbDataReader"/> which can be used to access the results.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            using (var lck = new ExecutingLock(this))
            {
                if (_connection == null)
                    throw new CalciteDbException("Connection must be available.");

                if (_connection.State != ConnectionState.Open)
                    throw new CalciteDbException("Connection must be open.");

                if (_connection._impl is null)
                    throw new InvalidOperationException();

                try
                {
                    var type = CommandType;
                    var text = CommandText;

                    // if we're doing an implicit return value for a stored procedure, it is always index 0, adn everything is offset
                    var offset = type == CommandType.StoredProcedure ? 1 : 0;

                    if (_prepared is not null)
                    {
                        foreach (CalciteParameter parameter in _parameters)
                            ApplyParameterValue(_prepared, parameter, offset);

                        try
                        {
                            // mark statement as executing
                            _executing = _prepared;

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                //_prepared.setQueryTimeout(CommandTimeout);
                                //_prepared.execute();
                                throw new NotImplementedException();
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            return new CalciteDataReader((CalciteCommand)this, _executing);
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }
                    else
                    {
                        try
                        {
                            _executing = null; // TODO start
                            //_executing.setQueryTimeout(CommandTimeout);

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                switch (type)
                                {
                                    case CommandType.Text:
                                        //_executing.execute(CommandText);
                                        break;
                                    case CommandType.StoredProcedure:
                                        //_executing.execute(BuildJdbcStoredProcedureCallString(text));
                                        break;
                                    case CommandType.TableDirect:
                                        throw new NotImplementedException();
                                }
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            return new CalciteDataReader((CalciteCommand)this, _executing);
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }

                    throw new InvalidOperationException();
                }
                catch (java.lang.Exception e)
                {
                    throw new CalciteDbException(e);
                }
            }
        }

        /// <summary>
        /// Attempts to cancel the execution of a DbCommand.
        /// </summary>
        public override void Cancel()
        {
            lock (_syncRoot)
                if (_executing != null)
                    throw new NotImplementedException();
                    //_executing.cancel();
        }

    }

}
