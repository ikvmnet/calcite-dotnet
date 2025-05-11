using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a SQL statement or stored procedure to execute against a <see cref="CalciteConnection"/>.
    /// </summary>
    public abstract partial class CalciteCommandBase : DbCommand
    {

        readonly object _syncRoot = new object();
        readonly CalciteParameterCollection _parameters = new CalciteParameterCollection();
        CalciteConnection? _connection;
        CommandType _commandType = CommandType.Text;
        string? _commandText;

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
            set => throw new NotImplementedException();
        }

        /// <summary>
        /// Gets or sets how the <see cref="CommandText"/> property is interpreted.
        /// </summary>
        public override CommandType CommandType
        {
            get => _commandType;
            set => throw new NotImplementedException();
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
            set => throw new NotImplementedException();
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
        /// Creates a new instance of a <see cref="DbParameter"/> object.
        /// </summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter()
        {
            return new CalciteParameter();
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Executes the command against its connection object, returning the number of rows affected.
        /// </summary>
        /// <returns></returns>
        public override int ExecuteNonQuery()
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        /// <summary>
        /// Attempts to cancel the execution of a DbCommand.
        /// </summary>
        public override void Cancel()
        {
            lock (_syncRoot)
                throw new NotImplementedException();
        }

    }

}
