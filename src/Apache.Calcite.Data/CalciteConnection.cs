using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using java.sql;

using org.apache.calcite.config;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// ADO.NET wrapper for an underlying Calcite connection.
    /// </summary>
    public class CalciteConnection : DbConnection
    {

        /// <summary>
        /// Initializes the type.
        /// </summary>
        static CalciteConnection()
        {
            GC.KeepAlive(typeof(org.apache.calcite.jdbc.Driver));
        }

        internal CalciteConnectionStringBuilder _connectionStringBuilder;
        internal java.sql.Connection? _impl;
        internal CalciteTransaction? _transaction;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnection()
        {
            _connectionStringBuilder = new CalciteConnectionStringBuilder();
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connectionString"></param>
        public CalciteConnection(string connectionString) :
            this(new CalciteConnectionStringBuilder(connectionString))
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connectionStringBuilder"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public CalciteConnection(CalciteConnectionStringBuilder connectionStringBuilder)
        {
            _connectionStringBuilder = connectionStringBuilder ?? throw new ArgumentNullException(nameof(connectionStringBuilder));
        }

        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        public override ConnectionState State => _impl == null ? ConnectionState.Closed : _impl.isClosed() ? ConnectionState.Closed : ConnectionState.Open;

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        internal CalciteConnectionStringBuilder? ConnectionStringBuilder => _connectionStringBuilder;

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
#if NET
        [AllowNull]
#endif
        public override string ConnectionString
        {
            get => ConnectionStringBuilder?.ConnectionString ?? "";
            set => SetConnectionString(value ?? "");
        }

        /// <summary>
        /// Sets the connection string.
        /// </summary>
        /// <param name="value"></param>
        /// <exception cref="CalciteDbException"></exception>
        void SetConnectionString(string value)
        {
            if (State != ConnectionState.Closed)
                throw new CalciteDbException("Connection must be closed to update connection string.");

            // reset connection string
            _connectionStringBuilder = new CalciteConnectionStringBuilder(value);
            _impl = null;
        }

        /// <summary>
        /// Gets the current database.
        /// </summary>
        public override string Database => _impl != null ? _impl.getCatalog() : throw new CalciteDbException("Connection is not open.");

        /// <summary>
        /// Gets the current datasource.
        /// </summary>
        public override string DataSource => "";

        /// <summary>
        /// Gets the version of the database server if available.
        /// </summary>
        public override string ServerVersion => _impl != null ? _impl.getMetaData().getDatabaseProductVersion() : throw new CalciteDbException("Connection is not open.");

        /// <summary>
        /// Attempts to change the database.
        /// </summary>
        /// <param name="databaseName"></param>
        public override void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open to change databases.");

            if (_impl is null)
                throw new InvalidOperationException();

            try
            {
                _impl.setCatalog(databaseName);
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Closes the connection.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Close()
        {
            if (State == ConnectionState.Closed)
                return;

            if (_impl is null)
                throw new InvalidOperationException();

            try
            {
                _impl.close();
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Open()
        {
            // already open
            if (_impl != null && State == ConnectionState.Open)
                return;

            // already open
            if (_impl != null && State == ConnectionState.Connecting)
                return;

            // was open at one time
            if (_impl != null)
                throw new CalciteDbException("Connection has already been opened.");

            // haven't configured the connection string
            if (_connectionStringBuilder == null)
                throw new CalciteDbException("Connection string has not been configured.");

            try
            {
                var props = new java.util.Properties();
                foreach (KeyValuePair<string, object> entry in _connectionStringBuilder)
                    props.setProperty(entry.Key, entry.Value.ToString());

                _impl = DriverManager.getConnection("jdbc:calcite:", props);
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the current database transaction.
        /// </summary>
        /// <returns></returns>
        public CalciteTransaction? Transaction => _transaction;

        /// <summary>
        /// Starts a new Calcite transaction by disabling auto-commit.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        /// <exception cref="CalciteDbException"></exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Creates a command for execution against the current connection.
        /// </summary>
        /// <returns></returns>
        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }

        /// <summary>
        /// Creates a command for execution against the current connection.
        /// </summary>
        /// <returns></returns>
        public new CalciteCommand CreateCommand()
        {
            return new CalciteCommand(this);
        }

    }

}
