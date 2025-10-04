using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using java.lang;
using java.sql;

using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.schema;
using org.apache.calcite.server;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// ADO.NET connection for Apache Calcite.
    /// </summary>
    public class CalciteConnection : DbConnection
    {
        
        CalciteConnectionStringBuilder? _connectionStringBuilder;
        CalciteConnectionImpl? _impl;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="properties"></param>
        public CalciteConnection(string url, IDictionary<string, string> properties)
        {
            if (url is null)
                throw new ArgumentNullException(nameof(url));

            _connectionStringBuilder = new CalciteConnectionStringBuilder() { Url = url };

            foreach (var kvp in properties)
                _connectionStringBuilder.Add(kvp.Key, kvp.Value);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connectionString"></param>
        public CalciteConnection(string connectionString) :
            this()
        {
            _connectionStringBuilder = new CalciteConnectionStringBuilder(connectionString);
        }

        /// <summary>
        /// Gets the root schema.
        /// </summary>
        public SchemaPlus? RootSchema { get; }

        /// <summary>
        /// Gets the type factory.
        /// </summary>
        public JavaTypeFactoryImpl? TypeFactory { get; }

        /// <summary>
        /// Gets the properties of the connection.
        /// </summary>
        public IReadOnlyDictionary<string, string> Properties { get; } = new Dictionary<string, string>();

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        public CalciteConnectionConfig? Config { get; }

        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        public override ConnectionState State => _impl == null ? ConnectionState.Closed : _impl.State;

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
        public override string Database => _connection != null ? _connection.getCatalog() : throw new CalciteDbException("Connection is not open.");

        /// <summary>
        /// Gets the current datasource.
        /// </summary>
        public override string DataSource => "";

        /// <summary>
        /// Gets the version of the database server if available.
        /// </summary>
        public override string ServerVersion => _connection != null ? _connection.getMetaData().getDatabaseProductVersion() : throw new CalciteDbException("Connection is not open.");

        /// <summary>
        /// Attempts to change the database.
        /// </summary>
        /// <param name="databaseName"></param>
        public override void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open to change databases.");

            if (_connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection.setCatalog(databaseName);
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

            if (_connection is null)
                throw new InvalidOperationException();

            try
            {
                if (_leaveOpen == false)
                    _connection.close();
            }
            catch (java.lang.Exception e)
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
            if (_connection != null && State == ConnectionState.Open)
                return;

            // already open
            if (_connection != null && State == ConnectionState.Connecting)
                return;

            // was open at one time
            if (_connection != null)
                throw new CalciteDbException("Connection has already been opened.");

            // haven't configured the connection string
            if (_connectionStringBuilder == null)
                throw new CalciteDbException("Connection string has not been configured.");

            try
            {
                var props = new java.util.Properties();
                foreach (KeyValuePair<string, object> entry in _connectionStringBuilder)
                    if (string.Equals(entry.Key, "url", StringComparison.OrdinalIgnoreCase) == false)
                        props.setProperty(entry.Key, entry.Value.ToString());

                _connection = DriverManager.getConnection(_connectionStringBuilder.Url, props);
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
        /// Starts a new JDBC transaction by disabling auto-commit.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        /// <exception cref="CalciteDbException"></exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
                throw new CalciteDbException("Connection is not opened.");

            if (_transaction != null)
                throw new CalciteDbException("JDBC only supports a single ambient transaction.");

            try
            {
                return _transaction = new CalciteTransaction(this, isolationLevel);
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
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
