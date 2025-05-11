using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// ADO.NET connection for Apache Calcite.
    /// </summary>
    public class CalciteConnection : DbConnection
    {

        internal CalciteConnectionStringBuilder? _connectionStringBuilder;
        CalciteConnectionConfig _config;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnection()
        {

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
        /// Gets the connection string.
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
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns the root schema.
        /// </summary>
        public SchemaPlus RootSchema => throw new NotImplementedException();

        /// <summary>
        /// Returns the type factory.
        /// </summary>
        public JavaTypeFactory TypeFactory => throw new NotImplementedException();

        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        public override ConnectionState State => throw new NotImplementedException();

        /// <summary>
        /// Gets the current database.
        /// </summary>
        public override string Database => throw new NotImplementedException();

        /// <summary>
        /// Gets the current datasource.
        /// </summary>
        public override string DataSource => "";

        /// <summary>
        /// Gets the version of the database server if available.
        /// </summary>
        public override string ServerVersion => throw new NotImplementedException();

        /// <summary>
        /// Attempts to change the database.
        /// </summary>
        /// <param name="databaseName"></param>
        public override void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open to change databases.");

            throw new NotImplementedException();
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Open()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Closes the connection.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Close()
        {
            if (State == ConnectionState.Closed)
                return;

            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the current database transaction.
        /// </summary>
        /// <returns></returns>
        public CalciteTransaction? Transaction => throw new NotImplementedException();

        /// <summary>
        /// Starts a new JDBC transaction by disabling auto-commit.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        /// <exception cref="CalciteDbException"></exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
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
