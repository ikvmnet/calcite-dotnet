using Microsoft.Data.SqlClient;

using System;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// A SQL Server LocalDB database, populated, and dropped with the test.
    /// </summary>
    /// <remarks>
    /// <para>
    /// LocalDB is the only SQL Server a test run can assume it can create for itself, and it exists on
    /// Windows alone. <see cref="IsAvailable"/> answers whether one is reachable; a suite running anywhere
    /// else skips rather than fails.
    /// </para>
    /// <para>
    /// The same database is reachable through three drivers, and the fixture offers all three:
    /// <see cref="DataSource"/> is SqlClient, <see cref="OdbcDataSource"/> is whichever SQL Server ODBC
    /// driver is installed, and <see cref="OleDbDataSource"/> whichever OLE DB provider is registered. One
    /// server described three ways is the only thing that shows a metadata provider reading its own
    /// driver's collections wrongly, because the right answer is the same for all three.
    /// </para>
    /// </remarks>
    sealed class SqlServerFixture : IDisposable
    {

        const string Instance = @"(localdb)\MSSQLLocalDB";

        /// <summary>
        /// Returns a connection string to the named database on the local instance.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        static string ConnectionStringFor(string database)
        {
            return new SqlConnectionStringBuilder()
            {
                DataSource = Instance,
                InitialCatalog = database,
                IntegratedSecurity = true,
                TrustServerCertificate = true,
                ConnectTimeout = 30,
            }.ConnectionString;
        }

        static SqlServerFixture? _shared;

        /// <summary>
        /// Gets the one database the suite shares, made on first use.
        /// </summary>
        /// <remarks>
        /// Shared rather than made per test because <c>CREATE DATABASE</c> against LocalDB costs a second or
        /// two and there are some two hundred tests behind it — per test that is ten minutes of a suite that
        /// otherwise runs in forty seconds. Nothing that reads it writes to it. <see cref="DisposeShared"/>
        /// is called from the assembly cleanup.
        /// </remarks>
        public static SqlServerFixture Shared => _shared ??= new SqlServerFixture();

        /// <summary>
        /// Drops the shared database, if one was ever made.
        /// </summary>
        public static void DisposeShared()
        {
            _shared?.Dispose();
            _shared = null;
        }

        static bool? _available;

        /// <summary>
        /// Gets whether a LocalDB instance can be reached, so the tests that need one can be skipped where
        /// there is none.
        /// </summary>
        /// <remarks>
        /// Asked once: a failing connection waits out the timeout, and doing that per test would cost more
        /// than the suite.
        /// </remarks>
        public static bool IsAvailable => _available ??= Probe();

        /// <summary>
        /// Opens a connection to <c>master</c> and reports whether it worked.
        /// </summary>
        /// <returns></returns>
        static bool Probe()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                return false;

            try
            {
                using var connection = new SqlConnection(ConnectionStringFor("master"));
                connection.Open();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// The SQL Server ODBC drivers worth trying, newest first.
        /// </summary>
        static readonly string[] OdbcDrivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"];

        /// <summary>
        /// The SQL Server OLE DB providers worth trying, newest first.
        /// </summary>
        static readonly string[] OleDbProviders = ["MSOLEDBSQL19", "MSOLEDBSQL", "SQLOLEDB"];

        static string? _odbcDriver;
        static bool _odbcProbed;
        static string? _oleDbProvider;
        static bool _oleDbProbed;

        /// <summary>
        /// Gets the name of an installed SQL Server ODBC driver, or <see langword="null"/> if there is none.
        /// </summary>
        public static string? OdbcDriver
        {
            get
            {
                if (_odbcProbed == false)
                {
                    _odbcDriver = IsAvailable ? FirstThatConnects(OdbcDrivers, d => new OdbcConnection(OdbcConnectionStringFor(d, "master"))) : null;
                    _odbcProbed = true;
                }

                return _odbcDriver;
            }
        }

        /// <summary>
        /// Gets the name of a registered SQL Server OLE DB provider, or <see langword="null"/> if there is
        /// none.
        /// </summary>
        public static string? OleDbProvider
        {
            get
            {
                if (_oleDbProbed == false)
                {
                    _oleDbProvider = IsAvailable ? FirstThatConnects(OleDbProviders, p => new OleDbConnection(OleDbConnectionStringFor(p, "master"))) : null;
                    _oleDbProbed = true;
                }

                return _oleDbProvider;
            }
        }

        /// <summary>
        /// Returns the first of the candidates a connection can be opened with.
        /// </summary>
        /// <param name="candidates"></param>
        /// <param name="connect"></param>
        /// <returns></returns>
        static string? FirstThatConnects(string[] candidates, Func<string, DbConnection> connect)
        {
            foreach (var candidate in candidates)
            {
                try
                {
                    using var connection = connect(candidate);
                    connection.Open();
                    return candidate;
                }
                catch (Exception)
                {
                    // an absent driver, an absent provider, or one built for the other architecture
                }
            }

            return null;
        }

        /// <summary>
        /// Returns an ODBC connection string to the named database on the local instance.
        /// </summary>
        /// <param name="driver"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        static string OdbcConnectionStringFor(string driver, string database)
        {
            // TrustServerCertificate matters to driver 18, which defaults to encrypting and then refuses
            // LocalDB's self-signed certificate
            return $"Driver={{{driver}}};Server={Instance};Database={database};Trusted_Connection=yes;TrustServerCertificate=yes;";
        }

        /// <summary>
        /// Returns an OLE DB connection string to the named database on the local instance.
        /// </summary>
        /// <param name="provider"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        static string OleDbConnectionStringFor(string provider, string database)
        {
            return $"Provider={provider};Data Source={Instance};Initial Catalog={database};Integrated Security=SSPI;";
        }

        readonly string _database;

        /// <summary>
        /// Creates a database holding the tables the tests query.
        /// </summary>
        public SqlServerFixture()
        {
            _database = $"calcite_ado_{Guid.NewGuid():N}";
            ExecuteOnMaster($"CREATE DATABASE [{_database}]");

            DataSource = new Source(ConnectionStringFor(_database));

            // the table from the report this fixture exists for: a scan of it read the information schema,
            // and the INT column's tinyint precision is what threw
            Execute("""
                CREATE TABLE dbo.SUPPLIERS (
                    PRODUCT   VARCHAR(64)  NOT NULL PRIMARY KEY,
                    SUPPLIER  VARCHAR(128) NOT NULL,
                    LEAD_DAYS INT          NOT NULL)
                """);

            Execute("""
                INSERT INTO dbo.SUPPLIERS (PRODUCT, SUPPLIER, LEAD_DAYS) VALUES
                    ('Widget', 'Acme', 3),
                    ('Gadget', 'Globex', 10),
                    ('Doohickey', 'Initech', 1)
                """);

            Execute("""
                CREATE TABLE dbo.EMPS (
                    EMPNO   INT           NOT NULL PRIMARY KEY,
                    NAME    NVARCHAR(64)  NOT NULL,
                    DEPTNO  INT           NULL,
                    SALARY  DECIMAL(9,2)  NULL)
                """);

            Execute("""
                INSERT INTO dbo.EMPS (EMPNO, NAME, DEPTNO, SALARY) VALUES
                    (1, 'Alice', 10, 100.50),
                    (2, 'Bob',   10, 200.00),
                    (3, 'Carol', 20, 300.25),
                    (4, 'Dave',  20, NULL)
                """);

            Execute("CREATE TABLE dbo.DEPTS (DEPTNO INT NOT NULL PRIMARY KEY, DNAME NVARCHAR(64) NOT NULL)");
            Execute("INSERT INTO dbo.DEPTS (DEPTNO, DNAME) VALUES (10, 'Sales'), (20, 'Engineering'), (30, 'Empty')");

            // one column of every type the server's information schema names differently, so that a gap in
            // the type mapping shows up as a failing test rather than as a table nobody can read
            Execute("""
                CREATE TABLE dbo.TYPES (
                    ID          INT              NOT NULL PRIMARY KEY,
                    C_BIT       BIT              NULL,
                    C_TINYINT   TINYINT          NULL,
                    C_SMALLINT  SMALLINT         NULL,
                    C_BIGINT    BIGINT           NULL,
                    C_DECIMAL   DECIMAL(12,3)    NULL,
                    C_NUMERIC   NUMERIC(8,4)     NULL,
                    C_MONEY     MONEY            NULL,
                    C_SMALLMONEY SMALLMONEY      NULL,
                    C_FLOAT     FLOAT            NULL,
                    C_REAL      REAL             NULL,
                    C_CHAR      CHAR(4)          NULL,
                    C_VARCHAR   VARCHAR(16)      NULL,
                    C_VARCHARMAX VARCHAR(MAX)    NULL,
                    C_NCHAR     NCHAR(4)         NULL,
                    C_NVARCHAR  NVARCHAR(16)     NULL,
                    C_DATE      DATE             NULL,
                    C_TIME      TIME(3)          NULL,
                    C_DATETIME  DATETIME         NULL,
                    C_SMALLDATETIME SMALLDATETIME NULL,
                    C_DATETIME2 DATETIME2(3)     NULL,
                    C_DATETIMEOFFSET DATETIMEOFFSET(3) NULL,
                    C_BINARY    BINARY(4)        NULL,
                    C_VARBINARY VARBINARY(16)    NULL,
                    C_GUID      UNIQUEIDENTIFIER NULL,
                    C_XML       XML              NULL)
                """);

            Execute("""
                INSERT INTO dbo.TYPES VALUES (
                    1,
                    1,
                    200,
                    -300,
                    9000000000,
                    123456789.125,
                    1234.5678,
                    12.3400,
                    1.2300,
                    1.5,
                    2.5,
                    'abcd',
                    'varchar',
                    'unbounded',
                    N'wxyz',
                    N'nvarchar',
                    '2020-01-15',
                    '01:02:03.500',
                    '2020-01-15T10:20:30',
                    '2020-01-15T10:20:00',
                    '2020-01-15T10:20:30.250',
                    '2020-01-15T10:20:30.250+00:00',
                    0x01020304,
                    0x0A0B,
                    '3f2504e0-4f89-11d3-9a0c-0305e82c3301',
                    '<a b="c"/>')
                """);

            Execute("INSERT INTO dbo.TYPES (ID) VALUES (2)");
        }

        /// <summary>
        /// Gets the data source the adapter is pointed at.
        /// </summary>
        public DbDataSource DataSource { get; }

        /// <summary>
        /// Gets the same database reached through ODBC.
        /// </summary>
        public DbDataSource OdbcDataSource => _odbcDataSource ??= new Source<OdbcConnection>(
            OdbcConnectionStringFor(OdbcDriver ?? throw new InvalidOperationException("no ODBC driver"), _database),
            s => new OdbcConnection(s));

        DbDataSource? _odbcDataSource;

        /// <summary>
        /// Gets the same database reached through OLE DB.
        /// </summary>
        public DbDataSource OleDbDataSource => _oleDbDataSource ??= new Source<OleDbConnection>(
            OleDbConnectionStringFor(OleDbProvider ?? throw new InvalidOperationException("no OLE DB provider"), _database),
            s => new OleDbConnection(s));

        DbDataSource? _oleDbDataSource;

        /// <summary>
        /// Runs a statement against the fixture's database.
        /// </summary>
        /// <param name="sql"></param>
        public void Execute(string sql)
        {
            using var connection = DataSource.CreateConnection();
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Runs a statement against <c>master</c>, which is where a database is created and dropped from.
        /// </summary>
        /// <param name="sql"></param>
        static void ExecuteOnMaster(string sql)
        {
            using var connection = new SqlConnection(ConnectionStringFor("master"));
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _odbcDataSource?.Dispose();
            _oleDbDataSource?.Dispose();
            DataSource.Dispose();
            SqlConnection.ClearAllPools();

            try
            {
                // a connection of our own still open would make the drop wait for it
                ExecuteOnMaster($"ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_database}]");
            }
            catch (SqlException)
            {
                // a database left behind on a developer's LocalDB is not worth failing a test over
            }
        }

        /// <summary>
        /// The <see cref="DbDataSource"/> Microsoft.Data.SqlClient does not ship.
        /// </summary>
        /// <param name="connectionString"></param>
        sealed class Source(string connectionString) : DbDataSource
        {

            /// <inheritdoc />
            public override string ConnectionString => connectionString;

            /// <inheritdoc />
            protected override DbConnection CreateDbConnection() => new SqlConnection(connectionString);

        }

        /// <summary>
        /// The <see cref="DbDataSource"/> neither System.Data.Odbc nor System.Data.OleDb ships.
        /// </summary>
        /// <typeparam name="TConnection">The connection type, which is what the metadata factory dispatches on.</typeparam>
        /// <param name="connectionString"></param>
        /// <param name="create"></param>
        sealed class Source<TConnection>(string connectionString, Func<string, TConnection> create) : DbDataSource
            where TConnection : DbConnection
        {

            /// <inheritdoc />
            public override string ConnectionString => connectionString;

            /// <inheritdoc />
            protected override DbConnection CreateDbConnection() => create(connectionString);

        }

    }

}
