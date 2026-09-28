using System;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

using Microsoft.Data.SqlClient;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// A populated SQL Server LocalDB database, dropped on dispose.
    /// </summary>
    /// <remarks>
    /// <para>
    /// LocalDB exists only on Windows. <see cref="IsAvailable"/> reports whether an instance is reachable,
    /// and tests that need one skip where it is not.
    /// </para>
    /// <para>
    /// The database is reachable through three drivers: <see cref="DataSource"/> is SqlClient,
    /// <see cref="OdbcDataSource"/> the first SQL Server ODBC driver that connects, and
    /// <see cref="OleDbDataSource"/> the first OLE DB provider that connects. Since the right answer is the
    /// same for all three, comparing them exposes a metadata provider that misreads its driver.
    /// </para>
    /// </remarks>
    sealed class SqlServerFixture : IDisposable
    {

        const string Instance = @"(localdb)\MSSQLLocalDB";

        /// <summary>
        /// Returns a SqlClient connection string to the named database on the local instance.
        /// </summary>
        /// <param name="database">The database to make the initial catalog.</param>
        /// <returns>A connection string using integrated security and trusting the server certificate.</returns>
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
        /// Gets the database the suite shares, created on first use.
        /// </summary>
        /// <remarks>
        /// Shared because creating a LocalDB database is slow. Tests only read it. <see cref="DisposeShared"/>
        /// is called from the assembly fixture.
        /// </remarks>
        public static SqlServerFixture Shared => _shared ??= new SqlServerFixture();

        /// <summary>
        /// Drops the shared database, if one was created.
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
        /// Probed once, because a failing connection waits out the timeout.
        /// </remarks>
        public static bool IsAvailable => _available ??= Probe();

        /// <summary>
        /// Opens a connection to <c>master</c> and reports whether it worked; always false off Windows.
        /// </summary>
        /// <returns><see langword="true"/> if a connection to <c>master</c> opened; otherwise
        /// <see langword="false"/>.</returns>
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
        /// Returns the first of the candidates a connection can be opened with, or <see langword="null"/>.
        /// </summary>
        /// <param name="candidates">The driver or provider names to try, in order of preference.</param>
        /// <param name="connect">Builds an unopened connection for a candidate.</param>
        /// <returns>The first candidate whose connection opened, or <see langword="null"/> if none did.</returns>
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
        /// <param name="driver">The installed ODBC driver's name.</param>
        /// <param name="database">The database to connect to.</param>
        /// <returns>A connection string using Windows authentication and trusting the server
        /// certificate.</returns>
        static string OdbcConnectionStringFor(string driver, string database)
        {
            // driver 18 encrypts by default and otherwise rejects LocalDB's self-signed certificate
            return $"Driver={{{driver}}};Server={Instance};Database={database};Trusted_Connection=yes;TrustServerCertificate=yes;";
        }

        /// <summary>
        /// Returns an OLE DB connection string to the named database on the local instance.
        /// </summary>
        /// <param name="provider">The registered OLE DB provider's name.</param>
        /// <param name="database">The database to connect to.</param>
        /// <returns>A connection string using Windows authentication.</returns>
        static string OleDbConnectionStringFor(string provider, string database)
        {
            return $"Provider={provider};Data Source={Instance};Initial Catalog={database};Integrated Security=SSPI;";
        }

        readonly string _database;

        /// <summary>
        /// Creates a uniquely named database holding the tables the tests query.
        /// </summary>
        public SqlServerFixture()
        {
            _database = $"calcite_ado_{Guid.NewGuid():N}";
            ExecuteOnMaster($"CREATE DATABASE [{_database}]");

            DataSource = new Source(ConnectionStringFor(_database));

            // a scan reads the column types from the information schema, which reports a precision for the
            // INT column
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

            // concatenation operands, one row with a null: the operator must reach the server in a form it
            // parses, and a null operand must still make the whole expression null
            Execute("""
                CREATE TABLE dbo.CAT (
                    ID INT         NOT NULL PRIMARY KEY,
                    A  VARCHAR(16) NULL,
                    B  VARCHAR(16) NULL)
                """);
            Execute("INSERT INTO dbo.CAT (ID, A, B) VALUES (1, 'aa', 'bb'), (2, 'cc', NULL), (3, 'dd', 'ee')");

            // one column of each type the information schema names differently, so a gap in the type mapping
            // fails a test; row 1 holds values and row 2 only nulls
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
        /// <param name="sql">The statement to run.</param>
        public void Execute(string sql)
        {
            using var connection = DataSource.CreateConnection();
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Runs a statement against <c>master</c>, where a database is created and dropped from.
        /// </summary>
        /// <param name="sql">The statement to run, typically <c>CREATE DATABASE</c> or <c>DROP
        /// DATABASE</c>.</param>
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
                // SINGLE_USER closes any connection still open, which would otherwise block the drop
                ExecuteOnMaster($"ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_database}]");
            }
            catch (SqlException)
            {
                // a database left behind on LocalDB is not a test failure
            }
        }

        /// <summary>
        /// A <see cref="DbDataSource"/> over SqlClient connections.
        /// </summary>
        /// <param name="connectionString">The connection string each connection is created with.</param>
        sealed class Source(string connectionString) : DbDataSource
        {

            /// <inheritdoc />
            public override string ConnectionString => connectionString;

            /// <inheritdoc />
            protected override DbConnection CreateDbConnection() => new SqlConnection(connectionString);

        }

        /// <summary>
        /// A <see cref="DbDataSource"/> over ODBC or OLE DB connections, which System.Data.Odbc and
        /// System.Data.OleDb do not provide.
        /// </summary>
        /// <typeparam name="TConnection">The connection type, which the metadata factory dispatches
        /// on.</typeparam>
        /// <param name="connectionString">The connection string each connection is created with.</param>
        /// <param name="create">Creates a connection from the connection string.</param>
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
