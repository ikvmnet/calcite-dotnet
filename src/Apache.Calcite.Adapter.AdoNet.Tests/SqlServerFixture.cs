using Microsoft.Data.SqlClient;

using System;
using System.Data.Common;
using System.Runtime.InteropServices;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// A SQL Server LocalDB database, populated, and dropped with the test.
    /// </summary>
    /// <remarks>
    /// LocalDB is the only SQL Server a test run can assume it can create for itself, and it exists on
    /// Windows alone. <see cref="IsAvailable"/> answers whether one is reachable; a suite running anywhere
    /// else skips rather than fails.
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
                Pooling = false,
            }.ConnectionString;
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

    }

}
