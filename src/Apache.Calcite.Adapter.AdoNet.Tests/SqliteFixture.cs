using Microsoft.Data.Sqlite;

using System;
using System.Data.Common;
using System.IO;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// A SQLite database on disk, populated, and disposed with the test.
    /// </summary>
    /// <remarks>
    /// On disk rather than in memory because the adapter opens its own connections: an in-memory SQLite
    /// database belongs to the connection that made it, so a second connection would find nothing there.
    /// </remarks>
    sealed class SqliteFixture : IDisposable
    {

        readonly string _path;

        /// <summary>
        /// Creates a database holding the standard tables the tests query.
        /// </summary>
        public SqliteFixture()
        {
            _path = Path.Combine(Path.GetTempPath(), $"calcite-ado-{Guid.NewGuid():N}.db");
            DataSource = new Source($"Data Source={_path}");

            Execute("""
                CREATE TABLE EMPS (
                    EMPNO   INTEGER NOT NULL,
                    NAME    NVARCHAR(64) NOT NULL,
                    DEPTNO  INTEGER NULL,
                    SALARY  REAL NULL,
                    HIREDATE DATE NULL)
                """);

            Execute("CREATE TABLE DEPTS (DEPTNO INTEGER NOT NULL, DNAME NVARCHAR(64) NOT NULL)");

            Execute("""
                INSERT INTO EMPS (EMPNO, NAME, DEPTNO, SALARY, HIREDATE) VALUES
                    (1, 'Alice',   10, 100.5, '2020-01-15'),
                    (2, 'Bob',     10, 200.0, '2021-06-30'),
                    (3, 'Carol',   20, 300.25, '2019-11-01'),
                    (4, 'Dave',    20, NULL,  NULL),
                    (5, 'Erin',    NULL, 50.0, '2022-03-15')
                """);

            Execute("INSERT INTO DEPTS (DEPTNO, DNAME) VALUES (10, 'Sales'), (20, 'Engineering'), (30, 'Empty')");
        }

        /// <summary>
        /// Gets the data source the adapter is pointed at.
        /// </summary>
        public DbDataSource DataSource { get; }

        /// <summary>
        /// Runs a statement against the database.
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

        /// <inheritdoc />
        public void Dispose()
        {
            DataSource.Dispose();
            SqliteConnection.ClearAllPools();

            try
            {
                if (File.Exists(_path))
                    File.Delete(_path);
            }
            catch (IOException)
            {
                // a file the process still holds open is not worth failing a test over
            }
        }

        /// <summary>
        /// The <see cref="DbDataSource"/> Microsoft.Data.Sqlite does not ship.
        /// </summary>
        /// <param name="connectionString"></param>
        sealed class Source(string connectionString) : DbDataSource
        {

            /// <inheritdoc />
            public override string ConnectionString => connectionString;

            /// <inheritdoc />
            protected override DbConnection CreateDbConnection() => new SqliteConnection(connectionString);

        }

    }

}
