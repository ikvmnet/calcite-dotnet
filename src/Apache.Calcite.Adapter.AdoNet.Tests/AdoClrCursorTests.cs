using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using System.Threading;
using System;

using Apache.Calcite.Adapter.AdoNet.Metadata;
using Apache.Calcite.Data;

using FluentAssertions;

using Microsoft.Data.Sqlite;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// The adapter under the cursor convention, which is the convention <c>Apache.Calcite.Data</c> plans
    /// into: the provider's reader is the plan's leaf.
    /// </summary>
    /// <remarks>
    /// What is held here: the converter chosen, the rows the provider reads over the adapter, that both
    /// advances read the same rows over one connection, that a failing statement fails the open of either
    /// kind, that a correlated sub-query is enriched from the cursor convention's correlate, and
    /// <see cref="AdoCursors"/> read directly, without a plan around it.
    /// </remarks>
    public class AdoClrCursorTests : IDisposable
    {

        SqliteFixture _sqlite = null!;
        CalciteConnection _connection = null!;

        public AdoClrCursorTests()
        {
            _sqlite = new SqliteFixture();
            _connection = OpenConnection(new CountingAdoDataSource(_sqlite.DataSource));
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _sqlite?.Dispose();
        }

        static CalciteConnection OpenConnection(AdoDataSource dataSource, bool decorrelate = true)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                ForceDecorrelate = decorrelate ? null : false,
            }.ToString())
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", dataSource, null, null)))
                .Build()
                .OpenConnection();
        }

        static string Explain(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR " + sql;

            using var r = cmd.ExecuteReader();
            var plan = new System.Text.StringBuilder();
            while (r.Read())
                plan.AppendLine(r.GetString(0));

            return plan.ToString();
        }

        static List<string> Rows(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;

            var rows = new List<string>();
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var values = new string[r.FieldCount];
                for (int i = 0; i < r.FieldCount; i++)
                    values[i] = r.IsDBNull(i) ? "null" : r.GetValue(i).ToString()!;

                rows.Add(string.Join("|", values));
            }

            return rows;
        }

        static async Task<List<string>> RowsAsync(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;

            var rows = new List<string>();
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var values = new string[r.FieldCount];
                for (int i = 0; i < r.FieldCount; i++)
                    values[i] = await r.IsDBNullAsync(i) ? "null" : r.GetValue(i).ToString()!;

                rows.Add(string.Join("|", values));
            }

            return rows;
        }

        /// <summary>
        /// The adapter converts straight into the cursor convention, pushed down intact.
        /// </summary>
        /// <remarks>
        /// One converter, the cursor convention's own, and nothing of Calcite's between the adapter and the
        /// reader: the route by way of <c>AdoToEnumerableConverter</c> answers the same rows and costs a
        /// converter more.
        /// </remarks>
        [Fact]
        public void ShouldCarryTheAdapterIntoTheCursorConvention()
        {
            var plan = Explain(_connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10");

            Assert.Contains("AdoToClrCursorConverter", plan);
            Assert.Contains("AdoProject", plan);
            Assert.False(plan.Contains("AdoToEnumerableConverter"), plan);
        }

        /// <summary>
        /// The two advances answer the same rows for the same statements.
        /// </summary>
        [Fact]
        public async Task ShouldReadTheSameRowsThroughBothAdvances()
        {
            foreach (var sql in new[]
            {
                "SELECT empno, name, deptno FROM ADO.emps ORDER BY empno",
                "SELECT name FROM ADO.emps WHERE deptno = 20 ORDER BY name",
                "SELECT deptno, COUNT(*) FROM ADO.emps GROUP BY deptno ORDER BY deptno",
                "SELECT salary FROM ADO.emps ORDER BY empno",
                "SELECT e.name, d.dname FROM ADO.emps e JOIN ADO.depts d ON e.deptno = d.deptno ORDER BY e.name",
            })
            {
                (await RowsAsync(_connection, sql)).Should().Equal(Rows(_connection, sql), sql);
            }
        }

        /// <summary>
        /// Either open opens one connection and either disposal closes it.
        /// </summary>
        [Fact]
        public async Task ShouldOpenOneConnectionAndCloseItThroughEitherOpen()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            Rows(connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10").Should().Equal(["1|Alice", "2|Bob"]);
            source.Opened.Should().Be(1);
            source.Closed.Should().Be(1);

            (await RowsAsync(connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10")).Should().Equal(["1|Alice", "2|Bob"]);
            source.Opened.Should().Be(2);
            source.Closed.Should().Be(2);
        }

        /// <summary>
        /// A failing statement fails the open of either kind, not the first advance.
        /// </summary>
        /// <remarks>
        /// The open is the acquisition: the connection is opened and the statement sent inside it, so a
        /// caller gets the failure back from <c>ExecuteReader</c> or <c>ExecuteReaderAsync</c>, as a caller
        /// of any provider expects.
        /// </remarks>
        [Fact]
        public async Task ShouldFailFromEitherOpen()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource) { Failing = true };
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps";

            Assert.Throws<CalciteException>(() => cmd.ExecuteReader());
            await Assert.ThrowsAsync<CalciteException>(async () => await cmd.ExecuteReaderAsync());
        }

        /// <summary>
        /// A correlated sub-query pushed down under the cursor convention's correlate still gets its parameters.
        /// </summary>
        /// <remarks>
        /// The correlation variables come off the implementor implementing the plan, which is the cursor
        /// convention's here, so the builder has to read them from that one. <c>forceDecorrelate=false</c>
        /// is what leaves a correlate in the plan at all.
        /// </remarks>
        [Fact]
        public async Task ShouldEnrichACorrelatedSubQueryUnderTheCursorCorrelate()
        {
            using var connection = OpenConnection(new CountingAdoDataSource(_sqlite.DataSource), decorrelate: false);
            const string Sql = "SELECT e.name, (SELECT d.dname FROM ADO.depts d WHERE d.deptno = e.deptno) FROM ADO.emps e";

            var plan = Explain(connection, Sql);
            Assert.Contains("ClrCursorCorrelate", plan);
            Assert.Contains("AdoFilter(condition=[=($0, $cor0.DEPTNO)])", plan);
            Assert.Contains("AdoToClrCursorConverter", plan);

            var expected = new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|null" };
            Assert.Equivalent(expected, await RowsAsync(connection, Sql), strict: true);
            Assert.Equivalent(expected, Rows(connection, Sql), strict: true);
        }


        /// <summary>
        /// A <c>uniqueidentifier</c> cast to the <c>UUID</c> a view gives it, projected, reads back as a GUID.
        /// </summary>
        /// <remarks>
        /// The cast is pushed into the ADO convention, so the reader is handed a <c>UUID</c> column and the
        /// row holds a <c>java.util.UUID</c> — which is the only thing <c>GetGuid</c> on this provider reads.
        /// SQL Server rather than the fixture's SQLite because SQLite has no type for the sixteen bytes: a
        /// type name it does not recognise gives a cast numeric affinity, so the value coming back would be a
        /// number rather than anything a GUID could be read out of.
        /// </remarks>
        [Fact]
        public void ShouldReadAUuidCastThroughTheCursorConvention()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");

            var server = SqlServerFixture.Shared;

            using var connection = new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
            }.ToString())
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", server.DataSource, null, "dbo")))
                .Build()
                .OpenConnection();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(C_GUID AS UUID) FROM ADO.TYPES WHERE ID = 1";

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read(), "expected one row");
            Assert.Equal(new Guid("3f2504e0-4f89-11d3-9a0c-0305e82c3301"), r.GetGuid(0));
            Assert.False(r.Read(), "and only that one");
        }

        [Fact]
        public void ShouldScanAnAdoTable()
        {
            Rows(_connection, "SELECT empno, name, deptno FROM ADO.emps ORDER BY empno")
                .Should().Equal(["1|Alice|10", "2|Bob|10", "3|Carol|20", "4|Dave|20", "5|Erin|null"]);
        }

        [Fact]
        public void ShouldFilterAnAdoTable()
        {
            Rows(_connection, "SELECT empno, name, deptno FROM ADO.emps WHERE deptno = 20 ORDER BY empno")
                .Should().Equal(["3|Carol|20", "4|Dave|20"]);
        }

        [Fact]
        public void ShouldAggregateAnAdoTable()
        {
            Rows(_connection, "SELECT deptno, COUNT(*) FROM ADO.emps GROUP BY deptno ORDER BY deptno")
                .Should().Equal(["10|2", "20|2", "null|1"]);
        }

        [Fact]
        public void ShouldJoinAcrossTheConverter()
        {
            Rows(_connection, "SELECT e.name, d.dname FROM ADO.emps e JOIN ADO.depts d ON e.deptno = d.deptno ORDER BY e.name")
                .Should().Equal(["Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering"]);
        }

        /// <summary>
        /// <see cref="AdoCursors"/> on its own, without a plan around it, opened and read either way.
        /// </summary>
        [Fact]
        public async Task ShouldReadRowsThroughEitherOpenOfTheReader()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);
            const string Sql = "SELECT NAME FROM EMPS WHERE DEPTNO = 10 ORDER BY EMPNO";

            var names = new List<string>();
            using (var cursor = AdoCursors.Open(source, Sql, r => r.GetString(0), null))
                while (cursor.Read())
                    names.Add(cursor.Current);

            await using (var cursor = await AdoCursors.OpenAsync(source, Sql, r => r.GetString(0), null, CancellationToken.None))
                while (await cursor.ReadAsync(CancellationToken.None))
                    names.Add(cursor.Current);

            names.Should().Equal(["Alice", "Bob", "Alice", "Bob"]);
            source.Opened.Should().Be(2);
            source.Closed.Should().Be(2);
        }

        /// <summary>
        /// The statement is sent at the open, before any row is asked for, and a cursor closed without a
        /// row still closes its connection.
        /// </summary>
        /// <remarks>
        /// Where the convention acquires, and the reason a failing statement reaches the caller from the
        /// call that executed it: <c>CalciteSession</c> opens the plan inside <c>ExecuteReader</c> and
        /// <c>ExecuteReaderAsync</c>.
        /// </remarks>
        [Fact]
        public async Task ShouldSendTheStatementAtTheOpen()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);

            var cursor = await AdoCursors.OpenAsync(source, "SELECT NAME FROM EMPS", r => r.GetString(0), null, CancellationToken.None);
            source.Opened.Should().Be(1, "the open sends the statement");

            await cursor.DisposeAsync();
            source.Closed.Should().Be(1, "a cursor closed without a row still closes its connection");
        }

        /// <summary>
        /// A statement the provider rejects fails the open of either kind and closes what was opened for it.
        /// </summary>
        [Fact]
        public async Task ShouldCloseTheConnectionWhenTheStatementFails()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);
            const string Sql = "SELECT NAME FROM NO_SUCH_TABLE";

            Assert.Throws<AdoCalciteException>(() => AdoCursors.Open(source, Sql, r => r.GetString(0), null));
            source.Closed.Should().Be(1);

            await Assert.ThrowsAsync<AdoCalciteException>(async () => await AdoCursors.OpenAsync(source, Sql, r => r.GetString(0), null, CancellationToken.None));
            source.Closed.Should().Be(2);
        }

        /// <summary>
        /// A read under a cancelled token is refused, and the cursor still closes its connection.
        /// </summary>
        /// <remarks>
        /// The token is the read's own, handed to the provider's <c>ReadAsync</c>: the statement was sent
        /// by the open, so what the token stops is the reading. How the provider's token reaches this from
        /// <c>ExecuteReaderAsync</c> and from each <c>ReadAsync</c> is <c>AdoCancellationTests</c>.
        /// </remarks>
        [Fact]
        public async Task ShouldObserveACancelledReadToken()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await using (var cursor = await AdoCursors.OpenAsync(source, "SELECT NAME FROM EMPS", r => r.GetString(0), null, CancellationToken.None))
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await cursor.ReadAsync(cancellation.Token));

            source.Closed.Should().Be(1, "and the connection the open acquired is closed");
        }

        /// <summary>
        /// The fixture's data source, counting the connections a plan opened and how many of them were
        /// disposed.
        /// </summary>
        /// <param name="dataSource"></param>
        internal sealed class CountingAdoDataSource(DbDataSource dataSource) : AdoDataSource
        {

            readonly AdoDatabaseMetadata _metadata = AdoDatabaseMetadataFactoryImpl.Instance.Create(dataSource);

            int _opened;
            int _closed;

            /// <summary>
            /// Gets the number of connections opened.
            /// </summary>
            public int Opened => Volatile.Read(ref _opened);

            /// <summary>
            /// Gets the number of connections handed out that were disposed.
            /// </summary>
            public int Closed => Volatile.Read(ref _closed);

            /// <summary>
            /// Gets or sets whether every open fails as the provider rejecting the statement would.
            /// </summary>
            public bool Failing { get; init; }

            /// <inheritdoc />
            public override DbConnection OpenConnection()
            {
                Interlocked.Increment(ref _opened);

                if (Failing)
                    throw new SqliteException("armed", 1);

                return new Counted(dataSource.OpenConnection(), this);
            }

            /// <inheritdoc />
            public override string ConnectionString => dataSource.ConnectionString;

            /// <inheritdoc />
            public override AdoDatabaseMetadata Metadata => _metadata;

            /// <summary>
            /// A connection that tells the source it was disposed.
            /// </summary>
            /// <param name="connection"></param>
            /// <param name="source"></param>
            sealed class Counted(DbConnection connection, CountingAdoDataSource source) : DbConnection
            {

                /// <inheritdoc />
                [AllowNull]
                public override string ConnectionString { get => connection.ConnectionString; set => connection.ConnectionString = value; }

                /// <inheritdoc />
                public override string Database => connection.Database;

                /// <inheritdoc />
                public override string DataSource => connection.DataSource;

                /// <inheritdoc />
                public override string ServerVersion => connection.ServerVersion;

                /// <inheritdoc />
                public override ConnectionState State => connection.State;

                /// <inheritdoc />
                public override void ChangeDatabase(string databaseName) => connection.ChangeDatabase(databaseName);

                /// <inheritdoc />
                public override void Close() => connection.Close();

                /// <inheritdoc />
                public override void Open() => connection.Open();

                /// <inheritdoc />
                public override Task OpenAsync(CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);

                /// <inheritdoc />
                protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => connection.BeginTransaction(isolationLevel);

                /// <inheritdoc />
                protected override DbCommand CreateDbCommand() => connection.CreateCommand();

                /// <inheritdoc />
                protected override void Dispose(bool disposing)
                {
                    if (disposing)
                    {
                        Interlocked.Increment(ref source._closed);
                        connection.Dispose();
                    }

                    base.Dispose(disposing);
                }

                /// <inheritdoc />
                public override async ValueTask DisposeAsync()
                {
                    Interlocked.Increment(ref source._closed);
                    await connection.DisposeAsync();
                    GC.SuppressFinalize(this);
                }

            }

        }

    }

}
