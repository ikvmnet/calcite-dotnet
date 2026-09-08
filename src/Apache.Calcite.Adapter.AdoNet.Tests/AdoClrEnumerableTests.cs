using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Adapter.AdoNet.Metadata;
using Apache.Calcite.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// The adapter reached through this provider's own connection, whose plans are compiled as expression
    /// trees rather than generated as Java source.
    /// </summary>
    /// <remarks>
    /// Every other test in this project opens <c>jdbc:calcite:</c> through <c>DriverManager</c>, which is
    /// Calcite's connection and Calcite's prepare. None of them says anything about this path. A plan over
    /// an ADO.NET schema is necessarily a mixed one — the adapter's own subtree stays in its convention —
    /// and the crossing depends on the connection's mode: by default the adapter converts straight into
    /// <c>ClrAsyncEnumerableConvention</c> through <c>AdoToClrAsyncEnumerableConverter</c>, and in
    /// synchronous mode straight into <c>ClrEnumerableConvention</c> through
    /// <c>AdoToClrEnumerableConverter</c>. Either way there is no linq4j layer between.
    /// </remarks>
    [TestClass]
    public class AdoClrEnumerableTests
    {

        SqliteFixture _sqlite = null!;
        CalciteConnection _connection = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();
            _connection = OpenConnection();
        }

        /// <summary>
        /// Opens a connection over the fixture's SQLite schema, in the mode asked for.
        /// </summary>
        CalciteConnection OpenConnection(bool synchronous = false)
        {
            return OpenConnection(root => root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null)), synchronous);
        }

        /// <summary>
        /// Opens a connection over the given <see cref="AdoDataSource"/>, in the mode asked for.
        /// </summary>
        static CalciteConnection OpenConnection(AdoDataSource dataSource, bool synchronous = false, bool decorrelate = true)
        {
            return OpenConnection(root => root.add("ADO", AdoSchema.Create(root, "ADO", dataSource, null, null)), synchronous, decorrelate);
        }

        /// <summary>
        /// Opens a connection whose root schema is built by <paramref name="configure"/>.
        /// </summary>
        static CalciteConnection OpenConnection(Action<org.apache.calcite.schema.SchemaPlus> configure, bool synchronous, bool decorrelate = true)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                Synchronous = synchronous ? true : null,
                ForceDecorrelate = decorrelate ? null : false,
            }.ToString())
                .ConfigureRootSchema(configure)
                .Build()
                .OpenConnection();
        }

        [TestCleanup]
        public void Cleanup()
        {
            _connection?.Dispose();
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Runs a query and returns its rows as strings, so a comparison does not depend on which numeric
        /// type a provider chose.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        List<string> Rows(string sql)
        {
            using var cmd = _connection.CreateCommand();
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

        /// <summary>
        /// Returns the rendered plan for <paramref name="sql"/> on <paramref name="connection"/>.
        /// </summary>
        static string Explain(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR " + sql;

            var plan = new System.Text.StringBuilder();
            using (var r = cmd.ExecuteReader())
                while (r.Read())
                    plan.AppendLine(r.GetValue(0)?.ToString());

            return plan.ToString();
        }

        /// <summary>
        /// The read path from the report, through this convention's own converter: a
        /// <c>uniqueidentifier</c> cast to the <c>UUID</c> a view gives it, projected. The cast is pushed
        /// into the ADO convention, so the reader is handed a <c>UUID</c> column and the row holds a
        /// <c>java.util.UUID</c> — which is the only thing <c>GetGuid</c> on this provider reads.
        /// </summary>
        /// <remarks>
        /// SQL Server rather than the fixture's SQLite because SQLite has no type for the sixteen bytes:
        /// a type name it does not recognise gives a cast numeric affinity, so the value coming back would
        /// be a number rather than anything a GUID could be read out of.
        /// </remarks>
        [TestMethod]
        public void ShouldReadAUuidCastThroughThisConvention()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Inconclusive("No SQL Server LocalDB instance is reachable on this machine.");

            var server = SqlServerFixture.Shared;

            using var connection = new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                Synchronous = true,
            }.ToString())
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", server.DataSource, null, "dbo")))
                .Build()
                .OpenConnection();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(C_GUID AS UUID) FROM ADO.TYPES WHERE ID = 1";

            using var r = cmd.ExecuteReader();
            Assert.IsTrue(r.Read(), "expected one row");
            Assert.AreEqual(new System.Guid("3f2504e0-4f89-11d3-9a0c-0305e82c3301"), r.GetGuid(0));
            Assert.IsFalse(r.Read(), "and only that one");
        }

        /// <summary>
        /// In synchronous mode the adapter converts straight into the synchronous convention.
        /// </summary>
        /// <remarks>
        /// One converter, and it is that convention's own. Reaching <c>AdoToEnumerableConverter</c> instead
        /// would still answer correctly, by way of a second converter, and no other assertion here would
        /// notice. The mode is pinned because the default plans asynchronously and reaches a different
        /// converter — <see cref="ShouldCarryTheAdapterIntoTheAsyncConvention"/> holds that plan.
        /// </remarks>
        [TestMethod]
        public void ShouldConvertStraightIntoThisConvention()
        {
            using var c = OpenConnection(synchronous: true);

            var plan = Explain(c, "SELECT empno, name FROM ADO.emps WHERE deptno = 10");

            StringAssert.Contains(plan, "AdoToClrEnumerableConverter");
            Assert.IsFalse(plan.Contains("AdoToEnumerableConverter"), plan);
        }

        /// <summary>
        /// By default the adapter converts straight into the asynchronous convention, pushed down intact.
        /// </summary>
        /// <remarks>
        /// The subtree under the converter is the adapter's own — an <c>AdoProject</c> rather than a scan
        /// with the work done above it — so the crossing loses no pushdown.
        ///
        /// <para>One converter, and no synchronous node between it and the reader. The route this replaced
        /// was a converter over <c>AdoToEnumerableConverter</c>, which answers the same rows and blocks a
        /// thread on the socket for every one of them, the ADO leaf being the one place in a plan with real
        /// network I/O to wait on.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldCarryTheAdapterIntoTheAsyncConvention()
        {
            var plan = Explain(_connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10");

            StringAssert.Contains(plan, "AdoToClrAsyncEnumerableConverter");
            StringAssert.Contains(plan, "AdoProject");
            Assert.IsFalse(plan.Contains("AdoToEnumerableConverter"), plan);
            Assert.IsFalse(plan.Contains("AdoToClrEnumerableConverter"), plan);
        }

        [TestMethod]
        public void ShouldScanAnAdoTable()
        {
            CollectionAssert.AreEquivalent(
                new[] { "1|Alice|10", "2|Bob|10", "3|Carol|20", "4|Dave|20", "5|Erin|null" },
                Rows("SELECT empno, name, deptno FROM ADO.emps ORDER BY empno"));
        }

        [TestMethod]
        public void ShouldFilterAnAdoTable()
        {
            CollectionAssert.AreEquivalent(
                new[] { "3|Carol|20", "4|Dave|20" },
                Rows("SELECT empno, name, deptno FROM ADO.emps WHERE deptno = 20"));
        }

        [TestMethod]
        public void ShouldAggregateAnAdoTable()
        {
            CollectionAssert.AreEquivalent(
                new[] { "10|2", "20|2", "null|1" },
                Rows("SELECT deptno, COUNT(*) FROM ADO.emps GROUP BY deptno ORDER BY deptno"));
        }

        [TestMethod]
        public void ShouldJoinAcrossTheConverter()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering" },
                Rows("SELECT e.name, d.dname FROM ADO.emps e JOIN ADO.depts d ON e.deptno = d.deptno ORDER BY e.name"));
        }

        /// <summary>
        /// The default plan opens its connection without blocking, which is the whole point of the converter
        /// it reaches.
        /// </summary>
        /// <remarks>
        /// The plan is what is being asserted, read through the one observable the data source has: a plan
        /// under <c>AdoToClrAsyncEnumerableConverter</c> calls <see cref="AdoDataSource.OpenConnectionAsync"/>
        /// and the synchronous route calls <see cref="AdoDataSource.OpenConnection"/>. Metadata's own
        /// connections do not reach either: those are opened against the <c>DbDataSource</c> the metadata was
        /// built from.
        /// </remarks>
        [TestMethod]
        public async Task ShouldOpenTheConnectionWithoutBlocking()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            CollectionAssert.AreEquivalent(new[] { "1|Alice", "2|Bob" }, await RowsAsync(connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10"));

            Assert.AreEqual(1, source.OpenedAsync);
            Assert.AreEqual(0, source.Opened);
        }

        /// <summary>
        /// In synchronous mode it is the blocking open, there being no asynchronous plan to open for.
        /// </summary>
        [TestMethod]
        public void ShouldOpenTheConnectionSynchronouslyInSynchronousMode()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source, synchronous: true);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps WHERE deptno = 10";
            using (var r = cmd.ExecuteReader())
                while (r.Read()) { }

            Assert.AreEqual(1, source.Opened);
            Assert.AreEqual(0, source.OpenedAsync);
        }

        /// <summary>
        /// A correlated sub-query pushed down asynchronously still gets its parameters.
        /// </summary>
        /// <remarks>
        /// The one thing the asynchronous converter does not share with the synchronous one is the
        /// implementor it reads correlation variables from, and a variable is registered on the implementor
        /// implementing the plan and is unknown to every other. Nothing below here would notice the wrong one
        /// at planning time. <c>forceDecorrelate=false</c> is what leaves a correlate in the plan at all —
        /// Calcite rewrites one into a join wherever it can, and by default always tries.
        /// </remarks>
        [TestMethod]
        public async Task ShouldEnrichACorrelatedSubQueryOnTheAsyncPath()
        {
            using var connection = OpenConnection(new CountingAdoDataSource(_sqlite.DataSource), decorrelate: false);
            const string Sql = "SELECT e.name, (SELECT d.dname FROM ADO.depts d WHERE d.deptno = e.deptno) FROM ADO.emps e";

            // the correlate has to still be there, and the inner sub-plan has to still be the adapter's, or
            // the query below proves nothing about the enricher
            var plan = Explain(connection, Sql);
            StringAssert.Contains(plan, "ClrAsyncEnumerableCorrelate");
            StringAssert.Contains(plan, "AdoFilter(condition=[=($0, $cor0.DEPTNO)])");
            StringAssert.Contains(plan, "AdoToClrAsyncEnumerableConverter");

            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|null" },
                await RowsAsync(connection, Sql));
        }

        /// <summary>
        /// <see cref="AdoSequences.ReadAsync{TRow}"/> on its own, without a plan around it.
        /// </summary>
        [TestMethod]
        public async Task ShouldReadRowsAsynchronously()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);

            var names = new List<string>();
            await foreach (var name in AdoSequences.ReadAsync(source, "SELECT NAME FROM EMPS WHERE DEPTNO = 10 ORDER BY EMPNO", r => r.GetString(0), null))
                names.Add(name);

            CollectionAssert.AreEqual(new[] { "Alice", "Bob" }, names);
            Assert.AreEqual(1, source.OpenedAsync);
            Assert.AreEqual(1, source.Closed);
        }

        /// <summary>
        /// The statement is not sent until the first row is asked for, an <see cref="IAsyncEnumerable{T}"/>
        /// having nowhere earlier to await.
        /// </summary>
        [TestMethod]
        public async Task ShouldNotOpenUntilTheFirstRowIsAskedFor()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);

            var rows = AdoSequences.ReadAsync(source, "SELECT NAME FROM EMPS", r => r.GetString(0), null);
            Assert.AreEqual(0, source.OpenedAsync);

            var enumerator = rows.GetAsyncEnumerator();

            try
            {
                Assert.IsTrue(await enumerator.MoveNextAsync());
                Assert.AreEqual(1, source.OpenedAsync);
            }
            finally
            {
                await enumerator.DisposeAsync();
            }
        }

        /// <summary>
        /// A statement the provider rejects closes what was opened for it.
        /// </summary>
        [TestMethod]
        public async Task ShouldCloseTheConnectionWhenTheStatementFails()
        {
            var source = new CountingAdoDataSource(_sqlite.DataSource);

            await Assert.ThrowsExactlyAsync<AdoCalciteException>(async () =>
            {
                await foreach (var _ in AdoSequences.ReadAsync(source, "SELECT NAME FROM NO_SUCH_TABLE", r => r.GetString(0), null))
                {

                }
            });

            Assert.AreEqual(1, source.Closed);
        }

        /// <summary>
        /// Runs a query through the asynchronous reader and returns its rows as strings.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
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
        /// The fixture's data source, counting which of the two opens a plan reached for and how many of the
        /// connections it handed out were disposed.
        /// </summary>
        /// <param name="dataSource"></param>
        sealed class CountingAdoDataSource(DbDataSource dataSource) : AdoDataSource
        {

            readonly AdoDatabaseMetadata _metadata = AdoDatabaseMetadataFactoryImpl.Instance.Create(dataSource);

            int _opened;
            int _openedAsync;
            int _closed;

            /// <summary>
            /// Gets the number of blocking opens.
            /// </summary>
            public int Opened => Volatile.Read(ref _opened);

            /// <summary>
            /// Gets the number of asynchronous opens.
            /// </summary>
            public int OpenedAsync => Volatile.Read(ref _openedAsync);

            /// <summary>
            /// Gets the number of connections handed out that were disposed.
            /// </summary>
            public int Closed => Volatile.Read(ref _closed);

            /// <inheritdoc />
            public override DbConnection OpenConnection()
            {
                Interlocked.Increment(ref _opened);

                return new Counted(dataSource.OpenConnection(), this);
            }

            /// <inheritdoc />
            public override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref _openedAsync);

                return new Counted(await dataSource.OpenConnectionAsync(cancellationToken), this);
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
