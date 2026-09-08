using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// <see cref="AdoSequences.Read{TRow}"/> and <see cref="AdoSequences.ReadAsync{TRow}"/> read by
    /// themselves, without a plan around them.
    /// </summary>
    /// <remarks>
    /// The two are twins with one stated difference — when the statement is sent — and a plan cannot show
    /// that, because a plan evaluates its expression and enumerates the result in one breath. Counting the
    /// connections a source has opened at each point is what shows it.
    /// </remarks>
    [TestClass]
    public class AdoSequencesTests
    {

        /// <summary>
        /// An <see cref="AdoDataSource"/> over the fixture that counts what it has opened.
        /// </summary>
        sealed class CountingDataSource(DbDataSource dataSource) : AdoDataSource
        {

            readonly AdoDatabaseMetadata _metadata = AdoDatabaseMetadataFactoryImpl.Instance.Create(dataSource);

            /// <summary>
            /// Gets how many connections have been opened, by either route.
            /// </summary>
            public int Opened { get; private set; }

            /// <summary>
            /// Gets how many of those were opened by <see cref="OpenConnectionAsync"/>.
            /// </summary>
            public int OpenedAsynchronously { get; private set; }

            /// <inheritdoc />
            public override DbConnection OpenConnection()
            {
                Opened++;
                return dataSource.OpenConnection();
            }

            /// <inheritdoc />
            public override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
            {
                Opened++;
                OpenedAsynchronously++;
                return await dataSource.OpenConnectionAsync(cancellationToken);
            }

            /// <inheritdoc />
            public override string ConnectionString => dataSource.ConnectionString;

            /// <inheritdoc />
            public override AdoDatabaseMetadata Metadata => _metadata;

        }

        SqliteFixture _sqlite = null!;
        CountingDataSource _dataSource = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();
            _dataSource = new CountingDataSource(_sqlite.DataSource);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _sqlite?.Dispose();
        }

        const string Sql = "SELECT NAME FROM EMPS ORDER BY EMPNO";

        static string Name(DbDataReader reader) => reader.GetString(0);

        static readonly string[] Names = ["Alice", "Bob", "Carol", "Dave", "Erin"];

        [TestMethod]
        public async Task ShouldReadTheRowsAsynchronously()
        {
            var rows = new List<string>();
            await foreach (var row in AdoSequences.ReadAsync(_dataSource, Sql, Name, null))
                rows.Add(row);

            CollectionAssert.AreEqual(Names, rows);
            Assert.AreEqual(1, _dataSource.OpenedAsynchronously);
        }

        /// <summary>
        /// The statement is sent on the first <c>MoveNextAsync</c> and not before.
        /// </summary>
        /// <remarks>
        /// The one place <see cref="AdoSequences.ReadAsync{TRow}"/> differs from
        /// <see cref="AdoSequences.Read{TRow}"/>, and it is a CLR limit rather than a choice: a method
        /// returning an <c>IAsyncEnumerable</c> cannot await before it returns, and opening the connection
        /// is an await. Pinned so that the difference is a stated one rather than a discovered one.
        /// </remarks>
        [TestMethod]
        public async Task ShouldNotSendTheStatementUntilEnumerated()
        {
            var rows = AdoSequences.ReadAsync(_dataSource, Sql, Name, null);
            Assert.AreEqual(0, _dataSource.Opened);

            var enumerator = rows.GetAsyncEnumerator();
            Assert.AreEqual(0, _dataSource.Opened, "GetAsyncEnumerator alone opens nothing.");

            try
            {
                Assert.IsTrue(await enumerator.MoveNextAsync());
                Assert.AreEqual(1, _dataSource.Opened);
            }
            finally
            {
                await enumerator.DisposeAsync();
            }
        }

        /// <summary>
        /// The synchronous twin sends it where it is called, which is what linq4j's leaf does in
        /// <c>enumerator()</c>.
        /// </summary>
        [TestMethod]
        public void ShouldSendTheStatementWhereTheSynchronousOneIsCalled()
        {
            var rows = AdoSequences.Read(_dataSource, Sql, Name, null);

            Assert.AreEqual(1, _dataSource.Opened);
            CollectionAssert.AreEqual(Names, new List<string>(rows));
        }

        /// <summary>
        /// A statement the provider refuses arrives as an <see cref="AdoCalciteException"/>, as it does from
        /// the synchronous twin, rather than as whichever exception the driver chose.
        /// </summary>
        [TestMethod]
        public async Task ShouldWrapAFailureFromTheProvider()
        {
            var rows = AdoSequences.ReadAsync(_dataSource, "SELECT NOPE FROM EMPS", Name, null);

            await Assert.ThrowsExactlyAsync<AdoCalciteException>(async () =>
            {
                await foreach (var row in rows)
                    Assert.Fail("a row was read from a statement the provider refused.");
            });
        }

        /// <summary>
        /// The connection is released when the sequence is, whether or not it was drained.
        /// </summary>
        /// <remarks>
        /// SQLite holds the file, so a fixture whose connections leaked cannot be deleted; this asserts the
        /// pool rather than the file, because the pool is what a provider with a network connection has.
        /// </remarks>
        [TestMethod]
        public async Task ShouldReleaseTheConnectionOnAnAbandonedSequence()
        {
            var enumerator = AdoSequences.ReadAsync(_dataSource, Sql, Name, null).GetAsyncEnumerator();
            Assert.IsTrue(await enumerator.MoveNextAsync());
            await enumerator.DisposeAsync();

            // if the first sequence had held its connection open this would be reading over a released one
            var rows = new List<string>();
            await foreach (var row in AdoSequences.ReadAsync(_dataSource, Sql, Name, null))
                rows.Add(row);

            CollectionAssert.AreEqual(Names, rows);
        }

        /// <summary>
        /// A token cancelled before enumeration stops the sequence, and no row is read.
        /// </summary>
        /// <remarks>
        /// The token reaches the sequence at <c>GetAsyncEnumerator</c>, which is where .NET puts it and
        /// where the <c>[EnumeratorCancellation]</c> parameter receives it — not at the call, which is why
        /// the plan can build this with <c>default</c> and still be cancellable. Which
        /// <see cref="OperationCanceledException"/> arrives is the provider's choice;
        /// <c>Microsoft.Data.Sqlite</c> answers a <see cref="TaskCanceledException"/> from the open.
        /// </remarks>
        [TestMethod]
        public async Task ShouldObserveACancelledToken()
        {
            using var source = new CancellationTokenSource();
            source.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await foreach (var row in AdoSequences.ReadAsync(_dataSource, Sql, Name, null).WithCancellation(source.Token))
                    Assert.Fail("a row was read under a cancelled token.");
            });
        }

    }

}
