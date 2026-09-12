using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Adapter.AdoNet.Metadata;
using Apache.Calcite.Data;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Requires that the token a consumer gives <c>ExecuteReaderAsync</c> is the token the provider's own
    /// <c>DbDataReader.ReadAsync</c> is called with, and says what a token given later reaches instead.
    /// </summary>
    /// <remarks>
    /// <c>ClrAsyncEnumerablePlanCancellationTests</c> establishes that a compiled plan carries a token to a
    /// leaf that suspends, over a table written for the purpose. This asks the same of the whole stack a
    /// consumer actually uses — <c>CalciteCommand.ExecuteReaderAsync</c>, <c>CalciteSession</c>, the
    /// implementor, <c>AdoToClrEnumerableConverter</c>, <c>AdoSequences.ReadAsync</c> — ending at a real
    /// <see cref="DbDataReader"/> over a real database. Nothing between those two ends holds a token: the
    /// convention has none in the plan, so what is measured is that the token
    /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> was given is threaded the whole way and nowhere
    /// replaced by a default.
    ///
    /// <para>The measurement is the token itself rather than an effect of it. A
    /// <see cref="CancellationToken"/> is a struct over its source, so comparing the one the provider was
    /// handed to the one the caller created settles which token arrived; asserting only that a cancelled
    /// read throws would pass just as well if the throw came from the reader above the leaf.</para>
    /// </remarks>
    [TestClass]
    public class AdoCancellationTests
    {

        SqliteFixture _sqlite = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();
        }

        [TestCleanup]
        public void Cleanup()
        {
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Opens a connection over the recording source.
        /// </summary>
        CalciteConnection OpenConnection(RecordingAdoDataSource source)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
            }.ToString())
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", source, null, null)))
                .Build()
                .OpenConnection();
        }

        /// <summary>
        /// The token given to <c>ExecuteReaderAsync</c> cancels the token the provider's reader is called
        /// with.
        /// </summary>
        /// <remarks>
        /// Linkage rather than identity. What reaches the leaf is the statement's own token — the one
        /// <c>StatementCancellation</c> makes, so that Calcite's cancel flag and this convention's operators
        /// can be driven by one cancellation — and it is linked to the caller's. Comparing the two tokens
        /// would fail on that and say nothing about whether cancelling one cancels the other, which is the
        /// property being asked for.
        /// </remarks>
        [TestMethod]
        public async Task ShouldCarryExecuteReaderAsyncsTokenToTheProvidersReader()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cancellation = new CancellationTokenSource();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync(cancellation.Token);
            (await reader.ReadAsync(cancellation.Token)).Should().BeTrue();

            source.ReadTokens.Should().NotBeEmpty("the provider's reader was called");
            source.ReadTokens.Should().AllSatisfy(t =>
            {
                t.CanBeCanceled.Should().BeTrue("a token that cannot be cancelled carries nothing");
                t.IsCancellationRequested.Should().BeFalse();
            });

            cancellation.Cancel();

            source.ReadTokens.Should().AllSatisfy(t => t.IsCancellationRequested.Should().BeTrue());
        }

        /// <summary>
        /// Cancelling that token stops the provider's reader rather than the loop above it.
        /// </summary>
        /// <remarks>
        /// Cancelled after a row has been read, so that what stops is a read in progress rather than the
        /// acquisition: <c>AdoSequences.ReadAsync</c> sends the statement at <c>GetAsyncEnumerator</c> and
        /// sends it synchronously, an acquisition being unable to await, so a token cancelled before the
        /// first read would be observed by the row loop and prove nothing about the leaf.
        /// </remarks>
        [TestMethod]
        public async Task ShouldStopTheProvidersReaderWhenTheCallerCancels()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cancellation = new CancellationTokenSource();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync(cancellation.Token);

            (await reader.ReadAsync(cancellation.Token)).Should().BeTrue();
            var before = source.Reads;

            cancellation.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancellation.Token));

            source.Reads.Should().Be(before, "the provider was not asked for another row");
        }

        /// <summary>
        /// A token given only to <c>ReadAsync</c> reaches the provider too, for the duration of the read.
        /// </summary>
        /// <remarks>
        /// The leaf is enumerated under the statement's token whether or not the caller gave
        /// <c>ExecuteReaderAsync</c> one — an <see cref="IAsyncEnumerable{T}"/> takes its token at
        /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, which <c>CalciteSession</c> calls once, and
        /// <c>MoveNextAsync</c> takes none — so a token arriving later at <c>DbDataReader.ReadAsync</c>
        /// reaches that leaf by cancelling the statement, registered for the length of the call. What shows
        /// here is the provider being handed a cancellable token over a statement no caller gave one to;
        /// that it goes on to stop a table already reading is <c>StatementCancellationTests</c>, which has a
        /// table that blocks to be cancelled in.
        ///
        /// <para>Before this, the leaf under a tokenless execute ran under
        /// <see cref="CancellationToken.None"/> and nothing given to <c>ReadAsync</c> could reach it: the
        /// read stopped between rows and left the provider waiting.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldCarryAPerReadTokenToTheProvider()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync();

            using var cancellation = new CancellationTokenSource();
            (await reader.ReadAsync(cancellation.Token)).Should().BeTrue();

            source.ReadTokens.Should().NotBeEmpty("the provider's reader was called");
            source.ReadTokens.Should().AllSatisfy(t => t.CanBeCanceled.Should().BeTrue("a token that cannot be cancelled carries nothing"));

            cancellation.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancellation.Token));
        }

        /// <summary>
        /// Every read takes its own token, and a reader is read to the end under a different one each time.
        /// </summary>
        /// <remarks>
        /// The shape a consumer with a per-operation timeout writes: a fresh token per <c>ReadAsync</c>, for
        /// as many rows as there are. Each is registered against the statement for the length of its own
        /// call and released at the end of it, so none of them accumulates and none of them outlives its
        /// read.
        ///
        /// <para>The provider sees one token throughout, and that is not a contradiction: the leaf is
        /// enumerating under the statement's token, fixed at <c>GetAsyncEnumerator</c>, and a per-read token
        /// reaches it by cancelling that rather than by replacing it. <c>MoveNextAsync</c> takes no token,
        /// so there is no replacing it.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldTakeADifferentTokenOnEveryRead()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync();

            var rows = 0;
            var used = new List<CancellationTokenSource>();

            while (true)
            {
                var perRead = new CancellationTokenSource();
                used.Add(perRead);

                if (await reader.ReadAsync(perRead.Token) == false)
                    break;

                rows++;
            }

            rows.Should().Be(5, "the fixture has five employees and every one was read under its own token");
            used.Should().HaveCountGreaterThan(rows, "a token was made for the read that found no row too");

            source.ReadTokens.Distinct().Should().HaveCount(1, "the leaf enumerates under the statement's token, not the caller's");

            foreach (var perRead in used)
                perRead.Dispose();
        }

        /// <summary>
        /// A token cancelled after its own read has returned does not reach the reader.
        /// </summary>
        /// <remarks>
        /// The registration is scoped to the call, so a caller that keeps its sources around — or cancels a
        /// per-operation timeout after the operation succeeded, which is the ordinary thing to do — does not
        /// kill a reader it is still using. Without the scope the first completed read would arm a
        /// cancellation that fires whenever that caller next tidies up.
        /// </remarks>
        [TestMethod]
        public async Task ShouldSurviveATokenCancelledAfterItsRead()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync();

            using var first = new CancellationTokenSource();
            (await reader.ReadAsync(first.Token)).Should().BeTrue();

            // the read is over; cancelling now reaches a registration that no longer exists
            first.Cancel();

            using var second = new CancellationTokenSource();
            (await reader.ReadAsync(second.Token)).Should().BeTrue("the reader is still usable");

            using var third = new CancellationTokenSource();
            (await reader.ReadAsync(third.Token)).Should().BeTrue();
        }

        /// <summary>
        /// A read asked for under a token already cancelled cancels the statement.
        /// </summary>
        /// <remarks>
        /// <c>SqlDataReader.ReadAsync</c> registers before it checks, and says why: "to catch any already
        /// expired tokens to be able to trigger cancellation event". So a dead token is not quietly
        /// declined — the statement goes with it, exactly as it would had the token died a moment into the
        /// call instead of a moment before it. Checking first would make those two cases differ by a race.
        /// </remarks>
        [TestMethod]
        public async Task ShouldCancelTheStatementOnAnAlreadyCancelledReadToken()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync();

            using var live = new CancellationTokenSource();
            (await reader.ReadAsync(live.Token)).Should().BeTrue();

            using var dead = new CancellationTokenSource();
            dead.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () => await reader.ReadAsync(dead.Token));

            source.ReadTokens.Should().AllSatisfy(t => t.IsCancellationRequested.Should().BeTrue("the statement was cancelled with it"));
        }

        /// <summary>
        /// A token already cancelled stops the execute rather than the first read.
        /// </summary>
        /// <remarks>
        /// Acquisition sends the statement, and for an adapter leaf it opens a connection to send it on —
        /// <c>AdoSequences.ReadAsync</c> does both inside <c>GetAsyncEnumerator</c>, which
        /// <c>CalciteSession</c> calls from <c>ExecuteReaderAsync</c>. Without the guard a caller who had
        /// already given up still opened a connection and ran a query, and only learned about it when it
        /// read.
        /// </remarks>
        [TestMethod]
        public async Task ShouldRefuseToExecuteOnACancelledToken()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await Assert.ThrowsAsync<OperationCanceledException>(async () => await cmd.ExecuteReaderAsync(cancellation.Token));

            source.Opened.Should().Be(0, "no connection was opened to the provider");
        }

        /// <summary>
        /// An <see cref="AdoDataSource"/> that records the token every <c>DbDataReader.ReadAsync</c> under it
        /// is called with.
        /// </summary>
        /// <remarks>
        /// The decoration runs the whole way down — connection, command, reader — because the token is only
        /// visible at the last of them. Every member forwards; only <c>ReadAsync</c> does anything else.
        /// </remarks>
        sealed class RecordingAdoDataSource(DbDataSource dataSource) : AdoDataSource
        {

            readonly AdoDatabaseMetadata _metadata = AdoDatabaseMetadataFactoryImpl.Instance.Create(dataSource);
            readonly List<CancellationToken> _readTokens = [];

            int _reads;
            int _opened;

            /// <summary>
            /// Gets the number of connections opened to the provider.
            /// </summary>
            public int Opened => Volatile.Read(ref _opened);

            /// <summary>
            /// Gets the tokens the provider's readers were called with, in order.
            /// </summary>
            public IReadOnlyList<CancellationToken> ReadTokens
            {
                get { lock (_readTokens) return _readTokens.ToArray(); }
            }

            /// <summary>
            /// Gets the number of calls made to a provider reader's <c>ReadAsync</c>.
            /// </summary>
            public int Reads => Volatile.Read(ref _reads);

            /// <summary>
            /// Records one call.
            /// </summary>
            void Record(CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _reads);

                lock (_readTokens)
                    _readTokens.Add(cancellationToken);
            }

            /// <inheritdoc />
            public override DbConnection OpenConnection()
            {
                Interlocked.Increment(ref _opened);

                return new Recorded(dataSource.OpenConnection(), this);
            }

            /// <inheritdoc />
            public override string ConnectionString => dataSource.ConnectionString;

            /// <inheritdoc />
            public override AdoDatabaseMetadata Metadata => _metadata;

            /// <summary>
            /// A connection whose commands are recorded.
            /// </summary>
            sealed class Recorded(DbConnection connection, RecordingAdoDataSource source) : DbConnection
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
                protected override DbCommand CreateDbCommand() => new RecordedCommand(connection.CreateCommand(), this, source);

                /// <inheritdoc />
                protected override void Dispose(bool disposing)
                {
                    if (disposing)
                        connection.Dispose();

                    base.Dispose(disposing);
                }

                /// <inheritdoc />
                public override async ValueTask DisposeAsync()
                {
                    await connection.DisposeAsync();
                    GC.SuppressFinalize(this);
                }

            }

            /// <summary>
            /// A command whose readers are recorded.
            /// </summary>
            sealed class RecordedCommand(DbCommand command, DbConnection owner, RecordingAdoDataSource source) : DbCommand
            {

                /// <inheritdoc />
                [AllowNull]
                public override string CommandText { get => command.CommandText; set => command.CommandText = value; }

                /// <inheritdoc />
                public override int CommandTimeout { get => command.CommandTimeout; set => command.CommandTimeout = value; }

                /// <inheritdoc />
                public override CommandType CommandType { get => command.CommandType; set => command.CommandType = value; }

                /// <inheritdoc />
                public override bool DesignTimeVisible { get => command.DesignTimeVisible; set => command.DesignTimeVisible = value; }

                /// <inheritdoc />
                public override UpdateRowSource UpdatedRowSource { get => command.UpdatedRowSource; set => command.UpdatedRowSource = value; }

                /// <inheritdoc />
                protected override DbConnection? DbConnection { get => owner; set { } }

                /// <inheritdoc />
                protected override DbParameterCollection DbParameterCollection => command.Parameters;

                /// <inheritdoc />
                protected override DbTransaction? DbTransaction { get => command.Transaction; set => command.Transaction = value; }

                /// <inheritdoc />
                public override void Cancel() => command.Cancel();

                /// <inheritdoc />
                public override int ExecuteNonQuery() => command.ExecuteNonQuery();

                /// <inheritdoc />
                public override object? ExecuteScalar() => command.ExecuteScalar();

                /// <inheritdoc />
                public override void Prepare() => command.Prepare();

                /// <inheritdoc />
                protected override DbParameter CreateDbParameter() => command.CreateParameter();

                /// <inheritdoc />
                protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new RecordedReader(command.ExecuteReader(behavior), source);

                /// <inheritdoc />
                protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
                {
                    return new RecordedReader(await command.ExecuteReaderAsync(behavior, cancellationToken), source);
                }

                /// <inheritdoc />
                protected override void Dispose(bool disposing)
                {
                    if (disposing)
                        command.Dispose();

                    base.Dispose(disposing);
                }

            }

            /// <summary>
            /// A reader that records the token each <c>ReadAsync</c> is given.
            /// </summary>
            sealed class RecordedReader(DbDataReader reader, RecordingAdoDataSource source) : DbDataReader
            {

                /// <inheritdoc />
                public override int Depth => reader.Depth;

                /// <inheritdoc />
                public override int FieldCount => reader.FieldCount;

                /// <inheritdoc />
                public override bool HasRows => reader.HasRows;

                /// <inheritdoc />
                public override bool IsClosed => reader.IsClosed;

                /// <inheritdoc />
                public override int RecordsAffected => reader.RecordsAffected;

                /// <inheritdoc />
                public override object this[int ordinal] => reader[ordinal];

                /// <inheritdoc />
                public override object this[string name] => reader[name];

                /// <inheritdoc />
                public override bool GetBoolean(int ordinal) => reader.GetBoolean(ordinal);

                /// <inheritdoc />
                public override byte GetByte(int ordinal) => reader.GetByte(ordinal);

                /// <inheritdoc />
                public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

                /// <inheritdoc />
                public override char GetChar(int ordinal) => reader.GetChar(ordinal);

                /// <inheritdoc />
                public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

                /// <inheritdoc />
                public override string GetDataTypeName(int ordinal) => reader.GetDataTypeName(ordinal);

                /// <inheritdoc />
                public override DateTime GetDateTime(int ordinal) => reader.GetDateTime(ordinal);

                /// <inheritdoc />
                public override decimal GetDecimal(int ordinal) => reader.GetDecimal(ordinal);

                /// <inheritdoc />
                public override double GetDouble(int ordinal) => reader.GetDouble(ordinal);

                /// <inheritdoc />
                public override Type GetFieldType(int ordinal) => reader.GetFieldType(ordinal);

                /// <inheritdoc />
                public override float GetFloat(int ordinal) => reader.GetFloat(ordinal);

                /// <inheritdoc />
                public override Guid GetGuid(int ordinal) => reader.GetGuid(ordinal);

                /// <inheritdoc />
                public override short GetInt16(int ordinal) => reader.GetInt16(ordinal);

                /// <inheritdoc />
                public override int GetInt32(int ordinal) => reader.GetInt32(ordinal);

                /// <inheritdoc />
                public override long GetInt64(int ordinal) => reader.GetInt64(ordinal);

                /// <inheritdoc />
                public override string GetName(int ordinal) => reader.GetName(ordinal);

                /// <inheritdoc />
                public override int GetOrdinal(string name) => reader.GetOrdinal(name);

                /// <inheritdoc />
                public override string GetString(int ordinal) => reader.GetString(ordinal);

                /// <inheritdoc />
                public override object GetValue(int ordinal) => reader.GetValue(ordinal);

                /// <inheritdoc />
                public override int GetValues(object[] values) => reader.GetValues(values);

                /// <inheritdoc />
                public override bool IsDBNull(int ordinal) => reader.IsDBNull(ordinal);

                /// <inheritdoc />
                public override DataTable? GetSchemaTable() => reader.GetSchemaTable();

                /// <inheritdoc />
                public override IEnumerator<object> GetEnumerator() => throw new NotSupportedException();

                /// <inheritdoc />
                public override bool NextResult() => reader.NextResult();

                /// <inheritdoc />
                public override bool Read() => reader.Read();

                /// <inheritdoc />
                public override Task<bool> ReadAsync(CancellationToken cancellationToken)
                {
                    source.Record(cancellationToken);
                    return reader.ReadAsync(cancellationToken);
                }

                /// <inheritdoc />
                public override void Close() => reader.Close();

                /// <inheritdoc />
                protected override void Dispose(bool disposing)
                {
                    if (disposing)
                        reader.Dispose();

                    base.Dispose(disposing);
                }

            }

        }

    }

}
