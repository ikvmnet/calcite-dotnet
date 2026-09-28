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

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests that the tokens given to <c>ExecuteReaderAsync</c> and <c>ReadAsync</c> reach the provider's own
    /// <c>DbDataReader.ReadAsync</c>.
    /// </summary>
    /// <remarks>
    /// <c>ClrCursorConventionCancellationTests</c> covers a compiled plan over a purpose-written table. These
    /// run the whole stack a consumer uses — <c>CalciteCommand</c>, <c>CalciteSession</c>,
    /// <c>AdoToClrCursorConverter</c>, <c>AdoCursors</c> — down to a real <see cref="DbDataReader"/> over
    /// SQLite, and inspect the tokens that reader is called with. Asserting only that a cancelled read throws
    /// would also pass if the throw came from an operator above the leaf.
    /// </remarks>
    public class AdoCancellationTests : IDisposable
    {

        SqliteFixture _sqlite = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public AdoCancellationTests()
        {
            _sqlite = new SqliteFixture();
        }

        /// <inheritdoc />
        public void Dispose()
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
        /// The token given to <c>ExecuteReaderAsync</c> cancels the statement, and every read after that
        /// finds the provider's reader stopped.
        /// </summary>
        /// <remarks>
        /// The provider does not see the caller's token itself. The leaf is opened under the statement's
        /// token, which <c>CalciteSession</c> links to the caller's, and each advance runs the provider's reader
        /// under that token and the read's own together. So a read after the caller's token is cancelled
        /// reaches the reader with a cancelled token, whatever token the read brought.
        /// </remarks>
        [Fact]
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

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(CancellationToken.None));

            source.ReadTokens[^1].IsCancellationRequested.Should().BeTrue("the read after the cancellation ran the reader under the statement's token, cancelled");
        }

        /// <summary>
        /// Once the token given to <c>ExecuteReaderAsync</c> is cancelled, a read throws without asking the
        /// provider for another row.
        /// </summary>
        /// <remarks>
        /// Cancelled after a row has been read: the statement is sent when the cursor is opened, so a token
        /// cancelled before that would stop the open and show nothing about the reader.
        /// </remarks>
        [Fact]
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

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancellation.Token));

            source.Reads.Should().Be(before, "the provider was not asked for another row");
        }

        /// <summary>
        /// A token given only to <c>ReadAsync</c> reaches the provider too, for the duration of the read.
        /// </summary>
        /// <remarks>
        /// The statement has a cancellable token even when <c>ExecuteReaderAsync</c> is given none, so the
        /// provider is handed a cancellable token here. That such a token stops a table already reading is
        /// covered by <c>StatementCancellationTests</c>, which has a table that blocks.
        /// </remarks>
        [Fact]
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

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancellation.Token));
        }

        /// <summary>
        /// Every read takes its own token, and a reader is read to the end under a different one each time.
        /// </summary>
        /// <remarks>
        /// The pattern of a consumer with a per-operation timeout: a fresh token for every <c>ReadAsync</c>.
        /// </remarks>
        [Fact]
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

            source.ReadTokens.Should().HaveCount(used.Count, "every read reached the provider's reader");
            source.ReadTokens.Distinct().Should().HaveCount(used.Count, "each under the token of its own call");

            foreach (var perRead in used)
                perRead.Dispose();
        }

        /// <summary>
        /// A token cancelled after its own read has returned does not reach the reader.
        /// </summary>
        /// <remarks>
        /// A read's token is registered against the statement only for the length of the call, so a caller
        /// that cancels a per-operation timeout after the operation succeeded does not cancel the statement.
        /// </remarks>
        [Fact]
        public async Task ShouldSurviveATokenCancelledAfterItsRead()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await using var reader = await cmd.ExecuteReaderAsync();

            using var first = new CancellationTokenSource();
            (await reader.ReadAsync(first.Token)).Should().BeTrue();

            // the read has returned, so its registration against the statement is gone
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
        /// As <c>SqlDataReader.ReadAsync</c> does, the reader registers the token against the statement before
        /// checking it, so a token that is already cancelled behaves like one cancelled during the call. The
        /// cancelled read never reaches the provider's reader; the next read, under a live token, reaches it
        /// under the statement's token as well and finds that cancelled.
        /// </remarks>
        [Fact]
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

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(dead.Token));

            source.ReadTokens.Should().HaveCount(1, "the dead read was refused before it reached the provider's reader");

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(live.Token));

            source.ReadTokens.Should().HaveCount(2, "the live read after it reached the reader");
            source.ReadTokens[1].IsCancellationRequested.Should().BeTrue("under the statement's token, which the dead read cancelled");
        }

        /// <summary>
        /// A token already cancelled stops the execute rather than the first read.
        /// </summary>
        /// <remarks>
        /// Opening the cursor opens a connection to the provider and sends the statement, and
        /// <c>ExecuteReaderAsync</c> opens it, so a cancelled token must stop the execute before any
        /// connection is opened.
        /// </remarks>
        [Fact]
        public async Task ShouldRefuseToExecuteOnACancelledToken()
        {
            var source = new RecordingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps ORDER BY empno";

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await cmd.ExecuteReaderAsync(cancellation.Token));

            source.Opened.Should().Be(0, "no connection was opened to the provider");
        }

        /// <summary>
        /// An <see cref="AdoDataSource"/> that records the token every <c>DbDataReader.ReadAsync</c> under it
        /// is called with.
        /// </summary>
        /// <remarks>
        /// Connection, command and reader are all wrapped, because the token is only visible at the reader.
        /// Every member forwards; <c>ReadAsync</c> also records its token, and <c>OpenConnection</c> counts.
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
