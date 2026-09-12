using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Requires that a cancellation reaches both of the channels a plan can read it on.
    /// </summary>
    /// <remarks>
    /// A statement's plan may hold nodes of two calling conventions and they do not cancel the same way. A
    /// node of <c>ClrEnumerableConvention</c> reads the <see cref="CancellationToken"/> its sequence was
    /// given at <c>GetAsyncEnumerator</c>; a node of Calcite's <c>EnumerableConvention</c> reads
    /// <c>DataContext.Variable.CANCEL_FLAG</c>. <c>AdoCancellationTests</c> in the adapter's suite holds the
    /// first, ending at a real <c>DbDataReader</c>. This holds the second, which is what a query touching a
    /// table Calcite scans is carried across a converter by — and which the reader path built, put in the
    /// <c>DataContext</c> and then dropped, so nothing ever set it.
    ///
    /// <para>The flag is asserted directly rather than through an effect of it. A cancelled statement fails
    /// at the reader too, the token being the same one, so a test that only required the read to throw would
    /// pass with the flag still false.</para>
    /// </remarks>
    public class StatementCancellationTests
    {

        /// <summary>
        /// Opens a connection whose root schema holds the given table under <c>CANCELS</c>.
        /// </summary>
        static CalciteConnection OpenConnection(Table table, bool synchronous = false)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                Synchronous = synchronous ? true : null,
            }.ToString())
                .ConfigureRootSchema(root => root.add("CANCELS", table))
                .Build()
                .OpenConnection();
        }

        /// <summary>
        /// The token given to <c>ExecuteReaderAsync</c> sets the flag a node of Calcite's convention reads.
        /// </summary>
        [Fact]
        public async Task Should_set_calcites_cancel_flag_from_the_callers_token()
        {
            var table = new FlagCapturingTable();
            using var connection = OpenConnection(table);

            using var cancellation = new CancellationTokenSource();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT x FROM CANCELS";

            await using var reader = await cmd.ExecuteReaderAsync(cancellation.Token);
            Assert.True(await reader.ReadAsync(cancellation.Token));

            // the scan has run, so the table is holding the statement's flag
            Assert.NotNull(table.CancelFlag);
            Assert.False(table.CancelFlag!.get());

            cancellation.Cancel();

            Assert.True(table.CancelFlag.get());
        }

        /// <summary>
        /// A token given only to <c>ReadAsync</c> reaches a table that is already reading.
        /// </summary>
        /// <remarks>
        /// The enumerator's own token was fixed at <c>GetAsyncEnumerator</c> and <c>MoveNextAsync</c> takes
        /// none, so what a per-call token gets is a registration against the statement's cancellation —
        /// which is how <c>SqlDataReader.ReadAsync</c> reaches a command already in flight. Cancelling it
        /// cancels the statement rather than the row, which is the only thing the shape allows.
        ///
        /// <para><b>The registration is scoped to the call</b>, as SqlClient's is, so this has to cancel
        /// while a read is in progress rather than after one has returned. A version of this that cancelled
        /// afterwards was written first and fails: the registration is gone by then, and a token handed to a
        /// read that has already finished reaching back to kill a reader the caller went on using is not the
        /// behaviour wanted. So the table blocks and another thread cancels, which is the arrangement the
        /// flag exists for — a <c>ScannableTable</c> holds its thread while it reads and cannot observe
        /// anything itself.</para>
        /// </remarks>
        [Fact]
        public async Task Should_reach_a_reading_table_from_a_per_read_token()
        {
            var table = new BlockingFlagTable();
            using var connection = OpenConnection(table);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT x FROM CANCELS";

            await using var reader = await cmd.ExecuteReaderAsync();

            using var cancellation = new CancellationTokenSource();

            // the first row comes back without the table blocking
            Assert.True(await reader.ReadAsync(cancellation.Token));

            // the second finds the table waiting on the flag, and only another thread can set it
            var reading = Task.Run(() => reader.ReadAsync(cancellation.Token));
            Assert.True(table.Blocked.Wait(System.TimeSpan.FromSeconds(30)));

            cancellation.Cancel();

            Assert.False(await reading.WaitAsync(System.TimeSpan.FromSeconds(30)));
            Assert.True(table.SawCancelFlag);
        }

        /// <summary>
        /// And on the synchronous route, where the flag is the only channel there is.
        /// </summary>
        /// <remarks>
        /// A pulled plan carries no token at all, so cancelling one is Calcite's flag or nothing. It takes
        /// another thread to matter — a blocking <c>Read</c> cannot observe its own cancellation — which is
        /// what the flag is for and why a table polls it between rows.
        /// </remarks>
        [Fact]
        public async Task Should_set_calcites_cancel_flag_on_the_synchronous_route()
        {
            var table = new FlagCapturingTable();
            using var connection = OpenConnection(table, synchronous: true);

            using var cancellation = new CancellationTokenSource();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT x FROM CANCELS";

            await using var reader = await cmd.ExecuteReaderAsync(cancellation.Token);
            Assert.True(await reader.ReadAsync(cancellation.Token));

            Assert.NotNull(table.CancelFlag);
            Assert.False(table.CancelFlag!.get());

            cancellation.Cancel();

            Assert.True(table.CancelFlag.get());
        }

        /// <summary>
        /// A table of Calcite's own SPI that keeps the cancel flag it was scanned with.
        /// </summary>
        /// <remarks>
        /// <c>ScannableTable</c> rather than <c>IClrScannableTable</c> deliberately: it puts the scan in
        /// <c>EnumerableConvention</c> with a converter above it, which is the arrangement the flag has to
        /// cross. Reading the flag at <c>scan</c> is what <c>ListTransientTable</c> does, and the CSV, file
        /// and Kafka adapters' tables; nothing polls it on a table's behalf.
        /// </remarks>
        sealed class FlagCapturingTable : AbstractTable, ScannableTable
        {

            /// <summary>
            /// Gets the flag the statement was given, once it has been scanned.
            /// </summary>
            public java.util.concurrent.atomic.AtomicBoolean? CancelFlag { get; private set; }

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder().add("X", SqlTypeName.INTEGER).build();
            }

            /// <inheritdoc />
            public Enumerable scan(DataContext root)
            {
                CancelFlag = DataContext.Variable.CANCEL_FLAG.get(root) as java.util.concurrent.atomic.AtomicBoolean;

                var list = new java.util.ArrayList();
                for (int i = 0; i < 4; i++)
                    list.add(new object[] { java.lang.Integer.valueOf(i) });

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table of Calcite's own SPI that yields one row and then waits on the cancel flag.
        /// </summary>
        /// <remarks>
        /// The shape of <c>ListTransientTable</c> and of the CSV adapter's enumerators, which poll the flag
        /// between rows: a table of Calcite's convention holds the reading thread, so this is the only way
        /// it learns anything. Ends its rows when the flag is set, so a cancelled read finishes rather than
        /// hanging.
        /// </remarks>
        sealed class BlockingFlagTable : AbstractTable, ScannableTable
        {

            /// <summary>
            /// Signalled once the table is waiting on the flag.
            /// </summary>
            public ManualResetEventSlim Blocked { get; } = new(false);

            /// <summary>
            /// Gets whether the wait ended because the flag was set, rather than by timing out.
            /// </summary>
            public bool SawCancelFlag { get; private set; }

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder().add("X", SqlTypeName.INTEGER).build();
            }

            /// <inheritdoc />
            public Enumerable scan(DataContext root)
            {
                var cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root) as java.util.concurrent.atomic.AtomicBoolean;

                return new Rows(this, cancelFlag);
            }

            /// <summary>
            /// One row, and then a wait.
            /// </summary>
            sealed class Rows(BlockingFlagTable table, java.util.concurrent.atomic.AtomicBoolean? cancelFlag) : AbstractEnumerable
            {

                /// <inheritdoc />
                public override Enumerator enumerator() => new Cursor(table, cancelFlag);

                /// <summary>
                /// Yields row zero, then waits for the flag before saying there are no more.
                /// </summary>
                sealed class Cursor(BlockingFlagTable table, java.util.concurrent.atomic.AtomicBoolean? cancelFlag) : Enumerator
                {

                    int _row;

                    /// <inheritdoc />
                    public object current() => new object[] { java.lang.Integer.valueOf(0) };

                    /// <inheritdoc />
                    public bool moveNext()
                    {
                        if (_row++ == 0)
                            return true;

                        table.Blocked.Set();

                        for (var waited = 0; waited < 30000; waited += 10)
                        {
                            if (cancelFlag is not null && cancelFlag.get())
                            {
                                table.SawCancelFlag = true;
                                return false;
                            }

                            Thread.Sleep(10);
                        }

                        return false;
                    }

                    /// <inheritdoc />
                    public void reset() => throw new java.lang.UnsupportedOperationException();

                    /// <inheritdoc />
                    public void close()
                    {

                    }

                    /// <inheritdoc />
                    public void Dispose() => close();

                }

            }

        }

    }

}
