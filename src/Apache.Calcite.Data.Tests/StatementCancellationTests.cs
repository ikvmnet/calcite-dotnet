using System;
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
    /// Covers cancellation reaching nodes of Calcite's <c>EnumerableConvention</c> through
    /// <c>DataContext.Variable.CANCEL_FLAG</c>.
    /// </summary>
    /// <remarks>
    /// A node of <c>ClrCursorConvention</c> reads the <see cref="CancellationToken"/> its open and each
    /// advance were given; a node of <c>EnumerableConvention</c> reads the cancel flag instead, which
    /// <c>StatementDataContext</c> derives from the statement's token. <c>AdoCancellationTests</c> in the
    /// adapter's suite covers the token; this covers the flag.
    ///
    /// <para>The tables here are Calcite's <c>ScannableTable</c> rather than this convention's SPI, so the
    /// scan is in <c>EnumerableConvention</c> and reads the flag.</para>
    ///
    /// <para>The flag is asserted directly. A cancelled statement also fails at the reader, which observes
    /// the same token, so requiring only that the read throws would pass with the flag still false.</para>
    /// </remarks>
    public class StatementCancellationTests
    {

        /// <summary>
        /// Opens a connection whose root schema holds the given table under <c>CANCELS</c>.
        /// </summary>
        static CalciteConnection OpenConnection(Table table)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
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
        /// <c>ReadAsync</c> registers its token against the statement's cancellation source for the length of
        /// the call, as <c>SqlDataReader.ReadAsync</c> does, and cancelling that source sets the flag. The
        /// cancellation therefore ends the statement, not just the read.
        ///
        /// <para>Because the registration lasts only for the call, the cancel has to arrive while a read is in
        /// progress. The table blocks on its second row and another thread cancels: a <c>ScannableTable</c>
        /// holds its thread while it reads, and the flag is how it learns of the cancel.</para>
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
        /// A token given to <c>ReadAsync</c> sets the flag over a plan opened synchronously too.
        /// </summary>
        /// <remarks>
        /// <c>ExecuteReader</c> opens the plan with no token, but the flag belongs to the statement's
        /// <c>StatementDataContext</c> rather than to the open, and <c>ReadAsync</c> registers its token
        /// against the statement's cancellation whichever way the reader was opened.
        /// </remarks>
        [Fact]
        public async Task Should_set_calcites_cancel_flag_from_a_per_read_token_over_a_synchronous_open()
        {
            var table = new FlagCapturingTable();
            using var connection = OpenConnection(table);

            using var cancellation = new CancellationTokenSource();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT x FROM CANCELS";

            await using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());

            Assert.NotNull(table.CancelFlag);
            Assert.False(table.CancelFlag!.get());

            cancellation.Cancel();

            // a cancelled token given to a read cancels the statement before the read throws, because the
            // registration is made before the token is checked, as in SqlDataReader.ReadAsync
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancellation.Token));

            Assert.True(table.CancelFlag.get());
        }

        /// <summary>
        /// A table of Calcite's own SPI that keeps the cancel flag it was scanned with.
        /// </summary>
        /// <remarks>
        /// <c>ScannableTable</c> rather than <c>IClrScannableTable</c>, so the scan is in
        /// <c>EnumerableConvention</c> under a converter. It reads the flag at <c>scan</c>, as Calcite's own
        /// tables that honour cancellation do; nothing polls the flag on a table's behalf.
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
        /// Polls the flag between rows, as Calcite's cancellable enumerators do: a table of Calcite's
        /// convention holds the reading thread, so the flag is its only signal. Ends its rows when the flag
        /// is set, so a cancelled read finishes rather than hanging.
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
