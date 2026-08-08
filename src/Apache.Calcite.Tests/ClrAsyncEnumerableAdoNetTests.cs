using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a query through the ADO.NET surface with the asynchronous convention doing the work.
    /// </summary>
    /// <remarks>
    /// The end that convention was written for, and the only tests that prove the whole path rather than a
    /// piece of it: an <c>IClrAsyncScannableTable</c> in the schema, the planner choosing
    /// <c>ClrAsyncEnumerableConvention</c>, the implementor compiling a
    /// <c>Func&lt;DataContext, IAsyncEnumerable&lt;object&gt;&gt;</c>, and <c>DbDataReader.ReadAsync</c>
    /// pulling the rows out.
    ///
    /// <para>Every one of them asserts <c>which</c> convention ran, not only that rows came back. There is
    /// no fallback to be caught out by any more — <c>ExecuteReaderAsync</c> throws where it cannot plan —
    /// but a test that checked only the rows would still be satisfied by a plan that never suspended, so
    /// each one reads the leaf's own counters.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerableAdoNetTests
    {

        const string Model =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]};Schema=adhoc";

        static (CalciteConnection Connection, AsyncRowsTable Table) Open()
        {
            var c = new CalciteConnection(Model);
            c.Open();

            var table = new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false);
            c.RootSchema.add("SALES", table);

            return (c, table);
        }

        /// <summary>
        /// A query over an asynchronous table is read asynchronously, end to end.
        /// </summary>
        [TestMethod]
        public async Task ShouldReadAnAsyncTableAsynchronously()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID, REGION FROM SALES ORDER BY ID";

                var rows = new List<string>();

                // the token that reaches the leaf is the one given here, not one given to a later ReadAsync
                using var cancellation = new CancellationTokenSource();
                using var reader = await cmd.ExecuteReaderAsync(cancellation.Token);

                while (await reader.ReadAsync(cancellation.Token))
                    rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

                rows.Should().Equal(["1|EAST", "2|EAST", "3|EAST", "4|WEST", "5|WEST", "6|WEST"]);
                table.Produced.Should().Be(6, "the rows must have come from the asynchronous table");
                table.SawCancellableToken.Should().BeTrue(
                    "the token given to ExecuteReaderAsync must reach the leaf, or the plan is asynchronous in name only");
            }
        }

        /// <summary>
        /// A table that can only produce rows asynchronously cannot be read synchronously at all.
        /// </summary>
        /// <remarks>
        /// Not a limitation to work around — it is the convention's whole premise arriving at the surface.
        /// An <c>IClrAsyncScannableTable</c> is not a <c>ScannableTable</c>, so neither the synchronous
        /// convention nor Calcite's own has a scan for it, and there is no converter that could carry its
        /// rows into one. <c>ExecuteReader</c> therefore fails to plan rather than blocking a thread per row,
        /// which is the trade this whole convention exists to make.
        ///
        /// <para>A schema may hold both kinds of table; it is the individual query that is one or the
        /// other.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldRefuseToReadAnAsyncOnlyTableSynchronously()
        {
            var (c, _) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                cmd.Invoking(x => x.ExecuteReader()).Should().Throw<Exception>();
            }
        }

        /// <summary>
        /// A query that cannot be planned asynchronously fails, rather than quietly running synchronously.
        /// </summary>
        /// <remarks>
        /// There is no fallback and this is what stands in its place. A table that is not an
        /// <c>IClrAsyncScannableTable</c> cannot be reached from this convention, there being no converter
        /// to carry its rows, and preparing the synchronous plan instead would hand back a reader that looks
        /// asynchronous and blocks a thread per row — which a caller cannot tell from the outside, so the
        /// failure has to be visible.
        ///
        /// <para>The same query read through <c>ExecuteReader</c> works, and that is the caller's way of
        /// saying they meant it.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldRefuseToPlanASyncOnlyTableAsynchronously()
        {
            using var c = new CalciteConnection(Model);
            c.Open();
            c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM SYNCONLY ORDER BY K, V";

            await cmd.Invoking(x => x.ExecuteReaderAsync()).Should().ThrowAsync<Exception>();

            var rows = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|A", "2|B", "2|C", "4|D"]);
        }

        /// <summary>
        /// A reader over an asynchronous plan can still be read synchronously.
        /// </summary>
        /// <remarks>
        /// <c>DbDataReader</c> is a contract, and a consumer that knows nothing but that interface — a
        /// micro-ORM, <c>DataTable.Load</c> — calls <c>Read</c>. A provider whose reader throws there is not
        /// a provider, so the asynchronous result blocks instead.
        ///
        /// <para>That is not the sync-over-async the convention refuses. That rule governs what a plan does
        /// inside itself, where a converter would insert blocking nobody chose and nobody could see; here
        /// the caller is choosing it in the open at the boundary.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldReadAnAsyncPlanSynchronouslyWhenAskedTo()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                using var reader = await cmd.ExecuteReaderAsync();

                var rows = new List<int>();
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([1, 2, 3, 4, 5, 6]);
                table.Produced.Should().Be(6, "the rows still come from the asynchronous plan");
            }
        }

        /// <summary>
        /// Cancelling a read stops the table producing rows.
        /// </summary>
        [TestMethod]
        public async Task ShouldCancelAReadInProgress()
        {
            using var c = new CalciteConnection(Model);
            c.Open();

            var rows = new object[10_000][];
            for (int i = 0; i < rows.Length; i++)
                rows[i] = [java.lang.Integer.valueOf(i), "R"];

            var table = new AsyncRowsTable(rows, AsyncTestRows.SortedRowType, false);
            c.RootSchema.add("BIG", table);

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM BIG";

            using var cancellation = new CancellationTokenSource();
            using var reader = await cmd.ExecuteReaderAsync(cancellation.Token);

            var read = 0;
            var cancelled = false;

            try
            {
                while (await reader.ReadAsync(cancellation.Token))
                    if (++read == 5)
                        cancellation.Cancel();
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue();
            table.Produced.Should().BeLessThan(100, "the table must stop producing, not run to 10,000");
        }

        /// <summary>
        /// A grouped aggregate over an asynchronous table.
        /// </summary>
        [TestMethod]
        public async Task ShouldRunAnAggregateAsynchronously()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION ORDER BY REGION";

                var rows = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                    rows.Add(reader.GetString(0) + "|" + reader.GetInt64(1));

                rows.Should().Equal(["EAST|3", "WEST|3"]);
                table.Produced.Should().Be(6);
            }
        }

        /// <summary>
        /// A one-column result is the value, read through the ADO.NET surface.
        /// </summary>
        [TestMethod]
        public async Task ShouldReadAOneColumnResult()
        {
            var (c, _) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                var rows = new List<int>();
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([1, 2, 3, 4, 5, 6]);
            }
        }

    }

}
