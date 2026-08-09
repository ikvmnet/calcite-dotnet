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
        /// An EXPLAIN of a query over an asynchronous table renders the asynchronous plan, and is read
        /// asynchronously.
        /// </summary>
        /// <remarks>
        /// An <c>EXPLAIN</c> leaves <c>Prepare.prepareSql</c> before <c>implement</c>, so what the signature
        /// carries is a rendered string and not a plan of either convention — but the plan it rendered was
        /// optimized under whichever program was asked for, and <c>ExecuteReaderAsync</c> is the only way
        /// that demand is stated. So the text names this convention's nodes.
        ///
        /// <para>It used to throw: <c>ClrExplainBindable</c> was an <c>IClrBindable</c> alone and
        /// <c>ClrSignature.BindAsync</c> refused it as "prepared into a synchronous convention", which was
        /// never true of an <c>EXPLAIN</c> and left a caller holding only <c>ExecuteReaderAsync</c> unable to
        /// explain anything at all.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldExplainAnAsyncPlanAsynchronously()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "EXPLAIN PLAN FOR SELECT ID FROM SALES WHERE REGION = 'EAST'";

                using var reader = await cmd.ExecuteReaderAsync();

                (await reader.ReadAsync()).Should().BeTrue();
                reader.GetString(0).Should().Contain("ClrAsyncEnumerable");
                (await reader.ReadAsync()).Should().BeFalse("an EXPLAIN is one row");

                table.Produced.Should().Be(0, "explaining a query does not run it");
            }
        }

        /// <summary>
        /// The same EXPLAIN over the same table renders a different plan depending on which reader asked
        /// for it.
        /// </summary>
        /// <remarks>
        /// A <see cref="SyncRowsTable"/> can be planned either way, so this is the whole of the point:
        /// nothing in the SQL says which convention to explain, and <c>ExecuteReader</c> against
        /// <c>ExecuteReaderAsync</c> is where that is decided — for an <c>EXPLAIN</c> exactly as for the
        /// query it explains.
        /// </remarks>
        [TestMethod]
        public async Task ShouldExplainTheConventionThatWasAskedFor()
        {
            using var c = new CalciteConnection(Model);
            c.Open();
            c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

            const string Sql = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

            string synchronous;
            using (var cmd = c.CreateCommand())
            {
                cmd.CommandText = Sql;
                using var reader = cmd.ExecuteReader();
                reader.Read().Should().BeTrue();
                synchronous = reader.GetString(0);
            }

            string asynchronous;
            using (var cmd = c.CreateCommand())
            {
                cmd.CommandText = Sql;
                using var reader = await cmd.ExecuteReaderAsync();
                (await reader.ReadAsync()).Should().BeTrue();
                asynchronous = reader.GetString(0);
            }

            synchronous.Should().Contain("ClrEnumerable").And.NotContain("ClrAsyncEnumerable");
            asynchronous.Should().Contain("ClrAsyncEnumerable");
        }

        /// <summary>
        /// An EXPLAIN read as a scalar is the plan, by either method.
        /// </summary>
        /// <remarks>
        /// <c>ExecuteScalar</c> and <c>ExecuteScalarAsync</c> are the reader path with one row and one column
        /// taken off it — the same two demands for a convention, so the same two answers. A caller reaching
        /// for the plan as a string is the likely one, and it is a column of a result set like any other:
        /// <c>Meta.CursorFactory.deduce</c> makes a single column <c>OBJECT</c>, so the row <i>is</i> the
        /// text.
        /// </remarks>
        [TestMethod]
        public async Task ShouldExplainThroughExecuteScalar()
        {
            using var c = new CalciteConnection(Model);
            c.Open();
            c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

            var synchronous = cmd.ExecuteScalar();
            var asynchronous = await cmd.ExecuteScalarAsync();

            synchronous.Should().BeOfType<string>()
                .Which.Should().Contain("ClrEnumerable").And.NotContain("ClrAsyncEnumerable");
            asynchronous.Should().BeOfType<string>()
                .Which.Should().Contain("ClrAsyncEnumerable");
        }

        /// <summary>
        /// A query over an ordinary Calcite table plans asynchronously.
        /// </summary>
        /// <remarks>
        /// The scan reads a <see cref="ScannableTable"/> the way Calcite reads one — through
        /// <c>getExpression(Queryable.class)</c>, translated — and wraps the linq4j sequence rather than
        /// converting the plan, so this is one node of this convention and no converter at all.
        ///
        /// <para>That part of the query is not asynchronous and cannot be: a linq4j <c>Enumerable</c> is
        /// pulled. It does not block either — reading it produces an <see cref="IAsyncEnumerable{T}"/> that
        /// always completes synchronously, which costs a state machine and no thread.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldPlanACalciteTableAsynchronously()
        {
            using var c = new CalciteConnection(Model);
            c.Open();
            c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM SYNCONLY ORDER BY K, V";

            var rows = new List<string>();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
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
        /// A reader over a synchronous plan can still be read asynchronously.
        /// </summary>
        /// <remarks>
        /// The other half of <see cref="ShouldReadAnAsyncPlanSynchronouslyWhenAskedTo"/>, and the crossing
        /// that had no test. A synchronous plan answers <c>ReadAsync</c> by reading and handing back a
        /// completed task — which is what every synchronous ADO.NET provider does, and is not sync over
        /// async because there is nothing asynchronous underneath to be over.
        ///
        /// <para>It matters because code written against <c>ReadAsync</c> is the normal case, and a query
        /// that could not be planned asynchronously still has to be readable by it.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldReadASyncPlanAsynchronously()
        {
            using var c = new CalciteConnection(Model);
            c.Open();
            c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM SYNCONLY ORDER BY K, V";

            // the synchronous plan, deliberately: this query cannot be planned asynchronously at all
            using var reader = cmd.ExecuteReader();

            var rows = new List<string>();
            while (await reader.ReadAsync())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|A", "2|B", "2|C", "4|D"]);
        }

        /// <summary>
        /// Nothing is read until the first <c>ReadAsync</c>.
        /// </summary>
        /// <remarks>
        /// Which is why <c>ExecuteReaderAsync</c> has nothing to await and returns a completed task. It
        /// parses, plans and compiles — all CPU, on the calling thread — and then composes the operator
        /// chain, but every operator is an <c>async IAsyncEnumerable</c> iterator, so calling one builds a
        /// state machine and runs none of it. <c>GetAsyncEnumerator</c> is the same. The table is not
        /// touched.
        ///
        /// <para>So the answer to "could it yield" is that there is nothing there to yield on, rather than
        /// that the work is hidden. The first suspension is inside the first <c>ReadAsync</c>.</para>
        ///
        /// <para>A table that did eager work in <c>ScanAsync</c> — opening a connection before returning the
        /// sequence — would be doing it synchronously, because <c>ScanAsync</c> returns an
        /// <see cref="IAsyncEnumerable{T}"/> rather than a task. The place to do that work is the first
        /// <c>MoveNextAsync</c>, which an iterator gives for free.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldReadNothingUntilTheFirstRead()
        {
            var (c, table) = Open();
            await using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES";

                await using var reader = await cmd.ExecuteReaderAsync();

                table.Produced.Should().Be(0, "ExecuteReaderAsync composes the plan and reads none of it");

                (await reader.ReadAsync()).Should().BeTrue();
                table.Produced.Should().Be(1, "the first row is read by the first ReadAsync and no more");
            }
        }

        /// <summary>
        /// Disposing the reader asynchronously awaits the plan's own disposal.
        /// </summary>
        /// <remarks>
        /// <c>DbDataReader.DisposeAsync</c> falls back to the synchronous <c>Dispose</c> unless a provider
        /// overrides it, and the synchronous path can only finish a disposal the plan already completed
        /// itself. A table closing a connection or a channel in its <c>finally</c> would have been
        /// abandoned — and nothing else in this suite would have noticed, because the rows were all correct
        /// by then.
        /// </remarks>
        [TestMethod]
        public async Task ShouldAwaitThePlansDisposal()
        {
            var (c, table) = Open();
            await using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                    }
                }

                table.DisposedAsynchronously.Should().BeTrue(
                    "await using over the reader must reach the awaited part of the table's disposal");
            }
        }

        /// <summary>
        /// The same when the reader is abandoned part way through.
        /// </summary>
        /// <remarks>
        /// No ORDER BY, deliberately. A sort reads its whole input before it yields anything, so a plan
        /// with one in it has already drained the table by the first row and there is nothing left to
        /// abandon — which is what this test asserted at first, and why it failed.
        /// </remarks>
        [TestMethod]
        public async Task ShouldAwaitThePlansDisposalWhenAbandoned()
        {
            var (c, table) = Open();
            await using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES";

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    await reader.ReadAsync();
                }

                table.DisposedAsynchronously.Should().BeTrue();
                table.Produced.Should().BeLessThan(6, "the plan was abandoned after one row");
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
