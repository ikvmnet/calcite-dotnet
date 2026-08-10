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
    /// <para>Every one of them asserts <c>which</c> convention ran, not only that rows came back. The
    /// convention is the connection's mode — asynchronous by default,
    /// <see cref="CalciteConnectionStringBuilder.Synchronous"/> for the other one — and a test that checked
    /// only the rows would be satisfied by a plan that never suspended, so each one reads the leaf's own
    /// counters.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerableAdoNetTests
    {

        const string Model =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]};Schema=adhoc";

        const string SynchronousModel = Model + ";Synchronous=true";

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
        /// A table that can only produce rows asynchronously is still read by a synchronous caller.
        /// </summary>
        /// <remarks>
        /// The plan is the connection's — asynchronous by default — and <c>Read</c> blocks on it per row,
        /// which is what <c>Read</c> over an asynchronous source means and where every ADO.NET provider
        /// blocks. The refusal this test used to hold belongs to the mode now:
        /// <see cref="ShouldRefuseAnAsyncOnlyTableInSynchronousMode"/>.
        /// </remarks>
        [TestMethod]
        public void ShouldReadAnAsyncOnlyTableSynchronously()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                var rows = new List<int>();
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([1, 2, 3, 4, 5, 6]);
                table.Produced.Should().Be(6, "the rows still come from the asynchronous table");
            }
        }

        /// <summary>
        /// In synchronous mode a query over an asynchronous-only table fails to plan, from either entry
        /// point.
        /// </summary>
        /// <remarks>
        /// An <c>IClrAsyncScannableTable</c> is not a <c>ScannableTable</c>, so neither the synchronous
        /// convention nor Calcite's own has a scan for it, and the synchronous planner is given no rule that
        /// would put the scan in the asynchronous convention for
        /// <c>ClrAsyncEnumerableToClrEnumerableConverter</c> to carry. That is the mode's trade: a
        /// connection that said <see cref="CalciteConnectionStringBuilder.Synchronous"/> asked for plans
        /// that never wait, so a query that could only wait fails visibly rather than blocking behind the
        /// mode's back — and it fails from <c>ExecuteReaderAsync</c> too, because the mode is the
        /// connection's rather than the entry point's.
        ///
        /// <para>A schema may hold both kinds of table; it is the individual query that is one or the
        /// other.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldRefuseAnAsyncOnlyTableInSynchronousMode()
        {
            using var c = new CalciteConnection(SynchronousModel);
            c.Open();
            c.RootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

            cmd.Invoking(x => x.ExecuteReader()).Should().Throw<Exception>();
            await cmd.Awaiting(x => x.ExecuteReaderAsync()).Should().ThrowAsync<Exception>();
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
        /// The same EXPLAIN over the same table renders the same plan from either entry point, and the
        /// connection's mode is what changes it.
        /// </summary>
        /// <remarks>
        /// A <see cref="SyncRowsTable"/> can be planned either way, so this is the whole of the point:
        /// nothing in the SQL says which convention to plan, and nothing in the entry point does either.
        /// The choice is the connection's — the way Calcite's own connection can ask for the bindable
        /// convention — so <c>ExecuteReader</c> and <c>ExecuteReaderAsync</c> explain the same plan, for an
        /// <c>EXPLAIN</c> exactly as for the query it explains.
        /// </remarks>
        [TestMethod]
        public async Task ShouldExplainTheModesConvention()
        {
            const string Sql = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

            using (var c = new CalciteConnection(Model))
            {
                c.Open();
                c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

                Explain(c, Sql).Should().Contain("ClrAsyncEnumerable");
                (await ExplainAsync(c, Sql)).Should().Contain("ClrAsyncEnumerable");
            }

            using (var c = new CalciteConnection(SynchronousModel))
            {
                c.Open();
                c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

                Explain(c, Sql).Should().Contain("ClrEnumerable").And.NotContain("ClrAsyncEnumerable");
                (await ExplainAsync(c, Sql)).Should().Contain("ClrEnumerable").And.NotContain("ClrAsyncEnumerable");
            }
        }

        /// <summary>
        /// Runs an EXPLAIN through the synchronous reader and returns the rendered plan.
        /// </summary>
        static string Explain(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            return reader.GetString(0);
        }

        /// <summary>
        /// Runs an EXPLAIN through the asynchronous reader and returns the rendered plan.
        /// </summary>
        static async Task<string> ExplainAsync(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            using var reader = await cmd.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            return reader.GetString(0);
        }

        /// <summary>
        /// An EXPLAIN read as a scalar is the plan, by either method, and the mode's plan by both.
        /// </summary>
        /// <remarks>
        /// <c>ExecuteScalar</c> and <c>ExecuteScalarAsync</c> are the reader path with one row and one column
        /// taken off it — the same connection, so the same plan. A caller reaching for the plan as a string
        /// is the likely one, and it is a column of a result set like any other:
        /// <c>Meta.CursorFactory.deduce</c> makes a single column <c>OBJECT</c>, so the row <i>is</i> the
        /// text.
        /// </remarks>
        [TestMethod]
        public async Task ShouldExplainThroughExecuteScalar()
        {
            using (var c = new CalciteConnection(Model))
            {
                c.Open();
                c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

                using var cmd = c.CreateCommand();
                cmd.CommandText = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

                cmd.ExecuteScalar().Should().BeOfType<string>()
                    .Which.Should().Contain("ClrAsyncEnumerable");
                (await cmd.ExecuteScalarAsync()).Should().BeOfType<string>()
                    .Which.Should().Contain("ClrAsyncEnumerable");
            }

            using (var c = new CalciteConnection(SynchronousModel))
            {
                c.Open();
                c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

                using var cmd = c.CreateCommand();
                cmd.CommandText = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

                cmd.ExecuteScalar().Should().BeOfType<string>()
                    .Which.Should().Contain("ClrEnumerable").And.NotContain("ClrAsyncEnumerable");
                (await cmd.ExecuteScalarAsync()).Should().BeOfType<string>()
                    .Which.Should().Contain("ClrEnumerable").And.NotContain("ClrAsyncEnumerable");
            }
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
        /// <para>That is not the sync-over-async the surface refuses, and the line is about who chose. A
        /// plan can block inside itself too — <c>ClrAsyncEnumerableToClrEnumerableConverter</c> does, once
        /// per row — and what makes that refusable is that the planner would be choosing it for a caller who
        /// could not see it, which is why the prepare pipeline registers one convention's rules and not
        /// both. Here the caller is choosing it in the open at the boundary.</para>
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
        /// The other half of <see cref="ShouldReadAnAsyncPlanSynchronouslyWhenAskedTo"/>. A synchronous plan
        /// answers <c>ReadAsync</c> by reading and handing back a completed task — which is what every
        /// synchronous ADO.NET provider does, and is not sync over async because there is nothing
        /// asynchronous underneath to be over.
        ///
        /// <para>It matters because code written against <c>ReadAsync</c> is the normal case, and a
        /// connection in synchronous mode still has to be readable by it.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldReadASyncPlanAsynchronously()
        {
            // the synchronous plan, deliberately: the connection's mode is what asks for it
            using var c = new CalciteConnection(SynchronousModel);
            c.Open();
            c.RootSchema.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM SYNCONLY ORDER BY K, V";

            using var reader = cmd.ExecuteReader();

            var rows = new List<string>();
            while (await reader.ReadAsync())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|A", "2|B", "2|C", "4|D"]);
        }

        /// <summary>
        /// A synchronous read over an asynchronous plan completes on a thread carrying a synchronization
        /// context.
        /// </summary>
        /// <remarks>
        /// The WinForms/WPF case, and the reason <c>CalciteAsyncEnumerableResult.Read</c> suppresses the
        /// context around the whole call rather than around the wait: the operators await without
        /// <c>ConfigureAwait(false)</c>, and a continuation captures the context at the moment of
        /// suspension — inside <c>MoveNextAsync</c>'s synchronous phase, before any wait begins. Measured
        /// with exactly this shape of context, one that queues and cannot pump while its thread is blocked:
        /// suppressing only around the wait deadlocks, suppressing before the call completes.
        ///
        /// <para>The read runs on a dedicated background thread with a join timeout, so a regression fails
        /// rather than hanging the suite.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldReadSynchronouslyUnderASynchronizationContext()
        {
            var (c, _) = Open();
            using (c)
            {
                List<int>? rows = null;
                Exception? error = null;

                var thread = new Thread(() =>
                {
                    try
                    {
                        SynchronizationContext.SetSynchronizationContext(new NonPumpingContext());

                        using var cmd = c.CreateCommand();
                        cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                        var read = new List<int>();
                        using var reader = cmd.ExecuteReader();
                        while (reader.Read())
                            read.Add(reader.GetInt32(0));

                        rows = read;
                    }
                    catch (Exception e)
                    {
                        error = e;
                    }
                });

                thread.IsBackground = true;
                thread.Start();
                thread.Join(TimeSpan.FromSeconds(30)).Should().BeTrue(
                    "a deadlock here means a continuation was captured by the context before it was suppressed");

                error.Should().BeNull();
                rows.Should().Equal([1, 2, 3, 4, 5, 6]);
            }
        }

        /// <summary>
        /// A context that accepts posts and can never run them while its thread is blocked — the observable
        /// shape of a blocked UI thread.
        /// </summary>
        sealed class NonPumpingContext : SynchronizationContext
        {

            /// <summary>
            /// Queued to a loop nobody pumps: a continuation posted here never runs, exactly as one posted
            /// to a blocked UI thread's message loop never runs.
            /// </summary>
            public override void Post(SendOrPostCallback d, object? state)
            {

            }

            /// <inheritdoc />
            public override SynchronizationContext CreateCopy() => this;

        }

        /// <summary>
        /// Nothing is read until the first <c>ReadAsync</c>.
        /// </summary>
        /// <remarks>
        /// "Read" is the word doing the work. Execute acquires: <c>GetAsyncEnumerator</c> now chains down
        /// the whole plan — <c>AcquisitionTimingTests</c> holds that a Calcite leaf's <c>enumerator()</c>
        /// runs there — but this leaf is an <c>IClrAsyncScannableTable</c> whose <c>ScanAsync</c> is a C#
        /// iterator, and an iterator's body, its counting included, runs nothing until the first
        /// <c>MoveNextAsync</c>. So the table produces no row at Execute, which is this test's claim.
        ///
        /// <para>A table that did eager work in <c>ScanAsync</c> — opening a connection before returning the
        /// sequence — would be doing it synchronously, because <c>ScanAsync</c> returns an
        /// <see cref="IAsyncEnumerable{T}"/> rather than a task. The place for acquisition-time work is
        /// <c>GetAsyncEnumerator</c>, which Execute now reaches; the place for awaited work is the first
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
