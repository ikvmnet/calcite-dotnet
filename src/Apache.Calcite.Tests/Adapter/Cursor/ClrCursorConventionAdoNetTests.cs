using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data;
using Apache.Calcite.Tests;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// The cursor convention as the ADO.NET provider reaches it: one plan per statement, opened by either
    /// entry point, advanced by either member of the reader.
    /// </summary>
    /// <remarks>
    /// There is no mode. <c>ExecuteReader</c> opens the plan synchronously and <c>ExecuteReaderAsync</c>
    /// with await; the reader either hands back answers <c>Read</c> and <c>ReadAsync(token)</c> over one
    /// position, and every test here that reads one way also reads the other. A table that can only
    /// produce rows asynchronously is read synchronously by a block at the leaf, with the synchronization
    /// context suppressed, which is what <c>Read</c> over an asynchronous source means in every provider.
    /// </remarks>
    public class ClrCursorConventionAdoNetTests
    {

        const string Model =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]};Schema=adhoc";

        static (CalciteConnection Connection, AsyncRowsTable Table) Open()
        {
            var table = new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false);
            var c = Open(root => root.add("SALES", table));

            return (c, table);
        }

        static CalciteConnection Open(Action<org.apache.calcite.schema.SchemaPlus> configure)
        {
            return new CalciteDataSourceBuilder(Model)
                .ConfigureRootSchema(configure)
                .Build()
                .OpenConnection();
        }

        static CalciteConnection OpenBare()
        {
            var c = new CalciteConnection(Model);
            c.Open();

            return c;
        }

        [Fact]
        public async Task ShouldReadAnAsyncTableAsynchronously()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID, REGION FROM SALES ORDER BY ID";

                var rows = new List<string>();

                // the token that reaches the leaf is the one given to the open
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

        [Fact]
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
        /// The four combinations of open and advance read the same rows from a table that can only be
        /// awaited.
        /// </summary>
        [Fact]
        public async Task ShouldReadAnAsyncOnlyTableEitherWayFromEitherOpen()
        {
            var table = new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false);
            using var c = Open(root => root.add("SALES", table));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

            var rows = new List<int>();
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

            rows.Should().Equal([1, 2, 3, 4, 5, 6]);

            var asyncRows = new List<int>();
            using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                    asyncRows.Add(reader.GetInt32(0));

            asyncRows.Should().Equal(rows);

            var openedThenAwaited = new List<int>();
            using (var reader = cmd.ExecuteReader())
                while (await reader.ReadAsync())
                    openedThenAwaited.Add(reader.GetInt32(0));

            openedThenAwaited.Should().Equal(rows);

            var awaitedThenRead = new List<int>();
            using (var reader = await cmd.ExecuteReaderAsync())
                while (reader.Read())
                    awaitedThenRead.Add(reader.GetInt32(0));

            awaitedThenRead.Should().Equal(rows);
            table.Produced.Should().Be(24, "every reading came from the asynchronous table");
        }

        /// <summary>
        /// One reader advanced by either member on alternate rows reads every row once, in order.
        /// </summary>
        [Fact]
        public async Task ShouldAlternateTheTwoAdvancesOnOneReader()
        {
            var (c, _) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES ORDER BY ID";

                var rows = new List<int>();
                using var reader = cmd.ExecuteReader();
                for (var i = 0; ; i++)
                {
                    var moved = i % 2 == 0 ? reader.Read() : await reader.ReadAsync();
                    if (moved == false)
                        break;

                    rows.Add(reader.GetInt32(0));
                }

                rows.Should().Equal([1, 2, 3, 4, 5, 6]);
            }
        }

        [Fact]
        public async Task ShouldExplainAnAsyncPlanAsynchronously()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "EXPLAIN PLAN FOR SELECT ID FROM SALES WHERE REGION = 'EAST'";

                using var reader = await cmd.ExecuteReaderAsync();

                (await reader.ReadAsync()).Should().BeTrue();
                reader.GetString(0).Should().Contain("ClrCursor");
                (await reader.ReadAsync()).Should().BeFalse("an EXPLAIN is one row");

                table.Produced.Should().Be(0, "explaining a query does not run it");
            }
        }

        [Fact]
        public async Task ShouldExplainTheSamePlanFromEitherOpen()
        {
            const string Sql = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

            using var c = Open(root => root.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));

            var opened = Explain(c, Sql);

            opened.Should().Contain("ClrCursor").And.NotContain("ClrEnumerableCalc", "a filter over a scan is the cursor convention's own");
            (await ExplainAsync(c, Sql)).Should().Be(opened, "how a plan is opened is not part of the plan");
        }

        static string Explain(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            return reader.GetString(0);
        }

        static async Task<string> ExplainAsync(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            using var reader = await cmd.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            return reader.GetString(0);
        }

        [Fact]
        public async Task ShouldExplainThroughExecuteScalar()
        {
            using var c = Open(root => root.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR SELECT K FROM SYNCONLY WHERE V = 'A'";

            cmd.ExecuteScalar().Should().BeOfType<string>()
                .Which.Should().Contain("ClrCursor");
            (await cmd.ExecuteScalarAsync()).Should().BeOfType<string>()
                .Which.Should().Contain("ClrCursor");
        }

        [Fact]
        public async Task ShouldPlanACalciteTableAsynchronously()
        {
            using var c = Open(root => root.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM SYNCONLY ORDER BY K, V";

            var rows = new List<string>();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|A", "2|B", "2|C", "4|D"]);
        }

        [Fact]
        public async Task ShouldReadAnAwaitedOpenSynchronously()
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
                table.Produced.Should().Be(6, "the rows still come from the asynchronous table");
            }
        }

        [Fact]
        public async Task ShouldReadASynchronousOpenAsynchronously()
        {
            using var c = Open(root => root.add("SYNCONLY", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM SYNCONLY ORDER BY K, V";

            using var reader = cmd.ExecuteReader();

            var rows = new List<string>();
            while (await reader.ReadAsync())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|A", "2|B", "2|C", "4|D"]);
        }

        [Fact]
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

        sealed class NonPumpingContext : SynchronizationContext
        {

            public override void Post(SendOrPostCallback d, object? state)
            {

            }

            public override SynchronizationContext CreateCopy() => this;

        }

        [Fact]
        public async Task ShouldReadNothingUntilTheFirstRead()
        {
            var (c, table) = Open();
            await using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM SALES";

                await using var reader = await cmd.ExecuteReaderAsync();

                table.Produced.Should().Be(0, "ExecuteReaderAsync opens the plan and reads none of it");

                (await reader.ReadAsync()).Should().BeTrue();
                table.Produced.Should().Be(1, "the first row is read by the first ReadAsync and no more");
            }
        }

        [Fact]
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

        [Fact]
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

        [Fact]
        public async Task ShouldCancelAReadInProgress()
        {
            var rows = new object[10_000][];
            for (int i = 0; i < rows.Length; i++)
                rows[i] = [java.lang.Integer.valueOf(i), "R"];

            var table = new AsyncRowsTable(rows, AsyncTestRows.SortedRowType, false);
            using var c = Open(root => root.add("BIG", table));

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
        /// A token given to one advance cancels that advance, and it reaches the leaf.
        /// </summary>
        [Fact]
        public async Task ShouldCancelAnAdvanceWithTheTokenGivenToIt()
        {
            var rows = new object[10_000][];
            for (int i = 0; i < rows.Length; i++)
                rows[i] = [java.lang.Integer.valueOf(i), "R"];

            var table = new AsyncRowsTable(rows, AsyncTestRows.SortedRowType, false);
            using var c = Open(root => root.add("BIG", table));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT K, V FROM BIG";

            // opened with no token at all
            using var reader = cmd.ExecuteReader();

            (await reader.ReadAsync()).Should().BeTrue();

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancellation.Token));
            table.Produced.Should().BeLessThan(100);
        }

        [Fact]
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

        [Fact]
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

        [Fact]
        public void ShouldRunAValuesQuery()
        {
            using var c = OpenBare();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) ORDER BY x";

            var rows = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|a", "2|b"]);
        }

        [Fact]
        public void ShouldRunAnAggregate()
        {
            using var c = OpenBare();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT y, COUNT(*) FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS t(x, y) GROUP BY y ORDER BY y";

            var rows = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                rows.Add(reader.GetString(0) + "|" + reader.GetInt64(1));

            rows.Should().Equal(["a|2", "b|1"]);
        }

        [Fact]
        public void ShouldRunAJoin()
        {
            using var c = OpenBare();
            using var cmd = c.CreateCommand();
            cmd.CommandText =
                "SELECT l.x, r.z FROM (VALUES (1, 'a'), (2, 'b')) AS l(x, y) " +
                "JOIN (VALUES (1, 'p'), (2, 'q')) AS r(w, z) ON l.x = r.w ORDER BY l.x";

            var rows = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|p", "2|q"]);
        }

    }

}
