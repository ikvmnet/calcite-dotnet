using System.Collections.Generic;
using System.Threading.Tasks;

using Apache.Calcite.Data;

using FluentAssertions;

using org.apache.calcite.rel.type;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Holds the provider to linq4j's timing: opening the plan is the acquisition, and it runs at Execute.
    /// </summary>
    /// <remarks>
    /// The awaiting side keeps the same timing. A sequence's <c>GetAsyncEnumerator</c> cannot await and
    /// would have to leave an awaited drain to the first advance; an open that awaits can await the drain,
    /// so a sort opened with
    /// <c>ExecuteReaderAsync</c> has read its whole input by the time the reader is handed back, exactly
    /// as one opened with <c>ExecuteReader</c> has.
    /// </remarks>
    public class ClrCursorDefaultsAcquisitionTests
    {

        const string Model =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]};Schema=adhoc";

        sealed class CountingTable : org.apache.calcite.schema.impl.AbstractTable, org.apache.calcite.schema.ScannableTable
        {

            public int EnumeratorCalls;

            public int Produced;

            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder().add("ID", org.apache.calcite.sql.type.SqlTypeName.INTEGER).build();
            }

            public org.apache.calcite.linq4j.Enumerable scan(org.apache.calcite.DataContext root)
            {
                return new CountingEnumerable(this);
            }

        }

        sealed class CountingEnumerable(CountingTable table) : org.apache.calcite.linq4j.AbstractEnumerable
        {

            public override org.apache.calcite.linq4j.Enumerator enumerator()
            {
                table.EnumeratorCalls++;
                return new CountingEnumerator(table);
            }

        }

        sealed class CountingEnumerator(CountingTable table) : org.apache.calcite.linq4j.Enumerator
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(1)],
                [java.lang.Integer.valueOf(2)],
                [java.lang.Integer.valueOf(3)],
            ];

            int _index = -1;

            public object? current() => Rows[_index];

            public bool moveNext()
            {
                if (_index + 1 >= Rows.Length)
                    return false;

                _index++;
                table.Produced++;
                return true;
            }

            public void reset() => _index = -1;

            public void close()
            {

            }

            public void Dispose()
            {

            }

        }

        static (CalciteConnection Connection, CountingTable Table) Open()
        {
            var table = new CountingTable();
            var c = new CalciteDataSourceBuilder(Model)
                .ConfigureRootSchema(root => root.add("T", table))
                .Build()
                .OpenConnection();

            return (c, table);
        }

        [Fact]
        public void ExecuteShouldAcquireTheLeafWithoutReading()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T WHERE ID > 1";

                using var reader = cmd.ExecuteReader();

                table.EnumeratorCalls.Should().Be(1, "Execute opens the plan, and the open acquires down to the leaf");
                table.Produced.Should().Be(0, "no row has been asked for");

                var rows = new List<int>();
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([2, 3]);
                table.Produced.Should().Be(3, "the filter read the whole input to answer");
            }
        }

        [Fact]
        public void ExecuteShouldRunASort()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T ORDER BY ID DESC";

                using var reader = cmd.ExecuteReader();

                table.EnumeratorCalls.Should().Be(1);
                table.Produced.Should().Be(3, "the sort drains its whole input inside the open, which Execute ran");

                var rows = new List<int>();
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([3, 2, 1]);
            }
        }

        [Fact]
        public async Task ExecuteAsyncShouldAcquireTheLeafWithoutReading()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T WHERE ID > 1";

                await using var reader = await cmd.ExecuteReaderAsync();

                table.EnumeratorCalls.Should().Be(1, "the awaiting open acquires down to the leaf, and Execute awaited it");
                table.Produced.Should().Be(0, "no row has been asked for");

                var rows = new List<int>();
                while (await reader.ReadAsync())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([2, 3]);
            }
        }

        [Fact]
        public async Task ExecuteAsyncShouldRunASort()
        {
            var (c, table) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T ORDER BY ID DESC";

                await using var reader = await cmd.ExecuteReaderAsync();

                table.EnumeratorCalls.Should().Be(1);
                table.Produced.Should().Be(3, "an open that awaits can await the drain, so the sort ran inside Execute");

                (await reader.ReadAsync()).Should().BeTrue();
                reader.GetInt32(0).Should().Be(3);
            }
        }

        /// <summary>
        /// A recursive statement runs through the provider: the transient table goes into the connection's
        /// root schema at Execute, the spool finds it there by name, each round is opened inside the
        /// advance that starts it, and the table comes out again when the reader is closed.
        /// </summary>
        /// <remarks>
        /// One command per reading, because a plan holding a transient table is not reread: the table keeps
        /// whatever its last round left in it.
        /// </remarks>
        [Fact]
        public void ExecuteShouldRunARecursiveQuery()
        {
            var (c, _) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM r WHERE n < 4) SELECT n FROM r ORDER BY 1";

                var rows = new List<int>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        rows.Add(reader.GetInt32(0));
                }

                rows.Should().Equal([1, 2, 3, 4]);
                c.RootSchema.getTable("r").Should().BeNull("the clean-up removed the transient table when the cursor was disposed");
            }
        }

        [Fact]
        public async Task ExecuteAsyncShouldRunARecursiveQuery()
        {
            var (c, _) = Open();
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM r WHERE n < 4) SELECT n FROM r ORDER BY 1";

                var rows = new List<int>();
                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                        rows.Add(reader.GetInt32(0));
                }

                rows.Should().Equal([1, 2, 3, 4]);
                c.RootSchema.getTable("r").Should().BeNull("the clean-up removed the transient table when the cursor was disposed");
            }
        }

    }

}
