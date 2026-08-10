using System.Collections.Generic;
using System.Threading.Tasks;

using Apache.Calcite.Data;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Obtaining an enumerator runs the plan; reading pulls rows.
    /// </summary>
    /// <remarks>
    /// linq4j's contract, transcribed into both conventions: an operator's <c>enumerator()</c> acquires its
    /// source's enumerator — a sort drains its input there, a linq4j leaf executes its statement there —
    /// and <c>moveNext</c> only reads. The provider obtains the enumerator at Execute, so Execute is where
    /// the plan runs. These tests read a counting table's two numbers apart:
    /// <c>EnumeratorCalls</c> says whether the leaf was <em>acquired</em>, and <c>Produced</c> says how many
    /// rows were <em>read</em> — and it is the gap between the two that the whole rewrite is about. A suite
    /// that only compared answers could never see it.
    /// </remarks>
    [TestClass]
    public class AcquisitionTimingTests
    {

        const string Model =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]};Schema=adhoc";

        const string SynchronousModel = Model + ";Synchronous=true";

        /// <summary>
        /// A <c>ScannableTable</c> of three rows that counts how often its enumerator is acquired and how
        /// many rows it has produced.
        /// </summary>
        sealed class CountingTable : org.apache.calcite.schema.impl.AbstractTable, org.apache.calcite.schema.ScannableTable
        {

            /// <summary>
            /// How many times <c>scan</c>'s enumerable was asked for an enumerator.
            /// </summary>
            public int EnumeratorCalls;

            /// <summary>
            /// How many rows have been read out of it, across every enumerator.
            /// </summary>
            public int Produced;

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder().add("ID", org.apache.calcite.sql.type.SqlTypeName.INTEGER).build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(org.apache.calcite.DataContext root)
            {
                return new CountingEnumerable(this);
            }

        }

        sealed class CountingEnumerable(CountingTable table) : org.apache.calcite.linq4j.AbstractEnumerable
        {

            /// <inheritdoc />
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

            /// <inheritdoc />
            public object? current() => Rows[_index];

            /// <inheritdoc />
            public bool moveNext()
            {
                if (_index + 1 >= Rows.Length)
                    return false;

                _index++;
                table.Produced++;
                return true;
            }

            /// <inheritdoc />
            public void reset() => _index = -1;

            /// <inheritdoc />
            public void close()
            {

            }

            /// <inheritdoc />
            public void Dispose()
            {

            }

        }

        static (CalciteConnection Connection, CountingTable Table) Open(string model)
        {
            var c = new CalciteConnection(model);
            c.Open();

            var table = new CountingTable();
            c.RootSchema.add("T", table);

            return (c, table);
        }

        /// <summary>
        /// A synchronous plan's Execute acquires the leaf without reading a row.
        /// </summary>
        [TestMethod]
        public void ExecuteShouldAcquireTheLeafWithoutReading()
        {
            var (c, table) = Open(SynchronousModel);
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T WHERE ID > 1";

                using var reader = cmd.ExecuteReader();

                table.EnumeratorCalls.Should().Be(1, "Execute obtains the enumerator, and the chain acquires down to the leaf");
                table.Produced.Should().Be(0, "no row has been asked for");

                var rows = new List<int>();
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([2, 3]);
                table.Produced.Should().Be(3, "the filter read the whole input to answer");
            }
        }

        /// <summary>
        /// A synchronous plan's sort drains its input at Execute, as linq4j's <c>orderBy</c> drains inside
        /// <c>enumerator()</c>.
        /// </summary>
        [TestMethod]
        public void ExecuteShouldRunASynchronousSort()
        {
            var (c, table) = Open(SynchronousModel);
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T ORDER BY ID DESC";

                using var reader = cmd.ExecuteReader();

                table.EnumeratorCalls.Should().Be(1);
                table.Produced.Should().Be(3, "the sort drains its whole input inside GetEnumerator, which Execute called");

                var rows = new List<int>();
                while (reader.Read())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([3, 2, 1]);
            }
        }

        /// <summary>
        /// The default mode's Execute acquires a Calcite leaf under the asynchronous root without reading
        /// a row.
        /// </summary>
        /// <remarks>
        /// The leaf is a linq4j sequence and its <c>enumerator()</c> is where a sub-plan executes — the
        /// AdoNet adapter runs its <c>DbCommand</c> there — so the crossing calls it at
        /// <c>GetAsyncEnumerator</c>, which Execute reaches. This is the asynchronous half of the rewrite's
        /// point: failures and side effects of starting a plan land at Execute, from either entry point.
        /// </remarks>
        [TestMethod]
        public async Task ExecuteAsyncShouldAcquireTheLeafWithoutReading()
        {
            var (c, table) = Open(Model);
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T WHERE ID > 1";

                await using var reader = await cmd.ExecuteReaderAsync();

                table.EnumeratorCalls.Should().Be(1, "GetAsyncEnumerator acquires down to the leaf, and Execute called it");
                table.Produced.Should().Be(0, "no row has been asked for");

                var rows = new List<int>();
                while (await reader.ReadAsync())
                    rows.Add(reader.GetInt32(0));

                rows.Should().Equal([2, 3]);
            }
        }

        /// <summary>
        /// An asynchronous sort acquires its input at Execute and drains at the first read — the one
        /// deviation the CLR imposes, stated where the operator states it.
        /// </summary>
        /// <remarks>
        /// <c>GetAsyncEnumerator</c> cannot await, so a drain that must await waits for the first
        /// <c>MoveNextAsync</c>. Acquisition is still eager: the leaf's <c>enumerator()</c> has run before
        /// the first read is asked for.
        /// </remarks>
        [TestMethod]
        public async Task AnAsynchronousSortShouldAcquireAtExecuteAndDrainAtTheFirstRead()
        {
            var (c, table) = Open(Model);
            using (c)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT ID FROM T ORDER BY ID DESC";

                await using var reader = await cmd.ExecuteReaderAsync();

                table.EnumeratorCalls.Should().Be(1, "acquisition is eager even where the drain cannot be");
                table.Produced.Should().Be(0, "an awaited drain waits for the first MoveNextAsync");

                (await reader.ReadAsync()).Should().BeTrue();
                table.Produced.Should().Be(3, "the first read ran the drain");
                reader.GetInt32(0).Should().Be(3);
            }
        }

    }

}
