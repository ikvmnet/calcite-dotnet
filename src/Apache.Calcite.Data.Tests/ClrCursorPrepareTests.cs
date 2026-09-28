using Apache.Calcite.Extensions;

using Xunit;
using Apache.Calcite.Extensions.Adapter.Cursor;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Runs queries through the ADO.NET surface on the CLR cursor convention.
    /// </summary>
    /// <remarks>
    /// <c>Apache.Calcite.Extensions</c> grants this project no <c>InternalsVisibleTo</c>, so these tests reach
    /// the engine only as a consumer of the published packages would, through <c>CalciteConnection</c>.
    /// </remarks>
    public class ClrCursorPrepareTests
    {

        static readonly string ConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            Schema = "adhoc",
        };

        static CalciteConnection Open()
        {
            var c = new CalciteConnection(ConnectionString);
            c.Open();
            return c;
        }

        [Fact]
        public void Query_should_run_through_the_clr_convention()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) ORDER BY x DESC";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(2, r.GetInt32(0));
            Assert.Equal("b", r.GetString(1));
            Assert.True(r.Read());
            Assert.Equal(1, r.GetInt32(0));
            Assert.Equal("a", r.GetString(1));
            Assert.False(r.Read());
        }

        [Fact]
        public void Aggregate_should_run_through_the_clr_convention()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT y, COUNT(*) FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS t(x, y) GROUP BY y ORDER BY y";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal("a", r.GetString(0));
            Assert.Equal(2L, r.GetInt64(1));
            Assert.True(r.Read());
            Assert.Equal("b", r.GetString(0));
            Assert.Equal(1L, r.GetInt64(1));
            Assert.False(r.Read());
        }

        [Fact]
        public void Window_aggregate_should_run_through_the_clr_convention()
        {
            // the convention's implementation of Calcite's WinAggContext must carry every member of the
            // Calcite version this project resolves, or the type fails to load and every window query fails
            // before it runs; any window query detects that
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT x, SUM(x) OVER (ORDER BY x) FROM (VALUES (1), (2), (4)) AS t(x) ORDER BY x";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(1, r.GetInt32(0));
            Assert.Equal(1, r.GetInt32(1));
            Assert.True(r.Read());
            Assert.Equal(2, r.GetInt32(0));
            Assert.Equal(3, r.GetInt32(1));
            Assert.True(r.Read());
            Assert.Equal(4, r.GetInt32(0));
            Assert.Equal(7, r.GetInt32(1));
            Assert.False(r.Read());
        }

        [Fact]
        public void Scalar_of_one_column_should_be_the_value()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT SUM(x) FROM (VALUES (1), (2), (4)) AS t(x)";

            Assert.Equal(7, cmd.ExecuteScalar());
        }

    }

}
