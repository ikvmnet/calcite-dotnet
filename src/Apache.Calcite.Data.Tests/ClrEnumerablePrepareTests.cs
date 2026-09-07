using Apache.Calcite.Extensions;

using Xunit;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// The ADO.NET surface driven by the CLR calling convention, from an assembly that sees only the public
    /// API of <c>Apache.Calcite.Extensions</c>.
    /// </summary>
    /// <remarks>
    /// This project has no <c>InternalsVisibleTo</c> from <c>Apache.Calcite.Extensions</c>, so it reaches
    /// the engine exactly as a consumer of the two packages would — through <c>CalciteConnection</c> and
    /// nothing else. That is the point of the tests as much as the queries are.
    /// </remarks>
    public class ClrEnumerablePrepareTests
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
            // This project resolves calcite-core 1.43, and 1.43's WinAggContext has a member 1.42's does
            // not. The class the convention hands to a window implementor is written against 1.42, so a
            // member it does not carry is a type that will not load here at all, and every window query
            // fails before it runs. Any window query is the guard.
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
