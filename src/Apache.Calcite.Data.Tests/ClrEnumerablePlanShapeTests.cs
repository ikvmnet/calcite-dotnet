using System.Text;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// The convention a connection's plans are actually built in.
    /// </summary>
    /// <remarks>
    /// A plan of Calcite's own convention still runs on a connection of this provider — a converter carries
    /// its rows, and nothing about the result says which convention produced it. So a silent fallback to
    /// <c>EnumerableConvention</c> would pass every other test in this project while defeating the point of
    /// the default. These read the plan back with <c>EXPLAIN PLAN FOR</c> and require that it is ours.
    ///
    /// <para>Nothing here needs a cost multiplier to hold. A plan built wholly in this convention crosses no
    /// converter, and a converter adds its own row count to the cumulative cost, so it is already the cheaper
    /// plan wherever every node has an implementation here.</para>
    /// </remarks>
    public class ClrEnumerablePlanShapeTests
    {

        static readonly string ConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            Schema = "adhoc",
        };

        /// <summary>
        /// Returns the plan a connection of this provider builds for a query.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static string Plan(string sql)
        {
            using var c = new CalciteConnection(ConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR " + sql;
            using var r = cmd.ExecuteReader();

            var sb = new StringBuilder();
            while (r.Read())
                sb.AppendLine(r.GetValue(0)?.ToString());

            return sb.ToString();
        }

        [Theory]
        [InlineData("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) WHERE x > 1")]
        [InlineData("SELECT y, COUNT(*) FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS t(x, y) GROUP BY y")]
        [InlineData("SELECT a.x FROM (VALUES (1), (2)) AS a(x) JOIN (VALUES (1), (3)) AS b(x) ON a.x = b.x")]
        [InlineData("SELECT x FROM (VALUES (3), (1), (2)) AS t(x) ORDER BY x LIMIT 2")]
        [InlineData("SELECT x FROM (VALUES (1), (2)) AS a(x) UNION SELECT x FROM (VALUES (2), (3)) AS b(x)")]
        public void Plan_should_be_built_in_the_clr_convention(string sql)
        {
            var plan = Plan(sql);

            Assert.NotEmpty(plan);

            foreach (var line in plan.Split('\n'))
            {
                var node = line.TrimStart();
                if (node.Length == 0)
                    continue;

                Assert.StartsWith("ClrEnumerable", node);
            }
        }

    }

}
