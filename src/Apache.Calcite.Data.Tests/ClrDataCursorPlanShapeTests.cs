using System.Text;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Holds the provider to the convention it plans into: the root of every plan is the cursor
    /// convention's, and what the cursor convention lacks is the sequence convention's beneath a converter.
    /// </summary>
    public class ClrDataCursorPlanShapeTests
    {

        static readonly string ConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            Schema = "adhoc",
        };

        static string Plan(string connectionString, string sql)
        {
            using var c = new CalciteConnection(connectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR " + sql;
            using var r = cmd.ExecuteReader();

            var sb = new StringBuilder();
            while (r.Read())
                sb.AppendLine(r.GetValue(0)?.ToString());

            return sb.ToString();
        }

        /// <summary>
        /// Asserts every node of <paramref name="plan"/> begins with one of <paramref name="prefixes"/>.
        /// </summary>
        static void AssertEveryNode(string plan, params string[] prefixes)
        {
            Assert.NotEmpty(plan);

            foreach (var line in plan.Split('\n'))
            {
                var node = line.TrimStart();
                if (node.Length == 0)
                    continue;

                Assert.Contains(prefixes, prefix => node.StartsWith(prefix));
            }
        }

        [Theory]
        [InlineData("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) WHERE x > 1")]
        [InlineData("SELECT y, COUNT(*) FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS t(x, y) GROUP BY y")]
        [InlineData("SELECT a.x FROM (VALUES (1), (2)) AS a(x) JOIN (VALUES (1), (3)) AS b(x) ON a.x = b.x")]
        [InlineData("SELECT x FROM (VALUES (3), (1), (2)) AS t(x) ORDER BY x LIMIT 2")]
        [InlineData("SELECT x FROM (VALUES (1), (2)) AS a(x) UNION SELECT x FROM (VALUES (2), (3)) AS b(x)")]
        public void Plan_should_be_rooted_in_the_cursor_convention(string sql)
        {
            var plan = Plan(ConnectionString, sql);

            Assert.StartsWith("ClrDataCursor", plan.TrimStart());

            // a node the cursor convention has not got is the sequence convention's, under the converter
            // between the two, and nothing is left to Calcite's
            AssertEveryNode(plan, "ClrDataCursor", "ClrEnumerable");
        }

    }

}
