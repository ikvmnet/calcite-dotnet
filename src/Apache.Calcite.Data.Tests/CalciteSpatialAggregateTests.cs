using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Verifies that each spatial aggregate uses its own accumulator class, whatever ran before it.
    /// </summary>
    /// <remarks>
    /// <c>ST_UNION</c> and <c>ST_COLLECT</c> are reflective user-defined aggregates whose accumulator classes,
    /// <c>UnionOperation</c> and <c>CollectOperation</c>, have no public fields. Both map to the same empty
    /// struct relational type, which Calcite interns process wide, so an accumulator's physical type must not
    /// derive its row class from that row type, or the second aggregate a process runs is given the first
    /// one's accumulator class. Each test runs the two in one order, on separate connections.
    /// </remarks>
    public class CalciteSpatialAggregateTests
    {

        const string ConnectionString = TestModels.InlineEmptyModelConnectionString + ";Fun=spatial;Pooling=false";

        static string Sql(string function) =>
            $"SELECT ST_AsText({function}(ST_MakePoint(\"x\", \"x\"))) FROM (VALUES (1, 1), (2, 1)) AS \"t\" (\"x\", \"g\") GROUP BY \"g\"";

        static string Aggregate(string function)
        {
            using var c = new CalciteConnection(ConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = Sql(function);

            return (string)cmd.ExecuteScalar()!;
        }

        /// <summary>
        /// Returns the result Calcite's own JDBC driver gives, which is the expected answer.
        /// </summary>
        static string Calcite(string function)
        {
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:fun=spatial");
            using var statement = connection.createStatement();
            var results = statement.executeQuery(Sql(function));
            Assert.True(results.next());

            return results.getString(1);
        }

        /// <summary>
        /// Runs both aggregates through this provider, one after the other, and only then asks Calcite's
        /// driver, so that the driver does not intern anything between the two.
        /// </summary>
        static void InOrder(string first, string second)
        {
            var firstAnswer = Aggregate(first);
            var secondAnswer = Aggregate(second);

            Assert.Equal(Calcite(first), firstAnswer);
            Assert.Equal(Calcite(second), secondAnswer);
        }

        [Fact]
        public void Union_should_keep_its_accumulator_after_a_collect() => InOrder("ST_COLLECT", "ST_UNION");

        [Fact]
        public void Collect_should_keep_its_accumulator_after_a_union() => InOrder("ST_UNION", "ST_COLLECT");

    }

}
