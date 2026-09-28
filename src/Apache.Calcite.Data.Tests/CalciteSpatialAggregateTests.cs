using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Verifies that the spatial aggregates keep their own accumulators from one statement to the next.
    /// </summary>
    /// <remarks>
    /// <c>ST_UNION</c> and <c>ST_COLLECT</c> are reflective user-defined aggregates whose accumulator classes,
    /// <c>UnionOperation</c> and <c>CollectOperation</c>, have no public fields. Each becomes the empty struct
    /// type as a relational type, and relational types are interned process wide on their digest, so the two
    /// share one. The accumulator's physical type once derived its row class from that shared row type, and
    /// the second aggregate the process ran was handed the first one's record: a statement failed to implement
    /// with "No coercion operator is defined between types UnionOperation and CollectOperation", on a fresh
    /// connection, depending only on what an earlier connection had run.
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
        /// What Calcite's own driver answers, which is what the aggregate should answer here.
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
        /// Runs both aggregates here, one after the other, and only then asks Calcite, so that nothing of
        /// Calcite's own driver reaches the interner between the two.
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
