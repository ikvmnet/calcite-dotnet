using Apache.Calcite.Data;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Statements that modify a table, run over a connection whose plans are compiled by this convention.
    /// </summary>
    /// <remarks>
    /// This convention has no table-modification node. It does not need one: its planner keeps Calcite's own
    /// rules, so the modification is implemented in <c>EnumerableConvention</c> and a converter carries it.
    /// These tests are what says that fallback actually runs, rather than merely being reachable on paper.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableDmlTests
    {

        static ClrEnumerableDmlTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.server.ServerDdlExecutor).Assembly);
        }

        static readonly string ServerDdl = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            Schema = "adhoc",
        };

        /// <summary>
        /// Opens a connection that plans into this convention.
        /// </summary>
        /// <returns></returns>
        static CalciteConnection Open()
        {
            var c = new CalciteConnection(ServerDdl);
            c.Open();
            return c;
        }

        [TestMethod]
        public void ShouldCreateInsertAndSelect()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"dml_t\" (\"id\" INTEGER NOT NULL, \"n\" VARCHAR(10))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"dml_t\" VALUES (1, 'a'), (2, 'b')";
            cmd.ExecuteNonQuery().Should().Be(2);

            cmd.CommandText = "SELECT \"n\" FROM \"dml_t\" WHERE \"id\" = 2";
            cmd.ExecuteScalar().Should().Be("b");
        }

        [TestMethod]
        public void ShouldAggregateOverAnInsertedTable()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"dml_agg\" (\"id\" INTEGER NOT NULL, \"g\" VARCHAR(10))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"dml_agg\" VALUES (1, 'x'), (2, 'x'), (3, 'y')";
            cmd.ExecuteNonQuery().Should().Be(3);

            cmd.CommandText = "SELECT COUNT(*) FROM \"dml_agg\" WHERE \"g\" = 'x'";
            System.Convert.ToInt64(cmd.ExecuteScalar()).Should().Be(2);
        }

    }

}
