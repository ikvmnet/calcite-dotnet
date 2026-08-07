using System.Collections.Generic;

using Apache.Calcite.Data;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a query through the ADO.NET surface with this convention doing the work.
    /// </summary>
    /// <remarks>
    /// The end this convention was written for. <c>CalciteConnection.PrepareFactory</c> is the seam, and
    /// <see cref="ClrEnumerablePrepare"/> is what goes in it; nothing else about the connection changes.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableAdoNetTests
    {

        const string Model =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]};Schema=adhoc";

        static CalciteConnection Open()
        {
            var c = new CalciteConnection(Model);
            c.Open();

            return c;
        }

        [TestMethod]
        public void ShouldRunAValuesQuery()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) ORDER BY x";

            var rows = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1));

            rows.Should().Equal(["1|a", "2|b"]);
        }

        [TestMethod]
        public void ShouldRunAnAggregate()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT y, COUNT(*) FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS t(x, y) GROUP BY y ORDER BY y";

            var rows = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                rows.Add(reader.GetString(0) + "|" + reader.GetInt64(1));

            rows.Should().Equal(["a|2", "b|1"]);
        }

        [TestMethod]
        public void ShouldRunAJoin()
        {
            using var c = Open();
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
