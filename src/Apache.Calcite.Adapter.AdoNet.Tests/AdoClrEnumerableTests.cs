using System.Collections.Generic;

using Apache.Calcite.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// The adapter reached through this provider's own connection, whose plans are compiled as expression
    /// trees rather than generated as Java source.
    /// </summary>
    /// <remarks>
    /// Every other test in this project opens <c>jdbc:calcite:</c> through <c>DriverManager</c>, which is
    /// Calcite's connection and Calcite's prepare. None of them says anything about this path. The adapter
    /// produces its rows through <c>AdoToEnumerableConverter</c>, a node of Calcite's convention, so a plan
    /// over an ADO.NET schema is necessarily a mixed one: the adapter's own subtree in
    /// <c>EnumerableConvention</c>, and a converter carrying its rows into this one.
    /// </remarks>
    [TestClass]
    public class AdoClrEnumerableTests
    {

        SqliteFixture _sqlite = null!;
        CalciteConnection _connection = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();

            _connection = new CalciteConnection(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
            }.ToString());

            _connection.Open();

            var root = _connection.RootSchema;
            root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _connection?.Dispose();
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Runs a query and returns its rows as strings, so a comparison does not depend on which numeric
        /// type a provider chose.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        List<string> Rows(string sql)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = sql;

            var rows = new List<string>();
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var values = new string[r.FieldCount];
                for (int i = 0; i < r.FieldCount; i++)
                    values[i] = r.IsDBNull(i) ? "null" : r.GetValue(i).ToString()!;

                rows.Add(string.Join("|", values));
            }

            return rows;
        }

        [TestMethod]
        public void ShouldScanAnAdoTable()
        {
            CollectionAssert.AreEquivalent(
                new[] { "1|Alice|10", "2|Bob|10", "3|Carol|20", "4|Dave|20", "5|Erin|null" },
                Rows("SELECT empno, name, deptno FROM ADO.emps ORDER BY empno"));
        }

        [TestMethod]
        public void ShouldFilterAnAdoTable()
        {
            CollectionAssert.AreEquivalent(
                new[] { "3|Carol|20", "4|Dave|20" },
                Rows("SELECT empno, name, deptno FROM ADO.emps WHERE deptno = 20"));
        }

        [TestMethod]
        public void ShouldAggregateAnAdoTable()
        {
            CollectionAssert.AreEquivalent(
                new[] { "10|2", "20|2", "null|1" },
                Rows("SELECT deptno, COUNT(*) FROM ADO.emps GROUP BY deptno ORDER BY deptno"));
        }

        [TestMethod]
        public void ShouldJoinAcrossTheConverter()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering" },
                Rows("SELECT e.name, d.dname FROM ADO.emps e JOIN ADO.depts d ON e.deptno = d.deptno ORDER BY e.name"));
        }

    }

}
