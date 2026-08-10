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
    /// Calcite's connection and Calcite's prepare. None of them says anything about this path. A plan over
    /// an ADO.NET schema is necessarily a mixed one — the adapter's own subtree stays in its convention —
    /// and the crossing depends on the connection's mode: by default the rows go through
    /// <c>AdoToEnumerableConverter</c>, Calcite's convention, under the converter into the asynchronous one;
    /// in synchronous mode the adapter converts straight into <c>ClrEnumerableConvention</c> through
    /// <c>AdoToClrEnumerableConverter</c>, with no linq4j layer between.
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
            _connection = OpenConnection();
        }

        /// <summary>
        /// Opens a connection over the fixture's SQLite schema, in the mode asked for.
        /// </summary>
        CalciteConnection OpenConnection(bool synchronous = false)
        {
            var c = new CalciteConnection(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                Synchronous = synchronous ? true : null,
            }.ToString());

            c.Open();

            var root = c.RootSchema;
            root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null));

            return c;
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

        /// <summary>
        /// Returns the rendered plan for <paramref name="sql"/> on <paramref name="connection"/>.
        /// </summary>
        static string Explain(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR " + sql;

            var plan = new System.Text.StringBuilder();
            using (var r = cmd.ExecuteReader())
                while (r.Read())
                    plan.AppendLine(r.GetValue(0)?.ToString());

            return plan.ToString();
        }

        /// <summary>
        /// In synchronous mode the adapter converts straight into the synchronous convention.
        /// </summary>
        /// <remarks>
        /// One converter, and it is that convention's own. Reaching <c>AdoToEnumerableConverter</c> instead
        /// would still answer correctly, by way of a second converter, and no other assertion here would
        /// notice. The mode is pinned because the default plans asynchronously, where the routes tie on the
        /// planner's row-count-only cost and the adapter's rows go through Calcite's converter —
        /// <see cref="ShouldCarryTheAdapterIntoTheAsyncConvention"/> holds that plan.
        /// </remarks>
        [TestMethod]
        public void ShouldConvertStraightIntoThisConvention()
        {
            using var c = OpenConnection(synchronous: true);

            var plan = Explain(c, "SELECT empno, name FROM ADO.emps WHERE deptno = 10");

            StringAssert.Contains(plan, "AdoToClrEnumerableConverter");
            Assert.IsFalse(plan.Contains("AdoToEnumerableConverter"), plan);
        }

        /// <summary>
        /// By default the adapter's subtree is carried into the asynchronous convention, pushed down intact.
        /// </summary>
        /// <remarks>
        /// The subtree under the converter is the adapter's own — an <c>AdoProject</c> rather than a scan
        /// with the work done above it — so the crossing costs a wrapper and loses no pushdown.
        /// </remarks>
        [TestMethod]
        public void ShouldCarryTheAdapterIntoTheAsyncConvention()
        {
            var plan = Explain(_connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10");

            StringAssert.Contains(plan, "EnumerableToClrAsyncEnumerableConverter");
            StringAssert.Contains(plan, "AdoProject");
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
