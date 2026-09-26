using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Apache.Calcite.Data;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// The adapter under the cursor convention, which is the convention <c>Apache.Calcite.Data</c> plans
    /// into: the provider's reader is the plan's leaf.
    /// </summary>
    /// <remarks>
    /// <see cref="AdoClrEnumerableTests"/> for the sequence convention. What is held here is what the
    /// cursor changes: the converter chosen, that both advances read the same rows over one connection,
    /// that a failing statement fails the open of either kind, and that a correlated sub-query is enriched
    /// from the cursor convention's correlate.
    /// </remarks>
    public class AdoClrDataCursorTests : IDisposable
    {

        SqliteFixture _sqlite = null!;
        CalciteConnection _connection = null!;

        public AdoClrDataCursorTests()
        {
            _sqlite = new SqliteFixture();
            _connection = OpenConnection(new AdoClrEnumerableTests.CountingAdoDataSource(_sqlite.DataSource));
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _sqlite?.Dispose();
        }

        static CalciteConnection OpenConnection(AdoDataSource dataSource, bool decorrelate = true)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                ForceDecorrelate = decorrelate ? null : false,
            }.ToString())
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", dataSource, null, null)))
                .Build()
                .OpenConnection();
        }

        static string Explain(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR " + sql;

            using var r = cmd.ExecuteReader();
            var plan = new System.Text.StringBuilder();
            while (r.Read())
                plan.AppendLine(r.GetString(0));

            return plan.ToString();
        }

        static List<string> Rows(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
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

        static async Task<List<string>> RowsAsync(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;

            var rows = new List<string>();
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var values = new string[r.FieldCount];
                for (int i = 0; i < r.FieldCount; i++)
                    values[i] = await r.IsDBNullAsync(i) ? "null" : r.GetValue(i).ToString()!;

                rows.Add(string.Join("|", values));
            }

            return rows;
        }

        /// <summary>
        /// The adapter converts straight into the cursor convention, pushed down intact.
        /// </summary>
        /// <remarks>
        /// One converter, the cursor convention's own, and nothing of either other convention between the
        /// adapter and the reader: the route by way of <c>AdoToClrEnumerableConverter</c> under a converter
        /// into the cursor convention answers the same rows and costs one converter more, and the route by
        /// way of Calcite's costs more still.
        /// </remarks>
        [Fact]
        public void ShouldCarryTheAdapterIntoTheCursorConvention()
        {
            var plan = Explain(_connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10");

            Assert.Contains("AdoToClrDataCursorConverter", plan);
            Assert.Contains("AdoProject", plan);
            Assert.False(plan.Contains("AdoToClrEnumerableConverter"), plan);
            Assert.False(plan.Contains("AdoToEnumerableConverter"), plan);
        }

        /// <summary>
        /// The two advances answer the same rows for the same statements.
        /// </summary>
        [Fact]
        public async Task ShouldReadTheSameRowsThroughBothAdvances()
        {
            foreach (var sql in new[]
            {
                "SELECT empno, name, deptno FROM ADO.emps ORDER BY empno",
                "SELECT name FROM ADO.emps WHERE deptno = 20 ORDER BY name",
                "SELECT deptno, COUNT(*) FROM ADO.emps GROUP BY deptno ORDER BY deptno",
                "SELECT salary FROM ADO.emps ORDER BY empno",
                "SELECT e.name, d.dname FROM ADO.emps e JOIN ADO.depts d ON e.deptno = d.deptno ORDER BY e.name",
            })
            {
                (await RowsAsync(_connection, sql)).Should().Equal(Rows(_connection, sql), sql);
            }
        }

        /// <summary>
        /// Either open opens one connection and either disposal closes it.
        /// </summary>
        [Fact]
        public async Task ShouldOpenOneConnectionAndCloseItThroughEitherOpen()
        {
            var source = new AdoClrEnumerableTests.CountingAdoDataSource(_sqlite.DataSource);
            using var connection = OpenConnection(source);

            Rows(connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10").Should().Equal(["1|Alice", "2|Bob"]);
            source.Opened.Should().Be(1);
            source.Closed.Should().Be(1);

            (await RowsAsync(connection, "SELECT empno, name FROM ADO.emps WHERE deptno = 10")).Should().Equal(["1|Alice", "2|Bob"]);
            source.Opened.Should().Be(2);
            source.Closed.Should().Be(2);
        }

        /// <summary>
        /// A failing statement fails the open of either kind, not the first advance.
        /// </summary>
        /// <remarks>
        /// The open is the acquisition: the connection is opened and the statement sent inside it, so a
        /// caller gets the failure back from <c>ExecuteReader</c> or <c>ExecuteReaderAsync</c>, as a caller
        /// of any provider expects.
        /// </remarks>
        [Fact]
        public async Task ShouldFailFromEitherOpen()
        {
            var source = new AdoClrEnumerableTests.CountingAdoDataSource(_sqlite.DataSource) { Failing = true };
            using var connection = OpenConnection(source);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT empno, name FROM ADO.emps";

            Assert.Throws<CalciteException>(() => cmd.ExecuteReader());
            await Assert.ThrowsAsync<CalciteException>(async () => await cmd.ExecuteReaderAsync());
        }

        /// <summary>
        /// A correlated sub-query pushed down under the cursor convention's correlate still gets its parameters.
        /// </summary>
        /// <remarks>
        /// The correlation variables come off the implementor implementing the plan, which is the cursor
        /// convention's here, so the builder has to read them from that one. <c>forceDecorrelate=false</c>
        /// is what leaves a correlate in the plan at all.
        /// </remarks>
        [Fact]
        public async Task ShouldEnrichACorrelatedSubQueryUnderTheCursorCorrelate()
        {
            using var connection = OpenConnection(new AdoClrEnumerableTests.CountingAdoDataSource(_sqlite.DataSource), decorrelate: false);
            const string Sql = "SELECT e.name, (SELECT d.dname FROM ADO.depts d WHERE d.deptno = e.deptno) FROM ADO.emps e";

            var plan = Explain(connection, Sql);
            Assert.Contains("ClrDataCursorCorrelate", plan);
            Assert.Contains("AdoFilter(condition=[=($0, $cor0.DEPTNO)])", plan);
            Assert.Contains("AdoToClrDataCursorConverter", plan);

            var expected = new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|null" };
            Assert.Equivalent(expected, await RowsAsync(connection, Sql), strict: true);
            Assert.Equivalent(expected, Rows(connection, Sql), strict: true);
        }

    }

}
