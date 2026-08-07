using System;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Tests that a view is expanded and queried correctly through the ADO.NET surface.
    /// </summary>
    /// <remarks>
    /// View expansion is supplied by <c>CalcitePreparingStmt.expandView</c>, which the prepare path
    /// reaches through Calcite's own preparing statement rather than through anything this project
    /// writes. Nothing else in this suite queries a view, so a change to how the preparing statement is
    /// constructed could drop view support without a single test failing. These are that test.
    ///
    /// <para>Both routes to a view are covered, because they enter at different points: a view declared
    /// in the model is a <c>ViewTable</c> built by <c>ModelHandler</c> at connection time, while
    /// <c>CREATE VIEW</c> is a DDL statement executed by <c>ServerDdlExecutor</c> against a live schema.
    /// Only the second needs the server parser.</para>
    /// </remarks>
    public class CalciteViewTests
    {

        static CalciteViewTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.server.ServerDdlExecutor).Assembly);
        }

        /// <summary>
        /// A model whose schema holds nothing but views: one over a VALUES, one over that view, and one
        /// whose SQL is given as a list of lines.
        /// </summary>
        static readonly string ViewModelConnectionString = new CalciteConnectionStringBuilder
        {
            Model =
                "inline:{" +
                "\"version\":\"1.0\"," +
                "\"defaultSchema\":\"adhoc\"," +
                "\"schemas\":[{" +
                    "\"name\":\"adhoc\"," +
                    "\"tables\":[" +
                        // each name is cast inside the VALUES, not after it: a VALUES unifies its literals
                        // to the widest CHAR, so a bare 'Bob' is padded to the width of 'Alice' and casting
                        // the padded CHAR to VARCHAR afterwards keeps the spaces
                        "{\"name\":\"EMPS\",\"type\":\"view\"," +
                         "\"sql\":\"SELECT * FROM (VALUES (10, CAST('Alice' AS VARCHAR(16)), 3), (20, CAST('Bob' AS VARCHAR(16)), 1), (30, CAST('Carol' AS VARCHAR(16)), 3)) AS T(ID, NAME, DEPTNO)\"}," +
                        "{\"name\":\"EMPS_IN_3\",\"type\":\"view\"," +
                         "\"sql\":\"SELECT ID, NAME FROM EMPS WHERE DEPTNO = 3\"}," +
                        "{\"name\":\"TWO_LINES\",\"type\":\"view\"," +
                         "\"sql\":[\"SELECT 1 AS X\",\"UNION ALL SELECT 2\"]}" +
                    "]}]}",
            Schema = "adhoc",
        };

        [Fact]
        public void Model_view_should_be_queryable()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ID, NAME FROM EMPS ORDER BY ID";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(10, r.GetInt32(0));
            Assert.Equal("Alice", r.GetString(1));
            Assert.True(r.Read());
            Assert.Equal(20, r.GetInt32(0));
            Assert.Equal("Bob", r.GetString(1));
            Assert.True(r.Read());
            Assert.Equal(30, r.GetInt32(0));
            Assert.Equal("Carol", r.GetString(1));
            Assert.False(r.Read());
        }

        /// <summary>
        /// A predicate applied over the view has to survive expansion, which is where the view's own plan
        /// and the caller's are merged rather than stacked.
        /// </summary>
        [Fact]
        public void Model_view_should_expand_under_an_outer_predicate()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT NAME FROM EMPS WHERE DEPTNO = 3 ORDER BY NAME";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal("Alice", r.GetString(0));
            Assert.True(r.Read());
            Assert.Equal("Carol", r.GetString(0));
            Assert.False(r.Read());
        }

        /// <summary>
        /// A view defined over another view expands twice.
        /// </summary>
        [Fact]
        public void View_over_a_view_should_expand()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT NAME FROM EMPS_IN_3 ORDER BY NAME";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal("Alice", r.GetString(0));
            Assert.True(r.Read());
            Assert.Equal("Carol", r.GetString(0));
            Assert.False(r.Read());
        }

        /// <summary>
        /// A view's row type is the source of the result's column metadata, so it is worth asserting
        /// separately from the rows: the metadata is built from the validated row type rather than from
        /// anything the view's plan produces.
        /// </summary>
        [Fact]
        public void Model_view_should_report_column_metadata()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ID, NAME FROM EMPS";

            using var r = cmd.ExecuteReader();

            Assert.Equal(2, r.FieldCount);
            Assert.Equal("ID", r.GetName(0));
            Assert.Equal("NAME", r.GetName(1));
            Assert.Equal(typeof(int), r.GetFieldType(0));
            Assert.Equal(typeof(string), r.GetFieldType(1));
        }

        /// <summary>
        /// <c>JsonView.sql</c> accepts a list of strings, which are joined with newlines.
        /// </summary>
        [Fact]
        public void Multi_line_view_sql_should_be_concatenated()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT X FROM TWO_LINES ORDER BY X";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(1, r.GetInt32(0));
            Assert.True(r.Read());
            Assert.Equal(2, r.GetInt32(0));
            Assert.False(r.Read());
        }

        /// <summary>
        /// The other route to a view: a DDL statement against a live schema, which needs the server
        /// parser exactly as <see cref="CalciteDdlTests"/> does.
        /// </summary>
        static readonly string ServerDdlConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            Schema = "adhoc",
        };

        [Fact]
        public void Created_view_should_be_queryable()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"viewsrc\" (\"id\" INTEGER NOT NULL, \"grp\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"viewsrc\" VALUES (1, 7), (2, 8), (3, 7)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "CREATE VIEW \"viewgrp7\" AS SELECT \"id\" FROM \"viewsrc\" WHERE \"grp\" = 7";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"id\" FROM \"viewgrp7\" ORDER BY \"id\"";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(1, r.GetInt32(0));
            Assert.True(r.Read());
            Assert.Equal(3, r.GetInt32(0));
            Assert.False(r.Read());
        }

    }

}
