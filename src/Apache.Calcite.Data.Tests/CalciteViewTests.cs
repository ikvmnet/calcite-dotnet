using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

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
    ///
    /// <para><b>A view is not a table, and the metadata collections have to say so separately.</b> Both
    /// routes register a view as a <c>TableMacro</c> of no arguments, which lands in the schema's function
    /// map rather than its table map — so <c>getTableNames()</c> does not see one, and
    /// <c>GetSchema("Tables")</c> listed no views at all until <c>CalciteSchemaInfo.TablesOf</c> started
    /// concatenating <c>getTablesBasedOnNullaryFunctions()</c> the way <c>CalciteMetaImpl.tables</c> does.
    /// Querying a view worked the whole time, which is why this needs its own tests.</para>
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

        /// <summary>
        /// The server parser with no model, so DDL lands in the root schema.
        /// </summary>
        /// <remarks>
        /// <c>ServerDdlExecutor.populate</c> — which is how <c>CREATE MATERIALIZED VIEW</c> and
        /// <c>CREATE TABLE ... AS SELECT</c> load their rows — builds its <c>INSERT</c> against
        /// <c>context.getRootSchema()</c> and nothing else, so a statement naming objects in a sub-schema
        /// cannot resolve them. Upstream's own tests use a connection shaped like this one for the same
        /// reason; see <see cref="Materialized_view_in_a_sub_schema_is_not_supported_upstream"/>.
        /// </remarks>
        static readonly string RootDdlConnectionString = new CalciteConnectionStringBuilder
        {
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
        };

        // ------------------------------------------------------------------------------------------
        // Querying a view
        // ------------------------------------------------------------------------------------------

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
        /// <c>SELECT *</c> is expanded against the view's row type rather than the underlying table's.
        /// </summary>
        [Fact]
        public void Star_over_a_view_should_expand_to_the_views_row_type()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM EMPS ORDER BY ID";

            using var r = cmd.ExecuteReader();

            Assert.Equal(3, r.FieldCount);
            Assert.Equal("ID", r.GetName(0));
            Assert.Equal("NAME", r.GetName(1));
            Assert.Equal("DEPTNO", r.GetName(2));
            Assert.True(r.Read());
            Assert.Equal(10, r.GetInt32(0));
        }

        /// <summary>
        /// A dynamic parameter in a predicate over a view: the parameter belongs to the outer query and
        /// the view's own plan has none, so expansion has to keep the two apart.
        /// </summary>
        [Fact]
        public void Parameterized_predicate_over_a_view_should_bind()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT NAME FROM EMPS WHERE ID = ?";
            var p = cmd.CreateParameter();
            p.Value = 20;
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal("Bob", r.GetString(0));
            Assert.False(r.Read());
        }

        [Fact]
        public void ExecuteScalar_over_a_view_should_aggregate()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM EMPS";

            Assert.Equal(3L, Convert.ToInt64(cmd.ExecuteScalar()));
        }

        [Fact]
        public async Task Async_reader_over_a_view_should_return_the_same_rows()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ID FROM EMPS ORDER BY ID";

            using var r = await cmd.ExecuteReaderAsync();

            Assert.True(await r.ReadAsync());
            Assert.Equal(10, r.GetInt32(0));
            Assert.True(await r.ReadAsync());
            Assert.Equal(20, r.GetInt32(0));
            Assert.True(await r.ReadAsync());
            Assert.Equal(30, r.GetInt32(0));
            Assert.False(await r.ReadAsync());
        }

        [Fact]
        public void Explain_over_a_view_should_produce_a_plan()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR SELECT ID FROM EMPS";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.False(string.IsNullOrWhiteSpace(r.GetString(0)));
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
        /// A view is expanded against the schema path it was <i>declared</i> with, not the connection's, so
        /// one in another schema resolves its own tables and is reachable by a qualified name.
        /// </summary>
        [Fact]
        public void View_in_another_schema_should_be_queryable_by_a_qualified_name()
        {
            var cs = new CalciteConnectionStringBuilder
            {
                Model =
                    "inline:{\"version\":\"1.0\",\"defaultSchema\":\"main\",\"schemas\":[" +
                    "{\"name\":\"main\"}," +
                    "{\"name\":\"other\",\"tables\":[" +
                        "{\"name\":\"SRC\",\"type\":\"view\",\"sql\":\"SELECT * FROM (VALUES (1), (2)) AS T(X)\"}," +
                        "{\"name\":\"V\",\"type\":\"view\",\"sql\":\"SELECT X FROM SRC WHERE X = 2\"}" +
                    "]}]}",
                Schema = "main",
            };

            using var c = new CalciteConnection(cs.ToString());
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT X FROM \"other\".V";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(2, r.GetInt32(0));
            Assert.False(r.Read());
        }

        /// <summary>
        /// A view whose definition does not resolve fails when it is expanded, which is at query time
        /// rather than at connection time — <c>ModelHandler</c> registers a macro without applying it.
        /// </summary>
        [Fact]
        public void View_over_a_missing_table_should_fail_when_it_is_expanded()
        {
            var cs = new CalciteConnectionStringBuilder
            {
                Model =
                    "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"," +
                    "\"tables\":[{\"name\":\"BAD\",\"type\":\"view\",\"sql\":\"SELECT * FROM NO_SUCH_TABLE\"}]}]}",
                Schema = "adhoc",
            };

            using var c = new CalciteConnection(cs.ToString());
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM BAD";

            Assert.ThrowsAny<Exception>(() => cmd.ExecuteReader());
        }

        /// <summary>
        /// <b>A view is analyzed under Calcite's default connection config, not this connection's.</b>
        /// A function the connection asked for works in a query and fails inside a view.
        /// </summary>
        /// <remarks>
        /// <c>ViewTableMacro.apply</c> calls
        /// <c>Schemas.analyzeView(MaterializedViewTable.MATERIALIZATION_CONNECTION, ...)</c>, and
        /// <c>Schemas.makeContext</c> builds its context from <c>connection.config()</c> — that connection
        /// being a <c>DriverManager.getConnection("jdbc:calcite:")</c> held in a <c>static final</c>, so its
        /// config is the default one. Everything the connection string says about <c>fun</c>, <c>lex</c>,
        /// <c>conformance</c> and the rest is therefore invisible while a view is being described, even
        /// though <c>ClrPreparingStmt.expandView</c> uses the real config when the view is later expanded
        /// into a plan.
        ///
        /// <para>NVL is in Calcite's Oracle library; this connection asks for <c>standard,oracle</c>. The
        /// first half of this test shows the connection honours that. The second half is the same
        /// expression inside a view, and it does not get that far — it fails in <c>apply</c>, so the view
        /// cannot even be described.</para>
        ///
        /// <para><c>Schemas.makeContext</c> has the fix already: given a null connection it uses
        /// <c>CalcitePrepare.Dummy.peek()</c>, which is this provider's own pushed context and carries the
        /// real config. Reaching it means supplying a <c>ViewTableMacro</c> subclass that overrides
        /// <c>apply</c> to pass null, and getting that subclass installed in place of Calcite's — which is
        /// the part with no public seam. Asserted rather than skipped so that doing so shows up here as a
        /// failure.</para>
        /// </remarks>
        [Fact]
        public void View_is_analyzed_under_the_default_config_not_the_connections()
        {
            var withOracleFun = new CalciteConnectionStringBuilder
            {
                Model =
                    "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"," +
                    "\"tables\":[{\"name\":\"NVLVIEW\",\"type\":\"view\"," +
                    "\"sql\":\"SELECT NVL(CAST(NULL AS INTEGER), 7) AS Y\"}]}]}",
                Schema = "adhoc",
                Fun = "standard,oracle",
            };

            using var c = new CalciteConnection(withOracleFun);
            c.Open();
            using var cmd = c.CreateCommand();

            // the connection really does have the Oracle library
            cmd.CommandText = "SELECT NVL(CAST(NULL AS INTEGER), 7) AS Y";
            using (var r = cmd.ExecuteReader())
            {
                Assert.True(r.Read());
                Assert.Equal(7, r.GetInt32(0));
            }

            // the same expression inside a view does not, because apply() analyzes it without that config
            cmd.CommandText = "SELECT Y FROM NVLVIEW";
            var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteReader());
            Assert.Contains("No match found for function signature NVL", ex.ToString(), StringComparison.Ordinal);
        }

        // ------------------------------------------------------------------------------------------
        // Views in the metadata collections
        // ------------------------------------------------------------------------------------------

        /// <summary>
        /// A model view is listed by <c>GetSchema("Tables")</c>, typed VIEW.
        /// </summary>
        /// <remarks>
        /// The type comes from <c>Table.getJdbcTableType().jdbcName</c>, as <c>CalciteMetaTable</c>'s does.
        /// The listing itself comes from <c>getTablesBasedOnNullaryFunctions()</c>, because a view is in
        /// the function map and <c>getTableNames()</c> reads the table map.
        /// </remarks>
        [Fact]
        public void GetSchema_Tables_should_list_a_model_view_as_a_view()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();

            var t = c.GetSchema("Tables");
            var rows = t.Rows.Cast<DataRow>()
                .Select(r => ((string)r["TABLE_SCHEMA"], (string)r["TABLE_NAME"], (string)r["TABLE_TYPE"]))
                .ToArray();

            Assert.Contains(("adhoc", "EMPS", "VIEW"), rows);
            Assert.Contains(("adhoc", "EMPS_IN_3", "VIEW"), rows);
            Assert.Contains(("adhoc", "TWO_LINES", "VIEW"), rows);
        }

        /// <summary>
        /// The TableType restriction filters views the same way it filters tables.
        /// </summary>
        [Fact]
        public void GetSchema_Tables_should_honour_the_table_type_restriction_for_views()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();

            var t = c.GetSchema("Tables", [null, null, null, "VIEW"]);
            var names = t.Rows.Cast<DataRow>().Select(r => (string)r["TABLE_NAME"]).ToArray();

            Assert.Contains("EMPS", names);
            Assert.All(t.Rows.Cast<DataRow>(), r => Assert.Equal("VIEW", (string)r["TABLE_TYPE"]));
        }

        /// <summary>
        /// A view's columns come from its expanded row type, so listing them expands the view.
        /// </summary>
        [Fact]
        public void GetSchema_Columns_should_list_a_model_views_columns()
        {
            using var c = new CalciteConnection(ViewModelConnectionString);
            c.Open();

            var t = c.GetSchema("Columns", [null, null, "EMPS", null]);
            var columns = t.Rows.Cast<DataRow>()
                .Select(r => ((string)r["COLUMN_NAME"], (string)r["DATA_TYPE"], (int)r["ORDINAL_POSITION"]))
                .ToArray();

            Assert.Equal([("ID", "INTEGER", 1), ("NAME", "VARCHAR", 2), ("DEPTNO", "INTEGER", 3)], columns);
        }

        /// <summary>
        /// A model whose schema holds one view that expands and one that cannot.
        /// </summary>
        static readonly string BrokenViewModelConnectionString = new CalciteConnectionStringBuilder
        {
            Model =
                "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"," +
                "\"tables\":[" +
                    "{\"name\":\"GOOD\",\"type\":\"view\",\"sql\":\"SELECT * FROM (VALUES (1)) AS T(X)\"}," +
                    "{\"name\":\"BROKEN\",\"type\":\"view\",\"sql\":\"SELECT * FROM NO_SUCH_TABLE\"}" +
                "]}]}",
            Schema = "adhoc",
        };

        /// <summary>
        /// Naming one view must not expand the others.
        /// </summary>
        /// <remarks>
        /// A view is expanded to be described — <c>ViewTableMacro.apply</c> runs the whole front end over
        /// its SQL — so where the restriction is applied is not a detail. Applied after, one unresolvable
        /// view in a schema breaks every metadata call that touches that schema, including calls about
        /// unrelated tables. <c>BROKEN</c> is here to fail if the ordering regresses; nothing in this test
        /// mentions it.
        /// </remarks>
        [Fact]
        public void GetSchema_Columns_for_one_view_should_not_expand_the_others()
        {
            using var c = new CalciteConnection(BrokenViewModelConnectionString);
            c.Open();

            var t = c.GetSchema("Columns", [null, null, "GOOD", null]);
            var names = t.Rows.Cast<DataRow>().Select(r => (string)r["COLUMN_NAME"]).ToArray();

            Assert.Equal(["X"], names);
        }

        [Fact]
        public void GetSchema_Tables_for_one_view_should_not_expand_the_others()
        {
            using var c = new CalciteConnection(BrokenViewModelConnectionString);
            c.Open();

            var t = c.GetSchema("Tables", [null, null, "GOOD", null]);
            var rows = t.Rows.Cast<DataRow>()
                .Select(r => ((string)r["TABLE_NAME"], (string)r["TABLE_TYPE"]))
                .ToArray();

            Assert.Equal([("GOOD", "VIEW")], rows);
        }

        /// <summary>
        /// The corollary, stated so it is not mistaken for an accident: an <i>unrestricted</i> listing does
        /// expand every view, because <c>TABLE_TYPE</c> comes from the expanded table, so a schema holding
        /// a view that no longer resolves cannot be listed whole.
        /// </summary>
        [Fact]
        public void GetSchema_Tables_unrestricted_expands_every_view()
        {
            using var c = new CalciteConnection(BrokenViewModelConnectionString);
            c.Open();

            Assert.ThrowsAny<Exception>(() => c.GetSchema("Tables"));
        }

        /// <summary>
        /// A view created by DDL is registered the same way a model view is — as a macro — so it has to be
        /// listed by the same route.
        /// </summary>
        [Fact]
        public void GetSchema_Tables_should_list_a_created_view_as_a_view()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"metasrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"metaview\" AS SELECT \"id\" FROM \"metasrc\"";
            cmd.ExecuteNonQuery();

            var t = c.GetSchema("Tables");
            var rows = t.Rows.Cast<DataRow>()
                .Select(r => ((string)r["TABLE_NAME"], (string)r["TABLE_TYPE"]))
                .ToArray();

            Assert.Contains(("metasrc", "TABLE"), rows);
            Assert.Contains(("metaview", "VIEW"), rows);
        }

        // ------------------------------------------------------------------------------------------
        // CREATE / REPLACE / DROP VIEW
        // ------------------------------------------------------------------------------------------

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

        /// <summary>
        /// A view holds its definition, not its rows, so a row inserted after the view was created is in
        /// it. This is what separates a view from the materialized view below.
        /// </summary>
        [Fact]
        public void Created_view_should_see_rows_inserted_after_it_was_created()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"latesrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"lateview\" AS SELECT \"id\" FROM \"latesrc\"";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"latesrc\" VALUES (42)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"id\" FROM \"lateview\"";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(42, r.GetInt32(0));
            Assert.False(r.Read());
        }

        /// <summary>
        /// A view's column list renames the query's columns, which <c>ServerDdlExecutor</c> does by
        /// rewriting the query before it ever becomes a macro.
        /// </summary>
        [Fact]
        public void Created_view_with_a_column_list_should_rename_its_columns()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"colsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"colsrc\" VALUES (5)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"colview\" (\"renamed\") AS SELECT \"id\" FROM \"colsrc\"";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"renamed\" FROM \"colview\"";
            using var r = cmd.ExecuteReader();

            Assert.Equal("renamed", r.GetName(0));
            Assert.True(r.Read());
            Assert.Equal(5, r.GetInt32(0));
        }

        /// <summary>
        /// A view over a join and an aggregate, which is the case where expansion produces a subtree rather
        /// than a scan and the outer query's own aggregate has to sit on top of it.
        /// </summary>
        [Fact]
        public void Created_view_over_a_join_and_an_aggregate_should_be_queryable()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"joina\" (\"id\" INTEGER NOT NULL, \"g\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE \"joinb\" (\"g\" INTEGER NOT NULL, \"nm\" VARCHAR(10) NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"joina\" VALUES (1, 7), (2, 7), (3, 8)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"joinb\" VALUES (7, 'seven'), (8, 'eight')";
            cmd.ExecuteNonQuery();
            cmd.CommandText =
                "CREATE VIEW \"joinview\" AS SELECT \"b\".\"nm\" AS \"nm\", COUNT(*) AS \"n\" " +
                "FROM \"joina\" \"a\" JOIN \"joinb\" \"b\" ON \"a\".\"g\" = \"b\".\"g\" GROUP BY \"b\".\"nm\"";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"nm\", \"n\" FROM \"joinview\" WHERE \"n\" > 1";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal("seven", r.GetString(0));
            Assert.Equal(2L, r.GetInt64(1));
            Assert.False(r.Read());
        }

        /// <summary>
        /// <c>CREATE VIEW</c> over a name that is already a nullary function is an error without REPLACE.
        /// </summary>
        [Fact]
        public void Creating_a_view_twice_should_fail_without_replace()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"dupsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"dupview\" AS SELECT \"id\" FROM \"dupsrc\"";
            cmd.ExecuteNonQuery();

            Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        }

        [Fact]
        public void Create_or_replace_view_should_redefine_the_view()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"repsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"repsrc\" VALUES (1), (2)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"repview\" AS SELECT \"id\" FROM \"repsrc\" WHERE \"id\" = 1";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE OR REPLACE VIEW \"repview\" AS SELECT \"id\" FROM \"repsrc\" WHERE \"id\" = 2";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"id\" FROM \"repview\"";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(2, r.GetInt32(0));
            Assert.False(r.Read());
        }

        [Fact]
        public void Dropped_view_should_no_longer_resolve()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"dropsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"dropview\" AS SELECT \"id\" FROM \"dropsrc\"";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "DROP VIEW \"dropview\"";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"id\" FROM \"dropview\"";
            Assert.ThrowsAny<Exception>(() => cmd.ExecuteReader());
        }

        /// <summary>
        /// A dropped view is also out of the metadata, which is a separate lookup from the one the
        /// validator uses.
        /// </summary>
        [Fact]
        public void Dropped_view_should_no_longer_be_listed()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"gonesrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"goneview\" AS SELECT \"id\" FROM \"gonesrc\"";
            cmd.ExecuteNonQuery();

            var before = c.GetSchema("Tables").Rows.Cast<DataRow>().Select(r => (string)r["TABLE_NAME"]).ToArray();
            Assert.Contains("goneview", before);

            cmd.CommandText = "DROP VIEW \"goneview\"";
            cmd.ExecuteNonQuery();

            var after = c.GetSchema("Tables").Rows.Cast<DataRow>().Select(r => (string)r["TABLE_NAME"]).ToArray();
            Assert.DoesNotContain("goneview", after);
        }

        [Fact]
        public void Dropping_a_missing_view_should_succeed_with_if_exists()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "DROP VIEW IF EXISTS \"neverexisted\"";

            Assert.Null(Record.Exception(() => cmd.ExecuteNonQuery()));
        }

        [Fact]
        public void Dropping_a_missing_view_should_fail_without_if_exists()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "DROP VIEW \"neverexisted\"";

            Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        }

        [Fact]
        public void View_created_in_the_root_schema_should_be_queryable()
        {
            using var c = new CalciteConnection(RootDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"rootsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"rootsrc\" VALUES (9)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"rootview\" AS SELECT \"id\" FROM \"rootsrc\"";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT \"id\" FROM \"rootview\"";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(9, r.GetInt32(0));
            Assert.False(r.Read());
        }

        // ------------------------------------------------------------------------------------------
        // Writing through a view
        // ------------------------------------------------------------------------------------------

        /// <summary>
        /// <c>ServerDdlExecutor</c> always builds its macro with <c>modifiable = false</c>, so a view it
        /// created is a plain <c>ViewTable</c> and not a <c>ModifiableView</c>. An INSERT into one is
        /// rejected by the validator.
        /// </summary>
        [Fact]
        public void Insert_into_a_created_view_should_be_rejected()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"rosrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE VIEW \"roview\" AS SELECT \"id\" FROM \"rosrc\"";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"roview\" VALUES (1)";
            Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        }

        /// <summary>
        /// A modifiable view does accept an INSERT, and fills the columns its own WHERE clause constrains.
        /// </summary>
        /// <remarks>
        /// There is no SQL route to one: <c>ServerDdlExecutor</c> hard-codes <c>modifiable = false</c> and a
        /// model view asking for <c>modifiable</c> can only sit over whatever the model can declare. So the
        /// macro is registered through the schema SPI, over a table <c>CREATE TABLE</c> made — that being a
        /// <c>MutableArrayTable</c>, which is the <c>ModifiableTable</c> the view needs underneath. This is
        /// the one test here that reaches Calcite through <see cref="CalciteConnection.RootSchema"/> rather
        /// than through SQL, because that is the only way in.
        /// </remarks>
        [Fact]
        public void Insert_into_a_modifiable_view_should_write_through_to_its_table()
        {
            using var c = new CalciteConnection(RootDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"modsrc\" (\"id\" INTEGER NOT NULL, \"g\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            var viewPath = new java.util.ArrayList();
            viewPath.add("modview");
            c.RootSchema.add("modview", org.apache.calcite.schema.impl.ViewTable.viewMacro(
                c.RootSchema,
                "SELECT \"id\" FROM \"modsrc\" WHERE \"g\" = 7",
                new java.util.ArrayList(),
                viewPath,
                java.lang.Boolean.TRUE));

            cmd.CommandText = "INSERT INTO \"modview\" VALUES (1)";
            cmd.ExecuteNonQuery();

            // the view's constraint supplies "g", which the INSERT never mentioned
            cmd.CommandText = "SELECT \"id\", \"g\" FROM \"modsrc\"";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal(1, r.GetInt32(0));
            Assert.Equal(7, r.GetInt32(1));
            Assert.False(r.Read());
        }

        // ------------------------------------------------------------------------------------------
        // Materialized views
        // ------------------------------------------------------------------------------------------

        /// <summary>
        /// <c>CREATE MATERIALIZED VIEW</c> is refused, and this pins why.
        /// </summary>
        /// <remarks>
        /// A materialized view is a real table holding a snapshot, so it has rows to load, and
        /// <c>ServerDdlExecutor.populate</c> loads them by building an INSERT and calling
        /// <c>context.getRelRunner().prepareStatement(rel).executeUpdate()</c>.
        /// <c>RelRunner.prepareStatement</c> is declared to return a <c>java.sql.PreparedStatement</c>, and
        /// this provider implements no JDBC, so <c>PrepareContext.getRelRunner</c> refuses.
        ///
        /// <para>The planning half is not what is missing — <c>ClrPrepareImpl.PrepareRel</c> is the
        /// <c>prepare2_</c> branch Calcite's own runner uses. What is missing is a hundred-odd members of
        /// <c>PreparedStatement</c> that exist so that two of them can be called.</para>
        ///
        /// <para><b>The table is created first and the statement fails after</b>, which is
        /// <c>ServerDdlExecutor</c>'s ordering and not something this side can change; the assertion below
        /// records it rather than pretending otherwise. <c>CREATE TABLE ... AS SELECT</c> shares the path —
        /// see <c>CalciteDdlTests</c>.</para>
        /// </remarks>
        [Fact]
        public void Create_materialized_view_should_be_refused_with_a_reason()
        {
            using var c = new CalciteConnection(RootDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"mvsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"mvsrc\" VALUES (1), (2)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "CREATE MATERIALIZED VIEW \"mv\" AS SELECT \"id\" FROM \"mvsrc\"";
            var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
            Assert.Contains("CREATE MATERIALIZED VIEW", ex.ToString(), StringComparison.Ordinal);

            // upstream added the table before it tried to fill it, so the name now resolves and is empty
            cmd.CommandText = "SELECT COUNT(*) FROM \"mv\"";
            Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
        }

        /// <summary>
        /// In a sub-schema the same statement fails earlier, and for a different reason that is Calcite's
        /// rather than this project's.
        /// </summary>
        /// <remarks>
        /// <c>populate</c> plans its INSERT through a <c>FrameworkConfig</c> whose default schema is
        /// <c>context.getRootSchema().plus()</c> — the root, unconditionally — so neither the new table nor
        /// the source of the query resolves when either lives one level down, and it fails at validation
        /// before any runner is asked for. Upstream never sees it: <c>ServerTest.connect()</c> opens a
        /// connection with no model, so everything it creates is in the root schema.
        ///
        /// <para>Worth keeping separate from the test above, because the two failures would not be fixed by
        /// the same thing: a runner would not make this one work.</para>
        /// </remarks>
        [Fact]
        public void Materialized_view_in_a_sub_schema_fails_before_a_runner_is_asked_for()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"submvsrc\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO \"submvsrc\" VALUES (1)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "CREATE MATERIALIZED VIEW \"submv\" AS SELECT \"id\" FROM \"submvsrc\"";
            var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());

            Assert.Contains("not found", ex.ToString(), StringComparison.OrdinalIgnoreCase);
        }

    }

}
