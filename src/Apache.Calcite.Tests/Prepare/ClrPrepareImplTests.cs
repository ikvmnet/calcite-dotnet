using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Extensions.Prepare.Tests
{

    /// <summary>
    /// Tests of the members of <c>CalcitePrepareImpl</c> and <c>Prepare</c> that <see cref="ClrPrepareImpl"/>
    /// ports, against the behaviour Calcite's source gives them.
    /// </summary>
    /// <remarks>
    /// Most of these members are private in Calcite, and most of what they produce (a type name, a precision,
    /// an origin) is reported to a caller without affecting any row, so a row comparison cannot check them.
    /// Where Calcite's member is reachable it is the oracle; otherwise the test asserts what its source does
    /// and names the member.
    /// </remarks>
    public class ClrPrepareImplTests
    {

        static readonly JavaTypeFactoryImpl TypeFactory = new();

        /// <summary>
        /// Prepares a statement and returns the column at <paramref name="ordinal"/>.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="ordinal">The zero-based position of the column.</param>
        /// <returns>The column's metadata as <c>ClrPrepareImpl</c> reports it.</returns>
        static ColumnMetaData Column(string sql, int ordinal = 0)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);
                return (ColumnMetaData)signature.Columns.get(ordinal);
            });
        }

        /// <summary>
        /// A one-column result is the value and a wider one is an array, whatever element type the caller
        /// asks for.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="expected">The name of the cursor factory's style.</param>
        /// <remarks>
        /// <c>Meta.CursorFactory.deduce</c> answers <c>OBJECT</c> for a single column before it looks at the
        /// class, so preparing with <c>Object[]</c> does not make a one-column row an array. An <c>EXPLAIN</c>
        /// has one column, as does DML's <c>ROWCOUNT</c>.
        /// </remarks>
        [Theory]
        [InlineData("SELECT ID FROM SALES", "OBJECT")]
        [InlineData("SELECT ID, REGION FROM SALES", "ARRAY")]
        [InlineData("EXPLAIN PLAN FOR SELECT ID FROM SALES", "OBJECT")]
        public void Cursor_style_should_follow_the_column_count(string sql, string expected)
        {
            var style = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1).CursorFactory.style.name());

            style.Should().Be(expected, sql);
        }

        /// <summary>
        /// <c>getTypeName</c> rewrites seven interval names and renders the collection and row types by
        /// <c>toString</c>; every other type reports its SQL type name.
        /// </summary>
        /// <param name="sql">A statement of one column of the type under test.</param>
        /// <param name="expected">The type name the column should report.</param>
        [Theory]
        [InlineData("SELECT INTERVAL '1' YEAR FROM SALES", "INTERVAL_YEAR")]
        [InlineData("SELECT INTERVAL '1-2' YEAR TO MONTH FROM SALES", "INTERVAL_YEAR_TO_MONTH")]
        [InlineData("SELECT INTERVAL '1 2' DAY TO HOUR FROM SALES", "INTERVAL_DAY_TO_HOUR")]
        [InlineData("SELECT INTERVAL '1 2:3' DAY TO MINUTE FROM SALES", "INTERVAL_DAY_TO_MINUTE")]
        [InlineData("SELECT INTERVAL '1 2:3:4' DAY TO SECOND FROM SALES", "INTERVAL_DAY_TO_SECOND")]
        [InlineData("SELECT INTERVAL '2:3' HOUR TO MINUTE FROM SALES", "INTERVAL_HOUR_TO_MINUTE")]
        [InlineData("SELECT INTERVAL '2:3:4' HOUR TO SECOND FROM SALES", "INTERVAL_HOUR_TO_SECOND")]
        [InlineData("SELECT INTERVAL '3:4' MINUTE TO SECOND FROM SALES", "INTERVAL_MINUTE_TO_SECOND")]
        [InlineData("SELECT ID FROM SALES", "INTEGER")]
        [InlineData("SELECT REGION FROM SALES", "VARCHAR")]
        [InlineData("SELECT CAST(ID AS DECIMAL(9, 2)) FROM SALES", "DECIMAL")]
        public void Type_name_should_be_what_calcite_reports(string sql, string expected)
        {
            Column(sql).type.name.Should().Be(expected, sql);
        }

        /// <summary>
        /// A precision or scale Calcite left unspecified is reported as zero rather than as the sentinel.
        /// </summary>
        /// <remarks>
        /// <c>getPrecision</c> and <c>getScale</c> test against <c>RelDataType.PRECISION_NOT_SPECIFIED</c>
        /// and <c>SCALE_NOT_SPECIFIED</c>, which are -1, so the sentinel never reaches <c>ColumnMetaData</c>
        /// or the reader's schema table.
        /// </remarks>
        [Fact]
        public void Unspecified_precision_and_scale_should_be_zero()
        {
            Assert.True(RelDataType.PRECISION_NOT_SPECIFIED < 0, "the sentinel this guards against is negative");
            Assert.True(RelDataType.SCALE_NOT_SPECIFIED < 0, "the sentinel this guards against is negative");

            var column = Column("SELECT ID FROM SALES");

            Assert.True(column.precision >= 0, $"precision was {column.precision}");
            Assert.True(column.scale >= 0, $"scale was {column.scale}");
        }

        /// <summary>
        /// A column's origins are read from the end of the list: column, then table, then schema.
        /// </summary>
        /// <remarks>
        /// <c>origin(origins, offsetFromEnd)</c> indexes <c>size() - 1 - offsetFromEnd</c>, and
        /// <c>metaData</c> passes 0, 2 and 1 for the column, catalog and schema in that order. Reading from the
        /// wrong end reports the catalog as the column name.
        /// </remarks>
        [Fact]
        public void Origins_should_be_read_from_the_end()
        {
            var column = Column("SELECT ID FROM SALES");

            Assert.Equal("ID", column.columnName);
            Assert.Equal("SALES", column.tableName);
            Assert.Equal("ID", column.label);
        }

        /// <summary>
        /// An expression has no origin, so its catalog, schema and table are null while its label stands.
        /// </summary>
        [Fact]
        public void An_expression_should_have_no_origin()
        {
            var column = Column("SELECT ID + 1 FROM SALES");

            Assert.Null(column.tableName);
            Assert.Null(column.schemaName);
            Assert.Null(column.catalogName);
        }

        /// <summary>
        /// A column reports the class its Avatica type names, not <c>Object</c>.
        /// </summary>
        /// <param name="sql">A statement of one column.</param>
        /// <param name="expected">The Java class name the column should report.</param>
        /// <remarks>
        /// <c>prepare2_</c> has two sources of a class name. <c>metaData</c> passes
        /// <c>avaticaType.columnClassName()</c> for a column, which follows the type; <c>getClassName</c>, which
        /// always returns <c>Object</c>, is used only for an <c>AvaticaParameter</c>.
        /// </remarks>
        [Theory]
        [InlineData("SELECT ID FROM SALES", "java.lang.Integer")]
        [InlineData("SELECT REGION FROM SALES", "java.lang.String")]
        [InlineData("SELECT CAST(ID AS DECIMAL(9, 2)) FROM SALES", "java.math.BigDecimal")]
        public void Column_class_name_should_follow_the_type(string sql, string expected)
        {
            Column(sql).columnClassName.Should().Be(expected, sql);
        }

        /// <summary>
        /// A statement is DML for exactly four kinds and SELECT for everything else.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="expected">The name of the statement type it should report.</param>
        /// <remarks>
        /// Mirrors <c>getStatementType(SqlKind)</c>. <c>EXPLAIN</c> is not among the four, so an
        /// <c>EXPLAIN</c> reports <c>SELECT</c> while taking the DML branch for its row type.
        /// </remarks>
        [Theory]
        [InlineData("SELECT ID FROM SALES", "SELECT")]
        [InlineData("EXPLAIN PLAN FOR SELECT ID FROM SALES", "SELECT")]
        public void Statement_type_should_be_what_calcite_reports(string sql, string expected)
        {
            var actual = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1).StatementType.name());

            actual.Should().Be(expected, sql);
        }

        /// <summary>
        /// An EXPLAIN of a plan renders the plan; an EXPLAIN of a type renders the type.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.prepareSql</c> has two exits for an <c>EXPLAIN</c>: depth <c>TYPE</c> returns before
        /// flattening and renders <c>RelOptUtil.dumpType</c>, and the default depth renders the plan after
        /// optimization.
        /// </remarks>
        [Fact]
        public void Explain_of_a_type_should_render_the_type()
        {
            var rows = ClrPrepareImplDifferentialTests.RunClr("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH TYPE FOR SELECT ID FROM SALES");

            Assert.Single(rows);
            Assert.Contains("INTEGER", rows[0]);
            Assert.False(rows[0].Contains("ClrEnumerable"), $"a type, not a plan: {rows[0]}");
        }

        /// <summary>
        /// An EXPLAIN of the physical plan renders nodes of this convention.
        /// </summary>
        [Fact]
        public void Explain_of_a_plan_should_render_the_plan()
        {
            var rows = ClrPrepareImplDifferentialTests.RunClr("EXPLAIN PLAN FOR SELECT ID FROM SALES");

            Assert.Single(rows);
            Assert.Contains("ClrCursor", rows[0]);
        }

        /// <summary>
        /// An EXPLAIN reports one column of four null origins and no collations.
        /// </summary>
        /// <remarks>
        /// <c>PreparedExplain.getFieldOrigins</c> returns <c>singletonList(nCopies(4, null))</c>, and
        /// <c>PreparedExplain</c> implements <c>PreparedResult</c> directly rather than extending
        /// <c>PreparedResultImpl</c>, so Calcite reports no collations for it.
        /// </remarks>
        [Fact]
        public void Explain_should_report_one_column_and_no_collations()
        {
            var signature = ClrPrepareFixture.WithContext("EXPLAIN PLAN FOR SELECT ID FROM SALES", (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of("EXPLAIN PLAN FOR SELECT ID FROM SALES"), typeof(object[]), -1));

            Assert.Equal(1, signature.Columns.size());
            Assert.Equal(0, signature.Collations.size());
            Assert.Equal(0, signature.Parameters.size());
        }

        /// <summary>
        /// A negative limit means no limit and zero means zero rows.
        /// </summary>
        /// <remarks>
        /// This follows <c>CalciteSignature.enumerable</c>, where -1 means no limit and 0 is a valid limit,
        /// unlike JDBC's reading of 0 as no limit.
        /// </remarks>
        [Fact]
        public void Zero_max_row_count_should_mean_zero_rows()
        {
            Assert.Empty(ClrPrepareImplDifferentialTests.RunClr("SELECT ID FROM SALES", 0));
            Assert.Equal(6, ClrPrepareImplDifferentialTests.RunClr("SELECT ID FROM SALES", -1).Count);
        }

        /// <summary>
        /// The cursor factory, which decides whether a one-column row is the value or an array, matches the
        /// one Calcite deduces.
        /// </summary>
        /// <param name="sql">The statement.</param>
        [Theory]
        [InlineData("SELECT ID FROM SALES")]
        [InlineData("SELECT * FROM SALES")]
        [InlineData("SELECT COUNT(*) FROM SALES")]
        public void Cursor_factory_should_agree_with_calcite(string sql)
        {
            var clr = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1).CursorFactory.style.name());

            var calcite = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                return prepare.prepareSql(context, CalcitePrepare.Query.of(sql), (java.lang.Class)typeof(object[]), -1).cursorFactory.style.name();
            });

            clr.Should().Be(calcite, sql);
        }

        /// <summary>
        /// The parameter list is built from the validated parameter row type, one entry per marker.
        /// </summary>
        /// <remarks>
        /// <c>prepare2_</c> builds an <c>AvaticaParameter</c> per field of
        /// <c>preparedResult.getParameterRowType()</c>, with the same precision, scale, ordinal and type
        /// name helpers the columns use.
        /// </remarks>
        [Fact]
        public void Parameters_should_be_described_per_marker()
        {
            var signature = ClrPrepareFixture.WithContext("", (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of("SELECT ID FROM SALES WHERE ID > ? AND REGION = ?"), typeof(object[]), -1));

            Assert.Equal(2, signature.Parameters.size());

            var first = (AvaticaParameter)signature.Parameters.get(0);
            Assert.Equal("java.lang.Object", first.className);
            Assert.False(string.IsNullOrEmpty(first.typeName));
        }

        /// <summary>
        /// The internal parameters carry the conformance the statement was planned under.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePreparingStmt.implement</c> puts <c>_conformance</c> into its internal parameters, and
        /// Calcite's driver hands that map to the <c>DataContext</c>. The signature exposes the map the plan was
        /// built against.
        /// </remarks>
        [Fact]
        public void Internal_parameters_should_carry_the_conformance()
        {
            var signature = ClrPrepareFixture.WithContext("", (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of("SELECT ID FROM SALES"), typeof(object[]), -1));

            Assert.True(signature.InternalParameters.containsKey("_conformance"),
                "the map the plan was built against reaches the caller");
        }

        /// <summary>
        /// The six statements <c>SIMPLE_SQLS</c> names skip planning and answer one row of one column
        /// called <c>EXPR$0</c>.
        /// </summary>
        /// <param name="sql">One of the six statements, spelled exactly as <c>SIMPLE_SQLS</c> holds it.</param>
        /// <remarks>
        /// <c>prepare_</c> tests <c>SIMPLE_SQLS.contains(query.sql)</c> before it builds a catalog reader,
        /// and <c>simplePrepare</c> answers a signature over <c>ImmutableList.of(1)</c>. The column is
        /// <c>SqlUtil.deriveAliasFromOrdinal(0)</c>, and the row is a <c>java.lang.Integer</c> because a
        /// one-column row is the value itself.
        /// </remarks>
        [Theory]
        [InlineData("SELECT 1")]
        [InlineData("select 1")]
        [InlineData("SELECT 1 FROM DUAL")]
        [InlineData("select 1 from dual")]
        [InlineData("values 1")]
        [InlineData("VALUES 1")]
        public void A_simple_statement_should_skip_planning(string sql)
        {
            var (columns, rows) = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);

                return (signature.Columns, new List<object>(signature.Bind(context.getDataContext())));
            });

            Assert.Equal(1, columns.size());
            Assert.Equal("EXPR$0", ((ColumnMetaData)columns.get(0)).columnName);
            Assert.Equal(new object[] { java.lang.Integer.valueOf(1) }, rows);
        }

        /// <summary>
        /// A statement <c>SIMPLE_SQLS</c> does not name is planned, and gives the same answer as the fast path:
        /// <c>SELECT 1</c> is named and <c>SELECT  1</c>, with two spaces, is not.
        /// </summary>
        [Fact]
        public void A_statement_the_fast_path_misses_should_answer_the_same()
        {
            var rows = ClrPrepareFixture.WithContext("SELECT  1", (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of("SELECT  1"), typeof(object[]), -1);

                return new List<object>(signature.Bind(context.getDataContext()));
            });

            Assert.Equal(new object[] { java.lang.Integer.valueOf(1) }, rows);
        }

        /// <summary>
        /// <c>AGGREGATE</c> over a measure is expanded by the <c>measure</c> pass of <c>Programs.standard</c>.
        /// </summary>
        /// <remarks>
        /// <c>Programs.measure</c> runs <c>MeasureRules</c> when <c>containsAggM2v</c> finds an <c>AGG_M2V</c>
        /// aggregate call, which is what <c>AGGREGATE(m)</c> converts to; without it the planner cannot
        /// implement the call. The statement follows Calcite's <c>measure.iq</c>, where <c>GROUP BY ()</c> is
        /// implicit under <c>AGGREGATE</c>.
        /// </remarks>
        [Fact]
        public void An_aggregate_over_a_measure_should_be_expanded()
        {
            const string sql = "SELECT AGGREGATE(m) AS a FROM (SELECT REGION, AVG(AMOUNT) AS MEASURE m FROM SALES)";

            var rows = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);

                return new List<object>(signature.Bind(context.getDataContext()));
            },
            p => p.setProperty("fun", "calcite"));

            // 10, 20, 20, 30, null, 10 -- AVG over the five that are not null, in INTEGER arithmetic
            Assert.Equal(new object[] { java.lang.Integer.valueOf(18) }, rows);
        }

        /// <summary>
        /// A correlated sub-query is decorrelated when <c>topDownGeneralDecorrelationEnabled</c> is set.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.prepareSql</c> decorrelates only when <c>forceDecorrelate</c> is set and this flag is
        /// not. With the flag set, decorrelation is left to <c>DecorrelateProgram</c>, which
        /// <c>Programs.standard</c> runs and which dispatches to <c>TopDownGeneralDecorrelator</c>; if either
        /// half is missing the plan keeps its correlation.
        /// </remarks>
        [Fact]
        public void A_correlated_sub_query_should_decorrelate_top_down()
        {
            const string sql = "SELECT ID FROM SALES s WHERE AMOUNT > (SELECT AVG(AMOUNT) FROM SALES t WHERE t.REGION = s.REGION)";

            var plan = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of("EXPLAIN PLAN FOR " + sql), typeof(object[]), -1);

                return (string)new List<object>(signature.Bind(context.getDataContext()))[0];
            },
            p => p.setProperty(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED.camelName(), "true"));

            plan.Should().NotMatchRegex("Correlate", plan);
        }

    }

}
