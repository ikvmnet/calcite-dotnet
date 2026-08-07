using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Prepare;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The ported members of <c>CalcitePrepareImpl</c> and <c>Prepare</c>, against the behaviour Calcite
    /// specifies for them.
    /// </summary>
    /// <remarks>
    /// Every one of these is a private static there, so it is written again here rather than called, and a
    /// row-level comparison cannot see most of them: a type name, a precision or an origin is reported to a
    /// caller and never touches a value. Where Calcite's own member is reachable it is the oracle; where it
    /// is not, the assertion states the behaviour its source specifies and names it.
    /// </remarks>
    [TestClass]
    public class ClrPrepareBehaviourTests
    {

        static readonly JavaTypeFactoryImpl TypeFactory = new();

        /// <summary>
        /// Prepares a statement and returns the one column it produces.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static ColumnMetaData Column(string sql, int ordinal = 0)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().Prepare(context, sql, (java.lang.Class)typeof(object[]), -1);
                return (ColumnMetaData)signature.Columns.get(ordinal);
            });
        }

        /// <summary>
        /// <c>getTypeName</c> rewrites seven interval names and renders the collection types by
        /// <c>toString</c>. Everything else is the SQL type's own name.
        /// </summary>
        [TestMethod]
        [DataRow("SELECT INTERVAL '1' YEAR FROM SALES", "INTERVAL_YEAR")]
        [DataRow("SELECT INTERVAL '1-2' YEAR TO MONTH FROM SALES", "INTERVAL_YEAR_TO_MONTH")]
        [DataRow("SELECT INTERVAL '1 2' DAY TO HOUR FROM SALES", "INTERVAL_DAY_TO_HOUR")]
        [DataRow("SELECT INTERVAL '1 2:3' DAY TO MINUTE FROM SALES", "INTERVAL_DAY_TO_MINUTE")]
        [DataRow("SELECT INTERVAL '1 2:3:4' DAY TO SECOND FROM SALES", "INTERVAL_DAY_TO_SECOND")]
        [DataRow("SELECT INTERVAL '2:3' HOUR TO MINUTE FROM SALES", "INTERVAL_HOUR_TO_MINUTE")]
        [DataRow("SELECT INTERVAL '2:3:4' HOUR TO SECOND FROM SALES", "INTERVAL_HOUR_TO_SECOND")]
        [DataRow("SELECT INTERVAL '3:4' MINUTE TO SECOND FROM SALES", "INTERVAL_MINUTE_TO_SECOND")]
        [DataRow("SELECT ID FROM SALES", "INTEGER")]
        [DataRow("SELECT REGION FROM SALES", "VARCHAR")]
        [DataRow("SELECT CAST(ID AS DECIMAL(9, 2)) FROM SALES", "DECIMAL")]
        public void Type_name_should_be_what_calcite_reports(string sql, string expected)
        {
            Assert.AreEqual(expected, Column(sql).type.name, sql);
        }

        /// <summary>
        /// A precision or scale Calcite left unspecified is reported as zero rather than as the sentinel.
        /// </summary>
        /// <remarks>
        /// <c>getPrecision</c> and <c>getScale</c> test against <c>RelDataType.PRECISION_NOT_SPECIFIED</c>
        /// and <c>SCALE_NOT_SPECIFIED</c>, which are -1. Reporting the sentinel would put -1 into
        /// <c>ColumnMetaData</c> and out through the reader's schema table.
        /// </remarks>
        [TestMethod]
        public void Unspecified_precision_and_scale_should_be_zero()
        {
            Assert.IsTrue(RelDataType.PRECISION_NOT_SPECIFIED < 0, "the sentinel this guards against is negative");
            Assert.IsTrue(RelDataType.SCALE_NOT_SPECIFIED < 0, "the sentinel this guards against is negative");

            var column = Column("SELECT ID FROM SALES");

            Assert.IsTrue(column.precision >= 0, $"precision was {column.precision}");
            Assert.IsTrue(column.scale >= 0, $"scale was {column.scale}");
        }

        /// <summary>
        /// A column's origins are read from the end of the list: column, then table, then schema.
        /// </summary>
        /// <remarks>
        /// <c>origin(origins, offsetFromEnd)</c> indexes <c>size() - 1 - offsetFromEnd</c>, and
        /// <c>metaData</c> passes 0, 2 and 1 for the column, catalog and schema in that order. Getting the
        /// direction wrong reports the catalog as the column name, which no row comparison would show.
        /// </remarks>
        [TestMethod]
        public void Origins_should_be_read_from_the_end()
        {
            var column = Column("SELECT ID FROM SALES");

            Assert.AreEqual("ID", column.columnName);
            Assert.AreEqual("SALES", column.tableName);
            Assert.AreEqual("ID", column.label);
        }

        /// <summary>
        /// An expression has no origin, so its catalog, schema and table are null while its label stands.
        /// </summary>
        [TestMethod]
        public void An_expression_should_have_no_origin()
        {
            var column = Column("SELECT ID + 1 FROM SALES");

            Assert.IsNull(column.tableName);
            Assert.IsNull(column.schemaName);
            Assert.IsNull(column.catalogName);
        }

        /// <summary>
        /// A column reports the class its Avatica type names, not <c>Object</c>.
        /// </summary>
        /// <remarks>
        /// Worth pinning because there are two class names in <c>prepare2_</c> and they differ.
        /// <c>metaData</c> passes <c>avaticaType.columnClassName()</c> for a column, which follows the type;
        /// <c>getClassName</c> — the one that returns <c>Object</c> unconditionally for CALCITE-2613 — is
        /// used for an <c>AvaticaParameter</c> and nowhere else. Reading the wrong one for a column would
        /// report every column as an Object and nothing about the rows would change.
        /// </remarks>
        [TestMethod]
        [DataRow("SELECT ID FROM SALES", "java.lang.Integer")]
        [DataRow("SELECT REGION FROM SALES", "java.lang.String")]
        [DataRow("SELECT CAST(ID AS DECIMAL(9, 2)) FROM SALES", "java.math.BigDecimal")]
        public void Column_class_name_should_follow_the_type(string sql, string expected)
        {
            Assert.AreEqual(expected, Column(sql).columnClassName, sql);
        }

        /// <summary>
        /// A statement is DML for exactly four kinds and SELECT for everything else.
        /// </summary>
        /// <remarks>
        /// <c>getStatementType(SqlKind)</c>. EXPLAIN is not among them, which is why an EXPLAIN reports
        /// SELECT while taking the DML branch for its row type.
        /// </remarks>
        [TestMethod]
        [DataRow("SELECT ID FROM SALES", "SELECT")]
        [DataRow("EXPLAIN PLAN FOR SELECT ID FROM SALES", "SELECT")]
        public void Statement_type_should_be_what_calcite_reports(string sql, string expected)
        {
            var actual = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepareImpl().Prepare(context, sql, (java.lang.Class)typeof(object[]), -1).StatementType.name());

            Assert.AreEqual(expected, actual, sql);
        }

        /// <summary>
        /// An EXPLAIN of a plan renders the plan; an EXPLAIN of a type renders the type.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.prepareSql</c> has two exits for an EXPLAIN and they are not the same: depth TYPE
        /// leaves before flattening and renders <c>RelOptUtil.dumpType</c>, and the default depth renders
        /// the plan after optimization. A port that took only one exit would answer both the same.
        /// </remarks>
        [TestMethod]
        public void Explain_of_a_type_should_render_the_type()
        {
            var rows = ClrPrepareDifferentialTests.RunClr("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH TYPE FOR SELECT ID FROM SALES");

            Assert.AreEqual(1, rows.Count);
            StringAssert.Contains(rows[0], "INTEGER");
            Assert.IsFalse(rows[0].Contains("ClrEnumerable"), $"a type, not a plan: {rows[0]}");
        }

        /// <summary>
        /// An EXPLAIN of the physical plan renders nodes of this convention.
        /// </summary>
        [TestMethod]
        public void Explain_of_a_plan_should_render_the_plan()
        {
            var rows = ClrPrepareDifferentialTests.RunClr("EXPLAIN PLAN FOR SELECT ID FROM SALES");

            Assert.AreEqual(1, rows.Count);
            StringAssert.Contains(rows[0], "ClrEnumerable");
        }

        /// <summary>
        /// An EXPLAIN reports one column of four null origins and no collations.
        /// </summary>
        /// <remarks>
        /// <c>PreparedExplain.getFieldOrigins</c> returns <c>singletonList(nCopies(4, null))</c>, and it
        /// implements <c>PreparedResult</c> directly rather than extending <c>PreparedResultImpl</c> — which
        /// is why Calcite's own driver reports no collations for one, and why ours does too.
        /// </remarks>
        [TestMethod]
        public void Explain_should_report_one_column_and_no_collations()
        {
            var signature = ClrPrepareFixture.WithContext("EXPLAIN PLAN FOR SELECT ID FROM SALES", (context, _) =>
                new ClrPrepareImpl().Prepare(context, "EXPLAIN PLAN FOR SELECT ID FROM SALES", (java.lang.Class)typeof(object[]), -1));

            Assert.AreEqual(1, signature.Columns.size());
            Assert.AreEqual(0, signature.Collations.size());
            Assert.AreEqual(0, signature.Parameters.size());
        }

        /// <summary>
        /// A negative limit means no limit and zero means zero rows.
        /// </summary>
        /// <remarks>
        /// <c>CalciteSignature.enumerable</c> says it outright: "In JDBC 0 means no limit. But for us, -1
        /// means no limit, and 0 is a valid limit." Taking JDBC's reading would return every row where the
        /// caller asked for none.
        /// </remarks>
        [TestMethod]
        public void Zero_max_row_count_should_mean_zero_rows()
        {
            Assert.AreEqual(0, ClrPrepareDifferentialTests.RunClr("SELECT ID FROM SALES", 0).Count);
            Assert.AreEqual(6, ClrPrepareDifferentialTests.RunClr("SELECT ID FROM SALES", -1).Count);
        }

        /// <summary>
        /// A row of one column is the value, not a one-element array.
        /// </summary>
        /// <remarks>
        /// The cursor factory Calcite deduces decides this, and it is the shape that has broken twice. Read
        /// against Calcite's own deduction rather than asserted by hand.
        /// </remarks>
        [TestMethod]
        [DataRow("SELECT ID FROM SALES")]
        [DataRow("SELECT * FROM SALES")]
        [DataRow("SELECT COUNT(*) FROM SALES")]
        public void Cursor_factory_should_agree_with_calcite(string sql)
        {
            var clr = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepareImpl().Prepare(context, sql, (java.lang.Class)typeof(object[]), -1).CursorFactory.style.name());

            var calcite = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                return prepare.prepareSql(context, CalcitePrepare.Query.of(sql), (java.lang.Class)typeof(object[]), -1).cursorFactory.style.name();
            });

            Assert.AreEqual(calcite, clr, sql);
        }

        /// <summary>
        /// The parameter list is built from the validated parameter row type, one entry per marker.
        /// </summary>
        /// <remarks>
        /// <c>prepare2_</c> builds an <c>AvaticaParameter</c> per field of
        /// <c>preparedResult.getParameterRowType()</c>, with the same precision, scale, ordinal and type
        /// name helpers the columns use.
        /// </remarks>
        [TestMethod]
        public void Parameters_should_be_described_per_marker()
        {
            var signature = ClrPrepareFixture.WithContext("", (context, _) =>
                new ClrPrepareImpl().Prepare(context, "SELECT ID FROM SALES WHERE ID > ? AND REGION = ?", (java.lang.Class)typeof(object[]), -1));

            Assert.AreEqual(2, signature.Parameters.size());

            var first = (AvaticaParameter)signature.Parameters.get(0);
            Assert.AreEqual("java.lang.Object", first.className);
            Assert.IsFalse(string.IsNullOrEmpty(first.typeName));
        }

        /// <summary>
        /// The internal parameters carry the conformance the statement was planned under.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePreparingStmt.implement</c> puts <c>_conformance</c> into its own map, and Calcite's
        /// driver hands that map to the <c>DataContext</c>. Ours is the map the plan was built against
        /// rather than the private one a subclass never writes to.
        /// </remarks>
        [TestMethod]
        public void Internal_parameters_should_carry_the_conformance()
        {
            var signature = ClrPrepareFixture.WithContext("", (context, _) =>
                new ClrPrepareImpl().Prepare(context, "SELECT ID FROM SALES", (java.lang.Class)typeof(object[]), -1));

            Assert.IsTrue(signature.InternalParameters.containsKey("_conformance"),
                "the map the plan was built against reaches the caller");
        }

    }

}
