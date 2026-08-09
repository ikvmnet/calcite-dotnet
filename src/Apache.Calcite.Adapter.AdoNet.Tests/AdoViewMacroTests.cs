using System;

using Apache.Calcite.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Registering a view over the ADO.NET adapter through <c>ViewTable.viewMacro</c>, and using it as
    /// a table.
    /// </summary>
    /// <remarks>
    /// This is the worked example of the programmatic route: no model, no <c>CREATE VIEW</c>, no server
    /// parser. A macro is registered on a schema and from then on the name is a table — selectable,
    /// joinable, and able to span the adapter and anything else in the root schema.
    /// </remarks>
    [TestClass]
    public class AdoViewMacroTests
    {

        static AdoViewMacroTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
        }

        SqliteFixture _sqlite = null!;
        CalciteConnection _connection = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();

            _connection = new CalciteConnection(new CalciteConnectionStringBuilder());
            _connection.Open();

            // the adapter, as one schema of the connection's root schema
            _connection.RootSchema.add("ADO", AdoSchema.Create(_connection.RootSchema, "ADO", _sqlite.DataSource, null, null));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _connection?.Dispose();
            _sqlite?.Dispose();
        }

        /// <summary>
        /// A view over two tables of the adapter, joined and filtered, then queried as a table.
        /// </summary>
        [TestMethod]
        public void View_over_the_adapter_should_behave_like_a_table()
        {
            _connection.RootSchema.add("STAFF", ViewTable.viewMacro(
                _connection.RootSchema,
                """
                SELECT E.EMPNO AS EMPNO, E.NAME AS NAME, D.DNAME AS DNAME
                FROM ADO.EMPS AS E
                JOIN ADO.DEPTS AS D ON E.DEPTNO = D.DEPTNO
                """,
                null,
                org.apache.calcite.jdbc.CalciteSchema.from(_connection.RootSchema).path("STAFF"),
                null));

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT NAME, DNAME FROM STAFF WHERE DNAME = 'Sales' ORDER BY NAME";

            using var r = cmd.ExecuteReader();

            Assert.IsTrue(r.Read());
            Assert.AreEqual("Alice", r.GetString(0));
            Assert.AreEqual("Sales", r.GetString(1));
            Assert.IsTrue(r.Read());
            Assert.AreEqual("Bob", r.GetString(0));
            Assert.AreEqual("Sales", r.GetString(1));
            Assert.IsFalse(r.Read());
        }

        /// <summary>
        /// The same view joined to a table that is not in the adapter at all, and aggregated.
        /// </summary>
        /// <remarks>
        /// The point of the exercise: one plan reaching a SQLite database through the adapter and an
        /// in-process <see cref="ScannableTable"/> beside it, with the view as the join's left input. The
        /// view is not a barrier — the planner sees the expanded subtree, pushes what it can into the
        /// adapter, and carries the rest across a converter.
        /// </remarks>
        [TestMethod]
        public void View_should_bridge_the_adapter_and_a_local_schema()
        {
            _connection.RootSchema.add("EXTRA", new GradeSchema());

            _connection.RootSchema.add("STAFF", ViewTable.viewMacro(
                _connection.RootSchema,
                """
                SELECT E.EMPNO AS EMPNO, E.NAME AS NAME, D.DNAME AS DNAME
                FROM ADO.EMPS AS E
                JOIN ADO.DEPTS AS D ON E.DEPTNO = D.DEPTNO
                """,
                null,
                org.apache.calcite.jdbc.CalciteSchema.from(_connection.RootSchema).path("STAFF"),
                null));

            using var cmd = _connection.CreateCommand();
            cmd.CommandText =
                """
                SELECT S.DNAME, COUNT(*) AS N
                FROM STAFF AS S
                JOIN EXTRA.GRADES AS G ON S.EMPNO = G.EMPNO
                WHERE G.GRADE = 'A'
                GROUP BY S.DNAME
                ORDER BY S.DNAME
                """;

            using var r = cmd.ExecuteReader();

            Assert.IsTrue(r.Read());
            Assert.AreEqual("Engineering", r.GetString(0));
            Assert.AreEqual(1L, r.GetInt64(1));
            Assert.IsTrue(r.Read());
            Assert.AreEqual("Sales", r.GetString(0));
            Assert.AreEqual(1L, r.GetInt64(1));
            Assert.IsFalse(r.Read());
        }

        /// <summary>
        /// A schema of one in-process table, standing for anything that is not the adapter.
        /// </summary>
        sealed class GradeSchema : AbstractSchema
        {

            readonly java.util.Map _tables = new java.util.HashMap();

            public GradeSchema()
            {
                _tables.put("GRADES", new GradesTable());
            }

            protected override java.util.Map getTableMap() => _tables;

            sealed class GradesTable : AbstractTable, ScannableTable
            {

                public override RelDataType getRowType(RelDataTypeFactory typeFactory)
                {
                    return typeFactory.builder()
                        .add("EMPNO", typeFactory.createSqlType(SqlTypeName.INTEGER))
                        .add("GRADE", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                        .build();
                }

                public Enumerable scan(DataContext root)
                {
                    var list = new java.util.ArrayList();
                    list.add(new object[] { java.lang.Integer.valueOf(1), "A" });
                    list.add(new object[] { java.lang.Integer.valueOf(2), "B" });
                    list.add(new object[] { java.lang.Integer.valueOf(3), "A" });

                    return Linq4j.asEnumerable(list);
                }

            }

        }

    }

}
