using System;

using Apache.Calcite.Data;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests a view over the ADO.NET adapter registered programmatically through <c>ViewTable.viewMacro</c>,
    /// without a model or <c>CREATE VIEW</c>, and queried as a table.
    /// </summary>
    public class AdoViewMacroTests : IDisposable
    {

        static AdoViewMacroTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
        }

        SqliteFixture _sqlite = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public AdoViewMacroTests()
        {
            _sqlite = new SqliteFixture();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Opens a connection whose root schema holds the adapter as <c>ADO</c>, then runs
        /// <paramref name="configure"/> over the root.
        /// </summary>
        CalciteConnection Open(Action<SchemaPlus> configure)
        {
            return new CalciteDataSourceBuilder()
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null)))
                .ConfigureRootSchema(configure)
                .Build()
                .OpenConnection();
        }

        /// <summary>
        /// A view over two tables of the adapter, joined and filtered, then queried as a table.
        /// </summary>
        [Fact]
        public void View_over_the_adapter_should_behave_like_a_table()
        {
            using var connection = Open(root => root.add("STAFF", ViewTable.viewMacro(
                root,
                """
                SELECT E.EMPNO AS EMPNO, E.NAME AS NAME, D.DNAME AS DNAME
                FROM ADO.EMPS AS E
                JOIN ADO.DEPTS AS D ON E.DEPTNO = D.DEPTNO
                """,
                null,
                org.apache.calcite.jdbc.CalciteSchema.from(root).path("STAFF"),
                null)));

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT NAME, DNAME FROM STAFF WHERE DNAME = 'Sales' ORDER BY NAME";

            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.Equal("Alice", r.GetString(0));
            Assert.Equal("Sales", r.GetString(1));
            Assert.True(r.Read());
            Assert.Equal("Bob", r.GetString(0));
            Assert.Equal("Sales", r.GetString(1));
            Assert.False(r.Read());
        }

        /// <summary>
        /// The same view joined to a table outside the adapter, and aggregated.
        /// </summary>
        /// <remarks>
        /// One plan reads SQLite through the adapter and an in-process <see cref="ScannableTable"/> beside it,
        /// with the view as the join's left input. The view is expanded into the plan rather than treated as
        /// an opaque table.
        /// </remarks>
        [Fact]
        public void View_should_bridge_the_adapter_and_a_local_schema()
        {
            using var connection = Open(root =>
            {
                root.add("EXTRA", new GradeSchema());

                root.add("STAFF", ViewTable.viewMacro(
                    root,
                    """
                    SELECT E.EMPNO AS EMPNO, E.NAME AS NAME, D.DNAME AS DNAME
                    FROM ADO.EMPS AS E
                    JOIN ADO.DEPTS AS D ON E.DEPTNO = D.DEPTNO
                    """,
                    null,
                    org.apache.calcite.jdbc.CalciteSchema.from(root).path("STAFF"),
                    null));
            });

            using var cmd = connection.CreateCommand();
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

            Assert.True(r.Read());
            Assert.Equal("Engineering", r.GetString(0));
            Assert.Equal(1L, r.GetInt64(1));
            Assert.True(r.Read());
            Assert.Equal("Sales", r.GetString(0));
            Assert.Equal(1L, r.GetInt64(1));
            Assert.False(r.Read());
        }

        /// <summary>
        /// A schema holding one in-process table, <c>GRADES</c>, outside the adapter.
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
