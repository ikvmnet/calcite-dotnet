using Apache.Calcite.Geography.Rel.Type;
using Apache.Calcite.Geography.Sql;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.tools;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// A table <c>GEO</c> with an <c>ID</c> column and two geometry columns, <c>GEOG</c> and <c>GEOM</c>, and
    /// the operator table that resolves Calcite's spatial functions and this package's alongside them.
    /// </summary>
    /// <remarks>
    /// Both columns have the type <see cref="GeographyTypes.Of"/> returns; the names only say how a query is
    /// meant to read them.
    /// </remarks>
    static class GeographyFixture
    {

        /// <summary>
        /// Returns the standard operator table chained with Calcite's spatial functions and
        /// <see cref="GeographyOperatorTable"/>.
        /// </summary>
        public static SqlOperatorTable OperatorTable()
        {
            return SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                new SqlSpatialTypeOperatorTable(),
                GeographyOperatorTable.Instance());
        }

        /// <summary>
        /// Returns a type factory of the kind a statement is planned with.
        /// </summary>
        public static RelDataTypeFactory TypeFactory()
        {
            return new JavaTypeFactoryImpl();
        }

        /// <summary>
        /// Parses and validates a query against a root schema holding <c>GEO</c>, and returns its row type.
        /// </summary>
        /// <param name="sql">The query text.</param>
        /// <returns>The validated row type.</returns>
        public static RelDataType Validate(string sql)
        {
            var schema = Frameworks.createRootSchema(true);
            schema.add("GEO", new GeographyTable());

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .operatorTable(OperatorTable())
                .build();

            var planner = Frameworks.getPlanner(config);

            // Pair's left and right fields are shadowed by its static methods of the same name, so C#
            // resolves them to method groups; Pair is a Map.Entry, so getValue reads right.
            return (RelDataType)planner.validateAndGetType(planner.parse(sql)).getValue();
        }

        /// <summary>
        /// The <c>GEO</c> table: <c>ID INTEGER</c>, <c>GEOG</c> and <c>GEOM</c>.
        /// </summary>
        sealed class GeographyTable : AbstractTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("GEOG", GeographyTypes.Of(typeFactory))
                    .add("GEOM", GeographyTypes.Of(typeFactory))
                    .build();
            }

        }

    }

}
