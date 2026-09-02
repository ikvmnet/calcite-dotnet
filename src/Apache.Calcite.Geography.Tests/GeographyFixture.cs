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
    /// A schema holding one geography column and one geometry column, and a planner that can see Calcite's
    /// spatial functions and this package's alongside them.
    /// </summary>
    /// <remarks>
    /// The two columns are the whole of the fixture, because the property under test is that the same query
    /// text is accepted over one and refused over the other.
    /// </remarks>
    static class GeographyFixture
    {

        /// <summary>
        /// The operator table a host chains, with Calcite's spatial functions in it so that <c>ST_DISTANCE</c>
        /// is a name that resolves at all.
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
        /// Parses and validates the given query against the fixture, and returns the type of its row.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
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
            // resolves either to a method group; a Pair is a Map.Entry and getValue is the way in
            return (RelDataType)planner.validateAndGetType(planner.parse(sql)).getValue();
        }

        /// <summary>
        /// A table with a geography column and a geometry column.
        /// </summary>
        sealed class GeographyTable : AbstractTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("GEOG", GeographyTypes.Of(typeFactory))
                    .add("GEOM", GeographyTypes.GeometryOf(typeFactory))
                    .build();
            }

        }

    }

}
