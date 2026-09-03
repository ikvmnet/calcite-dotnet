using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Rel.Type;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// A geography through a whole statement — planned, code-generated, compiled and run.
    /// </summary>
    /// <remarks>
    /// The design issue left this open: validation and code generation were measured and a full run was not.
    /// It runs. The engine here is Calcite's own <c>EnumerableConvention</c>, so the block is Java source
    /// compiled by Janino, which is the harder of the two cases — the body is a .NET method and Janino has to
    /// resolve the <c>cli.</c>-prefixed name IKVM gives a CLR class, which it could not do under IKVM 8.14.0
    /// or 8.15.0.
    /// </remarks>
    [TestClass]
    public class GeographyExecutionTests
    {

        /// <summary>
        /// Plans the given query into <c>EnumerableConvention</c> and runs it, returning each row's columns
        /// as they arrive.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        internal static List<object?[]> Run(string sql)
        {
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:");
            var calcite = (CalciteConnection)connection.unwrap((java.lang.Class)typeof(CalciteConnection));
            var schema = calcite.getRootSchema();
            schema.add("GEO", new GeographyTable());

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .operatorTable(GeographyFixture.OperatorTable())
                .programs(Programs.standard())
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var physical = planner.transform(0, logical.getTraitSet().replace(EnumerableConvention.INSTANCE), logical);

            var runner = (RelRunner)connection.unwrap((java.lang.Class)typeof(RelRunner));
            using var statement = runner.prepareStatement(physical);
            var results = statement.executeQuery();
            var count = results.getMetaData().getColumnCount();

            var rows = new List<object?[]>();

            while (results.next())
            {
                var row = new object?[count];
                for (var i = 0; i < count; i++)
                    row[i] = results.getObject(i + 1);

                rows.Add(row);
            }

            return rows;
        }

        /// <summary>
        /// A constructor and a measurement, with no table involved.
        /// </summary>
        [TestMethod]
        public void ShouldRunAConstructorAndAMeasurement()
        {
            var rows = Run("SELECT ST_GEOG_DISTANCE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('POINT(1 0)'))");

            rows.Count.Should().Be(1);
            ((java.lang.Number)rows[0][0]!).doubleValue().Should().BeApproximately(6371010.0 * Math.PI / 180, 0.001);
        }

        /// <summary>
        /// A geography column, read from a table and carried into a predicate.
        /// </summary>
        /// <remarks>
        /// The distance is written <c>200000.0</c>, which is <c>DECIMAL(7, 1)</c> and arrives at the body as
        /// a <c>BigDecimal</c>. Calcite's own <c>ST_DWITHIN</c> cannot be called that way at all — it takes a
        /// <c>double</c> and Janino refuses the call; see <c>GeographyFunctions.DWithin</c>.
        /// </remarks>
        [TestMethod]
        public void ShouldRunAPredicateOverAGeographyColumn()
        {
            var rows = Run("SELECT ID FROM GEO WHERE ST_GEOG_DWITHIN(GEOG, ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)");

            rows.Count.Should().Be(1);
            ((java.lang.Number)rows[0][0]!).intValue().Should().Be(1);
        }

        /// <summary>
        /// The crossing, run rather than validated: the same object comes out the other side and Calcite's
        /// planar function measures it in degrees.
        /// </summary>
        [TestMethod]
        public void ShouldRunTheCrossingIntoCalcitesOwnFunction()
        {
            var rows = Run("SELECT ST_DISTANCE(ST_GEOG_ASGEOM(GEOG), ST_GEOG_ASGEOM(GEOG)) FROM GEO WHERE ID = 1");

            rows.Count.Should().Be(1);
            ((java.lang.Number)rows[0][0]!).doubleValue().Should().Be(0);
        }

        /// <summary>
        /// Every operator the table declares, run.
        /// </summary>
        /// <remarks>
        /// Declaring an operator and running one are different things, and the gap between them is where a
        /// body whose parameters cannot be reached from generated code hides — the <c>BigDecimal</c> that
        /// stops Calcite's own <c>ST_DWITHIN</c> being called with <c>2.0</c> is exactly that shape of defect,
        /// and it was found by running rather than by declaring. So each of the twelve declarations is called
        /// here at least once, and the ones that answer a geography are wrapped in one that answers a value a
        /// result set can carry.
        /// </remarks>
        [TestMethod]
        public void ShouldRunEveryOperator()
        {
            var cases = new (string Sql, object Expected)[]
            {
                ("ST_GEOG_ISVALID(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))", true),
                ("ST_GEOG_ISVALID(ST_GEOG_GEOMFROMWKT('POINT(0 0)'))", true),
                ("ST_GEOG_ISVALID(ST_GEOG_GEOMFROMTEXT('POINT(0 0)', 4326))", true),
                ("ST_GEOG_ISVALID(ST_GEOG_GEOMFROMWKT('POINT(0 0)', 4326))", true),
                ("ST_GEOG_ISVALID(ST_GEOG_GEOMFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[0,0]}'))", true),
                ("ST_GEOG_ISVALID(ST_GEOM_ASGEOG(ST_GEOMFROMTEXT('POINT(0 0)')))", true),
                ("ST_ASTEXT(ST_GEOG_ASGEOM(ST_GEOG_GEOMFROMTEXT('POINT(0 0)')))", "POINT (0 0)"),
                ("ST_GEOG_DISTANCE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))", 0.0),
                ("ST_GEOG_DWITHIN(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('POINT(1 0)'), 200000.0)", true),
                ("ST_GEOG_WITHIN(ST_GEOG_GEOMFROMTEXT('POINT(1 1)'), ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("ST_GEOG_INTERSECTS(ST_GEOG_GEOMFROMTEXT('POINT(1 1)'), ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("ST_GEOG_ISVALID(ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),

                // the relations and the measurements
                ("ST_GEOG_CONTAINS(ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOG_GEOMFROMTEXT('POINT(1 1)'))", true),
                ("ST_GEOG_COVERS(ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))", true),
                ("ST_GEOG_COVEREDBY(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("ST_GEOG_DISJOINT(ST_GEOG_GEOMFROMTEXT('POINT(9 9)'), ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("ST_GEOG_EQUALS(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 1)'), ST_GEOG_GEOMFROMTEXT('LINESTRING(1 1, 0 0)'))", true),
                ("ST_GEOG_ENVELOPESINTERSECT(ST_GEOG_GEOMFROMTEXT('POINT(1 1)'), ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("ST_GEOG_LENGTH(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0)'))", 6371010.0 * Math.PI / 180),
                ("ST_GEOG_PERIMETER(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0)'))", 0.0),
                ("ST_GEOG_AREA(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0)'))", 0.0),
                ("ST_GEOG_MAXDISTANCE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('POINT(1 0)'))", 6371010.0 * Math.PI / 180),
            };

            var failures = new List<string>();

            foreach (var (sql, expected) in cases)
            {
                try
                {
                    var rows = Run($"SELECT {sql}");
                    var answer = rows[0][0];

                    var same = expected switch
                    {
                        bool b => answer is java.lang.Boolean j && j.booleanValue() == b,
                        double d => answer is java.lang.Number n && Math.Abs(n.doubleValue() - d) < 1e-6,
                        _ => Equals(answer?.ToString(), expected.ToString()),
                    };

                    if (same == false)
                        failures.Add($"{sql}: answered {answer}, wanted {expected}");
                }
                catch (Exception e)
                {
                    failures.Add($"{sql}: {e.Message}");
                }
            }

            failures.Should().BeEmpty(string.Join("\n", failures));
        }

        /// <summary>
        /// A table of two rows, each holding a geography a degree apart on the equator.
        /// </summary>
        sealed class GeographyTable : AbstractTable, ScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("GEOG", GeographyTypes.Of(typeFactory))
                    .build();
            }

            public Enumerable scan(DataContext root)
            {
                return Linq4j.asEnumerable(java.util.Arrays.asList([
                    new object[] { java.lang.Integer.valueOf(1), Geography("POINT(0.5 0)") },
                    new object[] { java.lang.Integer.valueOf(2), Geography("POINT(20 0)") },
                ]));
            }

            static Geometry Geography(string wkt)
            {
                return org.apache.calcite.runtime.SpatialTypeUtils.fromWkt(wkt);
            }

        }

    }

}
