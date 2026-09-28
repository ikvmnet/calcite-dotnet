using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Rel.Type;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Runs statements using the operators end to end — planned, code-generated, compiled and executed — under
    /// Calcite's <c>EnumerableConvention</c>.
    /// </summary>
    /// <remarks>
    /// The generated block is Java source compiled by Janino, which has to resolve the <c>cli.</c>-prefixed
    /// names IKVM gives the CLR classes that implement the operators. That requires IKVM 8.16.0 or later.
    /// </remarks>
    public class GeographyExecutionTests
    {

        /// <summary>
        /// Plans a query into <c>EnumerableConvention</c> over a schema holding <see cref="GeographyTable"/> as
        /// <c>GEO</c>, runs it, and returns each row's columns as <c>ResultSet.getObject</c> returns them.
        /// </summary>
        /// <param name="sql">The query text.</param>
        /// <returns>The rows, in the order the result set returns them.</returns>
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
        [Fact]
        public void ShouldRunAConstructorAndAMeasurement()
        {
            var rows = Run("SELECT CLR_ST_GEOG_DISTANCE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 0)'))");

            rows.Count.Should().Be(1);
            ((java.lang.Number)rows[0][0]!).doubleValue().Should().BeApproximately(111319.49079327357, 0.001);
        }

        /// <summary>
        /// A geometry column read from a table and passed to a predicate.
        /// </summary>
        /// <remarks>
        /// The distance <c>200000.0</c> is a <c>DECIMAL(7, 1)</c> literal and reaches the method as a
        /// <c>BigDecimal</c>. Calcite's own <c>ST_DWITHIN</c> takes a <c>double</c> and cannot be called with
        /// such a literal; see <c>GeographyFunctions.DWithin</c>.
        /// </remarks>
        [Fact]
        public void ShouldRunAPredicateOverAGeographyColumn()
        {
            var rows = Run("SELECT ID FROM GEO WHERE CLR_ST_GEOG_DWITHIN(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)");

            rows.Count.Should().Be(1);
            ((java.lang.Number)rows[0][0]!).intValue().Should().Be(1);
        }

        /// <summary>
        /// A geometry passed through <c>CLR_ST_GEOG_ASGEOM</c> can be given to Calcite's planar
        /// <c>ST_DISTANCE</c>.
        /// </summary>
        [Fact]
        public void ShouldRunTheCrossingIntoCalcitesOwnFunction()
        {
            var rows = Run("SELECT ST_DISTANCE(CLR_ST_GEOG_ASGEOM(GEOG), CLR_ST_GEOG_ASGEOM(GEOG)) FROM GEO WHERE ID = 1");

            rows.Count.Should().Be(1);
            ((java.lang.Number)rows[0][0]!).doubleValue().Should().Be(0);
        }

        /// <summary>
        /// Runs the constructors, conversions, relations and measurements as SQL and checks each answer.
        /// </summary>
        /// <remarks>
        /// An operator can validate and still fail in generated code, for instance when a literal argument's
        /// type does not match the method's parameter. Operators that return a geometry are wrapped in one that
        /// returns a value.
        /// </remarks>
        [Fact]
        public void ShouldRunEveryOperator()
        {
            var cases = new (string Sql, object Expected)[]
            {
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))", true),
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_GEOMFROMWKT('POINT(0 0)'))", true),
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)', 4326))", true),
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_GEOMFROMWKT('POINT(0 0)', 4326))", true),
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_GEOMFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[0,0]}'))", true),
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOM_ASGEOG(ST_GEOMFROMTEXT('POINT(0 0)')))", true),
                ("ST_ASTEXT(CLR_ST_GEOG_ASGEOM(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')))", "POINT (0 0)"),
                ("CLR_ST_GEOG_DISTANCE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))", 0.0),
                ("CLR_ST_GEOG_DWITHIN(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 0)'), 200000.0)", true),
                ("CLR_ST_GEOG_WITHIN(CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 1)'), CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("CLR_ST_GEOG_INTERSECTS(CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 1)'), CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),

                // the relations and the measurements
                ("CLR_ST_GEOG_CONTAINS(CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 1)'))", true),
                ("CLR_ST_GEOG_COVERS(CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))", true),
                ("CLR_ST_GEOG_COVEREDBY(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("CLR_ST_GEOG_DISJOINT(CLR_ST_GEOG_GEOMFROMTEXT('POINT(9 9)'), CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("CLR_ST_GEOG_EQUALS(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 1)'), CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(1 1, 0 0)'))", true),
                ("CLR_ST_GEOG_ENVELOPESINTERSECT(CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 1)'), CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))", true),
                ("CLR_ST_GEOG_LENGTH(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0)'))", 111319.49079327357),
                ("CLR_ST_GEOG_PERIMETER(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0)'))", 0.0),
                ("CLR_ST_GEOG_AREA(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0)'))", 0.0),
                ("CLR_ST_GEOG_MAXDISTANCE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 0)'))", 111319.49079327357),
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
        /// A scannable table of two rows, <c>(ID, GEOG)</c>: 1 at <c>POINT(0.5 0)</c> and 2 at
        /// <c>POINT(20 0)</c>.
        /// </summary>
        internal sealed class GeographyTable : AbstractTable, ScannableTable
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
