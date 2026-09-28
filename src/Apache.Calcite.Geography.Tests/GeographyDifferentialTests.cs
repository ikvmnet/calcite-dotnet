using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Compares the <c>CLR_ST_GEOG_*</c> relations and measurements with the Calcite <c>ST_*</c> functions of
    /// the same name, over shapes small enough and near enough the equator that geodesic and planar answers
    /// must agree.
    /// </summary>
    /// <remarks>
    /// Calcite's <c>ST_Within</c> is JTS's <c>geom1.within(geom2)</c>, <c>ST_Distance</c> is
    /// <c>geom1.distance(geom2)</c>, and so on; the geodesic operators mean the same with geodesic edges. For
    /// shapes a few thousandths of a degree across at the equator the difference between a geodesic edge and
    /// a straight line in degrees is far below any tolerance, so a disagreement is a defect. The cases where
    /// the two models do disagree are in <see cref="GeographyFunctionTests"/>.
    /// </remarks>
    public class GeographyDifferentialTests
    {

        /// <summary>
        /// Metres per degree of longitude at the equator on WGS84, where the shapes below sit.
        /// </summary>
        /// <remarks>
        /// On the ellipsoid a degree east at the equator is 111319.49 m and a degree north is 110574.39 m, so a
        /// planar distance scaled by this factor can be off by up to 0.67% depending on direction. The numeric
        /// comparisons are therefore a bound that catches a wrong unit, factor or shape, not a check of the
        /// model; <see cref="Wgs84MeasurementTests"/> checks the model.
        /// </remarks>
        const double Degree = 111319.49079327357;

        /// <summary>
        /// The relative difference allowed between a geodesic and a scaled planar measurement.
        /// </summary>
        const double ModelGap = 1e-2;

        /// <summary>
        /// Shapes of every dimension, overlapping, touching, nested and disjoint.
        /// </summary>
        static readonly string[] shapes =
        [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "POINT(0 0)",
            "POINT(0.002 0.002)",
            "POINT(0.004 0)",
            "POINT(0.01 0.01)",
            "MULTIPOINT((0.001 0.001), (0.002 0.002))",
            "LINESTRING(0 0, 0.004 0)",
            "LINESTRING(0 0, 0.004 0.004)",
            "LINESTRING(0.001 -0.002, 0.001 0.002)",
            "LINESTRING(0.001 0.001, 0.003 0.001, 0.003 0.003)",
            "MULTILINESTRING((0 0, 0.004 0), (0 0.004, 0.004 0.004))",
            "POLYGON((0 0, 0.004 0, 0.004 0.004, 0 0.004, 0 0))",
            "POLYGON((0.001 0.001, 0.003 0.001, 0.003 0.003, 0.001 0.003, 0.001 0.001))",
            "POLYGON((0.002 0.002, 0.006 0.002, 0.006 0.006, 0.002 0.006, 0.002 0.002))",
            "POLYGON((0.01 0.01, 0.02 0.01, 0.02 0.02, 0.01 0.02, 0.01 0.01))",
            "POLYGON((0 0, 0.006 0, 0.006 0.006, 0 0.006, 0 0), (0.002 0.002, 0.004 0.002, 0.004 0.004, 0.002 0.004, 0.002 0.002))",
            "MULTIPOLYGON(((0 0, 0.002 0, 0.002 0.002, 0 0.002, 0 0)), ((0.004 0.004, 0.006 0.004, 0.006 0.006, 0.004 0.006, 0.004 0.004)))",
            // two lines meeting end to end, which together cover a line that neither covers alone
            "MULTILINESTRING((0 0, 0.002 0), (0.002 0, 0.004 0))",
            // a line half inside a polygon and half out of it
            "LINESTRING(0.002 0.002, 0.008 0.002)",
            // a square meeting another only at a corner
            "POLYGON((0.004 0.004, 0.006 0.004, 0.006 0.006, 0.004 0.006, 0.004 0.004))",
            // dimensions mixed in one geography
            "GEOMETRYCOLLECTION(POINT(0.001 0.001), LINESTRING(0.002 0, 0.004 0))",
            // negative coordinates, to catch a sign error
            "POINT(-0.001 -0.001)",
            "POLYGON((-0.002 -0.002, 0.002 -0.002, 0.002 0.002, -0.002 0.002, -0.002 -0.002))",
        ];

        /// <summary>
        /// Invalid shapes, used only by the validity comparison.
        /// </summary>
        /// <remarks>
        /// They are kept out of the pairwise comparisons because neither model defines a relation over an
        /// invalid geometry.
        /// </remarks>
        static readonly string[] degenerate =
        [
            // a line that goes nowhere
            "LINESTRING(0 0, 0 0)",
            // a bow tie, whose edges cross
            "POLYGON((0 0, 0.004 0.004, 0.004 0, 0 0.004, 0 0))",
            // a ring with a repeated vertex
            "POLYGON((0 0, 0.004 0, 0.004 0, 0.004 0.004, 0 0.004, 0 0))",
        ];

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        /// <summary>
        /// Runs a binary predicate both ways over every ordered pair of shapes and reports every disagreement.
        /// </summary>
        /// <param name="geodesic">The <c>CLR_ST_GEOG_*</c> implementation.</param>
        /// <param name="planar">Calcite's <c>ST_*</c> implementation.</param>
        /// <param name="refusals">How many pairs Calcite is expected to refuse with an exception.</param>
        /// <remarks>
        /// A pair Calcite refuses is skipped, and the count is asserted so that the comparison cannot pass by
        /// skipping everything. JTS refuses a <c>GEOMETRYCOLLECTION</c> that reaches <c>Geometry.relate</c>
        /// (through <c>checkNotGeometryCollection</c>); multi-points, multi-lines and multi-polygons are not
        /// refused, and <c>contains</c> answers a rectangular container through <c>RectangleContains</c>
        /// without reaching <c>relate</c>, so which pairs are refused depends on both shapes.
        /// </remarks>
        static void Differ(Func<Geometry, Geometry, java.lang.Boolean?> geodesic, Func<Geometry, Geometry, bool> planar, int refusals)
        {
            var differences = new List<string>();
            var refused = 0;

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    bool theirs;

                    try
                    {
                        theirs = planar(a, b);
                    }
                    catch (java.lang.IllegalArgumentException)
                    {
                        refused++;
                        continue;
                    }

                    var ours = geodesic(a, b)!.booleanValue();

                    if (ours != theirs)
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
            refused.Should().Be(refusals);
        }

        [Fact]
        public void ShouldAgreeOnWithin()
        {
            Differ(GeographyFunctions.Within, SpatialTypeFunctions.ST_Within, refusals: 6);
        }

        [Fact]
        public void ShouldAgreeOnIntersects()
        {
            Differ(GeographyFunctions.Intersects, SpatialTypeFunctions.ST_Intersects, refusals: 0);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_WITHIN</c> answers over a geometry collection where Calcite's <c>ST_WITHIN</c> throws.
        /// </summary>
        /// <remarks>
        /// The container is the polygon with a hole rather than a square, because JTS answers a rectangular
        /// container without reaching the <c>relate</c> that refuses collections.
        ///
        /// <para>The collection is a point inside the polygon and a line along its southern edge. Every part
        /// lies in the polygon and the interiors meet at the point, so the collection is within it, though the
        /// line alone is not.</para>
        /// </remarks>
        [Fact]
        public void ShouldAnswerWithinOverACollectionWhereCalciteRefuses()
        {
            var collection = Wkt("GEOMETRYCOLLECTION(POINT(0.001 0.001), LINESTRING(0.002 0, 0.004 0))");
            var donut = Wkt("POLYGON((0 0, 0.006 0, 0.006 0.006, 0 0.006, 0 0), (0.002 0.002, 0.004 0.002, 0.004 0.004, 0.002 0.004, 0.002 0.002))");

            var refused = () => SpatialTypeFunctions.ST_Within(collection, donut);
            refused.Should().Throw<java.lang.IllegalArgumentException>().WithMessage("*GeometryCollection*");

            GeographyFunctions.Within(collection, donut)!.booleanValue().Should().BeTrue();

            // The line alone touches only the boundary, so it is not within the polygon; Calcite answers
            // this pair, and the same way.
            var line = Wkt("LINESTRING(0.002 0, 0.004 0)");
            GeographyFunctions.Within(line, donut)!.booleanValue().Should().BeFalse();
            SpatialTypeFunctions.ST_Within(line, donut).Should().BeFalse();
        }

        [Fact]
        public void ShouldAgreeOnContains()
        {
            Differ(GeographyFunctions.Contains, SpatialTypeFunctions.ST_Contains, refusals: 6);
        }

        [Fact]
        public void ShouldAgreeOnCovers()
        {
            Differ(GeographyFunctions.Covers, SpatialTypeFunctions.ST_Covers, refusals: 6);
        }

        [Fact]
        public void ShouldAgreeOnCoveredBy()
        {
            Differ(GeographyFunctions.CoveredBy, SpatialTypeFunctions.ST_CoveredBy, refusals: 6);
        }

        [Fact]
        public void ShouldAgreeOnDisjoint()
        {
            Differ(GeographyFunctions.Disjoint, SpatialTypeFunctions.ST_Disjoint, refusals: 0);
        }

        [Fact]
        public void ShouldAgreeOnEquals()
        {
            Differ(GeographyFunctions.Equals, SpatialTypeFunctions.ST_Equals, refusals: 1);
        }

        [Fact]
        public void ShouldAgreeOnEnvelopesIntersect()
        {
            Differ(GeographyFunctions.EnvelopesIntersect, SpatialTypeFunctions.ST_EnvelopesIntersect, refusals: 0);
        }

        [Fact]
        public void ShouldAgreeOnIsValid()
        {
            var differences = new List<string>();

            foreach (var shape in System.Linq.Enumerable.Concat(shapes, degenerate))
            {
                var geography = Wkt(shape);
                var ours = GeographyFunctions.IsValid(geography)!.booleanValue();
                var theirs = SpatialTypeFunctions.ST_IsValid(geography);

                if (ours != theirs)
                    differences.Add($"{shape}: ours {ours}, Calcite {theirs}");
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// The geodesic distance in metres agrees with Calcite's distance in degrees scaled by
        /// <see cref="Degree"/>, within <see cref="ModelGap"/>.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnDistance()
        {
            var differences = new List<string>();

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    var ours = GeographyFunctions.Distance(a, b)!.doubleValue();
                    var theirs = SpatialTypeFunctions.ST_Distance(a, b) * Degree;

                    if (Math.Abs(ours - theirs) > Math.Max(ModelGap * theirs, 1e-6))
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_DWITHIN</c> agrees with Calcite's <c>ST_DWITHIN</c> (<c>distance &lt;= d</c>) for every
        /// pair not within <see cref="ModelGap"/> of the threshold.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnDWithin()
        {
            var differences = new List<string>();

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    var threshold = 0.003;

                    // A boolean has no tolerance, so pairs whose distance is close enough to the threshold for
                    // the two models to decide differently are skipped.
                    if (Math.Abs(SpatialTypeFunctions.ST_Distance(a, b) - threshold) <= ModelGap * threshold)
                        continue;

                    var ours = GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(threshold * Degree))!.booleanValue();
                    var theirs = SpatialTypeFunctions.ST_DWithin(a, b, threshold);

                    if (ours != theirs)
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

    }

}
