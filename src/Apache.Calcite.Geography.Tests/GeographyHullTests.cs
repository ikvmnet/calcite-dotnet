using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_CONVEXHULL</c> and <c>CLR_ST_GEOG_SIMPLIFY</c>.
    /// </summary>
    public class GeographyHullTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        /// <summary>
        /// The hull of a box's four corners spans the box.
        /// </summary>
        [Fact]
        public void ShouldHullPointsToTheirBox()
        {
            var hull = GeographyFunctions.ConvexHull(Wkt("MULTIPOINT((0 0), (2 0), (2 2), (0 2))"));

            hull.Should().NotBeNull();
            hull!.getGeometryType().Should().BeOneOf("Polygon", "MultiPolygon");

            var box = hull.getEnvelopeInternal();
            box.getMinX().Should().BeApproximately(0, 1e-9);
            box.getMaxX().Should().BeApproximately(2, 1e-9);
        }

        /// <summary>
        /// A point inside the geodesic hull and outside the planar one.
        /// </summary>
        /// <remarks>
        /// The hull's northern edge is a geodesic between two points on the 60th parallel, and across sixty
        /// degrees of longitude it bows more than a degree toward the pole, so a point just north of the
        /// parallel is inside the geodesic hull and outside the planar one.
        /// </remarks>
        [Fact]
        public void ShouldHullFurtherNorthThanAPlanarHullDoes()
        {
            var corners = Wkt("MULTIPOINT((0 60), (60 60), (60 20), (0 20))");
            var probe = Wkt("POINT(30 61)");

            SpatialTypeFunctions.ST_Contains(SpatialTypeFunctions.ST_ConvexHull(corners), probe)
                .Should().BeFalse("the planar hull's northern edge is the parallel");

            var ours = GeographyFunctions.ConvexHull(corners)!;

            GeographyFunctions.Contains(ours, probe)!.booleanValue()
                .Should().BeTrue("the geodesic hull's northern edge bows past 61");
        }

        /// <summary>
        /// The hull of a shape covers the shape and fills its holes.
        /// </summary>
        [Fact]
        public void ShouldContainWhatItWasBuiltFrom()
        {
            var shape = Wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1))");
            var hull = GeographyFunctions.ConvexHull(shape)!;

            GeographyFunctions.Covers(hull, shape)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Area(hull)!.doubleValue()
                .Should().BeGreaterThan(GeographyFunctions.Area(shape)!.doubleValue(), "the hole is filled in");
        }

        /// <summary>
        /// The hull of an empty geometry is empty, and of null is null.
        /// </summary>
        [Fact]
        public void ShouldHullNothingToNothing()
        {
            GeographyFunctions.ConvexHull(Wkt("POLYGON EMPTY"))!.isEmpty().Should().BeTrue();
            GeographyFunctions.ConvexHull(null).Should().BeNull();
        }

        /// <summary>
        /// Simplifying removes vertices that lie within the tolerance of the simplified edge.
        /// </summary>
        [Fact]
        public void ShouldRemoveVerticesWithinTheTolerance()
        {
            // a square with a great many near-collinear points along its southern edge
            var points = new System.Text.StringBuilder("POLYGON((0 0");
            for (var i = 1; i < 40; i++)
                points.Append($", {i * 0.05} 0.000001");
            points.Append(", 2 0, 2 2, 0 2, 0 0))");

            var detailed = Wkt(points.ToString());
            var simplified = GeographyFunctions.Simplify(detailed, java.lang.Double.valueOf(1000.0))!;

            simplified.isEmpty().Should().BeFalse();
            simplified.getNumPoints().Should().BeLessThan(detailed.getNumPoints());
        }

        /// <summary>
        /// A tolerance of a millimetre leaves a square's area unchanged.
        /// </summary>
        [Fact]
        public void ShouldKeepTheShapeUnderATinyTolerance()
        {
            var square = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var simplified = GeographyFunctions.Simplify(square, java.lang.Double.valueOf(0.001))!;

            GeographyFunctions.Area(simplified)!.doubleValue()
                .Should().BeApproximately(GeographyFunctions.Area(square)!.doubleValue(), GeographyFunctions.Area(square)!.doubleValue() * 1e-5);
        }

        /// <summary>
        /// Simplifying a geometry with no area returns null, as the overlay operations do; so does a null
        /// argument.
        /// </summary>
        [Fact]
        public void ShouldDeclineToSimplifyAnythingWithoutAnArea()
        {
            GeographyFunctions.Simplify(Wkt("LINESTRING(0 0, 1 1, 2 0)"), java.lang.Double.valueOf(1000.0)).Should().BeNull();
            GeographyFunctions.Simplify(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Simplify(Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"), null).Should().BeNull();
        }

        [Fact]
        public void ShouldStampBothWithWgs84()
        {
            GeographyFunctions.ConvexHull(Wkt("MULTIPOINT((0 0), (2 0), (1 2))"))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);

            GeographyFunctions.Simplify(Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"), java.lang.Double.valueOf(1.0))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [Fact]
        public void ShouldRunEachAsAnOperator()
        {
            foreach (var sql in new[]
            {
                "CLR_ST_GEOG_AREA(CLR_ST_GEOG_CONVEXHULL(CLR_ST_GEOG_GEOMFROMTEXT('MULTIPOINT((0 0), (2 0), (1 2))')))",
                "CLR_ST_GEOG_AREA(CLR_ST_GEOG_SIMPLIFY(CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), 1.0))",
            })
            {
                var answer = GeographyExecutionTests.Run("SELECT " + sql)[0][0];

                (answer is java.lang.Number n ? n.doubleValue() : double.NaN).Should().BeGreaterThan(0, sql);
            }
        }

    }

}
