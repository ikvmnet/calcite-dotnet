using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The convex hull and simplification, both of which S2 already had.
    /// </summary>
    /// <remarks>
    /// The rest of #86's step five, less buffer, triangulation and grids. That issue deferred the group as
    /// large on the reasoning that each is a different algorithm geodesically, which is true — and
    /// <c>S2ConvexHullQuery</c> and <c>S2Polygon.initToSimplified</c> are those algorithms, already written.
    /// </remarks>
    [TestClass]
    public class GeographyHullTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        /// <summary>
        /// Four corners of a box hull to the box.
        /// </summary>
        [TestMethod]
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
        /// The hull's northern edge runs between two points on the 60th parallel and is a geodesic, so it
        /// bows poleward — by more than a degree across sixty degrees of longitude. A point just north of the
        /// parallel is therefore inside the hull on the Earth and outside the hull on a map, and the two
        /// answers are asserted side by side.
        /// </remarks>
        [TestMethod]
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
        /// The hull of a shape contains the shape.
        /// </summary>
        [TestMethod]
        public void ShouldContainWhatItWasBuiltFrom()
        {
            var shape = Wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1))");
            var hull = GeographyFunctions.ConvexHull(shape)!;

            GeographyFunctions.Covers(hull, shape)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Area(hull)!.doubleValue()
                .Should().BeGreaterThan(GeographyFunctions.Area(shape)!.doubleValue(), "the hole is filled in");
        }

        /// <summary>
        /// Nothing to hull is an empty hull.
        /// </summary>
        [TestMethod]
        public void ShouldHullNothingToNothing()
        {
            GeographyFunctions.ConvexHull(Wkt("POLYGON EMPTY"))!.isEmpty().Should().BeTrue();
            GeographyFunctions.ConvexHull(null).Should().BeNull();
        }

        /// <summary>
        /// Simplifying removes vertices and keeps the shape within the tolerance.
        /// </summary>
        [TestMethod]
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
        /// A tolerance of nothing removes nothing that matters.
        /// </summary>
        [TestMethod]
        public void ShouldKeepTheShapeUnderATinyTolerance()
        {
            var square = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var simplified = GeographyFunctions.Simplify(square, java.lang.Double.valueOf(0.001))!;

            GeographyFunctions.Area(simplified)!.doubleValue()
                .Should().BeApproximately(GeographyFunctions.Area(square)!.doubleValue(), GeographyFunctions.Area(square)!.doubleValue() * 1e-5);
        }

        /// <summary>
        /// Simplification is an areal operation, as the overlay set is.
        /// </summary>
        [TestMethod]
        public void ShouldDeclineToSimplifyAnythingWithoutAnArea()
        {
            GeographyFunctions.Simplify(Wkt("LINESTRING(0 0, 1 1, 2 0)"), java.lang.Double.valueOf(1000.0)).Should().BeNull();
            GeographyFunctions.Simplify(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Simplify(Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"), null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampBothWithWgs84()
        {
            GeographyFunctions.ConvexHull(Wkt("MULTIPOINT((0 0), (2 0), (1 2))"))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);

            GeographyFunctions.Simplify(Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"), java.lang.Double.valueOf(1.0))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            foreach (var sql in new[]
            {
                "ST_GEOG_AREA(ST_GEOG_CONVEXHULL(ST_GEOG_GEOMFROMTEXT('MULTIPOINT((0 0), (2 0), (1 2))')))",
                "ST_GEOG_AREA(ST_GEOG_SIMPLIFY(ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), 1.0))",
            })
            {
                var answer = GeographyExecutionTests.Run("SELECT " + sql)[0][0];

                (answer is java.lang.Number n ? n.doubleValue() : double.NaN).Should().BeGreaterThan(0, sql);
            }
        }

    }

}
