using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests the overlay operators: intersection, difference, symmetric difference and unary union.
    /// </summary>
    /// <remarks>
    /// The overlays are S2's polygon operations (<c>initToIntersection</c> and its neighbours), converted back
    /// into JTS rings. They accept only areal operands and return null otherwise, where Calcite's overlay any
    /// pair on the plane.
    /// </remarks>
    public class GeographyOverlayTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static double Area(Geometry? g)
        {
            return g is null ? 0 : GeographyFunctions.Area(g)!.doubleValue();
        }

        /// <summary>
        /// Two overlapping boxes intersect in the box they share.
        /// </summary>
        [Fact]
        public void ShouldIntersectTwoBoxes()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var b = Wkt("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))");

            var overlap = GeographyFunctions.Intersection(a, b);

            overlap.Should().NotBeNull();
            var box = overlap!.getEnvelopeInternal();

            box.getMinX().Should().BeApproximately(1, 1e-6);
            box.getMaxX().Should().BeApproximately(2, 1e-6);
            box.getMinY().Should().BeApproximately(1, 1e-3);
            box.getMaxY().Should().BeApproximately(2, 1e-3);

            // Slightly more than 2: the first box's northern edge is a geodesic from (0 2) to (2 2) and bows
            // north of the parallel. A planar overlay stops at exactly 2.
            box.getMaxY().Should().BeGreaterThan(2.0);
        }

        /// <summary>
        /// Two shapes that overlap geodesically but not in the plane have a non-empty intersection.
        /// </summary>
        /// <remarks>
        /// The wide box's northern edge runs along the 60th parallel from longitude 0 to 60, and as a geodesic
        /// it bows about three and a half degrees north at its middle. A small shape just north of the
        /// parallel is inside the geodesic box and outside the planar one, so Calcite's intersection is empty.
        /// </remarks>
        [Fact]
        public void ShouldIntersectWhereTheGeodesicEdgeReachesAndThePlanarOneDoesNot()
        {
            var wide = Wkt("POLYGON((0 60, 60 60, 60 20, 0 20, 0 60))");
            var sliver = Wkt("POLYGON((29 61, 31 61, 31 62, 29 62, 29 61))");

            SpatialTypeFunctions.ST_Intersection(wide, sliver).isEmpty().Should().BeTrue("no part of the sliver is south of 60");

            var ours = GeographyFunctions.Intersection(wide, sliver);

            ours.Should().NotBeNull();
            ours!.isEmpty().Should().BeFalse("the box's northern edge is a geodesic and bows north of 60");
            Area(ours).Should().BeGreaterThan(0);
        }

        /// <summary>
        /// The areas of <c>a - b</c> and <c>a ∩ b</c> sum to the area of <c>a</c>.
        /// </summary>
        [Fact]
        public void ShouldTakeOneAreaFromAnother()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var b = Wkt("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))");

            var difference = GeographyFunctions.Difference(a, b);
            var overlap = GeographyFunctions.Intersection(a, b);

            // within S2's snapping tolerance; see ShouldAnswerBothSidesOfASymmetricDifference
            (Area(difference) + Area(overlap)).Should().BeApproximately(Area(a), Area(a) * 1e-5);
        }

        /// <summary>
        /// The symmetric difference's area is the two areas less twice the overlap.
        /// </summary>
        [Fact]
        public void ShouldAnswerBothSidesOfASymmetricDifference()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var b = Wkt("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))");

            var symmetric = Area(GeographyFunctions.SymDifference(a, b));
            var overlap = Area(GeographyFunctions.Intersection(a, b));

            // Within S2's snapping tolerance: an overlay snaps its vertices to a level of the cell hierarchy,
            // so the pieces of a partition agree to about a part in a million.
            symmetric.Should().BeApproximately(Area(a) + Area(b) - 2 * overlap, Area(a) * 1e-5);
        }

        /// <summary>
        /// The unary union of a single polygon has the polygon's area.
        /// </summary>
        [Fact]
        public void ShouldMergeAShapeWithItself()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            Area(GeographyFunctions.UnaryUnion(a)).Should().BeApproximately(Area(a), Area(a) * 1e-5);
        }

        /// <summary>
        /// The intersection of disjoint shapes is empty.
        /// </summary>
        [Fact]
        public void ShouldAnswerEmptyWhereTheyDoNotMeet()
        {
            var a = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var b = Wkt("POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))");

            GeographyFunctions.Intersection(a, b)!.isEmpty().Should().BeTrue();
        }

        /// <summary>
        /// A polygon's hole survives an overlay.
        /// </summary>
        [Fact]
        public void ShouldKeepAHoleThroughTheOverlay()
        {
            var ring = Wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1))");
            var all = Wkt("POLYGON((-1 -1, 5 -1, 5 5, -1 5, -1 -1))");

            var kept = GeographyFunctions.Intersection(ring, all);

            kept.Should().NotBeNull();
            Area(kept).Should().BeApproximately(Area(ring), Area(ring) * 1e-5);
            ((org.locationtech.jts.geom.Polygon)kept!.getGeometryN(0)).getNumInteriorRing().Should().Be(1);
        }

        /// <summary>
        /// An operand with no area gives null.
        /// </summary>
        [Fact]
        public void ShouldDeclineAnythingWithoutAnArea()
        {
            var area = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            GeographyFunctions.Intersection(Wkt("LINESTRING(0 0, 1 1)"), area).Should().BeNull();
            GeographyFunctions.Difference(area, Wkt("POINT(1 1)")).Should().BeNull();
            GeographyFunctions.UnaryUnion(Wkt("POINT(1 1)")).Should().BeNull();
        }

        [Fact]
        public void ShouldAnswerNullForANullArgument()
        {
            var area = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            GeographyFunctions.Intersection(null, area).Should().BeNull();
            GeographyFunctions.Difference(area, null).Should().BeNull();
            GeographyFunctions.SymDifference(null, null).Should().BeNull();
            GeographyFunctions.UnaryUnion(null).Should().BeNull();
        }

        [Fact]
        public void ShouldRunEachAsAnOperator()
        {
            const string a = "CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')";
            const string b = "CLR_ST_GEOG_GEOMFROMTEXT('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')";

            foreach (var sql in new[]
            {
                $"CLR_ST_GEOG_AREA(CLR_ST_GEOG_INTERSECTION({a}, {b}))",
                $"CLR_ST_GEOG_AREA(CLR_ST_GEOG_DIFFERENCE({a}, {b}))",
                $"CLR_ST_GEOG_AREA(CLR_ST_GEOG_SYMDIFFERENCE({a}, {b}))",
                $"CLR_ST_GEOG_AREA(CLR_ST_GEOG_UNARYUNION({a}))",
            })
            {
                var answer = GeographyExecutionTests.Run("SELECT " + sql)[0][0];

                (answer is java.lang.Number n ? n.doubleValue() : double.NaN)
                    .Should().BeGreaterThan(0, sql);
            }
        }

    }

}
