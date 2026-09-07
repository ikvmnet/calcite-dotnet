using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The overlay set — intersection, difference, symmetric difference and unary union.
    /// </summary>
    /// <remarks>
    /// #86 deferred these as large, on the reasoning that a geodesic overlay is a different algorithm from a
    /// planar one rather than the same one in other units. That is true and it is S2's algorithm:
    /// <c>initToIntersection</c> and its neighbours answer on the sphere what JTS answers on a plane, so what
    /// was left to write is the conversion of the answer back into rings.
    ///
    /// <para>They are areal operations and this answers them for areas, declining anything else. Calcite's
    /// take any pair, JTS overlaying whatever it is handed, and following it there would mean answering a
    /// line clipped by a polygon on the plane — two models in one expression, which is the thing this package
    /// exists to prevent.</para>
    /// </remarks>
    [TestClass]
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
        [TestMethod]
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

            // and not exactly two: the northern edge of the first box is a geodesic from (0 2) to (2 2), which
            // bows north of the parallel, so the shared region reaches a little past it. A planar overlay
            // stops at exactly 2, and that is the difference rather than a defect
            box.getMaxY().Should().BeGreaterThan(2.0);
        }

        /// <summary>
        /// The one that says the model is not the plane's: an overlap that exists on the Earth and not on a
        /// map.
        /// </summary>
        /// <remarks>
        /// The northern edge of a wide box is a geodesic, and a geodesic between two points on a parallel
        /// bows poleward — for a box reaching from 0 to 60 degrees of longitude along the 60th parallel, by
        /// several degrees at the middle. So a small shape sitting north of the parallel is outside the box
        /// as a planar reading draws it and inside the box as it is. Calcite answers an empty intersection
        /// and this answers a real one.
        /// </remarks>
        [TestMethod]
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
        /// A difference takes one from the other, and the areas add up.
        /// </summary>
        [TestMethod]
        public void ShouldTakeOneAreaFromAnother()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var b = Wkt("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))");

            var difference = GeographyFunctions.Difference(a, b);
            var overlap = GeographyFunctions.Intersection(a, b);

            // to S2's snapping tolerance; see ShouldAnswerBothSidesOfASymmetricDifference
            (Area(difference) + Area(overlap)).Should().BeApproximately(Area(a), Area(a) * 1e-5);
        }

        /// <summary>
        /// And the symmetric difference is everything but the overlap, counted once each way.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerBothSidesOfASymmetricDifference()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
            var b = Wkt("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))");

            var symmetric = Area(GeographyFunctions.SymDifference(a, b));
            var overlap = Area(GeographyFunctions.Intersection(a, b));

            // to S2's snapping tolerance rather than to the last bit: an overlay snaps its vertices to a
            // level of the cell hierarchy, so the pieces of a partition agree to about a part in a million
            symmetric.Should().BeApproximately(Area(a) + Area(b) - 2 * overlap, Area(a) * 1e-5);
        }

        /// <summary>
        /// A union of a shape with itself is the shape.
        /// </summary>
        [TestMethod]
        public void ShouldMergeAShapeWithItself()
        {
            var a = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            Area(GeographyFunctions.UnaryUnion(a)).Should().BeApproximately(Area(a), Area(a) * 1e-5);
        }

        /// <summary>
        /// Shapes that do not meet intersect in nothing.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerEmptyWhereTheyDoNotMeet()
        {
            var a = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var b = Wkt("POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))");

            GeographyFunctions.Intersection(a, b)!.isEmpty().Should().BeTrue();
        }

        /// <summary>
        /// A hole comes back a hole.
        /// </summary>
        [TestMethod]
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
        /// Anything without an area is declined rather than answered on a plane.
        /// </summary>
        [TestMethod]
        public void ShouldDeclineAnythingWithoutAnArea()
        {
            var area = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            GeographyFunctions.Intersection(Wkt("LINESTRING(0 0, 1 1)"), area).Should().BeNull();
            GeographyFunctions.Difference(area, Wkt("POINT(1 1)")).Should().BeNull();
            GeographyFunctions.UnaryUnion(Wkt("POINT(1 1)")).Should().BeNull();
        }

        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            var area = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            GeographyFunctions.Intersection(null, area).Should().BeNull();
            GeographyFunctions.Difference(area, null).Should().BeNull();
            GeographyFunctions.SymDifference(null, null).Should().BeNull();
            GeographyFunctions.UnaryUnion(null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            const string a = "ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')";
            const string b = "ST_GEOG_GEOMFROMTEXT('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')";

            foreach (var sql in new[]
            {
                $"ST_GEOG_AREA(ST_GEOG_INTERSECTION({a}, {b}))",
                $"ST_GEOG_AREA(ST_GEOG_DIFFERENCE({a}, {b}))",
                $"ST_GEOG_AREA(ST_GEOG_SYMDIFFERENCE({a}, {b}))",
                $"ST_GEOG_AREA(ST_GEOG_UNARYUNION({a}))",
            })
            {
                var answer = GeographyExecutionTests.Run("SELECT " + sql)[0][0];

                (answer is java.lang.Number n ? n.doubleValue() : double.NaN)
                    .Should().BeGreaterThan(0, sql);
            }
        }

    }

}
