using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Positions taken relative to a line, and the narrowest way across a shape.
    /// </summary>
    /// <remarks>
    /// Both turn on the same fact as the rest of this package: the line between two coordinates is a geodesic
    /// and not a straight line in degrees, so a point partway along it is somewhere else, sideways is a
    /// different direction, and the pair of parallel lines that hold a shape are great circles.
    /// </remarks>
    [TestClass]
    public class GeographyAlongTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static Geometry Along(string wkt, double fraction, double offset)
        {
            return GeographyFunctions.LocateAlong(Wkt(wkt), java.lang.Double.valueOf(fraction), java.lang.Double.valueOf(offset))!;
        }

        /// <summary>
        /// Halfway along a geodesic is not halfway along a line drawn in degrees.
        /// </summary>
        /// <remarks>
        /// Across sixty degrees of longitude on the 60th parallel the geodesic bows to 63.4 at its middle, so
        /// the halfway point is three and a half degrees north of where Calcite puts it. The longitude agrees
        /// and the latitude does not, which is the bow and nothing else.
        /// </remarks>
        [TestMethod]
        public void ShouldPlaceTheMidpointOnTheGeodesic()
        {
            const string line = "LINESTRING(0 60, 60 60)";

            var theirs = SpatialTypeFunctions.ST_LocateAlong(Wkt(line), java.math.BigDecimal.valueOf(0.5), java.math.BigDecimal.ZERO)
                .getCoordinate();
            theirs.getY().Should().BeApproximately(60, 1e-9, "a planar midpoint stays on the parallel");

            var ours = Along(line, 0.5, 0).getCoordinate();

            ours.getX().Should().BeApproximately(30, 1e-6);
            ours.getY().Should().BeApproximately(63.435, 0.01);
        }

        /// <summary>
        /// The fraction runs from one end to the other.
        /// </summary>
        [TestMethod]
        public void ShouldRunFromEndToEnd()
        {
            const string line = "LINESTRING(0 0, 10 0)";

            Along(line, 0, 0).getCoordinate().getX().Should().BeApproximately(0, 1e-9);
            Along(line, 1, 0).getCoordinate().getX().Should().BeApproximately(10, 1e-9);
            Along(line, 0.25, 0).getCoordinate().getX().Should().BeApproximately(2.5, 0.01);
        }

        /// <summary>
        /// The offset is metres, to the left of the way the line is going.
        /// </summary>
        [TestMethod]
        public void ShouldOffsetInMetresToTheLeft()
        {
            const string line = "LINESTRING(0 0, 10 0)";

            var left = Along(line, 0.5, 10000).getCoordinate();
            var right = Along(line, 0.5, -10000).getCoordinate();

            left.getY().Should().BeGreaterThan(0, "left of due east is north");
            right.getY().Should().BeLessThan(0);

            var middle = Wkt("POINT(5 0)");
            GeographyFunctions.Distance(middle, Wkt($"POINT({left.getX()} {left.getY()})"))!.doubleValue()
                .Should().BeApproximately(10000, 1);
        }

        /// <summary>
        /// One point for every segment, of every part, as Calcite answers.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerOnePointPerSegment()
        {
            Along("LINESTRING(0 0, 1 0, 2 0, 3 0)", 0.5, 0).getNumGeometries().Should().Be(3);
            Along("MULTILINESTRING((0 0, 1 0), (5 0, 6 0))", 0.5, 0).getNumGeometries().Should().Be(2);
        }

        /// <summary>
        /// A shape's width is measured between great circles, not between lines drawn in degrees.
        /// </summary>
        /// <remarks>
        /// The narrow way across this sliver is north to south, and its two long edges bow. A planar diameter
        /// measures the gap between two horizontal lines and gets the full two degrees; the geodesic one is
        /// narrower, because the southern edge bows up into the shape.
        /// </remarks>
        [TestMethod]
        public void ShouldMeasureWidthBetweenGreatCircles()
        {
            var sliver = Wkt("POLYGON((0 60, 60 60, 60 62, 0 62, 0 60))");

            var diameter = GeographyFunctions.MinimumDiameter(sliver)!;
            diameter.isEmpty().Should().BeFalse();

            var width = GeographyFunctions.Length(diameter)!.doubleValue();
            var twoDegrees = GeographyFunctions.Distance(Wkt("POINT(0 60)"), Wkt("POINT(0 62)"))!.doubleValue();

            width.Should().BeGreaterThan(0);
            width.Should().BeLessThan(twoDegrees, "the southern edge bows north into the shape");
        }

        /// <summary>
        /// The diameter of a square is its side, near enough.
        /// </summary>
        [TestMethod]
        public void ShouldMeasureASquareAcrossItsSide()
        {
            var square = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var width = GeographyFunctions.Length(GeographyFunctions.MinimumDiameter(square)!)!.doubleValue();
            var side = GeographyFunctions.Distance(Wkt("POINT(0 0)"), Wkt("POINT(0 1)"))!.doubleValue();

            width.Should().BeApproximately(side, side * 0.01);
        }

        /// <summary>
        /// A shape with no width has no diameter to name.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerNothingForSomethingWithNoWidth()
        {
            GeographyFunctions.MinimumDiameter(Wkt("POINT(1 2)"))!.isEmpty().Should().BeTrue();
            GeographyFunctions.MinimumDiameter(Wkt("LINESTRING(0 0, 1 0)"))!.isEmpty().Should().BeTrue();
        }

        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.LocateAlong(null, java.lang.Double.valueOf(0.5), java.lang.Double.valueOf(0)).Should().BeNull();
            GeographyFunctions.LocateAlong(Wkt("LINESTRING(0 0, 1 0)"), null, java.lang.Double.valueOf(0)).Should().BeNull();
            GeographyFunctions.MinimumDiameter(null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampBothWithWgs84()
        {
            Along("LINESTRING(0 0, 1 0)", 0.5, 0).getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.MinimumDiameter(Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            var along = GeographyExecutionTests.Run(
                // through the centroid, a multi-point of one being its own centre, because ST_GEOG_Y wants a point
                "SELECT ST_GEOG_Y(ST_GEOG_CENTROID(ST_GEOG_LOCATEALONG(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 60, 60 60)'), 0.5, 0.0)))")[0][0];
            (along is java.lang.Number a ? a.doubleValue() : double.NaN).Should().BeApproximately(63.435, 0.01);

            var width = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_LENGTH(ST_GEOG_MINIMUMDIAMETER(ST_GEOG_GEOMFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')))")[0][0];
            (width is java.lang.Number w ? w.doubleValue() : double.NaN).Should().BeGreaterThan(0);
        }

    }

}
