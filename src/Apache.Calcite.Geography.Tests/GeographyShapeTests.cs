using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Two shapes defined by a distance, which is where degrees and metres part company.
    /// </summary>
    /// <remarks>
    /// An offset curve is meant to run at a constant distance from a line and an ellipse is meant to be the
    /// width and height it says. Neither is true of the planar versions anywhere but the equator: a degree
    /// north is not a degree east, so a planar offset is a different distance depending on which way the line
    /// happens to run, and a planar ellipse of equal width and height is a circle on the map and never on the
    /// ground.
    /// </remarks>
    [TestClass]
    public class GeographyShapeTests
    {

        const double Kilometre = 1000.0;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static double Apart(Geometry a, Geometry b)
        {
            return GeographyFunctions.Distance(a, b)!.doubleValue();
        }

        /// <summary>
        /// The offset runs at the distance asked for, whichever way the line goes.
        /// </summary>
        /// <remarks>
        /// The test the planar version fails. An east–west line and a north–south line offset by the same
        /// number of degrees end up different distances away; offset by the same number of metres they do
        /// not.
        /// </remarks>
        [TestMethod]
        public void ShouldOffsetTheSameDistanceWhicheverWayTheLineRuns()
        {
            foreach (var line in new[] { "LINESTRING(-1 0, 1 0)", "LINESTRING(0 -1, 0 1)" })
            {
                var offset = GeographyFunctions.OffsetCurve(Wkt(line), java.lang.Double.valueOf(10 * Kilometre))!;

                Apart(Wkt(line), offset).Should().BeApproximately(10 * Kilometre, 50, line);
            }
        }

        /// <summary>
        /// A positive distance goes left of the way the line is going, a negative one right.
        /// </summary>
        [TestMethod]
        public void ShouldOffsetLeftForAPositiveDistance()
        {
            var line = Wkt("LINESTRING(-1 0, 1 0)");

            var left = GeographyFunctions.OffsetCurve(line, java.lang.Double.valueOf(10 * Kilometre))!;
            var right = GeographyFunctions.OffsetCurve(line, java.lang.Double.valueOf(-10 * Kilometre))!;

            left.getCoordinate().getY().Should().BeGreaterThan(0, "left of due east is north");
            right.getCoordinate().getY().Should().BeLessThan(0);
        }

        /// <summary>
        /// The offset keeps the shape of the line: as many vertices, in the same order.
        /// </summary>
        [TestMethod]
        public void ShouldKeepTheVerticesOfTheLine()
        {
            var line = Wkt("LINESTRING(0 0, 1 0, 2 1)");
            var offset = GeographyFunctions.OffsetCurve(line, java.lang.Double.valueOf(Kilometre))!;

            offset.getGeometryType().Should().Be("LineString");
            offset.getNumPoints().Should().Be(line.getNumPoints());
        }

        /// <summary>
        /// Only a line is offset, as Calcite accepts only a line.
        /// </summary>
        [TestMethod]
        public void ShouldDeclineToOffsetAnythingButALine()
        {
            GeographyFunctions.OffsetCurve(Wkt("POINT(0 0)"), java.lang.Double.valueOf(1000)).Should().BeNull();
            GeographyFunctions.OffsetCurve(Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"), java.lang.Double.valueOf(1000)).Should().BeNull();
        }

        /// <summary>
        /// An ellipse is the width and height it says, in metres.
        /// </summary>
        [TestMethod]
        public void ShouldMeasureTheEllipseInMetres()
        {
            var ellipse = GeographyFunctions.MakeEllipse(
                Wkt("POINT(0 45)"), java.lang.Double.valueOf(100 * Kilometre), java.lang.Double.valueOf(50 * Kilometre))!;

            var centre = Wkt("POINT(0 45)");
            var box = ellipse.getEnvelopeInternal();

            var eastmost = Wkt($"POINT({box.getMaxX()} 45)");
            var northmost = Wkt($"POINT(0 {box.getMaxY()})");

            Apart(centre, eastmost).Should().BeApproximately(50 * Kilometre, 200, "half the width");
            Apart(centre, northmost).Should().BeApproximately(25 * Kilometre, 200, "half the height");
        }

        /// <summary>
        /// Equal width and height is a circle on the ground, which a planar one is not.
        /// </summary>
        /// <remarks>
        /// At 60 degrees north a degree of longitude is half a degree of latitude on the ground. Calcite's
        /// ellipse of equal width and height in degrees is therefore twice as wide as it is tall in metres;
        /// this one is round.
        /// </remarks>
        [TestMethod]
        public void ShouldBeRoundOnTheGroundRatherThanOnTheMap()
        {
            var centre = Wkt("POINT(0 60)");

            var ours = GeographyFunctions.MakeEllipse(centre, java.lang.Double.valueOf(40 * Kilometre), java.lang.Double.valueOf(40 * Kilometre))!;
            var ourBox = ours.getEnvelopeInternal();

            var ourWidth = Apart(Wkt($"POINT({ourBox.getMinX()} 60)"), Wkt($"POINT({ourBox.getMaxX()} 60)"));
            var ourHeight = Apart(Wkt($"POINT(0 {ourBox.getMinY()})"), Wkt($"POINT(0 {ourBox.getMaxY()})"));

            ourWidth.Should().BeApproximately(ourHeight, ourHeight * 0.02, "round on the ground");

            var theirs = SpatialTypeFunctions.ST_MakeEllipse(centre, java.math.BigDecimal.valueOf(0.5), java.math.BigDecimal.valueOf(0.5))!;
            var theirBox = theirs.getEnvelopeInternal();

            var theirWidth = Apart(Wkt($"POINT({theirBox.getMinX()} 60)"), Wkt($"POINT({theirBox.getMaxX()} 60)"));
            var theirHeight = Apart(Wkt($"POINT(0 {theirBox.getMinY()})"), Wkt($"POINT(0 {theirBox.getMaxY()})"));

            theirWidth.Should().BeLessThan(theirHeight * 0.6, "equal degrees is not equal metres at 60 north");
        }

        /// <summary>
        /// Only a point has an ellipse about it, as Calcite decides too.
        /// </summary>
        [TestMethod]
        public void ShouldDeclineToMakeAnEllipseAboutAnythingButAPoint()
        {
            SpatialTypeFunctions.ST_MakeEllipse(Wkt("LINESTRING(0 0, 1 1)"), java.math.BigDecimal.ONE, java.math.BigDecimal.ONE)
                .Should().BeNull();

            GeographyFunctions.MakeEllipse(Wkt("LINESTRING(0 0, 1 1)"), java.lang.Double.valueOf(1000), java.lang.Double.valueOf(1000))
                .Should().BeNull();
        }

        [TestMethod]
        public void ShouldAnswerNothingForNothingToDraw()
        {
            GeographyFunctions.MakeEllipse(Wkt("POINT(0 0)"), java.lang.Double.valueOf(0), java.lang.Double.valueOf(1000))!
                .isEmpty().Should().BeTrue();

            GeographyFunctions.OffsetCurve(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.MakeEllipse(Wkt("POINT(0 0)"), null, java.lang.Double.valueOf(1)).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampBothWithWgs84()
        {
            GeographyFunctions.OffsetCurve(Wkt("LINESTRING(0 0, 1 0)"), java.lang.Double.valueOf(1000))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);

            GeographyFunctions.MakeEllipse(Wkt("POINT(0 0)"), java.lang.Double.valueOf(1000), java.lang.Double.valueOf(1000))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            var offset = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_LENGTH(ST_GEOG_OFFSETCURVE(ST_GEOG_GEOMFROMTEXT('LINESTRING(-1 0, 1 0)'), 10000.0))")[0][0];
            (offset is java.lang.Number a ? a.doubleValue() : double.NaN).Should().BeGreaterThan(0);

            var ellipse = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_AREA(ST_GEOG_MAKEELLIPSE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 20000.0, 10000.0))")[0][0];
            (ellipse is java.lang.Number b ? b.doubleValue() : double.NaN).Should().BeGreaterThan(0);
        }

    }

}
