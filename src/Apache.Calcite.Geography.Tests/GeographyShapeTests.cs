using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_OFFSETCURVE</c> and <c>CLR_ST_GEOG_MAKEELLIPSE</c>, whose distances are in metres.
    /// </summary>
    /// <remarks>
    /// Away from the equator a degree north is not a degree east, so Calcite's planar offset depends on the
    /// line's direction and its ellipse of equal width and height is not round on the ground.
    /// </remarks>
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
        /// An east–west line and a north–south line are offset by the same distance in metres.
        /// </summary>
        [Fact]
        public void ShouldOffsetTheSameDistanceWhicheverWayTheLineRuns()
        {
            foreach (var line in new[] { "LINESTRING(-1 0, 1 0)", "LINESTRING(0 -1, 0 1)" })
            {
                var offset = GeographyFunctions.OffsetCurve(Wkt(line), java.lang.Double.valueOf(10 * Kilometre))!;

                Apart(Wkt(line), offset).Should().BeApproximately(10 * Kilometre, 50, line);
            }
        }

        /// <summary>
        /// A positive distance offsets to the left of the line's direction, a negative one to the right.
        /// </summary>
        [Fact]
        public void ShouldOffsetLeftForAPositiveDistance()
        {
            var line = Wkt("LINESTRING(-1 0, 1 0)");

            var left = GeographyFunctions.OffsetCurve(line, java.lang.Double.valueOf(10 * Kilometre))!;
            var right = GeographyFunctions.OffsetCurve(line, java.lang.Double.valueOf(-10 * Kilometre))!;

            left.getCoordinate().getY().Should().BeGreaterThan(0, "left of due east is north");
            right.getCoordinate().getY().Should().BeLessThan(0);
        }

        /// <summary>
        /// The offset is a line with as many vertices as the original.
        /// </summary>
        [Fact]
        public void ShouldKeepTheVerticesOfTheLine()
        {
            var line = Wkt("LINESTRING(0 0, 1 0, 2 1)");
            var offset = GeographyFunctions.OffsetCurve(line, java.lang.Double.valueOf(Kilometre))!;

            offset.getGeometryType().Should().Be("LineString");
            offset.getNumPoints().Should().Be(line.getNumPoints());
        }

        /// <summary>
        /// Offsetting anything but a line returns null.
        /// </summary>
        [Fact]
        public void ShouldDeclineToOffsetAnythingButALine()
        {
            GeographyFunctions.OffsetCurve(Wkt("POINT(0 0)"), java.lang.Double.valueOf(1000)).Should().BeNull();
            GeographyFunctions.OffsetCurve(Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"), java.lang.Double.valueOf(1000)).Should().BeNull();
        }

        /// <summary>
        /// An ellipse has the width and height given, in metres.
        /// </summary>
        [Fact]
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
        /// An ellipse of equal width and height is round on the ground; Calcite's is not.
        /// </summary>
        /// <remarks>
        /// At 60 degrees north a degree of longitude is about half a degree of latitude on the ground, so
        /// Calcite's ellipse of equal width and height in degrees is about half as wide as it is tall in
        /// metres.
        /// </remarks>
        [Fact]
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
        /// An ellipse about anything but a point is null, as in Calcite.
        /// </summary>
        [Fact]
        public void ShouldDeclineToMakeAnEllipseAboutAnythingButAPoint()
        {
            SpatialTypeFunctions.ST_MakeEllipse(Wkt("LINESTRING(0 0, 1 1)"), java.math.BigDecimal.ONE, java.math.BigDecimal.ONE)
                .Should().BeNull();

            GeographyFunctions.MakeEllipse(Wkt("LINESTRING(0 0, 1 1)"), java.lang.Double.valueOf(1000), java.lang.Double.valueOf(1000))
                .Should().BeNull();
        }

        [Fact]
        public void ShouldAnswerNothingForNothingToDraw()
        {
            GeographyFunctions.MakeEllipse(Wkt("POINT(0 0)"), java.lang.Double.valueOf(0), java.lang.Double.valueOf(1000))!
                .isEmpty().Should().BeTrue();

            GeographyFunctions.OffsetCurve(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.MakeEllipse(Wkt("POINT(0 0)"), null, java.lang.Double.valueOf(1)).Should().BeNull();
        }

        [Fact]
        public void ShouldStampBothWithWgs84()
        {
            GeographyFunctions.OffsetCurve(Wkt("LINESTRING(0 0, 1 0)"), java.lang.Double.valueOf(1000))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);

            GeographyFunctions.MakeEllipse(Wkt("POINT(0 0)"), java.lang.Double.valueOf(1000), java.lang.Double.valueOf(1000))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [Fact]
        public void ShouldRunEachAsAnOperator()
        {
            var offset = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_LENGTH(CLR_ST_GEOG_OFFSETCURVE(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(-1 0, 1 0)'), 10000.0))")[0][0];
            (offset is java.lang.Number a ? a.doubleValue() : double.NaN).Should().BeGreaterThan(0);

            var ellipse = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_AREA(CLR_ST_GEOG_MAKEELLIPSE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 20000.0, 10000.0))")[0][0];
            (ellipse is java.lang.Number b ? b.doubleValue() : double.NaN).Should().BeGreaterThan(0);
        }

        /// <summary>
        /// A closed line offsets to a closed line with the same number of vertices.
        /// </summary>
        /// <remarks>
        /// If the shared first and last vertex were treated as two ends, the first would take its direction
        /// from the edge leaving it and the last from the edge arriving, and the offset would not close.
        /// </remarks>
        [Fact]
        public void ShouldOffsetAClosedLineToAClosedLine()
        {
            var ring = Wkt("LINESTRING(-1 -1, 1 -1, 1 1, -1 1, -1 -1)");
            var offset = (org.locationtech.jts.geom.LineString)GeographyFunctions.OffsetCurve(ring, java.lang.Double.valueOf(10 * Kilometre))!;

            offset.isClosed().Should().BeTrue("the line it was drawn beside is closed");
            offset.getNumPoints().Should().Be(ring.getNumPoints(), "a vertex is carried, not added or dropped");
        }

        /// <summary>
        /// A ring's shared first vertex is offset as an interior vertex.
        /// </summary>
        /// <remarks>
        /// A closed result alone would not show this, since the shared vertex could be moved the same wrong
        /// way twice. The same corner is offset as the start of a ring and as the interior vertex of an open
        /// line, and must land in the same place.
        ///
        /// <para>A vertex is moved the full distance perpendicular to the direction from its predecessor to
        /// its successor, so a right-angled corner ends up the distance times cos 45° from either edge; the
        /// function documents that smoothing.</para>
        /// </remarks>
        [Fact]
        public void ShouldCarryTheSharedVertexAsAnInteriorOne()
        {
            var ring = Wkt("LINESTRING(-1 -1, 1 -1, 1 1, -1 1, -1 -1)");
            var open = Wkt("LINESTRING(-1 1, -1 -1, 1 -1)");

            var offsetRing = (org.locationtech.jts.geom.LineString)GeographyFunctions.OffsetCurve(ring, java.lang.Double.valueOf(10 * Kilometre))!;
            var offsetOpen = (org.locationtech.jts.geom.LineString)GeographyFunctions.OffsetCurve(open, java.lang.Double.valueOf(10 * Kilometre))!;

            var shared = offsetRing.getCoordinateN(0);
            var interior = offsetOpen.getCoordinateN(1);

            shared.x.Should().BeApproximately(interior.x, 1e-9, "the corner at (-1,-1) is the same corner in both");
            shared.y.Should().BeApproximately(interior.y, 1e-9, "the corner at (-1,-1) is the same corner in both");
        }


    }

}
