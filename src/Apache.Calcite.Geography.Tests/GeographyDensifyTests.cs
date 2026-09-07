using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Putting points on a geodesic: <c>ST_GEOG_DENSIFY</c> along one, <c>ST_GEOG_PROJECTPOINT</c> onto one.
    /// </summary>
    /// <remarks>
    /// Densifying is usually done to hand a planar consumer something that follows the true path — a map, an
    /// index, a store that only draws straight lines. Calcite's inserts its vertices along a straight line in
    /// degrees, which is exactly the path that consumer would have drawn anyway, so the operation buys
    /// nothing. These vertices are on the geodesic.
    ///
    /// <para>Between two points on a parallel away from the equator a geodesic bows poleward, and that is
    /// what every test here turns on. It is not a small effect: across ten degrees of longitude at 60°N the
    /// bow is about seven kilometres.</para>
    /// </remarks>
    [TestClass]
    public class GeographyDensifyTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        /// <summary>
        /// The inserted vertices lie north of the parallel the two ends sit on; Calcite's lie on it.
        /// </summary>
        [TestMethod]
        public void ShouldFollowTheGeodesicRatherThanTheParallel()
        {
            var line = Wkt("LINESTRING(0 60, 10 60)");

            var theirs = SpatialTypeFunctions.ST_Densify(line, java.math.BigDecimal.valueOf(1.0));
            foreach (var c in theirs.getCoordinates())
                c.getY().Should().BeApproximately(60, 1e-9, "a planar densify stays on the parallel");

            var ours = GeographyFunctions.Densify(line, java.lang.Double.valueOf(100000.0))!;
            var latitudes = ours.getCoordinates();

            latitudes.Length.Should().BeGreaterThan(2);
            latitudes[latitudes.Length / 2].getY().Should().BeGreaterThan(60.0);
        }

        /// <summary>
        /// No edge is left longer than the distance asked for.
        /// </summary>
        [TestMethod]
        public void ShouldLeaveNoEdgeLongerThanAsked()
        {
            var densified = GeographyFunctions.Densify(Wkt("LINESTRING(0 0, 10 0)"), java.lang.Double.valueOf(100000.0))!;
            var coordinates = densified.getCoordinates();

            for (var i = 0; i < coordinates.Length - 1; i++)
            {
                var edge = GeographyFunctions.Length(
                    Wkt($"LINESTRING({coordinates[i].getX()} {coordinates[i].getY()}, {coordinates[i + 1].getX()} {coordinates[i + 1].getY()})"))!;

                edge.doubleValue().Should().BeLessThanOrEqualTo(100000.0 + 1e-6);
            }
        }

        /// <summary>
        /// An edge already short enough is left alone, and so is a point.
        /// </summary>
        [TestMethod]
        public void ShouldLeaveWhatIsAlreadyShortEnough()
        {
            var line = Wkt("LINESTRING(0 0, 0.001 0)");

            GeographyFunctions.Densify(line, java.lang.Double.valueOf(1000000.0))!.getNumPoints().Should().Be(2);
            GeographyFunctions.Densify(Wkt("POINT(1 2)"), java.lang.Double.valueOf(1.0))!.toText().Should().Be("POINT (1 2)");
        }

        /// <summary>
        /// A polygon's rings are walked too, and the result is still a polygon.
        /// </summary>
        [TestMethod]
        public void ShouldWalkEveryRingOfAPolygon()
        {
            var polygon = Wkt("POLYGON((0 50, 10 50, 10 55, 0 55, 0 50))");
            var densified = GeographyFunctions.Densify(polygon, java.lang.Double.valueOf(100000.0))!;

            densified.getGeometryType().Should().Be("Polygon");
            densified.getNumPoints().Should().BeGreaterThan(polygon.getNumPoints());
        }

        /// <summary>
        /// A point projected onto a line lands on the geodesic, not on the parallel.
        /// </summary>
        [TestMethod]
        public void ShouldProjectOntoTheGeodesic()
        {
            var line = Wkt("LINESTRING(-10 60, 10 60)");
            var projected = GeographyFunctions.ProjectPoint(Wkt("POINT(0 0)"), line)!;

            projected.getCoordinate().getX().Should().BeApproximately(0, 1e-9);
            projected.getCoordinate().getY().Should().BeGreaterThan(60.0);
        }

        /// <summary>
        /// Nothing of more than one dimension is projected onto, as Calcite declines the same way.
        /// </summary>
        [TestMethod]
        public void ShouldDeclineToProjectOntoAnArea()
        {
            var polygon = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");

            SpatialTypeFunctions.ST_ProjectPoint(Wkt("POINT(5 5)"), polygon).Should().BeNull();
            GeographyFunctions.ProjectPoint(Wkt("POINT(5 5)"), polygon).Should().BeNull();
        }

        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.Densify(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Densify(Wkt("POINT(0 0)"), null).Should().BeNull();
            GeographyFunctions.ProjectPoint(null, Wkt("LINESTRING(0 0, 1 1)")).Should().BeNull();
            GeographyFunctions.ProjectPoint(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampBothWithWgs84()
        {
            GeographyFunctions.Densify(Wkt("LINESTRING(0 0, 10 0)"), java.lang.Double.valueOf(100000.0))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);

            GeographyFunctions.ProjectPoint(Wkt("POINT(0 0)"), Wkt("LINESTRING(1 -1, 1 1)"))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            GeographyExecutionTests.Run("SELECT ST_GEOG_NUMPOINTS(ST_GEOG_DENSIFY(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 10 0)'), 100000.0))")[0][0]
                .Should().BeAssignableTo<java.lang.Number>().Which.intValue().Should().BeGreaterThan(2);

            GeographyExecutionTests.Run("SELECT ST_GEOG_ASTEXT(ST_GEOG_PROJECTPOINT(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('LINESTRING(1 -1, 1 1)')))")[0][0]
                .Should().Be("POINT (1 0)");
        }

    }

}
