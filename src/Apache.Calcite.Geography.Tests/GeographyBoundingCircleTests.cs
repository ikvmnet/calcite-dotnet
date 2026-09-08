using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The smallest circle containing a geography, on the Earth rather than on a map.
    /// </summary>
    /// <remarks>
    /// A planar smallest circle measures its radius in degrees, so what it draws is an ellipse on the ground
    /// everywhere off the equator, and the centre it picks is the one that minimises a distance nobody
    /// travels. This is a circle of constant distance about a centre chosen by distance.
    ///
    /// <para>Containment is exact and minimality is approximate, which is the deliberate way round: the
    /// centre is walked toward rather than solved for, and the radius is measured afterwards from wherever
    /// the walk settled. A circle a fraction of a percent too wide is a worse bound and still a bound; one a
    /// fraction too narrow is wrong.</para>
    /// </remarks>
    [TestClass]
    public class GeographyBoundingCircleTests
    {

        const double Kilometre = 1000.0;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static Geometry Circle(string wkt)
        {
            return GeographyFunctions.BoundingCircle(Wkt(wkt))!;
        }

        /// <summary>
        /// The circle holds what it was built from. Every vertex, and every edge between them.
        /// </summary>
        [TestMethod]
        public void ShouldContainWhatItBounds()
        {
            foreach (var wkt in new[]
            {
                "MULTIPOINT((0 0), (1 0), (0.5 1))",
                "POLYGON((0 50, 2 50, 2 52, 0 52, 0 50))",
                "LINESTRING(-10 60, 10 60, 10 65)",
                "MULTIPOINT((0 0), (0 80))",
            })
            {
                GeographyFunctions.Covers(Circle(wkt), Wkt(wkt))!.booleanValue()
                    .Should().BeTrue(wkt);
            }
        }

        /// <summary>
        /// Two points are bounded by the circle on the geodesic between them, centred at its middle.
        /// </summary>
        /// <remarks>
        /// The case with a known answer. The centre is the midpoint of the geodesic and the radius is half
        /// its length, so both can be checked against a measurement rather than against another circle.
        /// </remarks>
        [TestMethod]
        public void ShouldCentreTwoPointsOnTheirMidpoint()
        {
            var a = Wkt("POINT(-1 0)");
            var b = Wkt("POINT(1 0)");
            var circle = Circle("MULTIPOINT((-1 0), (1 0))");

            var centre = GeographyFunctions.Centroid(circle)!;
            centre.getCoordinate().getX().Should().BeApproximately(0, 0.01);
            centre.getCoordinate().getY().Should().BeApproximately(0, 0.01);

            var half = GeographyFunctions.Distance(a, b)!.doubleValue() / 2;
            var reach = GeographyFunctions.Distance(centre, a)!.doubleValue();

            reach.Should().BeApproximately(half, half * 0.01);
        }

        /// <summary>
        /// The radius is metres, so the circle is the same size wherever the shape sits.
        /// </summary>
        /// <remarks>
        /// The one that separates the readings. Two shapes of the same size on the ground, one at the equator
        /// and one at sixty north, get circles of the same area. A planar bounding circle would give the
        /// northern one a smaller area, its degrees being worth less there.
        /// </remarks>
        [TestMethod]
        public void ShouldBoundTheSameSizeAtEveryLatitude()
        {
            // a degree of longitude at 60 north is half of one at the equator, so this pair is the same
            // width on the ground
            var equator = GeographyFunctions.Area(Circle("MULTIPOINT((-1 0), (1 0))"))!.doubleValue();
            var north = GeographyFunctions.Area(Circle("MULTIPOINT((-2 60), (2 60))"))!.doubleValue();

            north.Should().BeApproximately(equator, equator * 0.02);
        }

        /// <summary>
        /// A circle is not much larger than it must be.
        /// </summary>
        /// <remarks>
        /// For two points the least possible radius is half the distance between them, so the circle's area
        /// has a floor of π times that squared. The walk stops a little short of the exact centre and the ring
        /// is drawn a little wide to hold the shape between its vertices, so a few percent over is expected;
        /// far more than that would mean the walk was not converging.
        /// </remarks>
        [TestMethod]
        public void ShouldNotBeMuchLargerThanItMustBe()
        {
            var span = GeographyFunctions.Distance(Wkt("POINT(-1 0)"), Wkt("POINT(1 0)"))!.doubleValue();
            var least = Math.PI * Math.Pow(span / 2, 2);

            var area = GeographyFunctions.Area(Circle("MULTIPOINT((-1 0), (1 0))"))!.doubleValue();

            area.Should().BeGreaterThan(least * 0.99);
            area.Should().BeLessThan(least * 1.1);
        }

        /// <summary>
        /// One point has no width, and nothing has no circle.
        /// </summary>
        [TestMethod]
        public void ShouldDegenerateWhereThereIsNothingToBound()
        {
            Circle("POINT(1 2)").getGeometryType().Should().Be("Point");
            Circle("POINT EMPTY").isEmpty().Should().BeTrue();
            GeographyFunctions.BoundingCircle(null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampTheCircleWithWgs84()
        {
            Circle("MULTIPOINT((0 0), (1 1))").getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunAsAnOperator()
        {
            var area = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_AREA(ST_GEOG_BOUNDINGCIRCLE(ST_GEOG_GEOMFROMTEXT('MULTIPOINT((0 0), (1 0))')))")[0][0];

            (area is java.lang.Number n ? n.doubleValue() : double.NaN).Should().BeGreaterThan(0);
        }

    }

}
