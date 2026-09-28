using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_BOUNDINGCIRCLE</c>, whose radius is a distance on the ellipsoid rather than in
    /// degrees.
    /// </summary>
    /// <remarks>
    /// The centre is found iteratively and the radius is then measured from it, so the circle always contains
    /// the shape and may be slightly larger than the smallest one that would. The tests hold containment
    /// exactly and minimality within a tolerance.
    /// </remarks>
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
        /// The circle covers the geometry it was built from, edges included.
        /// </summary>
        [Fact]
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
        /// The circle around two points is centred on the midpoint of the geodesic between them, with a radius
        /// of half its length.
        /// </summary>
        [Fact]
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
        /// Two shapes of the same size on the ground get circles of the same area, whatever their latitude.
        /// </summary>
        /// <remarks>
        /// A planar bounding circle, measured in degrees, would give the pair at 60 degrees north a smaller
        /// area.
        /// </remarks>
        [Fact]
        public void ShouldBoundTheSameSizeAtEveryLatitude()
        {
            // A degree of longitude at 60 north is about half of one at the equator, so the two pairs are
            // about the same width on the ground.
            var equator = GeographyFunctions.Area(Circle("MULTIPOINT((-1 0), (1 0))"))!.doubleValue();
            var north = GeographyFunctions.Area(Circle("MULTIPOINT((-2 60), (2 60))"))!.doubleValue();

            north.Should().BeApproximately(equator, equator * 0.02);
        }

        /// <summary>
        /// The circle's area is within ten percent of the least possible.
        /// </summary>
        /// <remarks>
        /// For two points the least radius is half the distance between them. The centre is approximate and
        /// the 32-sided ring is drawn slightly outside the radius so that its edges do not cut inside it, so
        /// the area is expected to be a few percent over.
        /// </remarks>
        [Fact]
        public void ShouldNotBeMuchLargerThanItMustBe()
        {
            var span = GeographyFunctions.Distance(Wkt("POINT(-1 0)"), Wkt("POINT(1 0)"))!.doubleValue();
            var least = Math.PI * Math.Pow(span / 2, 2);

            var area = GeographyFunctions.Area(Circle("MULTIPOINT((-1 0), (1 0))"))!.doubleValue();

            area.Should().BeGreaterThan(least * 0.99);
            area.Should().BeLessThan(least * 1.1);
        }

        /// <summary>
        /// A single point gives that point, an empty geometry gives an empty polygon, and null gives null.
        /// </summary>
        [Fact]
        public void ShouldDegenerateWhereThereIsNothingToBound()
        {
            Circle("POINT(1 2)").getGeometryType().Should().Be("Point");
            Circle("POINT EMPTY").isEmpty().Should().BeTrue();
            GeographyFunctions.BoundingCircle(null).Should().BeNull();
        }

        [Fact]
        public void ShouldStampTheCircleWithWgs84()
        {
            Circle("MULTIPOINT((0 0), (1 1))").getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [Fact]
        public void ShouldRunAsAnOperator()
        {
            var area = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_AREA(CLR_ST_GEOG_BOUNDINGCIRCLE(CLR_ST_GEOG_GEOMFROMTEXT('MULTIPOINT((0 0), (1 0))')))")[0][0];

            (area is java.lang.Number n ? n.doubleValue() : double.NaN).Should().BeGreaterThan(0);
        }

    }

}
