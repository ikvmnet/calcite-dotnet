using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_BUFFER</c>, which buffers by a distance in metres.
    /// </summary>
    /// <remarks>
    /// The buffer is the union of the shape with circles drawn around points along it, so these tests check
    /// the definition as a property: a place nearer than the distance is inside, and a place further is
    /// not. Calcite's <c>ST_BUFFER</c> buffers by degrees, which covers less ground further from the equator.
    /// </remarks>
    public class GeographyBufferTests
    {

        const double Kilometre = 1000.0;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static Geometry Buffer(string wkt, double metres)
        {
            return GeographyFunctions.Buffer(Wkt(wkt), java.lang.Double.valueOf(metres))!;
        }

        static bool Contains(Geometry area, string point)
        {
            return GeographyFunctions.Contains(area, Wkt(point))!.booleanValue();
        }

        /// <summary>
        /// The buffer contains places nearer than the distance and not places further, in every direction.
        /// </summary>
        /// <remarks>
        /// The inner probe is at 0.9 of the distance because the ring is a polygon inscribed in the circle and
        /// falls slightly inside it between vertices.
        /// </remarks>
        [Fact]
        public void ShouldContainWhatIsNearerAndNotWhatIsFurther()
        {
            var buffer = Buffer("POINT(0 0)", 100 * Kilometre);
            var centre = new org.locationtech.jts.geom.Coordinate(0, 0);

            for (var azimuth = 0; azimuth < 360; azimuth += 45)
            {
                var near = Wgs84.Offset(centre, azimuth, 90 * Kilometre);
                var far = Wgs84.Offset(centre, azimuth, 105 * Kilometre);

                Contains(buffer, $"POINT({near.getX()} {near.getY()})").Should().BeTrue($"90km, azimuth {azimuth}");
                Contains(buffer, $"POINT({far.getX()} {far.getY()})").Should().BeFalse($"105km, azimuth {azimuth}");
            }
        }

        /// <summary>
        /// A point's buffer has slightly less than the area of a disc of that radius.
        /// </summary>
        /// <remarks>
        /// A geodesic disc of radius r covers about πr² while r is small beside the Earth. The ring is a
        /// 32-sided polygon inscribed in the circle, which loses about six parts in a thousand.
        /// </remarks>
        [Fact]
        public void ShouldCoverAboutTheAreaOfADisc()
        {
            var area = GeographyFunctions.Area(Buffer("POINT(0 0)", 100 * Kilometre))!.doubleValue();
            var disc = Math.PI * Math.Pow(100 * Kilometre, 2);

            area.Should().BeLessThan(disc);
            area.Should().BeGreaterThan(disc * 0.98);
        }

        /// <summary>
        /// A buffer of the same distance covers the same area at any latitude, where Calcite's shrinks.
        /// </summary>
        /// <remarks>
        /// At 60 degrees a degree of longitude is half what it is at the equator, so a buffer of a fixed
        /// number of degrees covers less ground there.
        /// </remarks>
        [Fact]
        public void ShouldCoverTheSameGroundAtEveryLatitude()
        {
            var equator = GeographyFunctions.Area(Buffer("POINT(0 0)", 50 * Kilometre))!.doubleValue();
            var north = GeographyFunctions.Area(Buffer("POINT(0 60)", 50 * Kilometre))!.doubleValue();

            north.Should().BeApproximately(equator, equator * 0.01);

            // Calcite's, buffered by half a degree at each latitude, do not.
            var theirsAtEquator = GeographyFunctions.Area(SpatialTypeFunctions.ST_Buffer(Wkt("POINT(0 0)"), 0.5))!.doubleValue();
            var theirsAtNorth = GeographyFunctions.Area(SpatialTypeFunctions.ST_Buffer(Wkt("POINT(0 60)"), 0.5))!.doubleValue();

            theirsAtNorth.Should().BeLessThan(theirsAtEquator * 0.75);
        }

        /// <summary>
        /// A line's buffer contains the whole line, not only its vertices.
        /// </summary>
        [Fact]
        public void ShouldBufferEveryPartOfALine()
        {
            var buffer = Buffer("LINESTRING(0 0, 1 0)", 10 * Kilometre);

            Contains(buffer, "POINT(0 0)").Should().BeTrue();
            Contains(buffer, "POINT(0.5 0)").Should().BeTrue("the middle of an edge is buffered, not only its ends");
            Contains(buffer, "POINT(1 0)").Should().BeTrue();
            Contains(buffer, "POINT(0.5 0.5)").Should().BeFalse("that is some fifty kilometres away");
        }

        /// <summary>
        /// A polygon's buffer covers the polygon's interior as well as a band around its boundary.
        /// </summary>
        [Fact]
        public void ShouldContainTheAreaItBuffers()
        {
            var square = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var buffer = GeographyFunctions.Buffer(square, java.lang.Double.valueOf(5 * Kilometre))!;

            GeographyFunctions.Covers(buffer, square)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Area(buffer)!.doubleValue()
                .Should().BeGreaterThan(GeographyFunctions.Area(square)!.doubleValue());
        }

        /// <summary>
        /// Doubling the distance about quadruples a point's buffer area.
        /// </summary>
        [Fact]
        public void ShouldGrowWithTheDistance()
        {
            var small = GeographyFunctions.Area(Buffer("POINT(0 0)", 10 * Kilometre))!.doubleValue();
            var large = GeographyFunctions.Area(Buffer("POINT(0 0)", 20 * Kilometre))!.doubleValue();

            large.Should().BeApproximately(small * 4, small * 4 * 0.01, "area goes as the square of the radius");
        }

        /// <summary>
        /// A distance of zero or less, or an empty geometry, gives an empty polygon; a null argument gives null.
        /// </summary>
        [Fact]
        public void ShouldAnswerEmptyForNothingToDo()
        {
            Buffer("POINT(0 0)", 0).isEmpty().Should().BeTrue();
            Buffer("POINT(0 0)", -1).isEmpty().Should().BeTrue();
            Buffer("POINT EMPTY", 1000).isEmpty().Should().BeTrue();

            GeographyFunctions.Buffer(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Buffer(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        [Fact]
        public void ShouldStampTheBufferWithWgs84()
        {
            Buffer("POINT(0 0)", 1000).getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [Fact]
        public void ShouldRunAsAnOperator()
        {
            var answer = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_AREA(CLR_ST_GEOG_BUFFER(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 10000.0))")[0][0];

            (answer is java.lang.Number n ? n.doubleValue() : double.NaN)
                .Should().BeApproximately(Math.PI * 1e8, Math.PI * 1e8 * 0.02);
        }

    }

}
