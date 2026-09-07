using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The buffer, which is the one operation here that S2 does not have.
    /// </summary>
    /// <remarks>
    /// Everything else geodesic in this package turned out to be an S2 call. S2's Java release has no buffer,
    /// so this one is built from the definition — the set of places within a distance of the shape — and its
    /// correctness is therefore worth testing as a property rather than by comparing shapes. What these
    /// assert is containment: a place nearer than the distance is in the answer, a place further is not.
    ///
    /// <para>Calcite's buffers by degrees, which on the ground means a different distance at every latitude
    /// and in every direction. Ours buffers by metres.</para>
    /// </remarks>
    [TestClass]
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
        /// What a buffer is: everything nearer than the distance and nothing further.
        /// </summary>
        /// <remarks>
        /// Walked around the compass, because a buffer that was right to the east and wrong to the north
        /// would be a buffer measured in degrees. The outer probe is at 1.05 rather than 1.0 because the ring
        /// is a polygon inscribed in the circle and so falls a little inside it.
        /// </remarks>
        [TestMethod]
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
        /// The area of a disc, near enough.
        /// </summary>
        /// <remarks>
        /// A geodesic disc of radius r covers about πr² while r is small beside the Earth. The answer is a
        /// little under that, being a thirty-two sided polygon inscribed in the circle rather than the circle
        /// — by the factor a regular polygon of that many sides loses, which is about six parts in a
        /// thousand.
        /// </remarks>
        [TestMethod]
        public void ShouldCoverAboutTheAreaOfADisc()
        {
            var area = GeographyFunctions.Area(Buffer("POINT(0 0)", 100 * Kilometre))!.doubleValue();
            var disc = Math.PI * Math.Pow(100 * Kilometre, 2);

            area.Should().BeLessThan(disc);
            area.Should().BeGreaterThan(disc * 0.98);
        }

        /// <summary>
        /// The distance is metres, so the same buffer covers the same ground wherever it is drawn.
        /// </summary>
        /// <remarks>
        /// The one that separates the two readings. A buffer of a fixed number of degrees covers less ground
        /// the further north it is drawn — at 60 degrees a degree of longitude is half what it is at the
        /// equator — so Calcite's answers shrink with latitude. These do not.
        /// </remarks>
        [TestMethod]
        public void ShouldCoverTheSameGroundAtEveryLatitude()
        {
            var equator = GeographyFunctions.Area(Buffer("POINT(0 0)", 50 * Kilometre))!.doubleValue();
            var north = GeographyFunctions.Area(Buffer("POINT(0 60)", 50 * Kilometre))!.doubleValue();

            north.Should().BeApproximately(equator, equator * 0.01);

            // and Calcite's, buffered by a degree at each, do not agree at all
            var theirsAtEquator = GeographyFunctions.Area(SpatialTypeFunctions.ST_Buffer(Wkt("POINT(0 0)"), 0.5))!.doubleValue();
            var theirsAtNorth = GeographyFunctions.Area(SpatialTypeFunctions.ST_Buffer(Wkt("POINT(0 60)"), 0.5))!.doubleValue();

            theirsAtNorth.Should().BeLessThan(theirsAtEquator * 0.75);
        }

        /// <summary>
        /// A line's buffer is a corridor, and every part of the line is inside it.
        /// </summary>
        [TestMethod]
        public void ShouldBufferEveryPartOfALine()
        {
            var buffer = Buffer("LINESTRING(0 0, 1 0)", 10 * Kilometre);

            Contains(buffer, "POINT(0 0)").Should().BeTrue();
            Contains(buffer, "POINT(0.5 0)").Should().BeTrue("the middle of an edge is buffered, not only its ends");
            Contains(buffer, "POINT(1 0)").Should().BeTrue();
            Contains(buffer, "POINT(0.5 0.5)").Should().BeFalse("that is some fifty kilometres away");
        }

        /// <summary>
        /// An area's buffer contains the area itself, rather than only skinning its boundary.
        /// </summary>
        [TestMethod]
        public void ShouldContainTheAreaItBuffers()
        {
            var square = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var buffer = GeographyFunctions.Buffer(square, java.lang.Double.valueOf(5 * Kilometre))!;

            GeographyFunctions.Covers(buffer, square)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Area(buffer)!.doubleValue()
                .Should().BeGreaterThan(GeographyFunctions.Area(square)!.doubleValue());
        }

        /// <summary>
        /// A larger distance is a larger buffer.
        /// </summary>
        [TestMethod]
        public void ShouldGrowWithTheDistance()
        {
            var small = GeographyFunctions.Area(Buffer("POINT(0 0)", 10 * Kilometre))!.doubleValue();
            var large = GeographyFunctions.Area(Buffer("POINT(0 0)", 20 * Kilometre))!.doubleValue();

            large.Should().BeApproximately(small * 4, small * 4 * 0.01, "area goes as the square of the radius");
        }

        /// <summary>
        /// Nothing to buffer, or nothing to buffer by, is nothing.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerEmptyForNothingToDo()
        {
            Buffer("POINT(0 0)", 0).isEmpty().Should().BeTrue();
            Buffer("POINT(0 0)", -1).isEmpty().Should().BeTrue();
            Buffer("POINT EMPTY", 1000).isEmpty().Should().BeTrue();

            GeographyFunctions.Buffer(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Buffer(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampTheBufferWithWgs84()
        {
            Buffer("POINT(0 0)", 1000).getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunAsAnOperator()
        {
            var answer = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_AREA(ST_GEOG_BUFFER(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 10000.0))")[0][0];

            (answer is java.lang.Number n ? n.doubleValue() : double.NaN)
                .Should().BeApproximately(Math.PI * 1e8, Math.PI * 1e8 * 0.02);
        }

    }

}
