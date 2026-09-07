using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The centre of a geography, which is a direction from the Earth's centre rather than an average of two
    /// coordinates.
    /// </summary>
    /// <remarks>
    /// The antimeridian is what separates the two readings, and not by a little. Averaging the longitudes of
    /// a shape sitting either side of longitude 180 gives zero, which is the far side of the planet; summing
    /// directions gives a point in the shape. That is the same failure the bounding rectangle has and the
    /// same fix.
    /// </remarks>
    [TestClass]
    public class GeographyCentroidTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static Geometry Centroid(string wkt)
        {
            return GeographyFunctions.Centroid(Wkt(wkt))!;
        }

        /// <summary>
        /// An ordinary shape near the origin agrees with the planar answer.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeWithCalciteNearTheOrigin()
        {
            var square = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            var ours = GeographyFunctions.Centroid(square)!;
            var theirs = SpatialTypeFunctions.ST_Centroid(square);

            ours.getCoordinate().getX().Should().BeApproximately(theirs.getCoordinate().getX(), 1e-3);
            ours.getCoordinate().getY().Should().BeApproximately(theirs.getCoordinate().getY(), 1e-3);
        }

        /// <summary>
        /// The one that separates the two readings: a shape across the antimeridian.
        /// </summary>
        /// <remarks>
        /// Every coordinate of this square is within a degree of longitude 180, so its centre is too. Calcite
        /// averages the longitudes — 179 and -179 and their like — and answers a point near longitude zero,
        /// half a world away. This answers a point in the square.
        /// </remarks>
        [TestMethod]
        public void ShouldNotPutTheCentreOnTheFarSideOfThePlanet()
        {
            var square = Wkt("POLYGON((179 0, -179 0, -179 2, 179 2, 179 0))");

            var theirs = SpatialTypeFunctions.ST_Centroid(square).getCoordinate();
            Math.Abs(theirs.getX()).Should().BeLessThan(1, "the planar centroid averages the longitudes to about zero");

            var ours = GeographyFunctions.Centroid(square)!.getCoordinate();
            Math.Abs(ours.getX()).Should().BeGreaterThan(179, "the centre of the square is on the antimeridian");
            ours.getY().Should().BeApproximately(1, 0.1);
        }

        /// <summary>
        /// An area outranks a line and a line outranks a point, as JTS orders them.
        /// </summary>
        [TestMethod]
        public void ShouldLetTheHighestDimensionDecide()
        {
            // the point is far away and contributes nothing, the square deciding alone
            var mixed = Centroid("GEOMETRYCOLLECTION(POLYGON((0 0, 2 0, 2 2, 0 2, 0 0)), POINT(40 40))");

            mixed.getCoordinate().getX().Should().BeApproximately(1, 0.01);
            mixed.getCoordinate().getY().Should().BeApproximately(1, 0.01);
        }

        /// <summary>
        /// A line's centre is weighted by length, so a long edge counts for more than a short one.
        /// </summary>
        [TestMethod]
        public void ShouldWeightALineByItsLength()
        {
            var centre = Centroid("LINESTRING(0 0, 10 0, 11 0)").getCoordinate();

            centre.getY().Should().BeApproximately(0, 1e-6);
            centre.getX().Should().BeApproximately(5.5, 0.01);
        }

        /// <summary>
        /// A set of points is their mean direction.
        /// </summary>
        [TestMethod]
        public void ShouldAverageAPointSet()
        {
            var centre = Centroid("MULTIPOINT((0 0), (2 0))").getCoordinate();

            centre.getX().Should().BeApproximately(1, 1e-6);
            centre.getY().Should().BeApproximately(0, 1e-6);
        }

        /// <summary>
        /// Where there is no direction to answer, there is no answer.
        /// </summary>
        /// <remarks>
        /// Two antipodal points sum to nothing, and every direction between them is as good as its opposite.
        /// An empty point says so, which is what JTS answers for a shape with no centroid too.
        /// </remarks>
        [TestMethod]
        public void ShouldAnswerNothingWhereThereIsNoCentre()
        {
            Centroid("MULTIPOINT((0 0), (180 0))").isEmpty().Should().BeTrue();
            Centroid("POINT EMPTY").isEmpty().Should().BeTrue();
            GeographyFunctions.Centroid(null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldStampTheCentreWithWgs84()
        {
            Centroid("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))").getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [TestMethod]
        public void ShouldRunAsAnOperator()
        {
            // the longitude comes back a few bits shy of one, a coordinate having gone out and back as a
            // unit vector, so this asks the operator for the number rather than for its spelling
            var longitude = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_X(ST_GEOG_CENTROID(ST_GEOG_GEOMFROMTEXT('MULTIPOINT((0 0), (2 0))')))")[0][0];

            (longitude is java.lang.Number n ? n.doubleValue() : double.NaN).Should().BeApproximately(1, 1e-12);
        }

    }

}
