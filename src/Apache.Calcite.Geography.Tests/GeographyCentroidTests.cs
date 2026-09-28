using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_CENTROID</c>, which averages directions from the Earth's centre rather than
    /// coordinates.
    /// </summary>
    /// <remarks>
    /// Averaging the longitudes of a shape either side of longitude 180 gives about zero, on the far side of
    /// the planet; averaging directions gives a point in the shape.
    /// </remarks>
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
        [Fact]
        public void ShouldAgreeWithCalciteNearTheOrigin()
        {
            var square = Wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");

            var ours = GeographyFunctions.Centroid(square)!;
            var theirs = SpatialTypeFunctions.ST_Centroid(square);

            ours.getCoordinate().getX().Should().BeApproximately(theirs.getCoordinate().getX(), 1e-3);
            ours.getCoordinate().getY().Should().BeApproximately(theirs.getCoordinate().getY(), 1e-3);
        }

        /// <summary>
        /// The centroid of a square across the antimeridian is in the square, where Calcite's is near
        /// longitude zero.
        /// </summary>
        [Fact]
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
        /// Only the parts of highest dimension contribute to a collection's centroid, as in JTS.
        /// </summary>
        [Fact]
        public void ShouldLetTheHighestDimensionDecide()
        {
            // The distant point would move the centroid if it counted.
            var mixed = Centroid("GEOMETRYCOLLECTION(POLYGON((0 0, 2 0, 2 2, 0 2, 0 0)), POINT(40 40))");

            mixed.getCoordinate().getX().Should().BeApproximately(1, 0.01);
            mixed.getCoordinate().getY().Should().BeApproximately(1, 0.01);
        }

        /// <summary>
        /// A line's centroid is weighted by edge length.
        /// </summary>
        [Fact]
        public void ShouldWeightALineByItsLength()
        {
            var centre = Centroid("LINESTRING(0 0, 10 0, 11 0)").getCoordinate();

            centre.getY().Should().BeApproximately(0, 1e-6);
            centre.getX().Should().BeApproximately(5.5, 0.01);
        }

        /// <summary>
        /// A multi-point's centroid is the mean of its points' directions.
        /// </summary>
        [Fact]
        public void ShouldAverageAPointSet()
        {
            var centre = Centroid("MULTIPOINT((0 0), (2 0))").getCoordinate();

            centre.getX().Should().BeApproximately(1, 1e-6);
            centre.getY().Should().BeApproximately(0, 1e-6);
        }

        /// <summary>
        /// Where the directions sum to zero, or there are none, the centroid is an empty point.
        /// </summary>
        /// <remarks>
        /// Two antipodal points have no single centre. JTS also answers an empty point for a shape with no
        /// centroid.
        /// </remarks>
        [Fact]
        public void ShouldAnswerNothingWhereThereIsNoCentre()
        {
            Centroid("MULTIPOINT((0 0), (180 0))").isEmpty().Should().BeTrue();
            Centroid("POINT EMPTY").isEmpty().Should().BeTrue();
            GeographyFunctions.Centroid(null).Should().BeNull();
        }

        [Fact]
        public void ShouldStampTheCentreWithWgs84()
        {
            Centroid("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))").getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        [Fact]
        public void ShouldRunAsAnOperator()
        {
            // The longitude is not exactly 1, having been converted to a unit vector and back, so this compares
            // the number with a tolerance rather than the text.
            var longitude = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_X(CLR_ST_GEOG_CENTROID(CLR_ST_GEOG_GEOMFROMTEXT('MULTIPOINT((0 0), (2 0))')))")[0][0];

            (longitude is java.lang.Number n ? n.doubleValue() : double.NaN).Should().BeApproximately(1, 1e-12);
        }

    }

}
