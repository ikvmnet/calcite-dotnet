using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Checks that distances, lengths and areas are measured on the WGS84 ellipsoid.
    /// </summary>
    /// <remarks>
    /// The reference figures are those a geodesic store (Cosmos DB with the Geography spatial configuration)
    /// returns. An adapter that rechecks a pushed-down predicate needs to agree with the store; a spherical
    /// model differs by up to about 0.56%, enough to discard rows the store correctly returned. On a sphere a
    /// degree east and a degree north of the equator are the same distance; on the ellipsoid they are not.
    /// </remarks>
    public class Wgs84MeasurementTests
    {

        /// <summary>
        /// One degree of longitude along the equator on WGS84, in metres.
        /// </summary>
        const double EastAtEquator = 111319.4907;

        /// <summary>
        /// One degree of latitude north from the equator on WGS84, in metres.
        /// </summary>
        const double NorthAtEquator = 110574.3885;

        /// <summary>
        /// One degree of arc on a sphere of radius 6371010 m, which a spherical model gives in both directions.
        /// </summary>
        const double SphericalDegree = 6371010.0 * Math.PI / 180;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static double Distance(string a, string b)
        {
            return GeographyFunctions.Distance(Wkt(a), Wkt(b))!.doubleValue();
        }

        /// <summary>
        /// The equatorial degree matches the reference figure and the WGS84 semi-major axis times <c>π/180</c>.
        /// </summary>
        [Fact]
        public void ShouldAnswerTheEquatorialDegree()
        {
            Distance("POINT(0 0)", "POINT(1 0)").Should().BeApproximately(EastAtEquator, 1e-3);

            Distance("POINT(0 0)", "POINT(1 0)").Should().BeApproximately(6378137.0 * Math.PI / 180, 1e-3);
        }

        /// <summary>
        /// The meridional degree matches its reference figure and differs from the equatorial degree.
        /// </summary>
        [Fact]
        public void ShouldAnswerADifferentMeridionalDegree()
        {
            var east = Distance("POINT(0 0)", "POINT(1 0)");
            var north = Distance("POINT(0 0)", "POINT(0 1)");

            north.Should().BeApproximately(NorthAtEquator, 1e-3);
            north.Should().NotBeApproximately(east, 100);
        }

        /// <summary>
        /// Both degrees differ from the spherical degree, by about +0.11% and -0.56%.
        /// </summary>
        [Fact]
        public void ShouldNoLongerAnswerTheSphere()
        {
            var east = Distance("POINT(0 0)", "POINT(1 0)");
            var north = Distance("POINT(0 0)", "POINT(0 1)");

            Relative(east, SphericalDegree).Should().BeApproximately(1.1e-3, 1e-4);
            Relative(north, SphericalDegree).Should().BeApproximately(-5.6e-3, 1e-4);
        }

        /// <summary>
        /// A line's length is the sum of its edges measured on the ellipsoid.
        /// </summary>
        [Fact]
        public void ShouldMeasureALengthOnTheEllipsoid()
        {
            var length = GeographyFunctions.Length(Wkt("LINESTRING(0 0, 1 0, 1 1)"))!.doubleValue();

            // One equatorial degree east, then one meridional degree north, which is the same length at any
            // longitude.
            length.Should().BeApproximately(EastAtEquator + NorthAtEquator, 1e-3);
        }

        /// <summary>
        /// A one-degree square at the equator has an area between 12,200 and 12,400 km².
        /// </summary>
        /// <remarks>
        /// On WGS84 the area is about 12,308 km² and a spherical model gives about 12,364 km²; both are within
        /// these bounds.
        /// </remarks>
        [Fact]
        public void ShouldMeasureAnAreaOnTheEllipsoid()
        {
            var area = GeographyFunctions.Area(Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))!.doubleValue();

            area.Should().BeGreaterThan(1.22e10).And.BeLessThan(1.24e10);
            Relative(area, SphericalDegree * SphericalDegree).Should().NotBe(0);
        }

        /// <summary>
        /// The distance between two shapes is measured on the ellipsoid between the closest points S2 finds.
        /// </summary>
        /// <remarks>
        /// The closest point of a line running north from the equator is its southern end, so the answer is
        /// the equatorial degree.
        /// </remarks>
        [Fact]
        public void ShouldMeasureBetweenTheClosestPairOfTwoShapes()
        {
            Distance("POINT(0 0)", "LINESTRING(1 0, 1 1)").Should().BeApproximately(EastAtEquator, 1e-3);
        }

        /// <summary>
        /// Shapes that meet are zero apart.
        /// </summary>
        [Fact]
        public void ShouldStillAnswerZeroWhereTheyMeet()
        {
            Distance("POINT(0.5 0.5)", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))").Should().Be(0);
            Distance("POINT(0 0)", "POINT(0 0)").Should().Be(0);
        }

        static double Relative(double ours, double reference)
        {
            return (ours - reference) / ours;
        }

    }

}
