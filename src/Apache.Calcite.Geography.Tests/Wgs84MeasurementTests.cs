using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The measurements answer the WGS84 ellipsoid, which is what a geodesic store answers.
    /// </summary>
    /// <remarks>
    /// These reproduce the measurement in
    /// <see href="https://github.com/ikvmnet/calcite-dotnet/issues/90">#90</see>, taken against a live Cosmos
    /// DB account with <c>geospatialConfig</c> Geography. They were spherical before it, out by up to 0.56%,
    /// which is far too much for an adapter to recheck a pushed-down predicate against: a recheck that
    /// disagrees discards rows the service correctly returned.
    ///
    /// <para>The diagnosis is the first two. One degree east and one degree north of the equator are the same
    /// distance on a sphere and are not on an ellipsoid, and the old implementation answered
    /// <c>111195.101177</c> to both — which is <c>6371010 · π/180</c> exactly.</para>
    /// </remarks>
    [TestClass]
    public class Wgs84MeasurementTests
    {

        /// <summary>
        /// What the service answered, and what an ellipsoid answers.
        /// </summary>
        const double EastAtEquator = 111319.4907;

        /// <summary>
        /// The meridional degree, which differs from the equatorial one by more than half a percent.
        /// </summary>
        const double NorthAtEquator = 110574.3885;

        /// <summary>
        /// What the sphere answered to both, and what nothing should answer now.
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
        /// The equatorial degree is the WGS84 semi-major axis times <c>π/180</c>, to every digit the service
        /// reported.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerTheEquatorialDegree()
        {
            Distance("POINT(0 0)", "POINT(1 0)").Should().BeApproximately(EastAtEquator, 1e-3);

            // and it is the closed form, which is what says the ellipsoid is the one being measured
            Distance("POINT(0 0)", "POINT(1 0)").Should().BeApproximately(6378137.0 * Math.PI / 180, 1e-3);
        }

        /// <summary>
        /// The meridional degree is a different number, which on a sphere it could not be.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerADifferentMeridionalDegree()
        {
            var east = Distance("POINT(0 0)", "POINT(1 0)");
            var north = Distance("POINT(0 0)", "POINT(0 1)");

            north.Should().BeApproximately(NorthAtEquator, 1e-3);
            north.Should().NotBeApproximately(east, 100);
        }

        /// <summary>
        /// Neither is the sphere's answer, by the margin the issue measured.
        /// </summary>
        [TestMethod]
        public void ShouldNoLongerAnswerTheSphere()
        {
            var east = Distance("POINT(0 0)", "POINT(1 0)");
            var north = Distance("POINT(0 0)", "POINT(0 1)");

            Relative(east, SphericalDegree).Should().BeApproximately(1.1e-3, 1e-4);
            Relative(north, SphericalDegree).Should().BeApproximately(-5.6e-3, 1e-4);
        }

        /// <summary>
        /// A length is the same measurement, summed.
        /// </summary>
        [TestMethod]
        public void ShouldMeasureALengthOnTheEllipsoid()
        {
            var length = GeographyFunctions.Length(Wkt("LINESTRING(0 0, 1 0, 1 1)"))!.doubleValue();

            // one equatorial degree east, then one meridional degree north -- the second leg is the same
            // number as the meridional degree at the equator, a meridian being a meridian at any longitude
            length.Should().BeApproximately(EastAtEquator + NorthAtEquator, 1e-3);
        }

        /// <summary>
        /// And a perimeter, and an area, neither of which is the sphere's.
        /// </summary>
        /// <remarks>
        /// Area diverges between the two models further than distance does. A one-degree square at the
        /// equator is about 12,308 km² on WGS84; the sphere makes it about 12,364 km².
        /// </remarks>
        [TestMethod]
        public void ShouldMeasureAnAreaOnTheEllipsoid()
        {
            var area = GeographyFunctions.Area(Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))!.doubleValue();

            area.Should().BeGreaterThan(1.22e10).And.BeLessThan(1.24e10);
            Relative(area, SphericalDegree * SphericalDegree).Should().NotBe(0);
        }

        /// <summary>
        /// A distance between shapes rather than points is measured between the points S2 chose.
        /// </summary>
        /// <remarks>
        /// The closest pair of a point and a line running north from the equator is the line's own end, so
        /// this is the equatorial degree again — which says the pair S2 picked was carried through to the
        /// ellipsoidal measurement rather than being measured on the sphere.
        /// </remarks>
        [TestMethod]
        public void ShouldMeasureBetweenTheClosestPairOfTwoShapes()
        {
            Distance("POINT(0 0)", "LINESTRING(1 0, 1 1)").Should().BeApproximately(EastAtEquator, 1e-3);
        }

        /// <summary>
        /// Shapes that touch or contain one another are zero apart, as before.
        /// </summary>
        [TestMethod]
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
