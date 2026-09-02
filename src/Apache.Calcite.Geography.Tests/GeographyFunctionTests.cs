using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// What the bodies behind the operators compute.
    /// </summary>
    /// <remarks>
    /// Called directly rather than through a query, because what is under test is the geodesy and not the
    /// plumbing. Where a case has a planar answer that differs, Calcite's own answer is asserted next to it:
    /// the disagreement is the reason this package exists, so a test that only pinned our number would not
    /// say anything.
    /// </remarks>
    [TestClass]
    public class GeographyFunctionTests
    {

        /// <summary>
        /// The radius S2 models the Earth with, in metres.
        /// </summary>
        const double EarthRadiusMeters = 6371010.0;

        /// <summary>
        /// One degree of arc on that sphere, in metres.
        /// </summary>
        const double Degree = EarthRadiusMeters * Math.PI / 180;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        [TestMethod]
        public void ShouldReadWkt()
        {
            var geography = Wkt("POINT(1 2)");

            geography.getGeometryType().Should().Be("Point");
            geography.getCoordinate().getX().Should().Be(1);
            geography.getCoordinate().getY().Should().Be(2);
            geography.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// The SRID a caller may name is the only one a geography can be in, and any other is refused rather
        /// than ignored.
        /// </summary>
        [TestMethod]
        public void ShouldReadWktWithAnSrid()
        {
            var geography = GeographyFunctions.FromWkt("POINT(1 2)", java.lang.Integer.valueOf(GeographyFunctions.Wgs84));

            geography.Should().NotBeNull();
            geography!.getSRID().Should().Be(GeographyFunctions.Wgs84);

            var refused = () => GeographyFunctions.FromWkt("POINT(1 2)", java.lang.Integer.valueOf(3857));
            refused.Should().Throw<java.lang.IllegalArgumentException>().WithMessage("*3857*");

            GeographyFunctions.FromWkt(null, java.lang.Integer.valueOf(GeographyFunctions.Wgs84)).Should().BeNull();
            GeographyFunctions.FromWkt("POINT(1 2)", null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldReadGeoJson()
        {
            var geography = GeographyFunctions.FromGeoJson("{\"type\":\"Point\",\"coordinates\":[1,2]}");

            geography.Should().NotBeNull();
            geography!.getCoordinate().getX().Should().Be(1);
            geography.getCoordinate().getY().Should().Be(2);
            geography.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// The crossings are re-typings and nothing else happens at run time.
        /// </summary>
        [TestMethod]
        public void ShouldCrossWithoutTouchingTheValue()
        {
            var geography = Wkt("POINT(1 2)");

            GeographyFunctions.AsGeometry(geography).Should().BeSameAs(geography);
            GeographyFunctions.AsGeography(geography).Should().BeSameAs(geography);
        }

        /// <summary>
        /// A degree of longitude on the equator is a degree of arc, and the answer is in metres.
        /// </summary>
        [TestMethod]
        public void ShouldMeasureADegreeOfArcInMetres()
        {
            var distance = GeographyFunctions.Distance(Wkt("POINT(0 0)"), Wkt("POINT(1 0)"));

            distance.Should().NotBeNull();
            distance!.doubleValue().Should().BeApproximately(Degree, 0.001);
        }

        /// <summary>
        /// The same call under Calcite answers one — the number of degrees — and the difference is not a
        /// scale factor.
        /// </summary>
        /// <remarks>
        /// A degree of longitude is a degree of arc on the equator and about a hundred and eleven metres less
        /// than that at fifty degrees north, while the planar answer is one in both places. No conversion of
        /// the result recovers the other, and no transformation of the inputs does either.
        /// </remarks>
        [TestMethod]
        public void ShouldDisagreeWithThePlanarDistanceByMoreThanAScaleFactor()
        {
            var equator = GeographyFunctions.Distance(Wkt("POINT(0 0)"), Wkt("POINT(1 0)"))!.doubleValue();
            var north = GeographyFunctions.Distance(Wkt("POINT(0 50)"), Wkt("POINT(1 50)"))!.doubleValue();

            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(Wkt("POINT(0 0)"), Wkt("POINT(1 0)")).Should().Be(1);
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(Wkt("POINT(0 50)"), Wkt("POINT(1 50)")).Should().Be(1);

            north.Should().BeLessThan(equator);
            (equator / north).Should().BeApproximately(1 / Math.Cos(50 * Math.PI / 180), 0.001);
        }

        [TestMethod]
        public void ShouldMeasureZeroBetweenIntersectingGeographies()
        {
            GeographyFunctions.Distance(Wkt("POINT(5 5)"), Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"))!.doubleValue().Should().Be(0);
        }

        /// <summary>
        /// The distance from a point to a meridian edge one degree of longitude away.
        /// </summary>
        /// <remarks>
        /// A meridian is a great circle, so the arc from the point to it is
        /// <c>asin(sin(1°) · cos(5°))</c> exactly.
        /// </remarks>
        [TestMethod]
        public void ShouldMeasureToTheNearestEdgeOfAPolygon()
        {
            var expected = Math.Asin(Math.Sin(Math.PI / 180) * Math.Cos(5 * Math.PI / 180)) * EarthRadiusMeters;
            var distance = GeographyFunctions.Distance(Wkt("POINT(11 5)"), Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"));

            distance!.doubleValue().Should().BeApproximately(expected, 0.5);
        }

        /// <summary>
        /// The nearest edge is the one that closes the ring.
        /// </summary>
        /// <remarks>
        /// A ring arrives from JTS with its first coordinate repeated at the end, and dropping that repeat —
        /// which is what an S2 loop wants — costs the ring its last edge. Nothing else here notices: the
        /// containment tests go through the loop, which is built from the de-duplicated vertices and closes
        /// itself, so only a distance or an intersection nearest that one edge tells the two apart. Without
        /// the closing edge this answers the distance to a corner instead, some five times larger.
        /// </remarks>
        [TestMethod]
        public void ShouldMeasureToTheEdgeThatClosesARing()
        {
            var expected = Math.Asin(Math.Sin(Math.PI / 180) * Math.Cos(5 * Math.PI / 180)) * EarthRadiusMeters;
            var distance = GeographyFunctions.Distance(Wkt("POINT(-1 5)"), Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"));

            distance!.doubleValue().Should().BeApproximately(expected, 0.5);
        }

        [TestMethod]
        public void ShouldAnswerDWithinAgainstTheDistance()
        {
            var a = Wkt("POINT(0 0)");
            var b = Wkt("POINT(1 0)");

            GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(Degree + 1))!.booleanValue().Should().BeTrue();
            GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(Degree - 1))!.booleanValue().Should().BeFalse();
        }

        [TestMethod]
        public void ShouldAnswerWithinAgainstAPolygon()
        {
            var polygon = Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");

            GeographyFunctions.Within(Wkt("POINT(5 5)"), polygon)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Within(Wkt("POINT(15 5)"), polygon)!.booleanValue().Should().BeFalse();
            GeographyFunctions.Within(Wkt("POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"), polygon)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Within(polygon, Wkt("POINT(5 5)"))!.booleanValue().Should().BeFalse();
        }

        [TestMethod]
        public void ShouldAnswerIntersects()
        {
            var polygon = Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");

            GeographyFunctions.Intersects(polygon, Wkt("POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))"))!.booleanValue().Should().BeTrue();
            GeographyFunctions.Intersects(polygon, Wkt("POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))"))!.booleanValue().Should().BeFalse();
            GeographyFunctions.Intersects(polygon, Wkt("LINESTRING(-5 5, 5 5)"))!.booleanValue().Should().BeTrue();
        }

        /// <summary>
        /// A polygon edge is a great-circle arc, and a great-circle arc between two points at the same
        /// latitude does not follow that parallel.
        /// </summary>
        /// <remarks>
        /// This is the disagreement that no scale factor and no reprojection recovers, and it is why the two
        /// readings need types that cannot be confused. The northern edge of the square runs from ten degrees
        /// north at longitude zero to ten degrees north at longitude ten; as a straight line in longitude and
        /// latitude it stays on the parallel, and as a great circle it reaches about ten degrees and two and
        /// a quarter minutes at the midpoint. A point between the two is inside one polygon and outside the
        /// other — not a different distance, a different answer.
        /// </remarks>
        [TestMethod]
        public void ShouldFollowAGreatCircleEdgeWhereCalciteFollowsAParallel()
        {
            var polygon = Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
            var point = Wkt("POINT(5 10.02)");

            GeographyFunctions.Within(point, polygon)!.booleanValue().Should().BeTrue();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Within(point, polygon).Should().BeFalse();
        }

        /// <summary>
        /// Two points either side of the antimeridian are a fifth of a degree apart, not three hundred and
        /// fifty-nine and four fifths.
        /// </summary>
        /// <remarks>
        /// The antimeridian is one of the four places the design issue names as having to be measured against
        /// a live store before any of this may recheck a pushed-down predicate. What is held here is the
        /// nearer half of that: that this convention is on the right side of the seam at all, and that the
        /// planar reading is not merely a different number but a different journey.
        /// </remarks>
        [TestMethod]
        public void ShouldMeasureAcrossTheAntimeridian()
        {
            var west = Wkt("POINT(179.9 0)");
            var east = Wkt("POINT(-179.9 0)");

            GeographyFunctions.Distance(west, east)!.doubleValue().Should().BeApproximately(0.2 * Degree, 0.001);
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(west, east).Should().BeApproximately(359.8, 1e-9);
        }

        /// <summary>
        /// The shortest way between two places on opposite meridians near a pole is over the pole.
        /// </summary>
        [TestMethod]
        public void ShouldMeasureOverThePole()
        {
            var here = Wkt("POINT(0 89.9)");
            var there = Wkt("POINT(180 89.9)");

            GeographyFunctions.Distance(here, there)!.doubleValue().Should().BeApproximately(0.2 * Degree, 0.001);
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(here, there).Should().BeApproximately(180, 1e-9);
        }

        /// <summary>
        /// The pole has a longitude in the coordinates and no longitude on the Earth, so every spelling of it
        /// is the same place.
        /// </summary>
        [TestMethod]
        public void ShouldTreatEverySpellingOfThePoleAsOnePlace()
        {
            var pole = Wkt("POINT(0 90)");
            var alsoPole = Wkt("POINT(180 90)");

            GeographyFunctions.Distance(pole, alsoPole)!.doubleValue().Should().Be(0);
            GeographyFunctions.Intersects(pole, alsoPole)!.booleanValue().Should().BeTrue();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(pole, alsoPole).Should().Be(180);
        }

        /// <summary>
        /// A polygon written across the antimeridian, where the two readings are not merely different but
        /// exact inversions of one another.
        /// </summary>
        /// <remarks>
        /// The ring runs from longitude 179 east to -179, which on the sphere is a two-degree box straddling
        /// the seam and in the plane is a three-hundred-and-fifty-eight-degree band that is everything except
        /// that box. So each of these three points is inside one polygon and outside the other, and there is
        /// no tolerance and no reprojection that reconciles them. This is why the two readings need types
        /// that cannot be confused.
        /// </remarks>
        [TestMethod]
        public void ShouldReadAPolygonAcrossTheAntimeridianInsideOutFromCalcite()
        {
            var box = Wkt("POLYGON((179 -1, -179 -1, -179 1, 179 1, 179 -1))");

            foreach (var wkt in new[] { "POINT(179.5 0)", "POINT(-179.5 0)" })
            {
                GeographyFunctions.Within(Wkt(wkt), box)!.booleanValue().Should().BeTrue(wkt);
                org.apache.calcite.runtime.SpatialTypeFunctions.ST_Within(Wkt(wkt), box).Should().BeFalse(wkt);
            }

            GeographyFunctions.Within(Wkt("POINT(0 0)"), box)!.booleanValue().Should().BeFalse();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Within(Wkt("POINT(0 0)"), box).Should().BeTrue();
        }

        [TestMethod]
        public void ShouldAnswerIsValid()
        {
            GeographyFunctions.IsValid(Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"))!.booleanValue().Should().BeTrue();
            GeographyFunctions.IsValid(Wkt("POINT(1 2)"))!.booleanValue().Should().BeTrue();
            GeographyFunctions.IsValid(Wkt("LINESTRING(0 0, 1 1)"))!.booleanValue().Should().BeTrue();
        }

        /// <summary>
        /// A coordinate that is not a place on the Earth. JTS has no opinion, because a plane has no edges.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseACoordinateThatIsNotOnTheEarth()
        {
            var polygon = Wkt("POLYGON((0 0, 400 0, 400 10, 0 10, 0 0))");

            GeographyFunctions.IsValid(polygon)!.booleanValue().Should().BeFalse();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_IsValid(polygon).Should().BeTrue();
        }

        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            var geography = Wkt("POINT(0 0)");

            GeographyFunctions.FromWkt(null).Should().BeNull();
            GeographyFunctions.FromGeoJson(null).Should().BeNull();
            GeographyFunctions.AsGeometry(null).Should().BeNull();
            GeographyFunctions.AsGeography(null).Should().BeNull();
            GeographyFunctions.Distance(geography, null).Should().BeNull();
            GeographyFunctions.Distance(null, geography).Should().BeNull();
            GeographyFunctions.DWithin(geography, geography, null).Should().BeNull();
            GeographyFunctions.Within(null, geography).Should().BeNull();
            GeographyFunctions.Intersects(geography, null).Should().BeNull();
            GeographyFunctions.IsValid(null).Should().BeNull();
        }

    }

}
