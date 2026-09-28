using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests the geodesic results of the constructors, relations and measurements, calling the methods
    /// directly rather than through a query.
    /// </summary>
    /// <remarks>
    /// Where Calcite's planar answer differs, it is asserted beside the geodesic one, so each test shows the
    /// case in which the two readings disagree.
    /// </remarks>
    public class GeographyFunctionTests
    {

        /// <summary>
        /// One degree of longitude at the equator on WGS84, in metres.
        /// </summary>
        /// <remarks>
        /// The semi-major axis times <c>π/180</c>. A degree of latitude is shorter, 110574.3885 m, because the
        /// ellipsoid is flattened. Figures below that have no closed form on the ellipsoid are recorded values
        /// rather than derived ones.
        /// </remarks>
        const double Degree = 111319.49079327357;

        /// <summary>
        /// The geodesic distance from a meridian to a point one degree of longitude off it at 5°N.
        /// </summary>
        /// <remarks>
        /// A recorded value. On a sphere this would be <c>asin(sin(1°) · cos(5°)) · R</c>; the ellipsoid has no
        /// such closed form. The closest point on the edge is found by S2 and the distance to it is then
        /// measured on the ellipsoid.
        /// </remarks>
        const double EdgeAtFiveDegrees = 110898.66346;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        [Fact]
        public void ShouldReadWkt()
        {
            var geography = Wkt("POINT(1 2)");

            geography.getGeometryType().Should().Be("Point");
            geography.getCoordinate().getX().Should().Be(1);
            geography.getCoordinate().getY().Should().Be(2);
            geography.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// An explicit SRID of 4326 is accepted, any other SRID is refused, and a null argument gives null.
        /// </summary>
        [Fact]
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

        [Fact]
        public void ShouldReadGeoJson()
        {
            var geography = GeographyFunctions.FromGeoJson("{\"type\":\"Point\",\"coordinates\":[1,2]}");

            geography.Should().NotBeNull();
            geography!.getCoordinate().getX().Should().Be(1);
            geography.getCoordinate().getY().Should().Be(2);
            geography.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// <c>AsGeometry</c> and <c>AsGeography</c> return their argument unchanged.
        /// </summary>
        [Fact]
        public void ShouldCrossWithoutTouchingTheValue()
        {
            var geography = Wkt("POINT(1 2)");

            GeographyFunctions.AsGeometry(geography).Should().BeSameAs(geography);
            GeographyFunctions.AsGeography(geography).Should().BeSameAs(geography);
        }

        /// <summary>
        /// The distance between two points a degree of longitude apart on the equator is <see cref="Degree"/>
        /// metres.
        /// </summary>
        [Fact]
        public void ShouldMeasureADegreeOfArcInMetres()
        {
            var distance = GeographyFunctions.Distance(Wkt("POINT(0 0)"), Wkt("POINT(1 0)"));

            distance.Should().NotBeNull();
            distance!.doubleValue().Should().BeApproximately(Degree, 0.001);
        }

        /// <summary>
        /// Calcite answers 1 (degree) for a degree of longitude at any latitude, while the geodesic distance
        /// shrinks with latitude, so no single scale factor converts one to the other.
        /// </summary>
        [Fact]
        public void ShouldDisagreeWithThePlanarDistanceByMoreThanAScaleFactor()
        {
            var equator = GeographyFunctions.Distance(Wkt("POINT(0 0)"), Wkt("POINT(1 0)"))!.doubleValue();
            var north = GeographyFunctions.Distance(Wkt("POINT(0 50)"), Wkt("POINT(1 50)"))!.doubleValue();

            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(Wkt("POINT(0 0)"), Wkt("POINT(1 0)")).Should().Be(1);
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(Wkt("POINT(0 50)"), Wkt("POINT(1 50)")).Should().Be(1);

            north.Should().BeLessThan(equator);
            // On a sphere the ratio would be exactly 1/cos(50 degrees); on the ellipsoid it is close but not
            // equal.
            (equator / north).Should().BeApproximately(1.55268, 1e-4);
            (equator / north).Should().NotBeApproximately(1 / Math.Cos(50 * Math.PI / 180), 1e-4);
        }

        [Fact]
        public void ShouldMeasureZeroBetweenIntersectingGeographies()
        {
            GeographyFunctions.Distance(Wkt("POINT(5 5)"), Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"))!.doubleValue().Should().Be(0);
        }

        /// <summary>
        /// The distance from a point to a polygon is the distance to its nearest edge, here a meridian one
        /// degree of longitude away.
        /// </summary>
        [Fact]
        public void ShouldMeasureToTheNearestEdgeOfAPolygon()
        {
            var distance = GeographyFunctions.Distance(Wkt("POINT(11 5)"), Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"));

            distance!.doubleValue().Should().BeApproximately(EdgeAtFiveDegrees, 0.5);
        }

        /// <summary>
        /// The distance is measured to the edge that closes the ring when that edge is nearest.
        /// </summary>
        /// <remarks>
        /// A JTS ring repeats its first coordinate at the end, and an S2 loop does not, so the closing edge is
        /// easily lost when converting. Containment would not show it, since an S2 loop closes itself; only a
        /// distance or intersection nearest that edge does. Without the edge this would measure to a corner,
        /// about five times further.
        /// </remarks>
        [Fact]
        public void ShouldMeasureToTheEdgeThatClosesARing()
        {
            var distance = GeographyFunctions.Distance(Wkt("POINT(-1 5)"), Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"));

            distance!.doubleValue().Should().BeApproximately(EdgeAtFiveDegrees, 0.5);
        }

        [Fact]
        public void ShouldAnswerDWithinAgainstTheDistance()
        {
            var a = Wkt("POINT(0 0)");
            var b = Wkt("POINT(1 0)");

            GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(Degree + 1))!.booleanValue().Should().BeTrue();
            GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(Degree - 1))!.booleanValue().Should().BeFalse();
        }

        [Fact]
        public void ShouldAnswerWithinAgainstAPolygon()
        {
            var polygon = Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");

            GeographyFunctions.Within(Wkt("POINT(5 5)"), polygon)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Within(Wkt("POINT(15 5)"), polygon)!.booleanValue().Should().BeFalse();
            GeographyFunctions.Within(Wkt("POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"), polygon)!.booleanValue().Should().BeTrue();
            GeographyFunctions.Within(polygon, Wkt("POINT(5 5)"))!.booleanValue().Should().BeFalse();
        }

        [Fact]
        public void ShouldAnswerIntersects()
        {
            var polygon = Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");

            GeographyFunctions.Intersects(polygon, Wkt("POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))"))!.booleanValue().Should().BeTrue();
            GeographyFunctions.Intersects(polygon, Wkt("POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))"))!.booleanValue().Should().BeFalse();
            GeographyFunctions.Intersects(polygon, Wkt("LINESTRING(-5 5, 5 5)"))!.booleanValue().Should().BeTrue();
        }

        /// <summary>
        /// A polygon edge is a great-circle arc, which between two points on the same parallel does not follow
        /// the parallel.
        /// </summary>
        /// <remarks>
        /// The square's northern edge runs along 10 degrees north from longitude 0 to 10. As a straight line
        /// in degrees it stays on the parallel; as a great circle it reaches about 10 degrees 2.25 minutes at
        /// its midpoint. A point between the two is inside the geodesic polygon and outside the planar one.
        /// </remarks>
        [Fact]
        public void ShouldFollowAGreatCircleEdgeWhereCalciteFollowsAParallel()
        {
            var polygon = Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
            var point = Wkt("POINT(5 10.02)");

            GeographyFunctions.Within(point, polygon)!.booleanValue().Should().BeTrue();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Within(point, polygon).Should().BeFalse();
        }

        /// <summary>
        /// Two points either side of the antimeridian are 0.2 degrees apart, where Calcite measures 359.8.
        /// </summary>
        [Fact]
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
        [Fact]
        public void ShouldMeasureOverThePole()
        {
            var here = Wkt("POINT(0 89.9)");
            var there = Wkt("POINT(180 89.9)");

            // A reference WGS84 geodesic distance for this pair, from an independent geodesic service.
            GeographyFunctions.Distance(here, there)!.doubleValue().Should().BeApproximately(22338.795683, 1e-3);
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(here, there).Should().BeApproximately(180, 1e-9);
        }

        /// <summary>
        /// A pole written with any longitude is the same place.
        /// </summary>
        [Fact]
        public void ShouldTreatEverySpellingOfThePoleAsOnePlace()
        {
            var pole = Wkt("POINT(0 90)");
            var alsoPole = Wkt("POINT(180 90)");

            GeographyFunctions.Distance(pole, alsoPole)!.doubleValue().Should().Be(0);
            GeographyFunctions.Intersects(pole, alsoPole)!.booleanValue().Should().BeTrue();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Distance(pole, alsoPole).Should().Be(180);
        }

        /// <summary>
        /// A polygon written across the antimeridian is read as the complement of Calcite's planar reading.
        /// </summary>
        /// <remarks>
        /// The ring runs east from longitude 179 to -179. On the sphere that is a two-degree box straddling
        /// the antimeridian; in the plane it is a 358-degree band excluding that box. Each point tested is
        /// inside one and outside the other.
        /// </remarks>
        [Fact]
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

        /// <summary>
        /// The area is in square metres; Calcite's planar area is in square degrees and answers 1.
        /// </summary>
        /// <remarks>
        /// The box's northern edge is a geodesic between two points at 1 degree north and runs slightly north
        /// of that parallel, so the polygon is slightly larger than the region between the parallels.
        /// </remarks>
        [Fact]
        public void ShouldMeasureAreaInSquareMetres()
        {
            var box = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var area = GeographyFunctions.Area(box)!.doubleValue();

            // A recorded value: the ellipsoid has no closed form for this area. A spherical model would give
            // about 0.45% more.
            area.Should().BeApproximately(12308778361.47, 1.0);

            org.apache.calcite.runtime.SpatialTypeFunctions.ST_Area(box)!.doubleValue().Should().BeApproximately(1, 1e-9);
        }

        [Fact]
        public void ShouldMeasureLengthAndPerimeterInMetres()
        {
            var line = Wkt("LINESTRING(0 0, 1 0)");
            var box = Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");

            GeographyFunctions.Length(line)!.doubleValue().Should().BeApproximately(Degree, 0.001);
            GeographyFunctions.Perimeter(line)!.doubleValue().Should().Be(0);
            GeographyFunctions.Area(line)!.doubleValue().Should().Be(0);

            // The southern edge is a degree of the equator; the meridian edges are a degree of latitude, and
            // the northern edge a geodesic at 1 degree north, all slightly shorter.
            GeographyFunctions.Perimeter(box)!.doubleValue()
                .Should().BeLessThan(4 * Degree).And.BeGreaterThan(3.9 * Degree);
            GeographyFunctions.Length(box)!.doubleValue()
                .Should().Be(GeographyFunctions.Perimeter(box)!.doubleValue());
        }

        /// <summary>
        /// The envelopes of two boxes meeting at the antimeridian intersect; Calcite's do not.
        /// </summary>
        [Fact]
        public void ShouldAnswerEnvelopesIntersectAcrossTheAntimeridian()
        {
            var west = Wkt("POLYGON((179 -1, 180 -1, 180 1, 179 1, 179 -1))");
            var east = Wkt("POLYGON((-180 -1, -179 -1, -179 1, -180 1, -180 -1))");

            GeographyFunctions.EnvelopesIntersect(west, east)!.booleanValue().Should().BeTrue();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_EnvelopesIntersect(west, east).Should().BeFalse();
        }

        [Fact]
        public void ShouldAnswerIsValid()
        {
            GeographyFunctions.IsValid(Wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"))!.booleanValue().Should().BeTrue();
            GeographyFunctions.IsValid(Wkt("POINT(1 2)"))!.booleanValue().Should().BeTrue();
            GeographyFunctions.IsValid(Wkt("LINESTRING(0 0, 1 1)"))!.booleanValue().Should().BeTrue();
        }

        /// <summary>
        /// A longitude outside the valid range makes a geometry invalid; Calcite's planar check accepts it.
        /// </summary>
        [Fact]
        public void ShouldRefuseACoordinateThatIsNotOnTheEarth()
        {
            var polygon = Wkt("POLYGON((0 0, 400 0, 400 10, 0 10, 0 0))");

            GeographyFunctions.IsValid(polygon)!.booleanValue().Should().BeFalse();
            org.apache.calcite.runtime.SpatialTypeFunctions.ST_IsValid(polygon).Should().BeTrue();
        }

        [Fact]
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
