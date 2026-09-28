using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_CLOSESTCOORDINATE</c>, <c>CLR_ST_GEOG_FURTHESTCOORDINATE</c>,
    /// <c>CLR_ST_GEOG_CLOSESTPOINT</c> and <c>CLR_ST_GEOG_LONGESTLINE</c>, which return geometry chosen by
    /// distance on the ellipsoid.
    /// </summary>
    /// <remarks>
    /// A degree of longitude at the equator is 111319.49 m and a degree of latitude is 110574.39 m, so
    /// candidates a planar reading calls equidistant can differ by 745 m, and the two readings can disagree
    /// about which is nearer.
    /// </remarks>
    public class GeographyNearestTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static string Text(Geometry? g)
        {
            return g is null ? "null" : g.toText();
        }

        /// <summary>
        /// A tie in degrees that is not a tie in metres is broken.
        /// </summary>
        /// <remarks>
        /// One degree east and one degree north of the origin are equidistant on a plane, so Calcite returns
        /// both. On the ellipsoid the northern one is 745 m nearer and is returned alone.
        /// </remarks>
        [Fact]
        public void ShouldBreakATieThatOnlyExistsOnAPlane()
        {
            var origin = Wkt("POINT(0 0)");
            var candidates = Wkt("MULTIPOINT((1 0), (0 1))");

            Text(SpatialTypeFunctions.ST_ClosestCoordinate(origin, candidates))
                .Should().Be("MULTIPOINT ((1 0), (0 1))");

            Text(GeographyFunctions.ClosestCoordinate(origin, candidates))
                .Should().Be("POINT (0 1)");
        }

        /// <summary>
        /// Of the same two candidates, the furthest is the eastern one.
        /// </summary>
        [Fact]
        public void ShouldChooseTheFurthestOnTheEllipsoidToo()
        {
            var origin = Wkt("POINT(0 0)");
            var candidates = Wkt("MULTIPOINT((1 0), (0 1))");

            Text(GeographyFunctions.FurthestCoordinate(origin, candidates)).Should().Be("POINT (1 0)");
        }

        /// <summary>
        /// A tie on the ellipsoid returns every tied coordinate, as Calcite does.
        /// </summary>
        [Fact]
        public void ShouldAnswerARealTieWithEveryCoordinate()
        {
            var answer = GeographyFunctions.ClosestCoordinate(Wkt("POINT(0 0)"), Wkt("MULTIPOINT((1 0), (-1 0))"));

            Text(answer).Should().Be("MULTIPOINT ((1 0), (-1 0))");
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_CLOSESTCOORDINATE</c> returns vertices of the geometry, where
        /// <c>CLR_ST_GEOG_CLOSESTPOINT</c> may return a point along an edge.
        /// </summary>
        [Fact]
        public void ShouldAnswerACoordinateRatherThanAPointOnAnEdge()
        {
            var line = Wkt("LINESTRING(1 -1, 1 1)");

            // the two ends are symmetric about the equator, so they tie
            Text(GeographyFunctions.ClosestCoordinate(Wkt("POINT(0 0)"), line)).Should().Be("MULTIPOINT ((1 -1), (1 1))");
            Text(GeographyFunctions.ClosestPoint(line, Wkt("POINT(0 0)"))).Should().Be("POINT (1 0)");
        }

        /// <summary>
        /// The closest point may fall part way along an edge, which is a geodesic.
        /// </summary>
        [Fact]
        public void ShouldAnswerAPointOnAnEdge()
        {
            var answer = GeographyFunctions.ClosestPoint(Wkt("LINESTRING(-1 1, 1 1)"), Wkt("POINT(0 0)"));

            answer.Should().NotBeNull();
            answer!.getCoordinate().getX().Should().BeApproximately(0, 1e-9);

            // north of the line's own ends, because a geodesic between two points on a parallel bows poleward
            answer.getCoordinate().getY().Should().BeGreaterThan(1.0);
        }

        /// <summary>
        /// The longest line joins the pair <c>CLR_ST_GEOG_MAXDISTANCE</c> measures.
        /// </summary>
        [Fact]
        public void ShouldJoinThePairMaxDistanceMeasures()
        {
            var a = Wkt("MULTIPOINT((0 0), (0.5 0))");
            var b = Wkt("MULTIPOINT((2 0), (3 0))");

            var line = GeographyFunctions.LongestLine(a, b);
            Text(line).Should().Be("LINESTRING (0 0, 3 0)");

            var length = GeographyFunctions.Length(line)!.doubleValue();
            var max = GeographyFunctions.MaxDistance(a, b)!.doubleValue();

            length.Should().BeApproximately(max, 1e-6);
        }

        /// <summary>
        /// <c>ClosestCoordinate</c>, <c>FurthestCoordinate</c>, <c>ClosestPoint</c> and <c>LongestLine</c> return
        /// <c>null</c> when either argument is <c>null</c>.
        /// </summary>
        [Fact]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.ClosestCoordinate(null, Wkt("POINT(0 0)")).Should().BeNull();
            GeographyFunctions.FurthestCoordinate(Wkt("POINT(0 0)"), null).Should().BeNull();
            GeographyFunctions.ClosestPoint(null, null).Should().BeNull();
            GeographyFunctions.LongestLine(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        /// <summary>
        /// Each of the four functions stamps its result with SRID 4326.
        /// </summary>
        [Fact]
        public void ShouldStampEveryAnswerWithWgs84()
        {
            var a = Wkt("LINESTRING(0 0, 1 1)");
            var b = Wkt("POINT(2 2)");

            GeographyFunctions.ClosestCoordinate(b, a)!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.FurthestCoordinate(b, a)!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.ClosestPoint(a, b)!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.LongestLine(a, b)!.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// All four run as SQL operators, which checks that each is bound to the right method.
        /// </summary>
        [Fact]
        public void ShouldRunEachAsAnOperator()
        {
            var cases = new (string Sql, string Expected)[]
            {
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_CLOSESTCOORDINATE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('MULTIPOINT((1 0), (0 1))')))", "POINT (0 1)"),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_FURTHESTCOORDINATE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('MULTIPOINT((1 0), (0 1))')))", "POINT (1 0)"),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_CLOSESTPOINT(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(1 -1, 1 1)'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')))", "POINT (1 0)"),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_LONGESTLINE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('MULTIPOINT((2 0), (3 0))')))", "LINESTRING (0 0, 3 0)"),
            };

            var failures = new System.Collections.Generic.List<string>();

            foreach (var (sql, expected) in cases)
            {
                var answer = GeographyExecutionTests.Run("SELECT " + sql)[0][0];

                if (Equals(answer, expected) == false)
                    failures.Add($"{sql}: answered {answer}, wanted {expected}");
            }

            failures.Should().BeEmpty(string.Join("\n", failures));
        }

    }

}
