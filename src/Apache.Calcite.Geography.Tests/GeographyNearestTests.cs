using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The nearest and furthest family, which answers a point rather than a number and chooses it by
    /// distance.
    /// </summary>
    /// <remarks>
    /// <see href="https://github.com/ikvmnet/calcite-dotnet/issues/90">#90</see> named these as measurements
    /// in disguise: each ranks candidates by how far away they are, so each is wrong in the same way a
    /// distance is wrong if the ranking is planar. They rank on the ellipsoid.
    ///
    /// <para>That is not a difference in the last digits. A degree of longitude at the equator is 111319.49
    /// metres and a degree of latitude is 110574.39, so two candidates that a planar reading calls
    /// equidistant are 745 metres apart in fact — and the two disagree about which is nearer whenever the
    /// candidates lie in different directions.</para>
    /// </remarks>
    [TestClass]
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
        /// The case the whole family exists for: a tie in degrees that is not a tie in metres.
        /// </summary>
        /// <remarks>
        /// One degree east and one degree north of the origin are the same distance on a plane, so Calcite
        /// answers both. On the Earth the northern one is 745 metres nearer, so there is nothing to tie and
        /// this answers it alone. Neither is a rounding difference; they are different answers.
        /// </remarks>
        [TestMethod]
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
        /// And the furthest of the same two is the other one.
        /// </summary>
        [TestMethod]
        public void ShouldChooseTheFurthestOnTheEllipsoidToo()
        {
            var origin = Wkt("POINT(0 0)");
            var candidates = Wkt("MULTIPOINT((1 0), (0 1))");

            Text(GeographyFunctions.FurthestCoordinate(origin, candidates)).Should().Be("POINT (1 0)");
        }

        /// <summary>
        /// A genuine tie is still answered with every coordinate that ties, as Calcite does.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerARealTieWithEveryCoordinate()
        {
            var answer = GeographyFunctions.ClosestCoordinate(Wkt("POINT(0 0)"), Wkt("MULTIPOINT((1 0), (-1 0))"));

            Text(answer).Should().Be("MULTIPOINT ((1 0), (-1 0))");
        }

        /// <summary>
        /// A coordinate of the geography, not a point on it — which is what makes this a different function
        /// from <c>ST_GEOG_CLOSESTPOINT</c>.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerACoordinateRatherThanAPointOnAnEdge()
        {
            var line = Wkt("LINESTRING(1 -1, 1 1)");

            // the two ends are symmetric about the equator, so as coordinates they genuinely tie
            Text(GeographyFunctions.ClosestCoordinate(Wkt("POINT(0 0)"), line)).Should().Be("MULTIPOINT ((1 -1), (1 1))");
            Text(GeographyFunctions.ClosestPoint(line, Wkt("POINT(0 0)"))).Should().Be("POINT (1 0)");
        }

        /// <summary>
        /// The closest point may fall part way along an edge, and the edge is a geodesic.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerAPointOnAnEdge()
        {
            var answer = GeographyFunctions.ClosestPoint(Wkt("LINESTRING(-1 1, 1 1)"), Wkt("POINT(0 0)"));

            answer.Should().NotBeNull();
            answer!.getCoordinate().getX().Should().BeApproximately(0, 1e-9);

            // north of the line's own ends, because a geodesic between two points on a parallel bows poleward
            answer.getCoordinate().getY().Should().BeGreaterThan(1.0);
        }

        /// <summary>
        /// The longest line joins the pair <c>ST_GEOG_MAXDISTANCE</c> measures.
        /// </summary>
        [TestMethod]
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
        /// Null in, null out, as everywhere else here.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.ClosestCoordinate(null, Wkt("POINT(0 0)")).Should().BeNull();
            GeographyFunctions.FurthestCoordinate(Wkt("POINT(0 0)"), null).Should().BeNull();
            GeographyFunctions.ClosestPoint(null, null).Should().BeNull();
            GeographyFunctions.LongestLine(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        /// <summary>
        /// Every one of the four stamped with WGS84, as every other constructor and editor is.
        /// </summary>
        [TestMethod]
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
        /// All four run as operators, which is what says the declarations bind to these bodies.
        /// </summary>
        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            var cases = new (string Sql, string Expected)[]
            {
                ("ST_GEOG_ASTEXT(ST_GEOG_CLOSESTCOORDINATE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('MULTIPOINT((1 0), (0 1))')))", "POINT (0 1)"),
                ("ST_GEOG_ASTEXT(ST_GEOG_FURTHESTCOORDINATE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('MULTIPOINT((1 0), (0 1))')))", "POINT (1 0)"),
                ("ST_GEOG_ASTEXT(ST_GEOG_CLOSESTPOINT(ST_GEOG_GEOMFROMTEXT('LINESTRING(1 -1, 1 1)'), ST_GEOG_GEOMFROMTEXT('POINT(0 0)')))", "POINT (1 0)"),
                ("ST_GEOG_ASTEXT(ST_GEOG_LONGESTLINE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('MULTIPOINT((2 0), (3 0))')))", "LINESTRING (0 0, 3 0)"),
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
