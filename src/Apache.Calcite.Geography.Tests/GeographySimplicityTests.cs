using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_ISSIMPLE</c> and <c>CLR_ST_GEOG_ISRING</c>, which test for self-intersection along
    /// geodesic edges.
    /// </summary>
    /// <remarks>
    /// The rules are JTS's: a point is simple; a multi-point is simple when no point repeats; a line is simple
    /// when no two edges meet except where they join; a polygon is simple, its self-intersection being a
    /// matter of validity; and a collection is simple when its parts are.
    /// </remarks>
    public class GeographySimplicityTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static bool Simple(string wkt)
        {
            return GeographyFunctions.IsSimple(Wkt(wkt))!.booleanValue();
        }

        static bool Ring(string wkt)
        {
            return GeographyFunctions.IsRing(Wkt(wkt))!.booleanValue();
        }

        /// <summary>
        /// A line that is simple in the plane crosses itself along geodesic edges.
        /// </summary>
        /// <remarks>
        /// The first edge spans sixty degrees of longitude on the 60th parallel and as a geodesic reaches 63.4
        /// degrees at its middle. The last edge, on the 62nd parallel, spans twenty-five degrees and reaches
        /// only 62.6, so it lies below the first edge's peak and above its ends, and the two cross. On a plane
        /// they are parallel segments two degrees apart, so Calcite calls the line simple. A geodesic between
        /// two points at latitude φ peaks at <c>atan(tan φ / cos(Δλ/2))</c>.
        /// </remarks>
        [Fact]
        public void ShouldSeeACrossingThatOnlyExistsOnTheEarth()
        {
            const string wkt = "LINESTRING(0 60, 60 60, 30 62, 5 62)";

            Wkt(wkt).isSimple().Should().BeTrue("the planar edges are two parallels two degrees apart");
            Simple(wkt).Should().BeFalse("the first edge bows to 63.4 and the last only to 62.6, so they cross");
        }

        /// <summary>
        /// A self-crossing line is not simple in either reading.
        /// </summary>
        [Fact]
        public void ShouldSeeAnOrdinaryCrossing()
        {
            Simple("LINESTRING(0 0, 2 2, 0 2, 2 0)").Should().BeFalse();
            Wkt("LINESTRING(0 0, 2 2, 0 2, 2 0)").isSimple().Should().BeFalse();
        }

        /// <summary>
        /// A line that does not touch itself is simple, closed or not.
        /// </summary>
        [Fact]
        public void ShouldAcceptALineAndARing()
        {
            Simple("LINESTRING(0 0, 1 0, 1 1)").Should().BeTrue();
            Simple("LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)").Should().BeTrue("closing is not touching");
        }

        /// <summary>
        /// A repeated vertex, other than the one closing a ring, makes a line or multi-point not simple.
        /// </summary>
        [Fact]
        public void ShouldRefuseARepeatedVertex()
        {
            Simple("LINESTRING(0 0, 1 0, 0 0, 1 1)").Should().BeFalse();
            Simple("MULTIPOINT((0 0), (1 1), (0 0))").Should().BeFalse();
            Simple("MULTIPOINT((0 0), (1 1))").Should().BeTrue();
        }

        /// <summary>
        /// A point and a polygon are simple.
        /// </summary>
        [Fact]
        public void ShouldCallAPointAndAnAreaSimple()
        {
            Simple("POINT(1 2)").Should().BeTrue();
            Simple("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))").Should().BeTrue();
        }

        /// <summary>
        /// Only a closed, simple line is a ring.
        /// </summary>
        [Fact]
        public void ShouldAskBothQuestionsOfARing()
        {
            Ring("LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)").Should().BeTrue();
            Ring("LINESTRING(0 0, 1 0, 1 1)").Should().BeFalse("it does not close");
            Ring("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))").Should().BeFalse("only a line can be a ring");
            Ring("POINT(0 0)").Should().BeFalse();
        }

        /// <summary>
        /// A closed line whose geodesic edges cross is not a ring, though its planar edges do not cross.
        /// </summary>
        [Fact]
        public void ShouldRefuseARingThatCrossesOnlyOnTheEarth()
        {
            const string wkt = "LINESTRING(0 60, 60 60, 30 62, 5 62, 0 60)";

            ((org.locationtech.jts.geom.LineString)Wkt(wkt)).isRing().Should().BeTrue("the planar edges do not cross");
            Ring(wkt).Should().BeFalse("the geodesic ones do");
        }

        [Fact]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.IsSimple(null).Should().BeNull();
            GeographyFunctions.IsRing(null).Should().BeNull();
        }

        [Fact]
        public void ShouldRunEachAsAnOperator()
        {
            var simple = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_ISSIMPLE(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0, 1 1)'))")[0][0];
            (simple is java.lang.Boolean a && a.booleanValue()).Should().BeTrue();

            var ring = GeographyExecutionTests.Run(
                "SELECT CLR_ST_GEOG_ISRING(CLR_ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)'))")[0][0];
            (ring is java.lang.Boolean b && b.booleanValue()).Should().BeTrue();
        }

    }

}
