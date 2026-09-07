using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Whether a geography touches itself, which is a question about where its edges go — and its edges are
    /// geodesics.
    /// </summary>
    /// <remarks>
    /// This joins <c>ST_GEOG_ISVALID</c>, which was here from the first increment. The rule is JTS's: a point
    /// is simple, a set of points is simple when none repeats, a line is simple when no two of its edges meet
    /// except where they are joined, an area is simple because its self-intersections are a question of
    /// validity instead, and a collection is simple when its parts are.
    /// </remarks>
    [TestClass]
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
        /// The one that separates the two readings: a line simple on the map and not on the Earth.
        /// </summary>
        /// <remarks>
        /// The first edge runs sixty degrees of longitude along the 60th parallel, and a geodesic between two
        /// points on a parallel bows poleward — this one to 63.4 degrees at its middle. The last edge is drawn
        /// at latitude 62 but spans only twenty-five degrees, so it bows to 62.6 and stays under the first
        /// edge's peak while sitting above its ends. They therefore cross. On a plane they are two horizontal
        /// segments two degrees apart and never meet, so Calcite calls the line simple.
        ///
        /// <para>How much an edge bows is <c>atan(tan φ / cos(Δλ/2))</c>, which grows with the span. It is the
        /// difference in span rather than the difference in latitude that puts one edge over the other: two
        /// edges spanning the same longitude bow alike however far apart their parallels are.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldSeeACrossingThatOnlyExistsOnTheEarth()
        {
            const string wkt = "LINESTRING(0 60, 60 60, 30 62, 5 62)";

            Wkt(wkt).isSimple().Should().BeTrue("the planar edges are two parallels two degrees apart");
            Simple(wkt).Should().BeFalse("the first edge bows to 63.4 and the last only to 62.6, so they cross");
        }

        /// <summary>
        /// An ordinary self-crossing line is not simple either way.
        /// </summary>
        [TestMethod]
        public void ShouldSeeAnOrdinaryCrossing()
        {
            Simple("LINESTRING(0 0, 2 2, 0 2, 2 0)").Should().BeFalse();
            Wkt("LINESTRING(0 0, 2 2, 0 2, 2 0)").isSimple().Should().BeFalse();
        }

        /// <summary>
        /// A line that does not touch itself is simple, and so is one that closes.
        /// </summary>
        [TestMethod]
        public void ShouldAcceptALineAndARing()
        {
            Simple("LINESTRING(0 0, 1 0, 1 1)").Should().BeTrue();
            Simple("LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)").Should().BeTrue("closing is not touching");
        }

        /// <summary>
        /// A vertex reached twice is a touch, save for the one that closes a ring.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseARepeatedVertex()
        {
            Simple("LINESTRING(0 0, 1 0, 0 0, 1 1)").Should().BeFalse();
            Simple("MULTIPOINT((0 0), (1 1), (0 0))").Should().BeFalse();
            Simple("MULTIPOINT((0 0), (1 1))").Should().BeTrue();
        }

        /// <summary>
        /// A point is simple, and so is an area whatever it does to itself.
        /// </summary>
        [TestMethod]
        public void ShouldCallAPointAndAnAreaSimple()
        {
            Simple("POINT(1 2)").Should().BeTrue();
            Simple("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))").Should().BeTrue();
        }

        /// <summary>
        /// Only a closed, simple line is a ring.
        /// </summary>
        [TestMethod]
        public void ShouldAskBothQuestionsOfARing()
        {
            Ring("LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)").Should().BeTrue();
            Ring("LINESTRING(0 0, 1 0, 1 1)").Should().BeFalse("it does not close");
            Ring("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))").Should().BeFalse("only a line can be a ring");
            Ring("POINT(0 0)").Should().BeFalse();
        }

        /// <summary>
        /// A ring whose edges cross on the Earth is not a ring here either.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseARingThatCrossesOnlyOnTheEarth()
        {
            const string wkt = "LINESTRING(0 60, 60 60, 30 62, 5 62, 0 60)";

            ((org.locationtech.jts.geom.LineString)Wkt(wkt)).isRing().Should().BeTrue("the planar edges do not cross");
            Ring(wkt).Should().BeFalse("the geodesic ones do");
        }

        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.IsSimple(null).Should().BeNull();
            GeographyFunctions.IsRing(null).Should().BeNull();
        }

        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            var simple = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_ISSIMPLE(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0, 1 1)'))")[0][0];
            (simple is java.lang.Boolean a && a.booleanValue()).Should().BeTrue();

            var ring = GeographyExecutionTests.Run(
                "SELECT ST_GEOG_ISRING(ST_GEOG_GEOMFROMTEXT('LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)'))")[0][0];
            (ring is java.lang.Boolean b && b.booleanValue()).Should().BeTrue();
        }

    }

}
