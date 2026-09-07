using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Every <c>ST_GEOG_*</c> operation against the <c>ST_*</c> it mirrors, over shapes small enough and near
    /// enough the equator that the sphere and the plane have to agree.
    /// </summary>
    /// <remarks>
    /// Calcite's function is the specification with the plane swapped for the sphere — <c>ST_Within</c> is
    /// <c>geom1.within(geom2)</c> and <c>ST_Distance</c> is <c>geom1.distance(geom2)</c>, so what these
    /// operations mean is what JTS means by those words. A shape a few thousandths of a degree across at the
    /// equator is a shape where the difference between a great-circle edge and a straight line in longitude
    /// and latitude is far below any tolerance, so the two models must answer the same thing; where they do
    /// not, this convention has a defect rather than a different reading.
    ///
    /// <para>The cases where the two genuinely disagree are the point of the package and are held separately,
    /// in <see cref="GeographyFunctionTests"/>. Nothing here is near one.</para>
    /// </remarks>
    [TestClass]
    public class GeographyDifferentialTests
    {

        /// <summary>
        /// Metres per degree of arc on the sphere S2 models the Earth as.
        /// </summary>
        /// <summary>
        /// Metres per degree of longitude at the equator on WGS84, which is where the shapes below sit.
        /// </summary>
        /// <remarks>
        /// A scale factor between a planar answer in degrees and a geodesic one in metres is a spherical
        /// idea, and these measurements are no longer spherical. There is no single number here: a degree
        /// east is 111319.49 and a degree north is 110574.39, so a comparison scaled by either is out by up
        /// to 0.67% depending on which way the two shapes lie.
        ///
        /// <para>So the numeric comparisons below are a <em>bound</em> rather than an oracle. They catch a
        /// wrong unit, a wrong factor, a wrong shape — what a differential test is for — and they cannot
        /// confirm the model. <c>Wgs84MeasurementTests</c> is what does that, against figures measured from a
        /// live geodesic service.</para>
        /// </remarks>
        const double Degree = 111319.49079327357;

        /// <summary>
        /// How far the two models may differ before a difference is real.
        /// </summary>
        const double ModelGap = 1e-2;

        /// <summary>
        /// Shapes of every dimension, overlapping, touching, nested and disjoint.
        /// </summary>
        static readonly string[] shapes =
        [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "POINT(0 0)",
            "POINT(0.002 0.002)",
            "POINT(0.004 0)",
            "POINT(0.01 0.01)",
            "MULTIPOINT((0.001 0.001), (0.002 0.002))",
            "LINESTRING(0 0, 0.004 0)",
            "LINESTRING(0 0, 0.004 0.004)",
            "LINESTRING(0.001 -0.002, 0.001 0.002)",
            "LINESTRING(0.001 0.001, 0.003 0.001, 0.003 0.003)",
            "MULTILINESTRING((0 0, 0.004 0), (0 0.004, 0.004 0.004))",
            "POLYGON((0 0, 0.004 0, 0.004 0.004, 0 0.004, 0 0))",
            "POLYGON((0.001 0.001, 0.003 0.001, 0.003 0.003, 0.001 0.003, 0.001 0.001))",
            "POLYGON((0.002 0.002, 0.006 0.002, 0.006 0.006, 0.002 0.006, 0.002 0.002))",
            "POLYGON((0.01 0.01, 0.02 0.01, 0.02 0.02, 0.01 0.02, 0.01 0.01))",
            "POLYGON((0 0, 0.006 0, 0.006 0.006, 0 0.006, 0 0), (0.002 0.002, 0.004 0.002, 0.004 0.004, 0.002 0.004, 0.002 0.002))",
            "MULTIPOLYGON(((0 0, 0.002 0, 0.002 0.002, 0 0.002, 0 0)), ((0.004 0.004, 0.006 0.004, 0.006 0.006, 0.004 0.006, 0.004 0.004)))",
            // two lines meeting end to end, which is the container a line is covered by in two pieces
            "MULTILINESTRING((0 0, 0.002 0), (0.002 0, 0.004 0))",
            // a line half inside a polygon and half out of it
            "LINESTRING(0.002 0.002, 0.008 0.002)",
            // a square meeting another only at a corner
            "POLYGON((0.004 0.004, 0.006 0.004, 0.006 0.006, 0.004 0.006, 0.004 0.004))",
            // dimensions mixed in one geography
            "GEOMETRYCOLLECTION(POINT(0.001 0.001), LINESTRING(0.002 0, 0.004 0))",
            // negative coordinates, where a sign carried the wrong way would show
            "POINT(-0.001 -0.001)",
            "POLYGON((-0.002 -0.002, 0.002 -0.002, 0.002 0.002, -0.002 0.002, -0.002 -0.002))",
        ];

        /// <summary>
        /// Shapes that are not valid, for the one operation that has something to say about them.
        /// </summary>
        /// <remarks>
        /// They are kept out of the pairwise set deliberately: what a relation means over an invalid geometry
        /// is not defined by either model, so a disagreement there would say nothing.
        /// </remarks>
        static readonly string[] degenerate =
        [
            // a line that goes nowhere
            "LINESTRING(0 0, 0 0)",
            // a bow tie, whose edges cross
            "POLYGON((0 0, 0.004 0.004, 0.004 0, 0 0.004, 0 0))",
            // a ring with a repeated vertex
            "POLYGON((0 0, 0.004 0, 0.004 0, 0.004 0.004, 0 0.004, 0 0))",
        ];

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        /// <summary>
        /// Runs a binary predicate over every ordered pair of shapes and reports every disagreement at once.
        /// </summary>
        /// <param name="geodesic"></param>
        /// <param name="planar"></param>
        /// <param name="refusals">How many of the pairs Calcite is expected to refuse to answer.</param>
        /// <remarks>
        /// A pair Calcite throws on has no answer to compare against and is skipped, but the number of them
        /// is asserted rather than left open: a change that made Calcite refuse everything would otherwise
        /// turn this suite green by emptying it. What is refused is a geometry collection reaching
        /// <c>Geometry.relate</c>, which calls <c>checkNotGeometryCollection</c> — a multi-point, a
        /// multi-line and a multi-polygon are not collections by that test, and <c>contains</c> does not
        /// always reach <c>relate</c>, since it answers a rectangular container through
        /// <c>RectangleContains</c> first. So it is particular pairs rather than particular shapes, and
        /// <see cref="ShouldAnswerWithinOverACollectionWhereCalciteRefuses"/> pins what this convention says
        /// about one of them.
        /// </remarks>
        static void Differ(Func<Geometry, Geometry, java.lang.Boolean?> geodesic, Func<Geometry, Geometry, bool> planar, int refusals)
        {
            var differences = new List<string>();
            var refused = 0;

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    bool theirs;

                    try
                    {
                        theirs = planar(a, b);
                    }
                    catch (java.lang.IllegalArgumentException)
                    {
                        refused++;
                        continue;
                    }

                    var ours = geodesic(a, b)!.booleanValue();

                    if (ours != theirs)
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
            refused.Should().Be(refusals);
        }

        [TestMethod]
        public void ShouldAgreeOnWithin()
        {
            Differ(GeographyFunctions.Within, SpatialTypeFunctions.ST_Within, refusals: 6);
        }

        [TestMethod]
        public void ShouldAgreeOnIntersects()
        {
            Differ(GeographyFunctions.Intersects, SpatialTypeFunctions.ST_Intersects, refusals: 0);
        }

        /// <summary>
        /// A geometry collection is answered rather than refused.
        /// </summary>
        /// <remarks>
        /// A deliberate divergence, and of the same kind as the one in <c>GeographyFunctions.DWithin</c>:
        /// Calcite cannot answer this at all, and reproducing an inability buys nothing. The container is the
        /// donut rather than a square because <c>Geometry.contains</c> answers a rectangular one through
        /// <c>RectangleContains</c> without reaching the relate that refuses collections — the refusal is a
        /// property of the pair and not of the collection.
        ///
        /// <para>The collection is a point inside the donut and a line lying along its southern edge, so
        /// every part of it lies in the donut, and the interiors meet at the point even though the line only
        /// touches the boundary. That is the answer JTS would give if its relate handled mixed
        /// dimensions.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldAnswerWithinOverACollectionWhereCalciteRefuses()
        {
            var collection = Wkt("GEOMETRYCOLLECTION(POINT(0.001 0.001), LINESTRING(0.002 0, 0.004 0))");
            var donut = Wkt("POLYGON((0 0, 0.006 0, 0.006 0.006, 0 0.006, 0 0), (0.002 0.002, 0.004 0.002, 0.004 0.004, 0.002 0.004, 0.002 0.002))");

            var refused = () => SpatialTypeFunctions.ST_Within(collection, donut);
            refused.Should().Throw<java.lang.IllegalArgumentException>().WithMessage("*GeometryCollection*");

            GeographyFunctions.Within(collection, donut)!.booleanValue().Should().BeTrue();

            // the line alone touches only the boundary, so its interior never meets the interior of the
            // donut and it is not within it — which Calcite can answer, and does answer the same way
            var line = Wkt("LINESTRING(0.002 0, 0.004 0)");
            GeographyFunctions.Within(line, donut)!.booleanValue().Should().BeFalse();
            SpatialTypeFunctions.ST_Within(line, donut).Should().BeFalse();
        }

        [TestMethod]
        public void ShouldAgreeOnContains()
        {
            Differ(GeographyFunctions.Contains, SpatialTypeFunctions.ST_Contains, refusals: 6);
        }

        [TestMethod]
        public void ShouldAgreeOnCovers()
        {
            Differ(GeographyFunctions.Covers, SpatialTypeFunctions.ST_Covers, refusals: 6);
        }

        [TestMethod]
        public void ShouldAgreeOnCoveredBy()
        {
            Differ(GeographyFunctions.CoveredBy, SpatialTypeFunctions.ST_CoveredBy, refusals: 6);
        }

        [TestMethod]
        public void ShouldAgreeOnDisjoint()
        {
            Differ(GeographyFunctions.Disjoint, SpatialTypeFunctions.ST_Disjoint, refusals: 0);
        }

        [TestMethod]
        public void ShouldAgreeOnEquals()
        {
            Differ(GeographyFunctions.Equals, SpatialTypeFunctions.ST_Equals, refusals: 1);
        }

        [TestMethod]
        public void ShouldAgreeOnEnvelopesIntersect()
        {
            Differ(GeographyFunctions.EnvelopesIntersect, SpatialTypeFunctions.ST_EnvelopesIntersect, refusals: 0);
        }

        [TestMethod]
        public void ShouldAgreeOnIsValid()
        {
            var differences = new List<string>();

            foreach (var shape in System.Linq.Enumerable.Concat(shapes, degenerate))
            {
                var geography = Wkt(shape);
                var ours = GeographyFunctions.IsValid(geography)!.booleanValue();
                var theirs = SpatialTypeFunctions.ST_IsValid(geography);

                if (ours != theirs)
                    differences.Add($"{shape}: ours {ours}, Calcite {theirs}");
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// The distance in metres against the distance in degrees, which at this scale on the equator is the
        /// same measurement in two units.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnDistance()
        {
            var differences = new List<string>();

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    var ours = GeographyFunctions.Distance(a, b)!.doubleValue();
                    var theirs = SpatialTypeFunctions.ST_Distance(a, b) * Degree;

                    if (Math.Abs(ours - theirs) > Math.Max(ModelGap * theirs, 1e-6))
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// <c>ST_DWithin</c> is <c>distance &lt;= d</c> and nothing else, so it is tested at a threshold that
        /// falls between the pairs rather than on one.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnDWithin()
        {
            var differences = new List<string>();

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    var threshold = 0.003;

                    // a boolean cannot be compared to a tolerance, so the band where the two models decide
                    // differently is skipped instead: within it the answer turns on which Earth is being
                    // measured, which is the disagreement rather than a defect. An adapter rechecking a
                    // pushed-down predicate has the same band and the same problem.
                    if (Math.Abs(SpatialTypeFunctions.ST_Distance(a, b) - threshold) <= ModelGap * threshold)
                        continue;

                    var ours = GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(threshold * Degree))!.booleanValue();
                    var theirs = SpatialTypeFunctions.ST_DWithin(a, b, threshold);

                    if (ours != theirs)
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

    }

}
