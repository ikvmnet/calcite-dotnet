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
        const double Degree = 6371010.0 * Math.PI / 180;

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
        static void Differ(Func<Geometry, Geometry, java.lang.Boolean?> geodesic, Func<Geometry, Geometry, bool> planar)
        {
            var differences = new List<string>();

            foreach (var left in shapes)
            {
                foreach (var right in shapes)
                {
                    var a = Wkt(left);
                    var b = Wkt(right);

                    var ours = geodesic(a, b)!.booleanValue();
                    var theirs = planar(a, b);

                    if (ours != theirs)
                        differences.Add($"{left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        [TestMethod]
        public void ShouldAgreeOnWithin()
        {
            Differ(GeographyFunctions.Within, SpatialTypeFunctions.ST_Within);
        }

        [TestMethod]
        public void ShouldAgreeOnIntersects()
        {
            Differ(GeographyFunctions.Intersects, SpatialTypeFunctions.ST_Intersects);
        }

        [TestMethod]
        public void ShouldAgreeOnIsValid()
        {
            var differences = new List<string>();

            foreach (var shape in shapes)
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

                    if (Math.Abs(ours - theirs) > Math.Max(1e-4 * theirs, 1e-6))
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
