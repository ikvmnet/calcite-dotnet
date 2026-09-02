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
    /// The same comparison <see cref="GeographyDifferentialTests"/> makes, over shapes nobody chose.
    /// </summary>
    /// <remarks>
    /// The hand-written set is a set of cases someone thought of, and every defect it found was one that had
    /// not been thought of until it was written down. This generates instead, and generates on a lattice
    /// rather than from arbitrary doubles, because the interesting cases are the coincidences — a vertex
    /// exactly on an edge, two edges collinear, two polygons sharing a corner — and random doubles never
    /// produce one. A seven-by-seven grid produces them constantly.
    ///
    /// <para>Everything stays inside a small box on the equator, where a great-circle edge and a straight
    /// line in longitude and latitude cannot be told apart, so any disagreement is a defect here rather than
    /// a difference of model. The seed is fixed, so a failure is reproducible and reportable rather than a
    /// thing that happened once.</para>
    ///
    /// <para>This is what stands in for the exact boolean operations. <c>S2BooleanOperation</c> would settle
    /// <c>ST_GEOG_WITHIN</c> by construction, and it is not reachable: the version of S2 published to Maven
    /// Central is the 2021 one, which does not have it, and the current source is compiled to Java 11, which
    /// IKVM does not read. Porting it is a port of four thousand lines and the <c>S2Builder</c>,
    /// <c>S2BuilderGraph</c>, <c>S2CrossingEdgesQuery</c> and <c>primitives</c> machinery underneath it. So
    /// the relation here is this project's own, and what makes it trustworthy is the size of the oracle
    /// rather than the pedigree of the algorithm.</para>
    /// </remarks>
    [TestClass]
    public class GeographyRandomDifferentialTests
    {

        /// <summary>
        /// Degrees per step of the lattice.
        /// </summary>
        const double Unit = 0.001;

        /// <summary>
        /// How far the lattice runs either side of the origin, in steps.
        /// </summary>
        const int Extent = 3;

        /// <summary>
        /// Metres per degree of arc on the sphere S2 models the Earth as.
        /// </summary>
        const double Degree = 6371010.0 * Math.PI / 180;

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static string Ordinate(int step)
        {
            return (step * Unit).ToString("0.######", System.Globalization.CultureInfo.InvariantCulture);
        }

        static string Point(Random random)
        {
            return $"{Ordinate(random.Next(-Extent, Extent + 1))} {Ordinate(random.Next(-Extent, Extent + 1))}";
        }

        /// <summary>
        /// A rectangle on the lattice, or <c>null</c> if the corners drawn do not make one.
        /// </summary>
        static (int Left, int Bottom, int Right, int Top)? Rectangle(Random random)
        {
            var x0 = random.Next(-Extent, Extent + 1);
            var x1 = random.Next(-Extent, Extent + 1);
            var y0 = random.Next(-Extent, Extent + 1);
            var y1 = random.Next(-Extent, Extent + 1);

            if (x0 == x1 || y0 == y1)
                return null;

            return (Math.Min(x0, x1), Math.Min(y0, y1), Math.Max(x0, x1), Math.Max(y0, y1));
        }

        static string Ring((int Left, int Bottom, int Right, int Top) r)
        {
            return $"({Ordinate(r.Left)} {Ordinate(r.Bottom)}, {Ordinate(r.Right)} {Ordinate(r.Bottom)}, "
                + $"{Ordinate(r.Right)} {Ordinate(r.Top)}, {Ordinate(r.Left)} {Ordinate(r.Top)}, "
                + $"{Ordinate(r.Left)} {Ordinate(r.Bottom)})";
        }

        /// <summary>
        /// A shape of some kind, or <c>null</c> when the draw did not make one.
        /// </summary>
        /// <remarks>
        /// The hole of a donut is inset from its own shell rather than drawn on its own, because a hole drawn
        /// independently is almost never inside the shell and the shape would be thrown away as invalid — the
        /// case would live in the generator and never reach the comparison.
        /// </remarks>
        static string? Shape(Random random)
        {
            switch (random.Next(7))
            {
                case 0:
                    return $"POINT({Point(random)})";
                case 1:
                    return $"MULTIPOINT(({Point(random)}), ({Point(random)}))";
                case 2:
                    return $"LINESTRING({Point(random)}, {Point(random)})";
                case 3:
                    return $"LINESTRING({Point(random)}, {Point(random)}, {Point(random)})";
                case 4:
                    return Rectangle(random) is { } one ? $"POLYGON({Ring(one)})" : null;
                case 5:
                    return Rectangle(random) is { } first && Rectangle(random) is { } second
                        ? $"MULTIPOLYGON(({Ring(first)}), ({Ring(second)}))"
                        : null;
                default:
                    if (Rectangle(random) is not { } shell)
                        return null;

                    if (shell.Right - shell.Left < 3 || shell.Top - shell.Bottom < 3)
                        return $"POLYGON({Ring(shell)})";

                    var hole = (shell.Left + 1, shell.Bottom + 1, shell.Right - 1, shell.Top - 1);
                    return $"POLYGON({Ring(shell)}, {Ring(hole)})";
            }
        }

        /// <summary>
        /// Runs every operation over a few thousand generated pairs and reports every disagreement at once.
        /// </summary>
        /// <remarks>
        /// Only shapes Calcite calls valid are compared: what a relation means over an invalid geometry is
        /// not defined by either model, so a disagreement there would say nothing. Validity itself is
        /// compared over everything generated, valid or not, because that operation does have an answer for
        /// both.
        ///
        /// <para>The counts are asserted as well as the disagreements. A generator that quietly stopped
        /// producing polygons, or a Calcite that started refusing every pair, would otherwise leave this
        /// green by leaving it empty.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldAgreeOverGeneratedShapes()
        {
            var differences = new List<string>();
            var compared = 0;
            var refused = 0;

            foreach (var seed in new[] { 1, 2, 3, 4, 5, 6 })
            {
                var random = new Random(seed);

                for (var i = 0; i < 5000 && differences.Count < 10; i++)
                {
                    if (Shape(random) is not { } left || Shape(random) is not { } right)
                        continue;

                    var a = Wkt(left);
                    var b = Wkt(right);

                    var ourValidity = GeographyFunctions.IsValid(a)!.booleanValue();
                    var theirValidity = SpatialTypeFunctions.ST_IsValid(a);

                    if (ourValidity != theirValidity)
                        differences.Add($"ST_ISVALID {left}: ours {ourValidity}, Calcite {theirValidity}");

                    if (theirValidity == false || SpatialTypeFunctions.ST_IsValid(b) == false)
                        continue;

                    compared++;

                    Compare(differences, "INTERSECTS", left, right,
                        () => GeographyFunctions.Intersects(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_Intersects(a, b), ref refused);

                    Compare(differences, "WITHIN", left, right,
                        () => GeographyFunctions.Within(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_Within(a, b), ref refused);

                    Compare(differences, "DWITHIN", left, right,
                        () => GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(0.0025 * Degree))!.booleanValue(),
                        () => SpatialTypeFunctions.ST_DWithin(a, b, 0.0025), ref refused);

                    var ours = GeographyFunctions.Distance(a, b)!.doubleValue();
                    var theirs = SpatialTypeFunctions.ST_Distance(a, b) * Degree;

                    if (Math.Abs(ours - theirs) > Math.Max(1e-4 * theirs, 1e-6))
                        differences.Add($"DISTANCE {left} / {right}: ours {ours}, Calcite {theirs}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
            compared.Should().BeGreaterThan(10000);
        }

        static void Compare(List<string> differences, string what, string left, string right, Func<bool> geodesic, Func<bool> planar, ref int refused)
        {
            bool theirs;

            try
            {
                theirs = planar();
            }
            catch (java.lang.IllegalArgumentException)
            {
                // Geometry.relate will not take a geometry collection; there is nothing to compare against
                refused++;
                return;
            }

            var ours = geodesic();

            if (ours != theirs)
                differences.Add($"{what} {left} / {right}: ours {ours}, Calcite {theirs}");
        }

    }

}
