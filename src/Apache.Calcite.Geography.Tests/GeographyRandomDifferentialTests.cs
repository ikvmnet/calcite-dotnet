using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Makes the comparison <see cref="GeographyDifferentialTests"/> makes, over generated shapes.
    /// </summary>
    /// <remarks>
    /// Shapes are drawn on a seven-by-seven lattice rather than from arbitrary doubles, so that coincidences
    /// (a vertex exactly on an edge, collinear edges, polygons sharing a corner) occur often. The lattice is a
    /// small box on the equator, where a geodesic edge and a straight line in degrees cannot be told apart,
    /// so any disagreement is a defect. The seeds are fixed, so a failure is reproducible.
    ///
    /// <para>The S2 Java library this package references has no <c>S2BooleanOperation</c>, so the relations
    /// such as <c>CLR_ST_GEOG_WITHIN</c> are this package's own; this comparison is the main check on
    /// them.</para>
    /// </remarks>
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
        /// Metres per degree of longitude at the equator on WGS84, where the shapes sit.
        /// </summary>
        /// <remarks>
        /// On the ellipsoid a degree east at the equator is 111319.49 m and a degree north is 110574.39 m, so a
        /// planar measurement scaled by this factor can be off by up to 0.67% depending on direction. The
        /// numeric comparisons are therefore a bound that catches a wrong unit, factor or shape, not a check
        /// of the model; <see cref="Wgs84MeasurementTests"/> checks the model.
        /// </remarks>
        const double Degree = 111319.49079327357;

        /// <summary>
        /// The relative difference allowed between a geodesic and a scaled planar measurement.
        /// </summary>
        const double ModelGap = 1e-2;

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
        /// A random shape of one of seven kinds, or <c>null</c> when the draw does not make one.
        /// </summary>
        /// <remarks>
        /// A polygon's hole is inset one step from its shell rather than drawn independently, because an
        /// independent hole is rarely inside the shell and the invalid shape would be skipped.
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
        /// Runs every relation and measurement over generated pairs and reports the disagreements.
        /// </summary>
        /// <remarks>
        /// Validity is compared for every generated left-hand shape. The relations and measurements are
        /// compared only for pairs Calcite calls valid, since neither model defines them over an invalid
        /// geometry. The number of pairs compared is asserted, so that the test cannot pass by comparing
        /// nothing. It stops early after ten disagreements.
        /// </remarks>
        [Fact]
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

                    Compare(differences, "CONTAINS", left, right,
                        () => GeographyFunctions.Contains(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_Contains(a, b), ref refused);

                    Compare(differences, "COVERS", left, right,
                        () => GeographyFunctions.Covers(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_Covers(a, b), ref refused);

                    Compare(differences, "COVEREDBY", left, right,
                        () => GeographyFunctions.CoveredBy(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_CoveredBy(a, b), ref refused);

                    Compare(differences, "DISJOINT", left, right,
                        () => GeographyFunctions.Disjoint(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_Disjoint(a, b), ref refused);

                    Compare(differences, "EQUALS", left, right,
                        () => GeographyFunctions.Equals(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_Equals(a, b), ref refused);

                    Compare(differences, "ENVELOPESINTERSECT", left, right,
                        () => GeographyFunctions.EnvelopesIntersect(a, b)!.booleanValue(), () => SpatialTypeFunctions.ST_EnvelopesIntersect(a, b), ref refused);

                    // skipped near the threshold, where the two models can decide differently; see Degree
                    if (Math.Abs(SpatialTypeFunctions.ST_Distance(a, b) - 0.0025) > ModelGap * 0.0025)
                        Compare(differences, "DWITHIN", left, right,
                            () => GeographyFunctions.DWithin(a, b, java.lang.Double.valueOf(0.0025 * Degree))!.booleanValue(),
                            () => SpatialTypeFunctions.ST_DWithin(a, b, 0.0025), ref refused);

                    Measure(differences, "DISTANCE", left, right,
                        () => GeographyFunctions.Distance(a, b)!.doubleValue(), SpatialTypeFunctions.ST_Distance(a, b) * Degree);

                    Measure(differences, "MAXDISTANCE", left, right,
                        () => GeographyFunctions.MaxDistance(a, b)!.doubleValue(), SpatialTypeFunctions.ST_MaxDistance(a, b)!.doubleValue() * Degree);

                    Measure(differences, "LENGTH", left, left,
                        () => GeographyFunctions.Length(a)!.doubleValue(), SpatialTypeFunctions.ST_Length(a)!.doubleValue() * Degree);

                    Measure(differences, "PERIMETER", left, left,
                        () => GeographyFunctions.Perimeter(a)!.doubleValue(), SpatialTypeFunctions.ST_Perimeter(a)!.doubleValue() * Degree);

                    // an area scales by the square of the length factor
                    Measure(differences, "AREA", left, left,
                        () => GeographyFunctions.Area(a)!.doubleValue(), SpatialTypeFunctions.ST_Area(a)!.doubleValue() * Degree * Degree);
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
            compared.Should().BeGreaterThan(10000);
        }

        /// <summary>
        /// Compares a geodesic measurement in metres with Calcite's planar one already scaled to metres.
        /// </summary>
        /// <remarks>
        /// The tolerance is <see cref="ModelGap"/> relative to the planar value, with an absolute floor for
        /// answers of zero.
        /// </remarks>
        static void Measure(List<string> differences, string what, string left, string right, Func<double> geodesic, double planar)
        {
            var ours = geodesic();

            if (Math.Abs(ours - planar) > Math.Max(ModelGap * Math.Abs(planar), 1e-6))
                differences.Add($"{what} {left} / {right}: ours {ours}, Calcite {planar}");
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
                // Geometry.relate refuses a geometry collection, so there is no answer to compare with.
                refused++;
                return;
            }

            var ours = geodesic();

            if (ours != theirs)
                differences.Add($"{what} {left} / {right}: ours {ours}, Calcite {theirs}");
        }

    }

}
