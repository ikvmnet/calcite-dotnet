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
    /// Compares the editing functions and the constructors with the Calcite <c>ST_*</c> function each one
    /// delegates to.
    /// </summary>
    /// <remarks>
    /// Like the accessors, these rearrange coordinates without geodesy, so the tests check that each name is
    /// wired to the right body.
    ///
    /// <para>The results differ from Calcite's in one respect: this package stamps them with SRID 4326, where
    /// Calcite's come back with SRID 0. The comparisons use WKT, which carries no SRID, and
    /// <see cref="ShouldStampEveryEditedGeographyWithWgs84"/> checks the stamp.</para>
    /// </remarks>
    public class GeographyEditingTests
    {

        static readonly string[] shapes =
        [
            "POINT(1 2)",
            "POINT Z(1 2 3)",
            "MULTIPOINT((1 2), (3 4))",
            "LINESTRING(0 0, 1 1, 2 0)",
            "LINESTRING(0 0, 1 1, 1 1, 2 0)",
            "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
            "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
            "POLYGON((0 0, 6 0, 6 6, 0 6, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))",
            "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))",
            "GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))",
        ];

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static readonly (string Name, Func<Geometry, object?> Ours, Func<Geometry, object?> Theirs)[] unary =
        [
            ("CLR_ST_GEOG_FLIPCOORDINATES", g => GeographyFunctions.FlipCoordinates(g), g => SpatialTypeFunctions.ST_FlipCoordinates(g)),
            ("CLR_ST_GEOG_FORCE2D", g => GeographyFunctions.Force2D(g), g => SpatialTypeFunctions.ST_Force2D(g)),
            ("CLR_ST_GEOG_FORCE3D", g => GeographyFunctions.Force3D(g), g => SpatialTypeFunctions.ST_Force3D(g)),
            ("CLR_ST_GEOG_NORMALIZE", g => GeographyFunctions.Normalize(g), g => SpatialTypeFunctions.ST_Normalize(g)),
            ("CLR_ST_GEOG_REMOVEHOLES", g => GeographyFunctions.RemoveHoles(g), g => SpatialTypeFunctions.ST_RemoveHoles(g)),
            ("CLR_ST_GEOG_REMOVEREPEATEDPOINTS", g => GeographyFunctions.RemoveRepeatedPoints(g), g => SpatialTypeFunctions.ST_RemoveRepeatedPoints(g)),
            ("CLR_ST_GEOG_REVERSE", g => GeographyFunctions.Reverse(g), g => SpatialTypeFunctions.ST_Reverse(g)),
            ("CLR_ST_GEOG_TOMULTILINE", g => GeographyFunctions.ToMultiLine(g), g => SpatialTypeFunctions.ST_ToMultiLine(g)),
            ("CLR_ST_GEOG_TOMULTIPOINT", g => GeographyFunctions.ToMultiPoint(g), g => SpatialTypeFunctions.ST_ToMultiPoint(g)),
            ("CLR_ST_GEOG_TOMULTISEGMENTS", g => GeographyFunctions.ToMultiSegments(g), g => SpatialTypeFunctions.ST_ToMultiSegments(g)),
        ];

        [Fact]
        public void ShouldAgreeWithCalciteOnEveryEditingFunction()
        {
            var differences = new List<string>();

            foreach (var shape in shapes)
            {
                var geography = Wkt(shape);

                foreach (var (name, ours, theirs) in unary)
                {
                    var mine = GeographyAccessorTests.Answer(() => ours(geography));
                    var calcite = GeographyAccessorTests.Answer(() => theirs(geography));

                    if (mine != calcite)
                        differences.Add($"{name} over {shape}: ours {mine}, Calcite {calcite}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        [Fact]
        public void ShouldAgreeWithCalciteOnTheEditingFunctionsThatTakeAnArgument()
        {
            var differences = new List<string>();
            var point = Wkt("POINT(9 9)");

            foreach (var shape in shapes)
            {
                var g = Wkt(shape);

                Compare(differences, $"CLR_ST_GEOG_ADDPOINT({shape}, POINT(9 9))",
                    () => GeographyFunctions.AddPoint(g, point), () => SpatialTypeFunctions.ST_AddPoint(g, point));

                for (var n = 0; n <= 2; n++)
                {
                    var index = java.lang.Integer.valueOf(n);

                    Compare(differences, $"CLR_ST_GEOG_ADDPOINT({shape}, POINT(9 9), {n})",
                        () => GeographyFunctions.AddPoint(g, point, index), () => SpatialTypeFunctions.ST_AddPoint(g, point, n));

                    Compare(differences, $"CLR_ST_GEOG_REMOVEPOINT({shape}, {n})",
                        () => GeographyFunctions.RemovePoint(g, index), () => SpatialTypeFunctions.ST_RemovePoint(g, n));
                }

                Compare(differences, $"CLR_ST_GEOG_ADDZ({shape}, 5)",
                    () => GeographyFunctions.AddZ(g, java.lang.Double.valueOf(5)),
                    () => SpatialTypeFunctions.ST_AddZ(g, java.math.BigDecimal.valueOf(5.0)));

                Compare(differences, $"CLR_ST_GEOG_REMOVEREPEATEDPOINTS({shape}, 0.5)",
                    () => GeographyFunctions.RemoveRepeatedPoints(g, java.lang.Double.valueOf(0.5)),
                    () => SpatialTypeFunctions.ST_RemoveRepeatedPoints(g, java.math.BigDecimal.valueOf(0.5)));
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        [Fact]
        public void ShouldAgreeWithCalciteOnTheConstructors()
        {
            var differences = new List<string>();
            var one = java.lang.Double.valueOf(1);
            var two = java.lang.Double.valueOf(2);
            var three = java.lang.Double.valueOf(3);

            Compare(differences, "CLR_ST_GEOG_POINT(1, 2)",
                () => GeographyFunctions.Point(one, two), () => SpatialTypeFunctions.ST_Point(Dec(1), Dec(2)));

            Compare(differences, "CLR_ST_GEOG_POINT(1, 2, 3)",
                () => GeographyFunctions.Point(one, two, three),
                () => SpatialTypeFunctions.ST_Point(Dec(1), Dec(2), Dec(3)));

            var a = Wkt("POINT(0 0)");
            var b = Wkt("POINT(1 1)");
            var c = Wkt("POINT(2 0)");

            Compare(differences, "CLR_ST_GEOG_MAKELINE(a, b)",
                () => GeographyFunctions.MakeLine(a, b), () => SpatialTypeFunctions.ST_MakeLine(a, b));

            Compare(differences, "CLR_ST_GEOG_MAKELINE(a, b, c)",
                () => GeographyFunctions.MakeLine(a, b, c), () => SpatialTypeFunctions.ST_MakeLine(a, b, c));

            var shell = Wkt("LINESTRING(0 0, 6 0, 6 6, 0 6, 0 0)");
            var hole = Wkt("LINESTRING(2 2, 4 2, 4 4, 2 4, 2 2)");

            Compare(differences, "CLR_ST_GEOG_MAKEPOLYGON(shell)",
                () => GeographyFunctions.MakePolygon(shell), () => SpatialTypeFunctions.ST_MakePolygon(shell));

            Compare(differences, "CLR_ST_GEOG_MAKEPOLYGON(shell, hole)",
                () => GeographyFunctions.MakePolygon(shell, hole), () => SpatialTypeFunctions.ST_MakePolygon(shell, hole));

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// Each typed reader returns a shape of its own kind and null for any other kind, as Calcite's do.
        /// </summary>
        [Fact]
        public void ShouldAgreeWithCalciteOnTheTypedReaders()
        {
            var differences = new List<string>();

            foreach (var wkt in new[]
            {
                "POINT(1 2)",
                "LINESTRING(0 0, 1 1)",
                "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
                "MULTIPOINT((1 2), (3 4))",
                "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
                "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))",
            })
            {
                Compare(differences, $"CLR_ST_GEOG_POINTFROMTEXT({wkt})",
                    () => GeographyFunctions.PointFromText(wkt), () => SpatialTypeFunctions.ST_PointFromText(wkt));
                Compare(differences, $"CLR_ST_GEOG_LINEFROMTEXT({wkt})",
                    () => GeographyFunctions.LineFromText(wkt), () => SpatialTypeFunctions.ST_LineFromText(wkt));
                Compare(differences, $"CLR_ST_GEOG_POLYFROMTEXT({wkt})",
                    () => GeographyFunctions.PolyFromText(wkt), () => SpatialTypeFunctions.ST_PolyFromText(wkt));
                Compare(differences, $"CLR_ST_GEOG_MPOINTFROMTEXT({wkt})",
                    () => GeographyFunctions.MPointFromText(wkt), () => SpatialTypeFunctions.ST_MPointFromText(wkt));
                Compare(differences, $"CLR_ST_GEOG_MLINEFROMTEXT({wkt})",
                    () => GeographyFunctions.MLineFromText(wkt), () => SpatialTypeFunctions.ST_MLineFromText(wkt));
                Compare(differences, $"CLR_ST_GEOG_MPOLYFROMTEXT({wkt})",
                    () => GeographyFunctions.MPolyFromText(wkt), () => SpatialTypeFunctions.ST_MPolyFromText(wkt));

                var wkb = GeographyFunctions.AsWkb(Wkt(wkt));

                Compare(differences, $"CLR_ST_GEOG_POINTFROMWKB({wkt})",
                    () => GeographyFunctions.PointFromWkb(wkb), () => SpatialTypeFunctions.ST_PointFromWKB(wkb));
                Compare(differences, $"CLR_ST_GEOG_LINEFROMWKB({wkt})",
                    () => GeographyFunctions.LineFromWkb(wkb), () => SpatialTypeFunctions.ST_LineFromWKB(wkb));
                Compare(differences, $"CLR_ST_GEOG_POLYFROMWKB({wkt})",
                    () => GeographyFunctions.PolyFromWkb(wkb), () => SpatialTypeFunctions.ST_PolyFromWKB(wkb));
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// Runs every editing operator and constructor as SQL and requires the same answer as calling its
        /// method.
        /// </summary>
        /// <remarks>
        /// Many of these operators share a signature, so only running them shows that, for example,
        /// <c>CLR_ST_GEOG_FORCE2D</c> is bound to <c>Force2D</c> and not <c>Force3D</c>. Every result is
        /// wrapped in <c>CLR_ST_GEOG_ASTEXT</c> so that the column is a string.
        /// </remarks>
        [Fact]
        public void ShouldRunEveryEditingOperatorAsAnOperator()
        {
            const string shape = "POLYGON((0 0, 6 0, 6 6, 0 6, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))";
            var geography = Wkt(shape);
            var subject = $"CLR_ST_GEOG_GEOMFROMTEXT('{shape}')";

            var wanted = new List<string>();
            var names = new List<string>();
            var expressions = new List<string>();

            foreach (var (name, ours, _) in unary)
            {
                // AsText rather than Geometry.toText, because the statement asks for AsText and the two differ:
                // ST_ASTEXT's writer is given the shape's number of ordinates, and toText always writes two.
                var answer = GeographyAccessorTests.Answer(() => GeographyFunctions.AsText(ours(geography) as Geometry));
                if (answer.EndsWith("Exception"))
                    continue;

                wanted.Add(answer);
                names.Add(name);
                expressions.Add($"CLR_ST_GEOG_ASTEXT({name}({subject}))");
            }

            var extra = new (string Sql, Func<object?> Ours)[]
            {
                // ST_AddPoint throws over anything but a line
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_ADDPOINT(CLR_ST_GEOG_LINEFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), CLR_ST_GEOG_POINT(9, 9)))",
                    () => GeographyFunctions.AddPoint(Wkt("LINESTRING(0 0, 1 1, 2 0)"), Wkt("POINT(9 9)"))),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_REMOVEPOINT(CLR_ST_GEOG_LINEFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), 1))",
                    () => GeographyFunctions.RemovePoint(Wkt("LINESTRING(0 0, 1 1, 2 0)"), java.lang.Integer.valueOf(1))),
                // a point, because ST_AddZ throws over a polygon (see ShouldInheritTheDefectInAddZOverAPolygon)
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_ADDZ(CLR_ST_GEOG_POINT(1, 2), 5))",
                    () => GeographyFunctions.AddZ(Wkt("POINT(1 2)"), java.lang.Double.valueOf(5))),
                ($"CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_REMOVEREPEATEDPOINTS({subject}, 0.5))",
                    () => GeographyFunctions.RemoveRepeatedPoints(geography, java.lang.Double.valueOf(0.5))),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_POINT(1, 2))",
                    () => GeographyFunctions.Point(java.lang.Double.valueOf(1), java.lang.Double.valueOf(2))),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_MAKEPOINT(1, 2))",
                    () => GeographyFunctions.Point(java.lang.Double.valueOf(1), java.lang.Double.valueOf(2))),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_MAKELINE(CLR_ST_GEOG_POINT(0, 0), CLR_ST_GEOG_POINT(1, 1), CLR_ST_GEOG_POINT(2, 0)))",
                    () => GeographyFunctions.MakeLine(Wkt("POINT(0 0)"), Wkt("POINT(1 1)"), Wkt("POINT(2 0)"))),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_MAKEPOLYGON(CLR_ST_GEOG_LINEFROMTEXT('LINESTRING(0 0, 6 0, 6 6, 0 6, 0 0)')))",
                    () => GeographyFunctions.MakePolygon(Wkt("LINESTRING(0 0, 6 0, 6 6, 0 6, 0 0)"))),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_POINTFROMTEXT('POINT(1 2)'))",
                    () => GeographyFunctions.PointFromText("POINT(1 2)")),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_POLYFROMTEXT('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'))",
                    () => GeographyFunctions.PolyFromText("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")),
                ("CLR_ST_GEOG_ASTEXT(CLR_ST_GEOG_MLINEFROMTEXT('MULTILINESTRING((0 0, 1 1))'))",
                    () => GeographyFunctions.MLineFromText("MULTILINESTRING((0 0, 1 1))")),
            };

            foreach (var (sql, ours) in extra)
            {
                var answer = GeographyAccessorTests.Answer(() => GeographyFunctions.AsText(ours() as Geometry));

                // An expression that throws would fail the whole statement; the comparisons above cover it.
                if (answer.EndsWith("Exception"))
                    continue;

                wanted.Add(answer);
                names.Add(sql);
                expressions.Add(sql);
            }

            var row = GeographyExecutionTests.Run("SELECT " + string.Join(", ", expressions))[0];

            for (var i = 0; i < wanted.Count; i++)
                GeographyAccessorTests.Render(row[i]).Should().Be(wanted[i], names[i]);
        }

        static java.math.BigDecimal Dec(double value)
        {
            return java.math.BigDecimal.valueOf(value);
        }

        static void Compare(List<string> differences, string what, Func<object?> ours, Func<object?> theirs)
        {
            var mine = GeographyAccessorTests.Answer(ours);
            var calcite = GeographyAccessorTests.Answer(theirs);

            if (mine != calcite)
                differences.Add($"{what}: ours {mine}, Calcite {calcite}");
        }

        /// <summary>
        /// Every edited or constructed geometry has SRID 4326.
        /// </summary>
        /// <remarks>
        /// The JTS transformers Calcite uses build through a geometry factory with no SRID, so Calcite's own
        /// results have SRID 0.
        /// </remarks>
        [Fact]
        public void ShouldStampEveryEditedGeographyWithWgs84()
        {
            var geography = Wkt("POLYGON((0 0, 6 0, 6 6, 0 6, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))");

            foreach (var (name, ours, _) in unary)
            {
                if (GeographyAccessorTests.Answer(() => ours(geography)).EndsWith("Exception"))
                    continue;

                (ours(geography) as Geometry)?.getSRID().Should().Be(GeographyFunctions.Wgs84, name);
            }

            GeographyFunctions.Point(java.lang.Double.valueOf(1), java.lang.Double.valueOf(2))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.MakeLine(Wkt("POINT(0 0)"), Wkt("POINT(1 1)"))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.PointFromText("POINT(1 2)")!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// <c>ST_ADDZ</c> throws over a polygon, and so does <c>CLR_ST_GEOG_ADDZ</c>.
        /// </summary>
        /// <remarks>
        /// This is a Calcite defect: the transformer <c>ST_ADDZ</c> builds through gives a <c>LinearRing</c> a
        /// coordinate sequence it cannot read, and JTS throws a null reference. <c>CLR_ST_GEOG_ADDZ</c>
        /// delegates to <c>ST_ADDZ</c> and reproduces it rather than diverge for a reason unrelated to geodesy.
        /// </remarks>
        [Fact]
        public void ShouldInheritTheDefectInAddZOverAPolygon()
        {
            var polygon = Wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))");
            var five = java.lang.Double.valueOf(5);

            ((Action)(() => SpatialTypeFunctions.ST_AddZ(polygon, Dec(5)))).Should().Throw<NullReferenceException>();
            ((Action)(() => GeographyFunctions.AddZ(polygon, five))).Should().Throw<NullReferenceException>();

            // Over a point it works, which is what the operator test uses.
            GeographyFunctions.AddZ(Wkt("POINT(1 2)"), five).Should().NotBeNull();
        }

        [Fact]
        public void ShouldAnswerNullForANullArgument()
        {
            var geography = Wkt("LINESTRING(0 0, 1 1)");

            foreach (var (name, ours, _) in unary)
                GeographyAccessorTests.Answer(() => ours(null!)).Should().Be("null", name);

            GeographyFunctions.AddPoint(null, geography).Should().BeNull();
            GeographyFunctions.AddPoint(geography, null).Should().BeNull();
            GeographyFunctions.RemovePoint(geography, null).Should().BeNull();
            GeographyFunctions.AddZ(geography, null).Should().BeNull();
            GeographyFunctions.RemoveRepeatedPoints(geography, null).Should().BeNull();
            GeographyFunctions.Point(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.MakeLine(null, geography).Should().BeNull();
            GeographyFunctions.MakePolygon(null).Should().BeNull();
            GeographyFunctions.PointFromText(null).Should().BeNull();
            GeographyFunctions.PointFromWkb(null).Should().BeNull();
        }

    }

}
