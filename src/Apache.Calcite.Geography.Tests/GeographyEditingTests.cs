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
    /// The editing functions and the constructors that build a geography out of parts, against the
    /// <c>ST_*</c> each one mirrors.
    /// </summary>
    /// <remarks>
    /// The same kind of surface as the accessors and held the same way: these rearrange coordinates without
    /// interpreting the space between them, so each is a delegation and the risk is the wiring rather than
    /// the arithmetic.
    ///
    /// <para>One divergence, and it is deliberate. Every one of these hands back a geography built from a
    /// geography, and it is stamped WGS84 on the way out — <c>ST_GEOG_FORCE2D</c> answers something with an
    /// SRID of 4326 where <c>ST_FORCE2D</c> answers something with an SRID of zero, because the transformers
    /// underneath build through a geometry factory that does not carry one across. Calcite has nothing to
    /// keep there and this package does: every geography it produces says what it is. The comparison below
    /// is on the coordinates rather than the stamp, and
    /// <see cref="ShouldStampEveryEditedGeographyWithWgs84"/> holds the stamp.</para>
    /// </remarks>
    [TestClass]
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
            ("ST_GEOG_FLIPCOORDINATES", g => GeographyFunctions.FlipCoordinates(g), g => SpatialTypeFunctions.ST_FlipCoordinates(g)),
            ("ST_GEOG_FORCE2D", g => GeographyFunctions.Force2D(g), g => SpatialTypeFunctions.ST_Force2D(g)),
            ("ST_GEOG_FORCE3D", g => GeographyFunctions.Force3D(g), g => SpatialTypeFunctions.ST_Force3D(g)),
            ("ST_GEOG_NORMALIZE", g => GeographyFunctions.Normalize(g), g => SpatialTypeFunctions.ST_Normalize(g)),
            ("ST_GEOG_REMOVEHOLES", g => GeographyFunctions.RemoveHoles(g), g => SpatialTypeFunctions.ST_RemoveHoles(g)),
            ("ST_GEOG_REMOVEREPEATEDPOINTS", g => GeographyFunctions.RemoveRepeatedPoints(g), g => SpatialTypeFunctions.ST_RemoveRepeatedPoints(g)),
            ("ST_GEOG_REVERSE", g => GeographyFunctions.Reverse(g), g => SpatialTypeFunctions.ST_Reverse(g)),
            ("ST_GEOG_TOMULTILINE", g => GeographyFunctions.ToMultiLine(g), g => SpatialTypeFunctions.ST_ToMultiLine(g)),
            ("ST_GEOG_TOMULTIPOINT", g => GeographyFunctions.ToMultiPoint(g), g => SpatialTypeFunctions.ST_ToMultiPoint(g)),
            ("ST_GEOG_TOMULTISEGMENTS", g => GeographyFunctions.ToMultiSegments(g), g => SpatialTypeFunctions.ST_ToMultiSegments(g)),
        ];

        [TestMethod]
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

        [TestMethod]
        public void ShouldAgreeWithCalciteOnTheEditingFunctionsThatTakeAnArgument()
        {
            var differences = new List<string>();
            var point = Wkt("POINT(9 9)");

            foreach (var shape in shapes)
            {
                var g = Wkt(shape);

                Compare(differences, $"ST_GEOG_ADDPOINT({shape}, POINT(9 9))",
                    () => GeographyFunctions.AddPoint(g, point), () => SpatialTypeFunctions.ST_AddPoint(g, point));

                for (var n = 0; n <= 2; n++)
                {
                    var index = java.lang.Integer.valueOf(n);

                    Compare(differences, $"ST_GEOG_ADDPOINT({shape}, POINT(9 9), {n})",
                        () => GeographyFunctions.AddPoint(g, point, index), () => SpatialTypeFunctions.ST_AddPoint(g, point, n));

                    Compare(differences, $"ST_GEOG_REMOVEPOINT({shape}, {n})",
                        () => GeographyFunctions.RemovePoint(g, index), () => SpatialTypeFunctions.ST_RemovePoint(g, n));
                }

                Compare(differences, $"ST_GEOG_ADDZ({shape}, 5)",
                    () => GeographyFunctions.AddZ(g, java.lang.Integer.valueOf(5)),
                    () => SpatialTypeFunctions.ST_AddZ(g, java.math.BigDecimal.valueOf(5.0)));

                Compare(differences, $"ST_GEOG_REMOVEREPEATEDPOINTS({shape}, 0.5)",
                    () => GeographyFunctions.RemoveRepeatedPoints(g, java.lang.Double.valueOf(0.5)),
                    () => SpatialTypeFunctions.ST_RemoveRepeatedPoints(g, java.math.BigDecimal.valueOf(0.5)));
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        [TestMethod]
        public void ShouldAgreeWithCalciteOnTheConstructors()
        {
            var differences = new List<string>();
            var one = java.lang.Integer.valueOf(1);
            var two = java.lang.Integer.valueOf(2);
            var three = java.lang.Integer.valueOf(3);

            Compare(differences, "ST_GEOG_POINT(1, 2)",
                () => GeographyFunctions.Point(one, two), () => SpatialTypeFunctions.ST_Point(Dec(1), Dec(2)));

            Compare(differences, "ST_GEOG_POINT(1, 2, 3)",
                () => GeographyFunctions.Point(one, two, three),
                () => SpatialTypeFunctions.ST_Point(Dec(1), Dec(2), Dec(3)));

            var a = Wkt("POINT(0 0)");
            var b = Wkt("POINT(1 1)");
            var c = Wkt("POINT(2 0)");

            Compare(differences, "ST_GEOG_MAKELINE(a, b)",
                () => GeographyFunctions.MakeLine(a, b), () => SpatialTypeFunctions.ST_MakeLine(a, b));

            Compare(differences, "ST_GEOG_MAKELINE(a, b, c)",
                () => GeographyFunctions.MakeLine(a, b, c), () => SpatialTypeFunctions.ST_MakeLine(a, b, c));

            var shell = Wkt("LINESTRING(0 0, 6 0, 6 6, 0 6, 0 0)");
            var hole = Wkt("LINESTRING(2 2, 4 2, 4 4, 2 4, 2 2)");

            Compare(differences, "ST_GEOG_MAKEPOLYGON(shell)",
                () => GeographyFunctions.MakePolygon(shell), () => SpatialTypeFunctions.ST_MakePolygon(shell));

            Compare(differences, "ST_GEOG_MAKEPOLYGON(shell, hole)",
                () => GeographyFunctions.MakePolygon(shell, hole), () => SpatialTypeFunctions.ST_MakePolygon(shell, hole));

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// A typed reader answers a shape of its own kind and null for anything else, which is Calcite's rule
        /// and the whole of what makes the nine of them worth having.
        /// </summary>
        [TestMethod]
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
                Compare(differences, $"ST_GEOG_POINTFROMTEXT({wkt})",
                    () => GeographyFunctions.PointFromText(wkt), () => SpatialTypeFunctions.ST_PointFromText(wkt));
                Compare(differences, $"ST_GEOG_LINEFROMTEXT({wkt})",
                    () => GeographyFunctions.LineFromText(wkt), () => SpatialTypeFunctions.ST_LineFromText(wkt));
                Compare(differences, $"ST_GEOG_POLYFROMTEXT({wkt})",
                    () => GeographyFunctions.PolyFromText(wkt), () => SpatialTypeFunctions.ST_PolyFromText(wkt));
                Compare(differences, $"ST_GEOG_MPOINTFROMTEXT({wkt})",
                    () => GeographyFunctions.MPointFromText(wkt), () => SpatialTypeFunctions.ST_MPointFromText(wkt));
                Compare(differences, $"ST_GEOG_MLINEFROMTEXT({wkt})",
                    () => GeographyFunctions.MLineFromText(wkt), () => SpatialTypeFunctions.ST_MLineFromText(wkt));
                Compare(differences, $"ST_GEOG_MPOLYFROMTEXT({wkt})",
                    () => GeographyFunctions.MPolyFromText(wkt), () => SpatialTypeFunctions.ST_MPolyFromText(wkt));

                var wkb = GeographyFunctions.AsWkb(Wkt(wkt));

                Compare(differences, $"ST_GEOG_POINTFROMWKB({wkt})",
                    () => GeographyFunctions.PointFromWkb(wkb), () => SpatialTypeFunctions.ST_PointFromWKB(wkb));
                Compare(differences, $"ST_GEOG_LINEFROMWKB({wkt})",
                    () => GeographyFunctions.LineFromWkb(wkb), () => SpatialTypeFunctions.ST_LineFromWKB(wkb));
                Compare(differences, $"ST_GEOG_POLYFROMWKB({wkt})",
                    () => GeographyFunctions.PolyFromWkb(wkb), () => SpatialTypeFunctions.ST_PolyFromWKB(wkb));
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// Every editing operator run as SQL, and answered the same as the body behind it.
        /// </summary>
        /// <remarks>
        /// The comparisons above are between two C# methods and cannot see which of them a name is wired to.
        /// The binding is by signature, so a declaration naming a method that does not exist with those
        /// parameters fails when the table is built rather than when a query reaches it — but ten of these
        /// take one geography and answer one geography, and nothing but running them tells
        /// <c>ST_GEOG_FORCE2D</c> wired to <c>Force3D</c> from <c>ST_GEOG_FORCE2D</c> wired to
        /// <c>Force2D</c>. Every result is wrapped in <c>ST_GEOG_ASTEXT</c>, since a geography is not
        /// something a result set carries.
        /// </remarks>
        [TestMethod]
        public void ShouldRunEveryEditingOperatorAsAnOperator()
        {
            const string shape = "POLYGON((0 0, 6 0, 6 6, 0 6, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))";
            var geography = Wkt(shape);
            var subject = $"ST_GEOG_GEOMFROMTEXT('{shape}')";

            var wanted = new List<string>();
            var names = new List<string>();
            var expressions = new List<string>();

            foreach (var (name, ours, _) in unary)
            {
                // through ST_GEOG_ASTEXT on both sides, because that is what the statement asks for and it is
                // not the same rendering as Geometry.toText: the writer ST_ASTEXT builds is told how many
                // ordinates the shape has, and the default one always writes two
                var answer = GeographyAccessorTests.Answer(() => GeographyFunctions.AsText(ours(geography) as Geometry));
                if (answer.EndsWith("Exception"))
                    continue;

                wanted.Add(answer);
                names.Add(name);
                expressions.Add($"ST_GEOG_ASTEXT({name}({subject}))");
            }

            var extra = new (string Sql, Func<object?> Ours)[]
            {
                // ST_AddPoint takes a line and throws over anything else, so this one gets a line
                ("ST_GEOG_ASTEXT(ST_GEOG_ADDPOINT(ST_GEOG_LINEFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), ST_GEOG_POINT(9, 9)))",
                    () => GeographyFunctions.AddPoint(Wkt("LINESTRING(0 0, 1 1, 2 0)"), Wkt("POINT(9 9)"))),
                ("ST_GEOG_ASTEXT(ST_GEOG_REMOVEPOINT(ST_GEOG_LINEFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'), 1))",
                    () => GeographyFunctions.RemovePoint(Wkt("LINESTRING(0 0, 1 1, 2 0)"), java.lang.Integer.valueOf(1))),
                // over a point, because ST_AddZ throws over a polygon; see
                // ShouldInheritTheDefectInAddZOverAPolygon
                ("ST_GEOG_ASTEXT(ST_GEOG_ADDZ(ST_GEOG_POINT(1, 2), 5))",
                    () => GeographyFunctions.AddZ(Wkt("POINT(1 2)"), java.lang.Integer.valueOf(5))),
                ($"ST_GEOG_ASTEXT(ST_GEOG_REMOVEREPEATEDPOINTS({subject}, 0.5))",
                    () => GeographyFunctions.RemoveRepeatedPoints(geography, java.lang.Double.valueOf(0.5))),
                ("ST_GEOG_ASTEXT(ST_GEOG_POINT(1, 2))",
                    () => GeographyFunctions.Point(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2))),
                ("ST_GEOG_ASTEXT(ST_GEOG_MAKEPOINT(1, 2))",
                    () => GeographyFunctions.Point(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2))),
                ("ST_GEOG_ASTEXT(ST_GEOG_MAKELINE(ST_GEOG_POINT(0, 0), ST_GEOG_POINT(1, 1), ST_GEOG_POINT(2, 0)))",
                    () => GeographyFunctions.MakeLine(Wkt("POINT(0 0)"), Wkt("POINT(1 1)"), Wkt("POINT(2 0)"))),
                ("ST_GEOG_ASTEXT(ST_GEOG_MAKEPOLYGON(ST_GEOG_LINEFROMTEXT('LINESTRING(0 0, 6 0, 6 6, 0 6, 0 0)')))",
                    () => GeographyFunctions.MakePolygon(Wkt("LINESTRING(0 0, 6 0, 6 6, 0 6, 0 0)"))),
                ("ST_GEOG_ASTEXT(ST_GEOG_POINTFROMTEXT('POINT(1 2)'))",
                    () => GeographyFunctions.PointFromText("POINT(1 2)")),
                ("ST_GEOG_ASTEXT(ST_GEOG_POLYFROMTEXT('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'))",
                    () => GeographyFunctions.PolyFromText("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")),
                ("ST_GEOG_ASTEXT(ST_GEOG_MLINEFROMTEXT('MULTILINESTRING((0 0, 1 1))'))",
                    () => GeographyFunctions.MLineFromText("MULTILINESTRING((0 0, 1 1))")),
            };

            foreach (var (sql, ours) in extra)
            {
                var answer = GeographyAccessorTests.Answer(() => GeographyFunctions.AsText(ours() as Geometry));

                // an expression that throws would end the whole statement rather than answer a column, and
                // what it would prove is already proven by the comparisons above
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
        /// A geography that came out of one of these says it is WGS84, whatever Calcite's own would have
        /// said.
        /// </summary>
        /// <remarks>
        /// The transformers underneath build through a geometry factory that does not carry an SRID across,
        /// so Calcite's answer is stamped zero. That is nothing to Calcite, which has no reference system to
        /// keep, and it is a small lie here: a geography is WGS84 and every one this package hands out says
        /// so.
        /// </remarks>
        [TestMethod]
        public void ShouldStampEveryEditedGeographyWithWgs84()
        {
            var geography = Wkt("POLYGON((0 0, 6 0, 6 6, 0 6, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))");

            foreach (var (name, ours, _) in unary)
            {
                if (GeographyAccessorTests.Answer(() => ours(geography)).EndsWith("Exception"))
                    continue;

                (ours(geography) as Geometry)?.getSRID().Should().Be(GeographyFunctions.Wgs84, name);
            }

            GeographyFunctions.Point(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.MakeLine(Wkt("POINT(0 0)"), Wkt("POINT(1 1)"))!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.PointFromText("POINT(1 2)")!
                .getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// <c>ST_ADDZ</c> throws over a polygon, and so does <c>ST_GEOG_ADDZ</c>.
        /// </summary>
        /// <remarks>
        /// A defect of Calcite's rather than one introduced here: the transformer it builds through hands a
        /// <c>LinearRing</c> a coordinate sequence it then cannot read, and the null reference comes out of
        /// JTS. It is inherited because the delegation is the whole of the implementation, and it is recorded
        /// rather than worked around — a geography that behaves differently from the geometry it mirrors, in
        /// a way that has nothing to do with geodesy, would be a divergence this package has no business
        /// introducing.
        /// </remarks>
        [TestMethod]
        public void ShouldInheritTheDefectInAddZOverAPolygon()
        {
            var polygon = Wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))");
            var five = java.lang.Integer.valueOf(5);

            ((Action)(() => SpatialTypeFunctions.ST_AddZ(polygon, Dec(5)))).Should().Throw<NullReferenceException>();
            ((Action)(() => GeographyFunctions.AddZ(polygon, five))).Should().Throw<NullReferenceException>();

            // over a point it is well behaved, which is what the operator is exercised with
            GeographyFunctions.AddZ(Wkt("POINT(1 2)"), five).Should().NotBeNull();
        }

        [TestMethod]
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
            GeographyFunctions.Point(null, java.lang.Integer.valueOf(1)).Should().BeNull();
            GeographyFunctions.MakeLine(null, geography).Should().BeNull();
            GeographyFunctions.MakePolygon(null).Should().BeNull();
            GeographyFunctions.PointFromText(null).Should().BeNull();
            GeographyFunctions.PointFromWkb(null).Should().BeNull();
        }

    }

}
