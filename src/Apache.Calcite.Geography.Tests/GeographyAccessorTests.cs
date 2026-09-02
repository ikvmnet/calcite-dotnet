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
    /// The accessors and the serializers, against the <c>ST_*</c> each one mirrors.
    /// </summary>
    /// <remarks>
    /// These read or rearrange coordinates without interpreting the space between them, so unlike the
    /// relations there is no geodesy in them at all and each is a delegation to the very method Calcite's
    /// operator of that name calls. What can go wrong is therefore not the arithmetic but the wiring: forty
    /// declarations written to one pattern is forty chances to point a name at the wrong body. Comparing
    /// every one against Calcite over shapes of every kind is what catches that.
    ///
    /// <para>The bounding-box accessors are the exception worth stating. <c>ST_GEOG_XMIN</c> and its four
    /// relatives are computed structurally and are wrong in the usual way for anything crossing the
    /// antimeridian, where the least longitude of a shape spanning the seam is not the westmost point of it.
    /// That is inherited rather than introduced, and it is a documentation problem rather than a second
    /// implementation; nothing here is near the seam.</para>
    /// </remarks>
    [TestClass]
    public class GeographyAccessorTests
    {

        /// <summary>
        /// Shapes of every kind an accessor has something to say about.
        /// </summary>
        static readonly string[] shapes =
        [
            "POINT(1 2)",
            "POINT Z(1 2 3)",
            "POINT EMPTY",
            "MULTIPOINT((1 2), (3 4))",
            "LINESTRING(0 0, 1 1, 2 0)",
            "LINESTRING(0 0, 1 1, 0 0)",
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

        /// <summary>
        /// Every one-argument accessor and serializer, ours beside Calcite's.
        /// </summary>
        static readonly (string Name, Func<Geometry, object?> Ours, Func<Geometry, object?> Theirs)[] unary =
        [
            ("ST_GEOG_X", g => GeographyFunctions.X(g), g => SpatialTypeFunctions.ST_X(g)),
            ("ST_GEOG_Y", g => GeographyFunctions.Y(g), g => SpatialTypeFunctions.ST_Y(g)),
            ("ST_GEOG_Z", g => GeographyFunctions.Z(g), g => SpatialTypeFunctions.ST_Z(g)),
            ("ST_GEOG_XMIN", g => GeographyFunctions.XMin(g), g => SpatialTypeFunctions.ST_XMin(g)),
            ("ST_GEOG_XMAX", g => GeographyFunctions.XMax(g), g => SpatialTypeFunctions.ST_XMax(g)),
            ("ST_GEOG_YMIN", g => GeographyFunctions.YMin(g), g => SpatialTypeFunctions.ST_YMin(g)),
            ("ST_GEOG_YMAX", g => GeographyFunctions.YMax(g), g => SpatialTypeFunctions.ST_YMax(g)),
            ("ST_GEOG_ZMIN", g => GeographyFunctions.ZMin(g), g => SpatialTypeFunctions.ST_ZMin(g)),
            ("ST_GEOG_ZMAX", g => GeographyFunctions.ZMax(g), g => SpatialTypeFunctions.ST_ZMax(g)),
            ("ST_GEOG_COORDDIM", g => GeographyFunctions.CoordDim(g), g => SpatialTypeFunctions.ST_CoordDim(g)),
            ("ST_GEOG_DIMENSION", g => GeographyFunctions.Dimension(g), g => SpatialTypeFunctions.ST_Dimension(g)),
            ("ST_GEOG_GEOMETRYTYPE", g => GeographyFunctions.GeometryType(g), g => SpatialTypeFunctions.ST_GeometryType(g)),
            ("ST_GEOG_GEOMETRYTYPECODE", g => GeographyFunctions.GeometryTypeCode(g), g => SpatialTypeFunctions.ST_GeometryTypeCode(g)),
            ("ST_GEOG_NPOINTS", g => GeographyFunctions.NPoints(g), g => SpatialTypeFunctions.ST_NPoints(g)),
            ("ST_GEOG_NUMPOINTS", g => GeographyFunctions.NumPoints(g), g => SpatialTypeFunctions.ST_NumPoints(g)),
            ("ST_GEOG_NUMGEOMETRIES", g => GeographyFunctions.NumGeometries(g), g => SpatialTypeFunctions.ST_NumGeometries(g)),
            ("ST_GEOG_NUMINTERIORRING", g => GeographyFunctions.NumInteriorRing(g), g => SpatialTypeFunctions.ST_NumInteriorRing(g)),
            ("ST_GEOG_NUMINTERIORRINGS", g => GeographyFunctions.NumInteriorRings(g), g => SpatialTypeFunctions.ST_NumInteriorRings(g)),
            ("ST_GEOG_STARTPOINT", g => GeographyFunctions.StartPoint(g), g => SpatialTypeFunctions.ST_StartPoint(g)),
            ("ST_GEOG_ENDPOINT", g => GeographyFunctions.EndPoint(g), g => SpatialTypeFunctions.ST_EndPoint(g)),
            ("ST_GEOG_EXTERIORRING", g => GeographyFunctions.ExteriorRing(g), g => SpatialTypeFunctions.ST_ExteriorRing(g)),
            ("ST_GEOG_BOUNDARY", g => GeographyFunctions.Boundary(g), g => SpatialTypeFunctions.ST_Boundary(g)),
            ("ST_GEOG_HOLES", g => GeographyFunctions.Holes(g), g => SpatialTypeFunctions.ST_Holes(g)),
            ("ST_GEOG_ISEMPTY", g => GeographyFunctions.IsEmpty(g), g => SpatialTypeFunctions.ST_IsEmpty(g)),
            ("ST_GEOG_IS3D", g => GeographyFunctions.Is3D(g), g => SpatialTypeFunctions.ST_Is3D(g)),
            ("ST_GEOG_ISCLOSED", g => GeographyFunctions.IsClosed(g), g => SpatialTypeFunctions.ST_IsClosed(g)),
            ("ST_GEOG_SRID", g => GeographyFunctions.Srid(g), g => SpatialTypeFunctions.ST_SRID(g)),
            ("ST_GEOG_ASTEXT", g => GeographyFunctions.AsText(g), g => SpatialTypeFunctions.ST_AsText(g)),
            ("ST_GEOG_ASWKT", g => GeographyFunctions.AsWkt(g), g => SpatialTypeFunctions.ST_AsWKT(g)),
            ("ST_GEOG_ASEWKT", g => GeographyFunctions.AsEwkt(g), g => SpatialTypeFunctions.ST_AsEWKT(g)),
            ("ST_GEOG_ASGEOJSON", g => GeographyFunctions.AsGeoJson(g), g => SpatialTypeFunctions.ST_AsGeoJSON(g)),
            ("ST_GEOG_ASGML", g => GeographyFunctions.AsGml(g), g => SpatialTypeFunctions.ST_AsGML(g)),
            ("ST_GEOG_ASBINARY", g => GeographyFunctions.AsBinary(g), g => SpatialTypeFunctions.ST_AsBinary(g)),
            ("ST_GEOG_ASWKB", g => GeographyFunctions.AsWkb(g), g => SpatialTypeFunctions.ST_AsWKB(g)),
            ("ST_GEOG_ASEWKB", g => GeographyFunctions.AsEwkb(g), g => SpatialTypeFunctions.ST_AsEWKB(g)),
        ];

        /// <summary>
        /// Renders an answer so that two of them can be compared whatever their type.
        /// </summary>
        /// <param name="answer"></param>
        /// <returns></returns>
        internal static string Render(object? answer)
        {
            return answer switch
            {
                null => "null",
                Geometry geometry => geometry.toText(),
                // one side of a pair returns a boxed Boolean and the other a primitive, and the two spell
                // themselves differently — java.lang.Boolean lowercase, System.Boolean capitalised
                java.lang.Boolean boxed => boxed.booleanValue() ? "true" : "false",
                bool value => value ? "true" : "false",
                // a ByteString renders itself as lowercase hex, and the same value read back out of a result
                // set is a byte array, which is what says the operator was typed VARBINARY
                byte[] bytes => Convert.ToHexString(bytes).ToLowerInvariant(),
                _ => answer.ToString() ?? "null",
            };
        }

        /// <summary>
        /// Calls a function and renders what came back, an exception included.
        /// </summary>
        /// <param name="call"></param>
        /// <returns></returns>
        /// <remarks>
        /// Some of Calcite's accessors throw over a shape they do not apply to, and a delegation throws the
        /// same way. That is an answer to compare like any other, so the kind of the exception is rendered
        /// rather than allowed to end the run.
        /// </remarks>
        internal static string Answer(Func<object?> call)
        {
            try
            {
                return Render(call());
            }
            catch (Exception e)
            {
                return e.GetType().Name;
            }
        }

        [TestMethod]
        public void ShouldAgreeWithCalciteOnEveryAccessor()
        {
            var differences = new List<string>();

            foreach (var shape in shapes)
            {
                var geography = Wkt(shape);

                foreach (var (name, ours, theirs) in unary)
                {
                    var mine = Answer(() => ours(geography));
                    var calcite = Answer(() => theirs(geography));

                    if (mine != calcite)
                        differences.Add($"{name} over {shape}: ours {mine}, Calcite {calcite}");
                }
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        /// <summary>
        /// Every accessor, called as SQL rather than as a method, and answered the same.
        /// </summary>
        /// <remarks>
        /// The comparison above is between two C# methods and says nothing about which of them a name is
        /// wired to. This runs the operator, so a declaration pointing <c>ST_GEOG_XMIN</c> at
        /// <c>XMax</c> — or typed as though it returned something else — is caught here and only here.
        /// Everything goes in one statement per shape rather than one per operator, which is thirty-five
        /// declarations checked in three round trips.
        ///
        /// <para>An accessor that throws over the shape is left out of the statement rather than expected to
        /// throw: an exception in one column ends the whole query, and what it would prove is already proven
        /// by the comparison above.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldRunEveryAccessorAsAnOperator()
        {
            foreach (var shape in new[]
            {
                "POINT(1 2)",
                "LINESTRING(0 0, 1 1, 2 0)",
                "POLYGON((0 0, 6 0, 6 6, 0 6, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))",
            })
            {
                var geography = Wkt(shape);
                var wanted = new List<string>();
                var expressions = new List<string>();
                var names = new List<string>();

                foreach (var (name, ours, _) in unary)
                {
                    var answer = Answer(() => ours(geography));
                    if (answer.EndsWith("Exception"))
                        continue;

                    wanted.Add(answer);
                    names.Add(name);
                    expressions.Add($"{name}(ST_GEOG_GEOMFROMTEXT('{shape}'))");
                }

                var row = GeographyExecutionTests.Run("SELECT " + string.Join(", ", expressions))[0];

                for (var i = 0; i < wanted.Count; i++)
                    Render(row[i]).Should().Be(wanted[i], $"{names[i]} over {shape}");
            }
        }

        [TestMethod]
        public void ShouldAgreeWithCalciteOnTheIndexedAccessors()
        {
            var differences = new List<string>();

            foreach (var shape in shapes)
            {
                var geography = Wkt(shape);

                for (var n = 0; n <= 2; n++)
                {
                    var index = java.lang.Integer.valueOf(n);

                    Compare(differences, $"ST_GEOG_POINTN({shape}, {n})",
                        () => GeographyFunctions.PointN(geography, index), () => SpatialTypeFunctions.ST_PointN(geography, n));

                    Compare(differences, $"ST_GEOG_GEOMETRYN({shape}, {n})",
                        () => GeographyFunctions.GeometryN(geography, index), () => SpatialTypeFunctions.ST_GeometryN(geography, n));

                    Compare(differences, $"ST_GEOG_INTERIORRING({shape}, {n})",
                        () => GeographyFunctions.InteriorRing(geography, index), () => SpatialTypeFunctions.ST_InteriorRing(geography, n));
                }

                foreach (var other in shapes)
                    Compare(differences, $"ST_GEOG_ORDERINGEQUALS({shape}, {other})",
                        () => GeographyFunctions.OrderingEquals(geography, Wkt(other)),
                        () => SpatialTypeFunctions.ST_OrderingEquals(geography, Wkt(other)));
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
        }

        static void Compare(List<string> differences, string what, Func<object?> ours, Func<object?> theirs)
        {
            var mine = Answer(ours);
            var calcite = Answer(theirs);

            if (mine != calcite)
                differences.Add($"{what}: ours {mine}, Calcite {calcite}");
        }

        /// <summary>
        /// Every format writes something the matching reader takes back.
        /// </summary>
        /// <remarks>
        /// The serializers and the constructors are declared as a pair for each format, and a round trip is
        /// what says the pair was wired to one another rather than to a neighbour. GML is left out because
        /// Calcite writes a form its own reader does not take, which is its defect and not one to reproduce
        /// by asserting it.
        /// </remarks>
        [TestMethod]
        public void ShouldReadBackEveryFormatItWrites()
        {
            foreach (var shape in new[] { "POINT(1 2)", "LINESTRING(0 0, 1 1, 2 0)", "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))" })
            {
                var geography = Wkt(shape);

                GeographyFunctions.FromWkt(GeographyFunctions.AsText(geography))!.toText().Should().Be(geography.toText(), shape);
                GeographyFunctions.FromEwkt(GeographyFunctions.AsEwkt(geography))!.toText().Should().Be(geography.toText(), shape);
                GeographyFunctions.FromGeoJson(GeographyFunctions.AsGeoJson(geography))!.toText().Should().Be(geography.toText(), shape);
                GeographyFunctions.FromWkb(GeographyFunctions.AsWkb(geography))!.toText().Should().Be(geography.toText(), shape);
                GeographyFunctions.FromEwkb(GeographyFunctions.AsEwkb(geography))!.toText().Should().Be(geography.toText(), shape);
            }
        }

        [TestMethod]
        public void ShouldStampEveryConstructorWithWgs84()
        {
            var geography = Wkt("POINT(1 2)");

            GeographyFunctions.FromWkt("POINT(1 2)")!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.FromEwkt("POINT(1 2)")!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.FromGeoJson("{\"type\":\"Point\",\"coordinates\":[1,2]}")!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.FromWkb(GeographyFunctions.AsWkb(geography))!.getSRID().Should().Be(GeographyFunctions.Wgs84);
            GeographyFunctions.FromEwkb(GeographyFunctions.AsEwkb(geography))!.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// An SRID a geography cannot be in is refused however it arrives — as an argument, or written into
        /// the text or the bytes themselves.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseAnSridThatIsNotWgs84()
        {
            var wrong = java.lang.Integer.valueOf(3857);
            var wkb = GeographyFunctions.AsWkb(Wkt("POINT(1 2)"));

            ((Action)(() => GeographyFunctions.FromWkt("POINT(1 2)", wrong))).Should().Throw<java.lang.IllegalArgumentException>();
            ((Action)(() => GeographyFunctions.FromWkb(wkb, wrong))).Should().Throw<java.lang.IllegalArgumentException>();
            ((Action)(() => GeographyFunctions.FromGml("<gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point>", wrong))).Should().Throw<java.lang.IllegalArgumentException>();
            // Calcite spells the prefix srid:N; rather than the PostGIS SRID=N;, and reads no other form
            ((Action)(() => GeographyFunctions.FromEwkt("srid:3857;POINT(1 2)"))).Should().Throw<java.lang.IllegalArgumentException>();
        }

        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            foreach (var (name, ours, _) in unary)
                Answer(() => ours(null!)).Should().Be("null", name);

            GeographyFunctions.PointN(null, java.lang.Integer.valueOf(1)).Should().BeNull();
            GeographyFunctions.PointN(Wkt("POINT(1 2)"), null).Should().BeNull();
            GeographyFunctions.OrderingEquals(null, Wkt("POINT(1 2)")).Should().BeNull();
            GeographyFunctions.FromEwkt(null).Should().BeNull();
            GeographyFunctions.FromWkb(null).Should().BeNull();
            GeographyFunctions.FromEwkb(null).Should().BeNull();
            GeographyFunctions.FromGml(null).Should().BeNull();
        }

    }

}
