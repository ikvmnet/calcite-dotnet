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
    /// Compares the accessors and serializers with the Calcite <c>ST_*</c> function each one delegates to.
    /// </summary>
    /// <remarks>
    /// These read or rearrange coordinates without any geodesy, so each delegates to the method Calcite's
    /// operator of the same name calls. What these tests check is the wiring: that each name reaches the
    /// right body and is typed to match it.
    ///
    /// <para><c>CLR_ST_GEOG_XMIN</c> and its relatives are computed from coordinates, so for a shape crossing
    /// the antimeridian the least longitude is not the westmost point. No shape here is near the
    /// antimeridian.</para>
    /// </remarks>
    public class GeographyAccessorTests
    {

        /// <summary>
        /// One shape of each kind the accessors distinguish.
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
        /// Every one-argument accessor and serializer, with the Calcite function it is compared with.
        /// </summary>
        static readonly (string Name, Func<Geometry, object?> Ours, Func<Geometry, object?> Theirs)[] unary =
        [
            ("CLR_ST_GEOG_X", g => GeographyFunctions.X(g), g => SpatialTypeFunctions.ST_X(g)),
            ("CLR_ST_GEOG_Y", g => GeographyFunctions.Y(g), g => SpatialTypeFunctions.ST_Y(g)),
            ("CLR_ST_GEOG_Z", g => GeographyFunctions.Z(g), g => SpatialTypeFunctions.ST_Z(g)),
            ("CLR_ST_GEOG_XMIN", g => GeographyFunctions.XMin(g), g => SpatialTypeFunctions.ST_XMin(g)),
            ("CLR_ST_GEOG_XMAX", g => GeographyFunctions.XMax(g), g => SpatialTypeFunctions.ST_XMax(g)),
            ("CLR_ST_GEOG_YMIN", g => GeographyFunctions.YMin(g), g => SpatialTypeFunctions.ST_YMin(g)),
            ("CLR_ST_GEOG_YMAX", g => GeographyFunctions.YMax(g), g => SpatialTypeFunctions.ST_YMax(g)),
            ("CLR_ST_GEOG_ZMIN", g => GeographyFunctions.ZMin(g), g => SpatialTypeFunctions.ST_ZMin(g)),
            ("CLR_ST_GEOG_ZMAX", g => GeographyFunctions.ZMax(g), g => SpatialTypeFunctions.ST_ZMax(g)),
            ("CLR_ST_GEOG_COORDDIM", g => GeographyFunctions.CoordDim(g), g => SpatialTypeFunctions.ST_CoordDim(g)),
            ("CLR_ST_GEOG_DIMENSION", g => GeographyFunctions.Dimension(g), g => SpatialTypeFunctions.ST_Dimension(g)),
            ("CLR_ST_GEOG_GEOMETRYTYPE", g => GeographyFunctions.GeometryType(g), g => SpatialTypeFunctions.ST_GeometryType(g)),
            ("CLR_ST_GEOG_GEOMETRYTYPECODE", g => GeographyFunctions.GeometryTypeCode(g), g => SpatialTypeFunctions.ST_GeometryTypeCode(g)),
            ("CLR_ST_GEOG_NPOINTS", g => GeographyFunctions.NPoints(g), g => SpatialTypeFunctions.ST_NPoints(g)),
            ("CLR_ST_GEOG_NUMPOINTS", g => GeographyFunctions.NumPoints(g), g => SpatialTypeFunctions.ST_NumPoints(g)),
            ("CLR_ST_GEOG_NUMGEOMETRIES", g => GeographyFunctions.NumGeometries(g), g => SpatialTypeFunctions.ST_NumGeometries(g)),
            ("CLR_ST_GEOG_NUMINTERIORRING", g => GeographyFunctions.NumInteriorRing(g), g => SpatialTypeFunctions.ST_NumInteriorRing(g)),
            ("CLR_ST_GEOG_NUMINTERIORRINGS", g => GeographyFunctions.NumInteriorRings(g), g => SpatialTypeFunctions.ST_NumInteriorRings(g)),
            ("CLR_ST_GEOG_STARTPOINT", g => GeographyFunctions.StartPoint(g), g => SpatialTypeFunctions.ST_StartPoint(g)),
            ("CLR_ST_GEOG_ENDPOINT", g => GeographyFunctions.EndPoint(g), g => SpatialTypeFunctions.ST_EndPoint(g)),
            ("CLR_ST_GEOG_EXTERIORRING", g => GeographyFunctions.ExteriorRing(g), g => SpatialTypeFunctions.ST_ExteriorRing(g)),
            ("CLR_ST_GEOG_BOUNDARY", g => GeographyFunctions.Boundary(g), g => SpatialTypeFunctions.ST_Boundary(g)),
            ("CLR_ST_GEOG_HOLES", g => GeographyFunctions.Holes(g), g => SpatialTypeFunctions.ST_Holes(g)),
            ("CLR_ST_GEOG_ISEMPTY", g => GeographyFunctions.IsEmpty(g), g => SpatialTypeFunctions.ST_IsEmpty(g)),
            ("CLR_ST_GEOG_IS3D", g => GeographyFunctions.Is3D(g), g => SpatialTypeFunctions.ST_Is3D(g)),
            ("CLR_ST_GEOG_ISCLOSED", g => GeographyFunctions.IsClosed(g), g => SpatialTypeFunctions.ST_IsClosed(g)),
            ("CLR_ST_GEOG_SRID", g => GeographyFunctions.Srid(g), g => SpatialTypeFunctions.ST_SRID(g)),
            ("CLR_ST_GEOG_ASTEXT", g => GeographyFunctions.AsText(g), g => SpatialTypeFunctions.ST_AsText(g)),
            ("CLR_ST_GEOG_ASWKT", g => GeographyFunctions.AsWkt(g), g => SpatialTypeFunctions.ST_AsWKT(g)),
            ("CLR_ST_GEOG_ASEWKT", g => GeographyFunctions.AsEwkt(g), g => SpatialTypeFunctions.ST_AsEWKT(g)),
            ("CLR_ST_GEOG_ASGEOJSON", g => GeographyFunctions.AsGeoJson(g), g => SpatialTypeFunctions.ST_AsGeoJSON(g)),
            ("CLR_ST_GEOG_ASGML", g => GeographyFunctions.AsGml(g), g => SpatialTypeFunctions.ST_AsGML(g)),
            ("CLR_ST_GEOG_ASBINARY", g => GeographyFunctions.AsBinary(g), g => SpatialTypeFunctions.ST_AsBinary(g)),
            ("CLR_ST_GEOG_ASWKB", g => GeographyFunctions.AsWkb(g), g => SpatialTypeFunctions.ST_AsWKB(g)),
            ("CLR_ST_GEOG_ASEWKB", g => GeographyFunctions.AsEwkb(g), g => SpatialTypeFunctions.ST_AsEWKB(g)),
        ];

        /// <summary>
        /// Renders an answer as a string, so that answers of different runtime types can be compared.
        /// </summary>
        /// <param name="answer">The value a function or query returned; may be <c>null</c>.</param>
        /// <returns><c>null</c> as <c>"null"</c>, a geometry as WKT, a boolean in lowercase, a byte array as lowercase
        /// hex, and anything else by <c>ToString</c>.</returns>
        internal static string Render(object? answer)
        {
            return answer switch
            {
                null => "null",
                Geometry geometry => geometry.toText(),
                // One side of a pair may return a java.lang.Boolean and the other a bool, and they print
                // differently ("true" and "True").
                java.lang.Boolean boxed => boxed.booleanValue() ? "true" : "false",
                bool value => value ? "true" : "false",
                // A ByteString prints as lowercase hex; the same value read from a result set is a byte array
                // when the operator is typed VARBINARY.
                byte[] bytes => Convert.ToHexString(bytes).ToLowerInvariant(),
                _ => answer.ToString() ?? "null",
            };
        }

        /// <summary>
        /// Calls a function and renders its result, or the name of the exception type it throws.
        /// </summary>
        /// <remarks>
        /// Some of Calcite's accessors throw over a shape they do not apply to, and throwing the same exception
        /// is part of agreeing with them.
        /// </remarks>
        /// <param name="call">The function to call.</param>
        /// <returns>The rendered result, or the simple name of the exception's type if the call throws.</returns>
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

        [Fact]
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
        /// Runs every accessor as a SQL operator and requires the same answer as calling its method.
        /// </summary>
        /// <remarks>
        /// This checks the operator declarations: an operator bound to the wrong method, or declared with the
        /// wrong return type, fails here. All accessors go into one statement per shape. An accessor that
        /// throws over the shape is left out, since one throwing column would end the whole query.
        /// </remarks>
        [Fact]
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
                    expressions.Add($"{name}(CLR_ST_GEOG_GEOMFROMTEXT('{shape}'))");
                }

                var row = GeographyExecutionTests.Run("SELECT " + string.Join(", ", expressions))[0];

                for (var i = 0; i < wanted.Count; i++)
                    Render(row[i]).Should().Be(wanted[i], $"{names[i]} over {shape}");
            }
        }

        [Fact]
        public void ShouldAgreeWithCalciteOnTheIndexedAccessors()
        {
            var differences = new List<string>();

            foreach (var shape in shapes)
            {
                var geography = Wkt(shape);

                for (var n = 0; n <= 2; n++)
                {
                    var index = java.lang.Integer.valueOf(n);

                    Compare(differences, $"CLR_ST_GEOG_POINTN({shape}, {n})",
                        () => GeographyFunctions.PointN(geography, index), () => SpatialTypeFunctions.ST_PointN(geography, n));

                    Compare(differences, $"CLR_ST_GEOG_GEOMETRYN({shape}, {n})",
                        () => GeographyFunctions.GeometryN(geography, index), () => SpatialTypeFunctions.ST_GeometryN(geography, n));

                    Compare(differences, $"CLR_ST_GEOG_INTERIORRING({shape}, {n})",
                        () => GeographyFunctions.InteriorRing(geography, index), () => SpatialTypeFunctions.ST_InteriorRing(geography, n));
                }

                foreach (var other in shapes)
                    Compare(differences, $"CLR_ST_GEOG_ORDERINGEQUALS({shape}, {other})",
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
        /// Each format's writer produces something that format's reader reads back to the same geometry.
        /// </summary>
        /// <remarks>
        /// GML is left out: Calcite's <c>ST_AsGML</c> writes a form its own GML reader does not accept.
        /// </remarks>
        [Fact]
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

        [Fact]
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
        /// An SRID other than 4326 is refused, whether passed as an argument or written into EWKT.
        /// </summary>
        [Fact]
        public void ShouldRefuseAnSridThatIsNotWgs84()
        {
            var wrong = java.lang.Integer.valueOf(3857);
            var wkb = GeographyFunctions.AsWkb(Wkt("POINT(1 2)"));

            ((Action)(() => GeographyFunctions.FromWkt("POINT(1 2)", wrong))).Should().Throw<java.lang.IllegalArgumentException>();
            ((Action)(() => GeographyFunctions.FromWkb(wkb, wrong))).Should().Throw<java.lang.IllegalArgumentException>();
            ((Action)(() => GeographyFunctions.FromGml("<gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point>", wrong))).Should().Throw<java.lang.IllegalArgumentException>();
            // Calcite's EWKT prefix is srid:N; rather than PostGIS's SRID=N;, and it reads no other form.
            ((Action)(() => GeographyFunctions.FromEwkt("srid:3857;POINT(1 2)"))).Should().Throw<java.lang.IllegalArgumentException>();
        }

        [Fact]
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
