using System;

using Apache.Calcite.Geography.Runtime;
using Apache.Calcite.Geography.Sql.Type;

using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Geography.Sql
{

    /// <summary>
    /// The <c>ST_GEOG_*</c> operators.
    /// </summary>
    /// <remarks>
    /// Chained by a host onto whatever it already has:
    ///
    /// <code>
    /// SqlOperatorTables.chain(SqlStdOperatorTable.instance(), GeographyOperatorTable.Instance())
    /// </code>
    ///
    /// <para>There is no other way in. A function declared through a schema — <c>SchemaPlus.add(name,
    /// Function)</c> — cannot take a geography parameter at all; see
    /// <see cref="GeographyOperandTypeChecker"/> for the assertion it dies on. So a caller reaching Calcite
    /// through a plain connection and a model file cannot resolve these names by themselves: something has to
    /// chain this table for them.</para>
    ///
    /// <para>Each operator is a <c>SqlUserDefinedFunction</c> over a <c>ScalarFunctionImpl</c>, which is what
    /// gives the call an implementor: <c>RexImpTable.get</c> answers a user-defined function by asking its
    /// <c>Function</c> for one, and its own map is private with no <c>RexImplementorTable</c> to add to until
    /// 1.43. The body is a .NET method and no class name is written out — this convention's translator holds
    /// the method itself, and Calcite's own engine reaches a CLR class through the class-loader stamp
    /// <c>IKVM.Maven.Sdk</c> puts on <c>calcite-core</c>.</para>
    ///
    /// <para>Each operator is also a public field, which <c>SqlSpatialTypeOperatorTable</c> is not, and the
    /// reason is that Calcite has a handle we cannot have. Its own spatial pushdown rules recognise a call by
    /// <c>SqlKind</c> — <c>SpatialRules</c> matches <c>SqlKind.ST_DWITHIN</c> and <c>SqlKind.ST_CONTAINS</c>,
    /// which reach the operator from a <c>@Hints({"SqlKind:ST_DWITHIN"})</c> annotation on the method that
    /// <c>CalciteCatalogReader.toOp</c> reads. <c>SqlKind</c> is a closed enum, so <c>ST_GEOG_DWITHIN</c>
    /// cannot be added to it, and taking <c>ST_DWITHIN</c> would be worse than having nothing: Calcite's own
    /// rules would then match a geodesic call and plan it as a planar one. So an adapter that wants to push
    /// one of these down has the operator itself or its name, and the field is the exact one.</para>
    ///
    /// <para>This is the first increment of the surface. Calcite's spatial library is about 130 names and
    /// every one of them needs an <c>ST_GEOG_</c> declaration, because Calcite's own reject the type. What is
    /// here is the type's constructors, the two crossings, and the five operations a geodesic store actually
    /// pushes.</para>
    /// </remarks>
    public sealed class GeographyOperatorTable : SqlOperatorTable
    {

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGEOJSON(VARCHAR)</c>. Reads a geography from GeoJSON.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromGeoJson =
            Function("ST_GEOG_GEOMFROMGEOJSON", nameof(GeographyFunctions.FromGeoJson), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["geoJson"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMTEXT(VARCHAR)</c>. Reads a geography from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromText =
            Function("ST_GEOG_GEOMFROMTEXT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKT(VARCHAR)</c>. Reads a geography from WKT; Calcite's alias for the same
        /// thing, mirrored.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromWkt =
            Function("ST_GEOG_GEOMFROMWKT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMTEXT(VARCHAR, INTEGER)</c>. Reads a geography from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromTextWithSrid =
            Function("ST_GEOG_GEOMFROMTEXT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKT(VARCHAR, INTEGER)</c>. Reads a geography from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromWktWithSrid =
            Function("ST_GEOG_GEOMFROMWKT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_ASGEOM(GEOGRAPHY)</c>. Reads a geography as a geometry.
        /// </summary>
        public static readonly SqlFunction StGeogAsGeom =
            Function("ST_GEOG_ASGEOM", nameof(GeographyFunctions.AsGeometry), GeographyReturnTypes.Geometry,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOM_ASGEOG(GEOMETRY)</c>. Reads a geometry as a geography.
        /// </summary>
        public static readonly SqlFunction StGeomAsGeog =
            Function("ST_GEOM_ASGEOG", nameof(GeographyFunctions.AsGeography), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geom"]);

        /// <summary>
        /// <c>ST_GEOG_DISTANCE(GEOGRAPHY, GEOGRAPHY)</c>. The distance between two geographies, in metres.
        /// </summary>
        public static readonly SqlFunction StGeogDistance =
            Function("ST_GEOG_DISTANCE", nameof(GeographyFunctions.Distance), ReturnTypes.DOUBLE_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_DWITHIN(GEOGRAPHY, GEOGRAPHY, DOUBLE)</c>. Whether two geographies are within the given
        /// distance in metres.
        /// </summary>
        public static readonly SqlFunction StGeogDWithin =
            Function("ST_GEOG_DWITHIN", nameof(GeographyFunctions.DWithin), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Numeric], ["geog1", "geog2", "distance"]);

        /// <summary>
        /// <c>ST_GEOG_WITHIN(GEOGRAPHY, GEOGRAPHY)</c>. Whether the first geography lies within the second.
        /// </summary>
        public static readonly SqlFunction StGeogWithin =
            Function("ST_GEOG_WITHIN", nameof(GeographyFunctions.Within), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_INTERSECTS(GEOGRAPHY, GEOGRAPHY)</c>. Whether two geographies have any point in common.
        /// </summary>
        public static readonly SqlFunction StGeogIntersects =
            Function("ST_GEOG_INTERSECTS", nameof(GeographyFunctions.Intersects), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_ISVALID(GEOGRAPHY)</c>. Whether the geography is valid on the sphere.
        /// </summary>
        public static readonly SqlFunction StGeogIsValid =
            Function("ST_GEOG_ISVALID", nameof(GeographyFunctions.IsValid), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_X(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogX =
            Function("ST_GEOG_X", nameof(GeographyFunctions.X), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_Y(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogY =
            Function("ST_GEOG_Y", nameof(GeographyFunctions.Y), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_Z(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogZ =
            Function("ST_GEOG_Z", nameof(GeographyFunctions.Z), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_XMIN(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogXMin =
            Function("ST_GEOG_XMIN", nameof(GeographyFunctions.XMin), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_XMAX(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogXMax =
            Function("ST_GEOG_XMAX", nameof(GeographyFunctions.XMax), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_YMIN(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogYMin =
            Function("ST_GEOG_YMIN", nameof(GeographyFunctions.YMin), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_YMAX(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogYMax =
            Function("ST_GEOG_YMAX", nameof(GeographyFunctions.YMax), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ZMIN(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogZMin =
            Function("ST_GEOG_ZMIN", nameof(GeographyFunctions.ZMin), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ZMAX(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogZMax =
            Function("ST_GEOG_ZMAX", nameof(GeographyFunctions.ZMax), GeographyReturnTypes.Double,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_COORDDIM(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogCoordDim =
            Function("ST_GEOG_COORDDIM", nameof(GeographyFunctions.CoordDim), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_DIMENSION(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogDimension =
            Function("ST_GEOG_DIMENSION", nameof(GeographyFunctions.Dimension), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYTYPE(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogGeometryType =
            Function("ST_GEOG_GEOMETRYTYPE", nameof(GeographyFunctions.GeometryType), GeographyReturnTypes.Text,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYTYPECODE(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogGeometryTypeCode =
            Function("ST_GEOG_GEOMETRYTYPECODE", nameof(GeographyFunctions.GeometryTypeCode), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_NPOINTS(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogNPoints =
            Function("ST_GEOG_NPOINTS", nameof(GeographyFunctions.NPoints), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_NUMPOINTS(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogNumPoints =
            Function("ST_GEOG_NUMPOINTS", nameof(GeographyFunctions.NumPoints), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_NUMGEOMETRIES(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogNumGeometries =
            Function("ST_GEOG_NUMGEOMETRIES", nameof(GeographyFunctions.NumGeometries), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_NUMINTERIORRING(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogNumInteriorRing =
            Function("ST_GEOG_NUMINTERIORRING", nameof(GeographyFunctions.NumInteriorRing), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_NUMINTERIORRINGS(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogNumInteriorRings =
            Function("ST_GEOG_NUMINTERIORRINGS", nameof(GeographyFunctions.NumInteriorRings), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_STARTPOINT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogStartPoint =
            Function("ST_GEOG_STARTPOINT", nameof(GeographyFunctions.StartPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ENDPOINT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogEndPoint =
            Function("ST_GEOG_ENDPOINT", nameof(GeographyFunctions.EndPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_EXTERIORRING(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogExteriorRing =
            Function("ST_GEOG_EXTERIORRING", nameof(GeographyFunctions.ExteriorRing), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_BOUNDARY(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogBoundary =
            Function("ST_GEOG_BOUNDARY", nameof(GeographyFunctions.Boundary), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_HOLES(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogHoles =
            Function("ST_GEOG_HOLES", nameof(GeographyFunctions.Holes), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ISEMPTY(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogIsEmpty =
            Function("ST_GEOG_ISEMPTY", nameof(GeographyFunctions.IsEmpty), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_IS3D(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogIs3D =
            Function("ST_GEOG_IS3D", nameof(GeographyFunctions.Is3D), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ISCLOSED(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogIsClosed =
            Function("ST_GEOG_ISCLOSED", nameof(GeographyFunctions.IsClosed), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_SRID(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogSrid =
            Function("ST_GEOG_SRID", nameof(GeographyFunctions.Srid), GeographyReturnTypes.Integer,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASTEXT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsText =
            Function("ST_GEOG_ASTEXT", nameof(GeographyFunctions.AsText), GeographyReturnTypes.Text,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASWKT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsWkt =
            Function("ST_GEOG_ASWKT", nameof(GeographyFunctions.AsWkt), GeographyReturnTypes.Text,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASEWKT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsEwkt =
            Function("ST_GEOG_ASEWKT", nameof(GeographyFunctions.AsEwkt), GeographyReturnTypes.Text,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASGEOJSON(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsGeoJson =
            Function("ST_GEOG_ASGEOJSON", nameof(GeographyFunctions.AsGeoJson), GeographyReturnTypes.Text,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASGML(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsGml =
            Function("ST_GEOG_ASGML", nameof(GeographyFunctions.AsGml), GeographyReturnTypes.Text,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASBINARY(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsBinary =
            Function("ST_GEOG_ASBINARY", nameof(GeographyFunctions.AsBinary), GeographyReturnTypes.Binary,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASWKB(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsWkb =
            Function("ST_GEOG_ASWKB", nameof(GeographyFunctions.AsWkb), GeographyReturnTypes.Binary,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ASEWKB(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction StGeogAsEwkb =
            Function("ST_GEOG_ASEWKB", nameof(GeographyFunctions.AsEwkb), GeographyReturnTypes.Binary,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_POINTN(GEOGRAPHY, INTEGER)</c>. Returns the nth coordinate of a line.
        /// </summary>
        public static readonly SqlFunction StGeogPointN =
            Function("ST_GEOG_POINTN", nameof(GeographyFunctions.PointN), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Numeric], ["geog", "n"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYN(GEOGRAPHY, INTEGER)</c>. Returns the nth part of the geography.
        /// </summary>
        public static readonly SqlFunction StGeogGeometryN =
            Function("ST_GEOG_GEOMETRYN", nameof(GeographyFunctions.GeometryN), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Numeric], ["geog", "n"]);

        /// <summary>
        /// <c>ST_GEOG_INTERIORRING(GEOGRAPHY, INTEGER)</c>. Returns the nth hole of a polygon.
        /// </summary>
        public static readonly SqlFunction StGeogInteriorRing =
            Function("ST_GEOG_INTERIORRING", nameof(GeographyFunctions.InteriorRing), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Numeric], ["geog", "n"]);

        /// <summary>
        /// <c>ST_GEOG_ORDERINGEQUALS(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether two geographies name the same coordinates in the same order.
        /// </summary>
        public static readonly SqlFunction StGeogOrderingEquals =
            Function("ST_GEOG_ORDERINGEQUALS", nameof(GeographyFunctions.OrderingEquals), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMEWKT(VARCHAR)</c>. Returns a geography read from EWKT.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromEwkt =
            Function("ST_GEOG_GEOMFROMEWKT", nameof(GeographyFunctions.FromEwkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["ewkt"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKB(VARBINARY)</c>. Returns a geography read from WKB.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromWkb =
            Function("ST_GEOG_GEOMFROMWKB", nameof(GeographyFunctions.FromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKB(VARBINARY, INTEGER)</c>. Returns a geography read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromWkbWithSrid =
            Function("ST_GEOG_GEOMFROMWKB", nameof(GeographyFunctions.FromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Numeric], ["wkb", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMEWKB(VARBINARY)</c>. Returns a geography read from EWKB.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromEwkb =
            Function("ST_GEOG_GEOMFROMEWKB", nameof(GeographyFunctions.FromEwkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["ewkb"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGML(VARCHAR)</c>. Returns a geography read from GML.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromGml =
            Function("ST_GEOG_GEOMFROMGML", nameof(GeographyFunctions.FromGml), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["gml"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGML(VARCHAR, INTEGER)</c>. Returns a geography read from GML; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromGmlWithSrid =
            Function("ST_GEOG_GEOMFROMGML", nameof(GeographyFunctions.FromGml), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["gml", "srid"]);

        /// <summary>
        /// Declares one operator.
        /// </summary>
        /// <param name="name">The SQL name.</param>
        /// <param name="method">The name of the method on <see cref="GeographyFunctions"/> that implements
        /// it.</param>
        /// <param name="returnType"></param>
        /// <param name="operands">What each position takes.</param>
        /// <param name="names">The name of each position.</param>
        /// <returns></returns>
        static SqlFunction Function(string name, string method, SqlReturnTypeInference returnType, GeographyOperand[] operands, string[] names)
        {
            // by signature and not by name. ScalarFunctionImpl.create(class, name) goes through
            // ReflectiveFunctionBase.findMethod, which answers the first method of that name and would pick
            // between two arities of a constructor at random. Deriving the signature from the operands is
            // also what keeps the declaration and the body from drifting apart: a position that takes a
            // geography is a Geometry parameter, and there is one place that says so.
            var parameters = new java.lang.Class[operands.Length];
            for (var i = 0; i < operands.Length; i++)
                parameters[i] = ClassOf(operands[i]);

            var found = ((java.lang.Class)typeof(GeographyFunctions)).getMethod(method, parameters) ??
                throw new InvalidOperationException($"No method '{method}' on '{nameof(GeographyFunctions)}'.");

            return new SqlUserDefinedFunction(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                returnType,
                null,
                new GeographyOperandTypeChecker(operands, names),
                ScalarFunctionImpl.create(found));
        }

        /// <summary>
        /// The parameter class a position of the given kind is passed as.
        /// </summary>
        /// <param name="operand"></param>
        /// <returns></returns>
        /// <remarks>
        /// A number is a <c>Number</c> rather than a <c>Double</c> because a literal arrives as whatever type
        /// it had, and <c>2.0</c> is a <c>BigDecimal</c>; see <c>GeographyFunctions.DWithin</c>.
        /// </remarks>
        static java.lang.Class ClassOf(GeographyOperand operand)
        {
            return operand switch
            {
                GeographyOperand.Geography => (java.lang.Class)typeof(org.locationtech.jts.geom.Geometry),
                GeographyOperand.Geometry => (java.lang.Class)typeof(org.locationtech.jts.geom.Geometry),
                GeographyOperand.Character => (java.lang.Class)typeof(string),
                GeographyOperand.Numeric => (java.lang.Class)typeof(java.lang.Number),
                GeographyOperand.Binary => (java.lang.Class)typeof(org.apache.calcite.avatica.util.ByteString),
                _ => throw new NotSupportedException($"No parameter class for '{operand}'."),
            };
        }

        /// <summary>
        /// The one instance.
        /// </summary>
        /// <remarks>
        /// Declared after the operators deliberately: a static field initializer runs in textual order, and
        /// one placed above them builds the table out of ten nulls.
        /// </remarks>
        static readonly GeographyOperatorTable instance = new();

        /// <summary>
        /// Returns the operator table.
        /// </summary>
        /// <returns></returns>
        public static GeographyOperatorTable Instance()
        {
            return instance;
        }

        readonly SqlOperatorTable operators;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        GeographyOperatorTable()
        {
            operators = SqlOperatorTables.of([
                StGeogGeomFromGeoJson,
                StGeogGeomFromText,
                StGeogGeomFromWkt,
                StGeogGeomFromTextWithSrid,
                StGeogGeomFromWktWithSrid,
                StGeogAsGeom,
                StGeomAsGeog,
                StGeogDistance,
                StGeogDWithin,
                StGeogWithin,
                StGeogIntersects,
                StGeogIsValid,
                StGeogX,
                StGeogY,
                StGeogZ,
                StGeogXMin,
                StGeogXMax,
                StGeogYMin,
                StGeogYMax,
                StGeogZMin,
                StGeogZMax,
                StGeogCoordDim,
                StGeogDimension,
                StGeogGeometryType,
                StGeogGeometryTypeCode,
                StGeogNPoints,
                StGeogNumPoints,
                StGeogNumGeometries,
                StGeogNumInteriorRing,
                StGeogNumInteriorRings,
                StGeogStartPoint,
                StGeogEndPoint,
                StGeogExteriorRing,
                StGeogBoundary,
                StGeogHoles,
                StGeogIsEmpty,
                StGeogIs3D,
                StGeogIsClosed,
                StGeogSrid,
                StGeogAsText,
                StGeogAsWkt,
                StGeogAsEwkt,
                StGeogAsGeoJson,
                StGeogAsGml,
                StGeogAsBinary,
                StGeogAsWkb,
                StGeogAsEwkb,
                StGeogPointN,
                StGeogGeometryN,
                StGeogInteriorRing,
                StGeogOrderingEquals,
                StGeogGeomFromEwkt,
                StGeogGeomFromWkb,
                StGeogGeomFromWkbWithSrid,
                StGeogGeomFromEwkb,
                StGeogGeomFromGml,
                StGeogGeomFromGmlWithSrid,
            ]);
        }

        /// <inheritdoc />
        public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, java.util.List operatorList, SqlNameMatcher nameMatcher)
        {
            operators.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        }

        /// <inheritdoc />
        public java.util.List getOperatorList()
        {
            return operators.getOperatorList();
        }

    }

}
