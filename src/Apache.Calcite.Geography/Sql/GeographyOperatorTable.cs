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
        /// <c>ST_GEOG_FLIPCOORDINATES(GEOGRAPHY)</c>. Returns the geography with longitude and latitude swapped.
        /// </summary>
        public static readonly SqlFunction StGeogFlipCoordinates =
            Function("ST_GEOG_FLIPCOORDINATES", nameof(GeographyFunctions.FlipCoordinates), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_FORCE2D(GEOGRAPHY)</c>. Returns the geography with any third ordinate dropped.
        /// </summary>
        public static readonly SqlFunction StGeogForce2D =
            Function("ST_GEOG_FORCE2D", nameof(GeographyFunctions.Force2D), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_FORCE3D(GEOGRAPHY)</c>. Returns the geography with a third ordinate on every coordinate.
        /// </summary>
        public static readonly SqlFunction StGeogForce3D =
            Function("ST_GEOG_FORCE3D", nameof(GeographyFunctions.Force3D), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_NORMALIZE(GEOGRAPHY)</c>. Returns the geography in its canonical form.
        /// </summary>
        public static readonly SqlFunction StGeogNormalize =
            Function("ST_GEOG_NORMALIZE", nameof(GeographyFunctions.Normalize), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_REMOVEHOLES(GEOGRAPHY)</c>. Returns the geography with the holes taken out of its polygons.
        /// </summary>
        public static readonly SqlFunction StGeogRemoveHoles =
            Function("ST_GEOG_REMOVEHOLES", nameof(GeographyFunctions.RemoveHoles), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_REMOVEREPEATEDPOINTS(GEOGRAPHY)</c>. Returns the geography with repeated coordinates dropped.
        /// </summary>
        public static readonly SqlFunction StGeogRemoveRepeatedPoints =
            Function("ST_GEOG_REMOVEREPEATEDPOINTS", nameof(GeographyFunctions.RemoveRepeatedPoints), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_REVERSE(GEOGRAPHY)</c>. Returns the geography with its coordinates in the opposite order.
        /// </summary>
        public static readonly SqlFunction StGeogReverse =
            Function("ST_GEOG_REVERSE", nameof(GeographyFunctions.Reverse), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_TOMULTILINE(GEOGRAPHY)</c>. Returns the lines of the geography as a multi-line.
        /// </summary>
        public static readonly SqlFunction StGeogToMultiLine =
            Function("ST_GEOG_TOMULTILINE", nameof(GeographyFunctions.ToMultiLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_TOMULTIPOINT(GEOGRAPHY)</c>. Returns the coordinates of the geography as a multi-point.
        /// </summary>
        public static readonly SqlFunction StGeogToMultiPoint =
            Function("ST_GEOG_TOMULTIPOINT", nameof(GeographyFunctions.ToMultiPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_TOMULTISEGMENTS(GEOGRAPHY)</c>. Returns the edges of the geography as a multi-line.
        /// </summary>
        public static readonly SqlFunction StGeogToMultiSegments =
            Function("ST_GEOG_TOMULTISEGMENTS", nameof(GeographyFunctions.ToMultiSegments), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOG_ADDPOINT(GEOGRAPHY, GEOGRAPHY)</c>. Returns the line with the coordinate added at its end.
        /// </summary>
        public static readonly SqlFunction StGeogAddPoint =
            Function("ST_GEOG_ADDPOINT", nameof(GeographyFunctions.AddPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["line", "point"]);

        /// <summary>
        /// <c>ST_GEOG_ADDPOINT(GEOGRAPHY, GEOGRAPHY, INTEGER)</c>. Returns the line with the coordinate added at the given index.
        /// </summary>
        public static readonly SqlFunction StGeogAddPointAtIndex =
            Function("ST_GEOG_ADDPOINT", nameof(GeographyFunctions.AddPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Numeric], ["line", "point", "index"]);

        /// <summary>
        /// <c>ST_GEOG_REMOVEPOINT(GEOGRAPHY, INTEGER)</c>. Returns the line with the coordinate at the given index taken out.
        /// </summary>
        public static readonly SqlFunction StGeogRemovePoint =
            Function("ST_GEOG_REMOVEPOINT", nameof(GeographyFunctions.RemovePoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Numeric], ["line", "index"]);

        /// <summary>
        /// <c>ST_GEOG_ADDZ(GEOGRAPHY, NUMERIC)</c>. Returns the geography with the given amount added to every third ordinate.
        /// </summary>
        public static readonly SqlFunction StGeogAddZ =
            Function("ST_GEOG_ADDZ", nameof(GeographyFunctions.AddZ), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Numeric], ["geog", "z"]);

        /// <summary>
        /// <c>ST_GEOG_REMOVEREPEATEDPOINTS(GEOGRAPHY, NUMERIC)</c>. Returns the geography with coordinates closer together than the tolerance dropped.
        /// </summary>
        public static readonly SqlFunction StGeogRemoveRepeatedPointsWithTolerance =
            Function("ST_GEOG_REMOVEREPEATEDPOINTS", nameof(GeographyFunctions.RemoveRepeatedPoints), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Numeric], ["geog", "tolerance"]);

        /// <summary>
        /// <c>ST_GEOG_POINT(NUMERIC, NUMERIC)</c>. Returns the place at the given longitude and latitude.
        /// </summary>
        public static readonly SqlFunction StGeogPoint =
            Function("ST_GEOG_POINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Numeric, GeographyOperand.Numeric], ["x", "y"]);

        /// <summary>
        /// <c>ST_GEOG_POINT(NUMERIC, NUMERIC, NUMERIC)</c>. Returns the place at the given longitude, latitude and third ordinate.
        /// </summary>
        public static readonly SqlFunction StGeogPoint3D =
            Function("ST_GEOG_POINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Numeric, GeographyOperand.Numeric, GeographyOperand.Numeric], ["x", "y", "z"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOINT(NUMERIC, NUMERIC)</c>. Returns the place at the given longitude and latitude.
        /// </summary>
        public static readonly SqlFunction StGeogMakePoint =
            Function("ST_GEOG_MAKEPOINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Numeric, GeographyOperand.Numeric], ["x", "y"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOINT(NUMERIC, NUMERIC, NUMERIC)</c>. Returns the place at the given longitude, latitude and third ordinate.
        /// </summary>
        public static readonly SqlFunction StGeogMakePoint3D =
            Function("ST_GEOG_MAKEPOINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Numeric, GeographyOperand.Numeric, GeographyOperand.Numeric], ["x", "y", "z"]);

        /// <summary>
        /// <c>ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 2 places.
        /// </summary>
        public static readonly SqlFunction StGeogMakeLine2 =
            Function("ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 3 places.
        /// </summary>
        public static readonly SqlFunction StGeogMakeLine3 =
            Function("ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2", "geog3"]);

        /// <summary>
        /// <c>ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 4 places.
        /// </summary>
        public static readonly SqlFunction StGeogMakeLine4 =
            Function("ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2", "geog3", "geog4"]);

        /// <summary>
        /// <c>ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 5 places.
        /// </summary>
        public static readonly SqlFunction StGeogMakeLine5 =
            Function("ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2", "geog3", "geog4", "geog5"]);

        /// <summary>
        /// <c>ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 6 places.
        /// </summary>
        public static readonly SqlFunction StGeogMakeLine6 =
            Function("ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2", "geog3", "geog4", "geog5", "geog6"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY)</c>. Returns the polygon with the given shell and no holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon1 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography], ["shell"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and one hole.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon2 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 2 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon3 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 3 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon4 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 4 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon5 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 5 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon6 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 6 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon7 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 7 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon8 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 8 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon9 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6", "hole7"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 9 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon10 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6", "hole7", "hole8"]);

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 10 holes.
        /// </summary>
        public static readonly SqlFunction StGeogMakePolygon11 =
            Function("ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Geography], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6", "hole7", "hole8", "hole9"]);

        /// <summary>
        /// <c>ST_GEOG_LINEFROMTEXT(VARCHAR)</c>. Returns a line read from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogLineFromText =
            Function("ST_GEOG_LINEFROMTEXT", nameof(GeographyFunctions.LineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_LINEFROMTEXT(VARCHAR, INTEGER)</c>. Returns a line read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogLineFromTextWithSrid =
            Function("ST_GEOG_LINEFROMTEXT", nameof(GeographyFunctions.LineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_LINEFROMWKB(VARBINARY)</c>. Returns a line read from WKB.
        /// </summary>
        public static readonly SqlFunction StGeogLineFromWkb =
            Function("ST_GEOG_LINEFROMWKB", nameof(GeographyFunctions.LineFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>ST_GEOG_LINEFROMWKB(VARBINARY, INTEGER)</c>. Returns a line read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogLineFromWkbWithSrid =
            Function("ST_GEOG_LINEFROMWKB", nameof(GeographyFunctions.LineFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Numeric], ["wkb", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_MLINEFROMTEXT(VARCHAR)</c>. Returns a multi-line read from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogMLineFromText =
            Function("ST_GEOG_MLINEFROMTEXT", nameof(GeographyFunctions.MLineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_MLINEFROMTEXT(VARCHAR, INTEGER)</c>. Returns a multi-line read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogMLineFromTextWithSrid =
            Function("ST_GEOG_MLINEFROMTEXT", nameof(GeographyFunctions.MLineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_MPOINTFROMTEXT(VARCHAR)</c>. Returns a multi-point read from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogMPointFromText =
            Function("ST_GEOG_MPOINTFROMTEXT", nameof(GeographyFunctions.MPointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_MPOINTFROMTEXT(VARCHAR, INTEGER)</c>. Returns a multi-point read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogMPointFromTextWithSrid =
            Function("ST_GEOG_MPOINTFROMTEXT", nameof(GeographyFunctions.MPointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_MPOLYFROMTEXT(VARCHAR)</c>. Returns a multi-polygon read from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogMPolyFromText =
            Function("ST_GEOG_MPOLYFROMTEXT", nameof(GeographyFunctions.MPolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_MPOLYFROMTEXT(VARCHAR, INTEGER)</c>. Returns a multi-polygon read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogMPolyFromTextWithSrid =
            Function("ST_GEOG_MPOLYFROMTEXT", nameof(GeographyFunctions.MPolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_POINTFROMTEXT(VARCHAR)</c>. Returns a point read from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogPointFromText =
            Function("ST_GEOG_POINTFROMTEXT", nameof(GeographyFunctions.PointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_POINTFROMTEXT(VARCHAR, INTEGER)</c>. Returns a point read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogPointFromTextWithSrid =
            Function("ST_GEOG_POINTFROMTEXT", nameof(GeographyFunctions.PointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_POINTFROMWKB(VARBINARY)</c>. Returns a point read from WKB.
        /// </summary>
        public static readonly SqlFunction StGeogPointFromWkb =
            Function("ST_GEOG_POINTFROMWKB", nameof(GeographyFunctions.PointFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>ST_GEOG_POINTFROMWKB(VARBINARY, INTEGER)</c>. Returns a point read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogPointFromWkbWithSrid =
            Function("ST_GEOG_POINTFROMWKB", nameof(GeographyFunctions.PointFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Numeric], ["wkb", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_POLYFROMTEXT(VARCHAR)</c>. Returns a polygon read from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogPolyFromText =
            Function("ST_GEOG_POLYFROMTEXT", nameof(GeographyFunctions.PolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_POLYFROMTEXT(VARCHAR, INTEGER)</c>. Returns a polygon read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogPolyFromTextWithSrid =
            Function("ST_GEOG_POLYFROMTEXT", nameof(GeographyFunctions.PolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_POLYFROMWKB(VARBINARY)</c>. Returns a polygon read from WKB.
        /// </summary>
        public static readonly SqlFunction StGeogPolyFromWkb =
            Function("ST_GEOG_POLYFROMWKB", nameof(GeographyFunctions.PolyFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>ST_GEOG_POLYFROMWKB(VARBINARY, INTEGER)</c>. Returns a polygon read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogPolyFromWkbWithSrid =
            Function("ST_GEOG_POLYFROMWKB", nameof(GeographyFunctions.PolyFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Numeric], ["wkb", "srid"]);

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
                StGeogFlipCoordinates,
                StGeogForce2D,
                StGeogForce3D,
                StGeogNormalize,
                StGeogRemoveHoles,
                StGeogRemoveRepeatedPoints,
                StGeogReverse,
                StGeogToMultiLine,
                StGeogToMultiPoint,
                StGeogToMultiSegments,
                StGeogAddPoint,
                StGeogAddPointAtIndex,
                StGeogRemovePoint,
                StGeogAddZ,
                StGeogRemoveRepeatedPointsWithTolerance,
                StGeogPoint,
                StGeogPoint3D,
                StGeogMakePoint,
                StGeogMakePoint3D,
                StGeogMakeLine2,
                StGeogMakeLine3,
                StGeogMakeLine4,
                StGeogMakeLine5,
                StGeogMakeLine6,
                StGeogMakePolygon1,
                StGeogMakePolygon2,
                StGeogMakePolygon3,
                StGeogMakePolygon4,
                StGeogMakePolygon5,
                StGeogMakePolygon6,
                StGeogMakePolygon7,
                StGeogMakePolygon8,
                StGeogMakePolygon9,
                StGeogMakePolygon10,
                StGeogMakePolygon11,
                StGeogLineFromText,
                StGeogLineFromTextWithSrid,
                StGeogLineFromWkb,
                StGeogLineFromWkbWithSrid,
                StGeogMLineFromText,
                StGeogMLineFromTextWithSrid,
                StGeogMPointFromText,
                StGeogMPointFromTextWithSrid,
                StGeogMPolyFromText,
                StGeogMPolyFromTextWithSrid,
                StGeogPointFromText,
                StGeogPointFromTextWithSrid,
                StGeogPointFromWkb,
                StGeogPointFromWkbWithSrid,
                StGeogPolyFromText,
                StGeogPolyFromTextWithSrid,
                StGeogPolyFromWkb,
                StGeogPolyFromWkbWithSrid,
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
