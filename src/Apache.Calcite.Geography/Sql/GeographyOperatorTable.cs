using System;
using System.Collections.Generic;

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
    /// The <c>CLR_ST_GEOG_*</c> operators.
    /// </summary>
    /// <remarks>
    /// Chained by a host onto whatever it already has:
    ///
    /// <code>
    /// SqlOperatorTables.chain(SqlStdOperatorTable.instance(), GeographyOperatorTable.Instance())
    /// </code>
    ///
    /// <para><see cref="Schema.GeographySchema"/> is the other way in, and is the one an adapter uses,
    /// because a schema is what brings functions along with tables. It costs something: what
    /// <c>CalciteCatalogReader.toOp</c> builds around a schema declaration is a plain
    /// <c>SqlUserDefinedFunction</c> over the bare <c>Function</c>, so the checker, the return type strategy
    /// and every fact <see cref="GeographyFunction"/> declares are Calcite's own from there on.
    /// <see cref="Rebind"/> is how a call that came in that way gets this table's declaration back, and
    /// <c>GeographyRules</c> is what runs it.</para>
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
    /// <c>CalciteCatalogReader.toOp</c> reads. <c>SqlKind</c> is a closed enum, so <c>CLR_ST_GEOG_DWITHIN</c>
    /// cannot be added to it, and taking <c>ST_DWITHIN</c> would be worse than having nothing: Calcite's own
    /// rules would then match a geodesic call and plan it as a planar one. So an adapter that wants to push
    /// one of these down has the operator itself or its name, and the field is the exact one.</para>
    ///
    /// <para>Calcite's spatial library is about 144 names and every one it reads geodesically needs a
    /// <c>CLR_ST_GEOG_</c> declaration of its own, because Calcite's own read the plane. What is here is the
    /// constructors, the two crossings, the relations, the measurements, the accessors, the editing
    /// operations and every format both ways; the package's README says what is not, and why.</para>
    /// </remarks>
    public sealed class GeographyOperatorTable : SqlOperatorTable
    {

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMGEOJSON(VARCHAR)</c>. Reads a geography from GeoJSON.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromGeoJson =
            Function("CLR_ST_GEOG_GEOMFROMGEOJSON", nameof(GeographyFunctions.FromGeoJson), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["geoJson"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMTEXT(VARCHAR)</c>. Reads a geography from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromText =
            Function("CLR_ST_GEOG_GEOMFROMTEXT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMWKT(VARCHAR)</c>. Reads a geography from WKT; Calcite's alias for the same
        /// thing, mirrored.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromWkt =
            Function("CLR_ST_GEOG_GEOMFROMWKT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMTEXT(VARCHAR, INTEGER)</c>. Reads a geography from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromTextWithSrid =
            Function("CLR_ST_GEOG_GEOMFROMTEXT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMWKT(VARCHAR, INTEGER)</c>. Reads a geography from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromWktWithSrid =
            Function("CLR_ST_GEOG_GEOMFROMWKT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASGEOM(GEOGRAPHY)</c>. Reads a geography as a geometry.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsGeom =
            Function("CLR_ST_GEOG_ASGEOM", nameof(GeographyFunctions.AsGeometry), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOM_ASGEOG(GEOMETRY)</c>. Reads a geometry as a geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeomAsGeog =
            Function("CLR_ST_GEOM_ASGEOG", nameof(GeographyFunctions.AsGeography), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geom"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_DISTANCE(GEOGRAPHY, GEOGRAPHY)</c>. The distance between two geographies, in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogDistance =
            Function("CLR_ST_GEOG_DISTANCE", nameof(GeographyFunctions.Distance), ReturnTypes.DOUBLE_NULLABLE,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_DWITHIN(GEOGRAPHY, GEOGRAPHY, DOUBLE)</c>. Whether two geographies are within the given
        /// distance in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogDWithin =
            Function("CLR_ST_GEOG_DWITHIN", nameof(GeographyFunctions.DWithin), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog1", "geog2", "distance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_WITHIN(GEOGRAPHY, GEOGRAPHY)</c>. Whether the first geography lies within the second.
        /// </summary>
        public static readonly SqlFunction ClrStGeogWithin =
            Function("CLR_ST_GEOG_WITHIN", nameof(GeographyFunctions.Within), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_INTERSECTS(GEOGRAPHY, GEOGRAPHY)</c>. Whether two geographies have any point in common.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIntersects =
            Function("CLR_ST_GEOG_INTERSECTS", nameof(GeographyFunctions.Intersects), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ISVALID(GEOGRAPHY)</c>. Whether the geography is valid on the sphere.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIsValid =
            Function("CLR_ST_GEOG_ISVALID", nameof(GeographyFunctions.IsValid), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_X(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogX =
            Function("CLR_ST_GEOG_X", nameof(GeographyFunctions.X), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_Y(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogY =
            Function("CLR_ST_GEOG_Y", nameof(GeographyFunctions.Y), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_Z(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogZ =
            Function("CLR_ST_GEOG_Z", nameof(GeographyFunctions.Z), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_XMIN(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogXMin =
            Function("CLR_ST_GEOG_XMIN", nameof(GeographyFunctions.XMin), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_XMAX(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogXMax =
            Function("CLR_ST_GEOG_XMAX", nameof(GeographyFunctions.XMax), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_YMIN(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogYMin =
            Function("CLR_ST_GEOG_YMIN", nameof(GeographyFunctions.YMin), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_YMAX(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogYMax =
            Function("CLR_ST_GEOG_YMAX", nameof(GeographyFunctions.YMax), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ZMIN(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogZMin =
            Function("CLR_ST_GEOG_ZMIN", nameof(GeographyFunctions.ZMin), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ZMAX(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogZMax =
            Function("CLR_ST_GEOG_ZMAX", nameof(GeographyFunctions.ZMax), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_COORDDIM(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogCoordDim =
            Function("CLR_ST_GEOG_COORDDIM", nameof(GeographyFunctions.CoordDim), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_DIMENSION(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogDimension =
            Function("CLR_ST_GEOG_DIMENSION", nameof(GeographyFunctions.Dimension), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMETRYTYPE(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeometryType =
            Function("CLR_ST_GEOG_GEOMETRYTYPE", nameof(GeographyFunctions.GeometryType), GeographyReturnTypes.Text,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMETRYTYPECODE(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeometryTypeCode =
            Function("CLR_ST_GEOG_GEOMETRYTYPECODE", nameof(GeographyFunctions.GeometryTypeCode), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_NPOINTS(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogNPoints =
            Function("CLR_ST_GEOG_NPOINTS", nameof(GeographyFunctions.NPoints), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMPOINTS(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogNumPoints =
            Function("CLR_ST_GEOG_NUMPOINTS", nameof(GeographyFunctions.NumPoints), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMGEOMETRIES(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogNumGeometries =
            Function("CLR_ST_GEOG_NUMGEOMETRIES", nameof(GeographyFunctions.NumGeometries), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMINTERIORRING(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogNumInteriorRing =
            Function("CLR_ST_GEOG_NUMINTERIORRING", nameof(GeographyFunctions.NumInteriorRing), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMINTERIORRINGS(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogNumInteriorRings =
            Function("CLR_ST_GEOG_NUMINTERIORRINGS", nameof(GeographyFunctions.NumInteriorRings), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_STARTPOINT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogStartPoint =
            Function("CLR_ST_GEOG_STARTPOINT", nameof(GeographyFunctions.StartPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ENDPOINT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogEndPoint =
            Function("CLR_ST_GEOG_ENDPOINT", nameof(GeographyFunctions.EndPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_EXTERIORRING(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogExteriorRing =
            Function("CLR_ST_GEOG_EXTERIORRING", nameof(GeographyFunctions.ExteriorRing), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_BOUNDARY(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogBoundary =
            Function("CLR_ST_GEOG_BOUNDARY", nameof(GeographyFunctions.Boundary), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_HOLES(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogHoles =
            Function("CLR_ST_GEOG_HOLES", nameof(GeographyFunctions.Holes), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ISEMPTY(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIsEmpty =
            Function("CLR_ST_GEOG_ISEMPTY", nameof(GeographyFunctions.IsEmpty), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_IS3D(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIs3D =
            Function("CLR_ST_GEOG_IS3D", nameof(GeographyFunctions.Is3D), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ISCLOSED(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIsClosed =
            Function("CLR_ST_GEOG_ISCLOSED", nameof(GeographyFunctions.IsClosed), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_SRID(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogSrid =
            Function("CLR_ST_GEOG_SRID", nameof(GeographyFunctions.Srid), GeographyReturnTypes.Integer,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASTEXT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsText =
            Function("CLR_ST_GEOG_ASTEXT", nameof(GeographyFunctions.AsText), GeographyReturnTypes.Text,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASWKT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsWkt =
            Function("CLR_ST_GEOG_ASWKT", nameof(GeographyFunctions.AsWkt), GeographyReturnTypes.Text,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASEWKT(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsEwkt =
            Function("CLR_ST_GEOG_ASEWKT", nameof(GeographyFunctions.AsEwkt), GeographyReturnTypes.Text,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASGEOJSON(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsGeoJson =
            Function("CLR_ST_GEOG_ASGEOJSON", nameof(GeographyFunctions.AsGeoJson), GeographyReturnTypes.Text,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASGML(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsGml =
            Function("CLR_ST_GEOG_ASGML", nameof(GeographyFunctions.AsGml), GeographyReturnTypes.Text,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASBINARY(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsBinary =
            Function("CLR_ST_GEOG_ASBINARY", nameof(GeographyFunctions.AsBinary), GeographyReturnTypes.Binary,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASWKB(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsWkb =
            Function("CLR_ST_GEOG_ASWKB", nameof(GeographyFunctions.AsWkb), GeographyReturnTypes.Binary,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ASEWKB(GEOGRAPHY)</c>.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAsEwkb =
            Function("CLR_ST_GEOG_ASEWKB", nameof(GeographyFunctions.AsEwkb), GeographyReturnTypes.Binary,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTN(GEOGRAPHY, INTEGER)</c>. Returns the nth coordinate of a line.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPointN =
            Function("CLR_ST_GEOG_POINTN", nameof(GeographyFunctions.PointN), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Integral], ["geog", "n"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMETRYN(GEOGRAPHY, INTEGER)</c>. Returns the nth part of the geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeometryN =
            Function("CLR_ST_GEOG_GEOMETRYN", nameof(GeographyFunctions.GeometryN), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Integral], ["geog", "n"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_INTERIORRING(GEOGRAPHY, INTEGER)</c>. Returns the nth hole of a polygon.
        /// </summary>
        public static readonly SqlFunction ClrStGeogInteriorRing =
            Function("CLR_ST_GEOG_INTERIORRING", nameof(GeographyFunctions.InteriorRing), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Integral], ["geog", "n"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ORDERINGEQUALS(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether two geographies name the same coordinates in the same order.
        /// </summary>
        public static readonly SqlFunction ClrStGeogOrderingEquals =
            Function("CLR_ST_GEOG_ORDERINGEQUALS", nameof(GeographyFunctions.OrderingEquals), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMEWKT(VARCHAR)</c>. Returns a geography read from EWKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromEwkt =
            Function("CLR_ST_GEOG_GEOMFROMEWKT", nameof(GeographyFunctions.FromEwkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["ewkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMWKB(VARBINARY)</c>. Returns a geography read from WKB.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromWkb =
            Function("CLR_ST_GEOG_GEOMFROMWKB", nameof(GeographyFunctions.FromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMWKB(VARBINARY, INTEGER)</c>. Returns a geography read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromWkbWithSrid =
            Function("CLR_ST_GEOG_GEOMFROMWKB", nameof(GeographyFunctions.FromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Integral], ["wkb", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMEWKB(VARBINARY)</c>. Returns a geography read from EWKB.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromEwkb =
            Function("CLR_ST_GEOG_GEOMFROMEWKB", nameof(GeographyFunctions.FromEwkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["ewkb"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMGML(VARCHAR)</c>. Returns a geography read from GML.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromGml =
            Function("CLR_ST_GEOG_GEOMFROMGML", nameof(GeographyFunctions.FromGml), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["gml"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMGML(VARCHAR, INTEGER)</c>. Returns a geography read from GML; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogGeomFromGmlWithSrid =
            Function("CLR_ST_GEOG_GEOMFROMGML", nameof(GeographyFunctions.FromGml), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["gml", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_FLIPCOORDINATES(GEOGRAPHY)</c>. Returns the geography with longitude and latitude swapped.
        /// </summary>
        public static readonly SqlFunction ClrStGeogFlipCoordinates =
            Function("CLR_ST_GEOG_FLIPCOORDINATES", nameof(GeographyFunctions.FlipCoordinates), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_FORCE2D(GEOGRAPHY)</c>. Returns the geography with any third ordinate dropped.
        /// </summary>
        public static readonly SqlFunction ClrStGeogForce2D =
            Function("CLR_ST_GEOG_FORCE2D", nameof(GeographyFunctions.Force2D), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_FORCE3D(GEOGRAPHY)</c>. Returns the geography with a third ordinate on every coordinate.
        /// </summary>
        public static readonly SqlFunction ClrStGeogForce3D =
            Function("CLR_ST_GEOG_FORCE3D", nameof(GeographyFunctions.Force3D), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_NORMALIZE(GEOGRAPHY)</c>. Returns the geography in its canonical form.
        /// </summary>
        public static readonly SqlFunction ClrStGeogNormalize =
            Function("CLR_ST_GEOG_NORMALIZE", nameof(GeographyFunctions.Normalize), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEHOLES(GEOGRAPHY)</c>. Returns the geography with the holes taken out of its polygons.
        /// </summary>
        public static readonly SqlFunction ClrStGeogRemoveHoles =
            Function("CLR_ST_GEOG_REMOVEHOLES", nameof(GeographyFunctions.RemoveHoles), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEREPEATEDPOINTS(GEOGRAPHY)</c>. Returns the geography with repeated coordinates dropped.
        /// </summary>
        public static readonly SqlFunction ClrStGeogRemoveRepeatedPoints =
            Function("CLR_ST_GEOG_REMOVEREPEATEDPOINTS", nameof(GeographyFunctions.RemoveRepeatedPoints), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_REVERSE(GEOGRAPHY)</c>. Returns the geography with its coordinates in the opposite order.
        /// </summary>
        public static readonly SqlFunction ClrStGeogReverse =
            Function("CLR_ST_GEOG_REVERSE", nameof(GeographyFunctions.Reverse), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_TOMULTILINE(GEOGRAPHY)</c>. Returns the lines of the geography as a multi-line.
        /// </summary>
        public static readonly SqlFunction ClrStGeogToMultiLine =
            Function("CLR_ST_GEOG_TOMULTILINE", nameof(GeographyFunctions.ToMultiLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_TOMULTIPOINT(GEOGRAPHY)</c>. Returns the coordinates of the geography as a multi-point.
        /// </summary>
        public static readonly SqlFunction ClrStGeogToMultiPoint =
            Function("CLR_ST_GEOG_TOMULTIPOINT", nameof(GeographyFunctions.ToMultiPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_TOMULTISEGMENTS(GEOGRAPHY)</c>. Returns the edges of the geography as a multi-line.
        /// </summary>
        public static readonly SqlFunction ClrStGeogToMultiSegments =
            Function("CLR_ST_GEOG_TOMULTISEGMENTS", nameof(GeographyFunctions.ToMultiSegments), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ADDPOINT(GEOGRAPHY, GEOGRAPHY)</c>. Returns the line with the coordinate added at its end.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAddPoint =
            Function("CLR_ST_GEOG_ADDPOINT", nameof(GeographyFunctions.AddPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["line", "point"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ADDPOINT(GEOGRAPHY, GEOGRAPHY, INTEGER)</c>. Returns the line with the coordinate added at the given index.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAddPointAtIndex =
            Function("CLR_ST_GEOG_ADDPOINT", nameof(GeographyFunctions.AddPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Integral], ["line", "point", "index"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEPOINT(GEOGRAPHY, INTEGER)</c>. Returns the line with the coordinate at the given index taken out.
        /// </summary>
        public static readonly SqlFunction ClrStGeogRemovePoint =
            Function("CLR_ST_GEOG_REMOVEPOINT", nameof(GeographyFunctions.RemovePoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Integral], ["line", "index"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ADDZ(GEOGRAPHY, NUMERIC)</c>. Returns the geography with the given amount added to every third ordinate.
        /// </summary>
        public static readonly SqlFunction ClrStGeogAddZ =
            Function("CLR_ST_GEOG_ADDZ", nameof(GeographyFunctions.AddZ), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog", "z"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEREPEATEDPOINTS(GEOGRAPHY, NUMERIC)</c>. Returns the geography with coordinates closer together than the tolerance dropped.
        /// </summary>
        public static readonly SqlFunction ClrStGeogRemoveRepeatedPointsWithTolerance =
            Function("CLR_ST_GEOG_REMOVEREPEATEDPOINTS", nameof(GeographyFunctions.RemoveRepeatedPoints), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog", "tolerance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINT(NUMERIC, NUMERIC)</c>. Returns the place at the given longitude and latitude.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPoint =
            Function("CLR_ST_GEOG_POINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Fractional, GeographyOperand.Fractional], ["x", "y"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINT(NUMERIC, NUMERIC, NUMERIC)</c>. Returns the place at the given longitude, latitude and third ordinate.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPoint3D =
            Function("CLR_ST_GEOG_POINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Fractional, GeographyOperand.Fractional, GeographyOperand.Fractional], ["x", "y", "z"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOINT(NUMERIC, NUMERIC)</c>. Returns the place at the given longitude and latitude.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePoint =
            Function("CLR_ST_GEOG_MAKEPOINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Fractional, GeographyOperand.Fractional], ["x", "y"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOINT(NUMERIC, NUMERIC, NUMERIC)</c>. Returns the place at the given longitude, latitude and third ordinate.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePoint3D =
            Function("CLR_ST_GEOG_MAKEPOINT", nameof(GeographyFunctions.Point), GeographyReturnTypes.Geography,
                [GeographyOperand.Fractional, GeographyOperand.Fractional, GeographyOperand.Fractional], ["x", "y", "z"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 2 places.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakeLine2 =
            Function("CLR_ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 3 places.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakeLine3 =
            Function("CLR_ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2", "geog3"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 4 places.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakeLine4 =
            Function("CLR_ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2", "geog3", "geog4"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 5 places.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakeLine5 =
            Function("CLR_ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2", "geog3", "geog4", "geog5"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the line through 6 places.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakeLine6 =
            Function("CLR_ST_GEOG_MAKELINE", nameof(GeographyFunctions.MakeLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2", "geog3", "geog4", "geog5", "geog6"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY)</c>. Returns the polygon with the given shell and no holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon1 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["shell"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and one hole.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon2 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 2 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon3 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 3 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon4 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 4 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon5 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 5 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon6 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 6 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon7 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 7 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon8 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 8 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon9 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6", "hole7"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 9 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon10 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6", "hole7", "hole8"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON(GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY, GEOGRAPHY)</c>. Returns the polygon with the given shell and 10 holes.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakePolygon11 =
            Function("CLR_ST_GEOG_MAKEPOLYGON", nameof(GeographyFunctions.MakePolygon), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry, GeographyOperand.Geometry], ["shell", "hole0", "hole1", "hole2", "hole3", "hole4", "hole5", "hole6", "hole7", "hole8", "hole9"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMTEXT(VARCHAR)</c>. Returns a line read from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLineFromText =
            Function("CLR_ST_GEOG_LINEFROMTEXT", nameof(GeographyFunctions.LineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMTEXT(VARCHAR, INTEGER)</c>. Returns a line read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLineFromTextWithSrid =
            Function("CLR_ST_GEOG_LINEFROMTEXT", nameof(GeographyFunctions.LineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMWKB(VARBINARY)</c>. Returns a line read from WKB.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLineFromWkb =
            Function("CLR_ST_GEOG_LINEFROMWKB", nameof(GeographyFunctions.LineFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMWKB(VARBINARY, INTEGER)</c>. Returns a line read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLineFromWkbWithSrid =
            Function("CLR_ST_GEOG_LINEFROMWKB", nameof(GeographyFunctions.LineFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Integral], ["wkb", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MLINEFROMTEXT(VARCHAR)</c>. Returns a multi-line read from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMLineFromText =
            Function("CLR_ST_GEOG_MLINEFROMTEXT", nameof(GeographyFunctions.MLineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MLINEFROMTEXT(VARCHAR, INTEGER)</c>. Returns a multi-line read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMLineFromTextWithSrid =
            Function("CLR_ST_GEOG_MLINEFROMTEXT", nameof(GeographyFunctions.MLineFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOINTFROMTEXT(VARCHAR)</c>. Returns a multi-point read from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMPointFromText =
            Function("CLR_ST_GEOG_MPOINTFROMTEXT", nameof(GeographyFunctions.MPointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOINTFROMTEXT(VARCHAR, INTEGER)</c>. Returns a multi-point read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMPointFromTextWithSrid =
            Function("CLR_ST_GEOG_MPOINTFROMTEXT", nameof(GeographyFunctions.MPointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOLYFROMTEXT(VARCHAR)</c>. Returns a multi-polygon read from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMPolyFromText =
            Function("CLR_ST_GEOG_MPOLYFROMTEXT", nameof(GeographyFunctions.MPolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOLYFROMTEXT(VARCHAR, INTEGER)</c>. Returns a multi-polygon read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMPolyFromTextWithSrid =
            Function("CLR_ST_GEOG_MPOLYFROMTEXT", nameof(GeographyFunctions.MPolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMTEXT(VARCHAR)</c>. Returns a point read from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPointFromText =
            Function("CLR_ST_GEOG_POINTFROMTEXT", nameof(GeographyFunctions.PointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMTEXT(VARCHAR, INTEGER)</c>. Returns a point read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPointFromTextWithSrid =
            Function("CLR_ST_GEOG_POINTFROMTEXT", nameof(GeographyFunctions.PointFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMWKB(VARBINARY)</c>. Returns a point read from WKB.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPointFromWkb =
            Function("CLR_ST_GEOG_POINTFROMWKB", nameof(GeographyFunctions.PointFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMWKB(VARBINARY, INTEGER)</c>. Returns a point read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPointFromWkbWithSrid =
            Function("CLR_ST_GEOG_POINTFROMWKB", nameof(GeographyFunctions.PointFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Integral], ["wkb", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMTEXT(VARCHAR)</c>. Returns a polygon read from WKT.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPolyFromText =
            Function("CLR_ST_GEOG_POLYFROMTEXT", nameof(GeographyFunctions.PolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMTEXT(VARCHAR, INTEGER)</c>. Returns a polygon read from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPolyFromTextWithSrid =
            Function("CLR_ST_GEOG_POLYFROMTEXT", nameof(GeographyFunctions.PolyFromText), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Integral], ["wkt", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMWKB(VARBINARY)</c>. Returns a polygon read from WKB.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPolyFromWkb =
            Function("CLR_ST_GEOG_POLYFROMWKB", nameof(GeographyFunctions.PolyFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary], ["wkb"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMWKB(VARBINARY, INTEGER)</c>. Returns a polygon read from WKB; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPolyFromWkbWithSrid =
            Function("CLR_ST_GEOG_POLYFROMWKB", nameof(GeographyFunctions.PolyFromWkb), GeographyReturnTypes.Geography,
                [GeographyOperand.Binary, GeographyOperand.Integral], ["wkb", "srid"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_CONTAINS(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether the first geography contains the second.
        /// </summary>
        public static readonly SqlFunction ClrStGeogContains =
            Function("CLR_ST_GEOG_CONTAINS", nameof(GeographyFunctions.Contains), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);



        /// <summary>
        /// <c>CLR_ST_GEOG_COVERS(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether no point of the second geography is outside the first.
        /// </summary>
        public static readonly SqlFunction ClrStGeogCovers =
            Function("CLR_ST_GEOG_COVERS", nameof(GeographyFunctions.Covers), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_COVEREDBY(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether no point of the first geography is outside the second.
        /// </summary>
        public static readonly SqlFunction ClrStGeogCoveredBy =
            Function("CLR_ST_GEOG_COVEREDBY", nameof(GeographyFunctions.CoveredBy), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);



        /// <summary>
        /// <c>CLR_ST_GEOG_DISJOINT(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether two geographies have no point in common.
        /// </summary>
        public static readonly SqlFunction ClrStGeogDisjoint =
            Function("CLR_ST_GEOG_DISJOINT", nameof(GeographyFunctions.Disjoint), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_EQUALS(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether two geographies are the same set of places.
        /// </summary>
        public static readonly SqlFunction ClrStGeogEquals =
            Function("CLR_ST_GEOG_EQUALS", nameof(GeographyFunctions.Equals), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);





        /// <summary>
        /// <c>CLR_ST_GEOG_ENVELOPESINTERSECT(GEOGRAPHY, GEOGRAPHY)</c>. Returns whether the bounding boxes of two geographies meet.
        /// </summary>
        public static readonly SqlFunction ClrStGeogEnvelopesIntersect =
            Function("CLR_ST_GEOG_ENVELOPESINTERSECT", nameof(GeographyFunctions.EnvelopesIntersect), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_AREA(GEOGRAPHY)</c>. Returns the area of the geography in square metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogArea =
            Function("CLR_ST_GEOG_AREA", nameof(GeographyFunctions.Area), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LENGTH(GEOGRAPHY)</c>. Returns the length of the geography in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLength =
            Function("CLR_ST_GEOG_LENGTH", nameof(GeographyFunctions.Length), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_PERIMETER(GEOGRAPHY)</c>. Returns the perimeter of the areal part of the geography in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogPerimeter =
            Function("CLR_ST_GEOG_PERIMETER", nameof(GeographyFunctions.Perimeter), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_OFFSETCURVE(GEOGRAPHY, DOUBLE)</c>. Returns the line drawn a distance in metres to one side of this one.
        /// </summary>
        public static readonly SqlFunction ClrStGeogOffsetCurve =
            Function("CLR_ST_GEOG_OFFSETCURVE", nameof(GeographyFunctions.OffsetCurve), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["line", "distance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEELLIPSE(GEOGRAPHY, DOUBLE, DOUBLE)</c>. Returns an ellipse of the given width and height in metres about a point.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMakeEllipse =
            Function("CLR_ST_GEOG_MAKEELLIPSE", nameof(GeographyFunctions.MakeEllipse), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional, GeographyOperand.Fractional], ["point", "width", "height"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LOCATEALONG(GEOGRAPHY, DOUBLE, DOUBLE)</c>. Returns a point on every segment, a fraction of the way along it and offset sideways by a distance in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLocateAlong =
            Function("CLR_ST_GEOG_LOCATEALONG", nameof(GeographyFunctions.LocateAlong), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional, GeographyOperand.Fractional], ["geog", "fraction", "offset"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MINIMUMDIAMETER(GEOGRAPHY)</c>. Returns the shortest line across the geography's width.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMinimumDiameter =
            Function("CLR_ST_GEOG_MINIMUMDIAMETER", nameof(GeographyFunctions.MinimumDiameter), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_BOUNDINGCIRCLE(GEOGRAPHY)</c>. Returns the smallest circle containing the geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeogBoundingCircle =
            Function("CLR_ST_GEOG_BOUNDINGCIRCLE", nameof(GeographyFunctions.BoundingCircle), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ISSIMPLE(GEOGRAPHY)</c>. Returns whether the geography touches itself nowhere it should not.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIsSimple =
            Function("CLR_ST_GEOG_ISSIMPLE", nameof(GeographyFunctions.IsSimple), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ISRING(GEOGRAPHY)</c>. Returns whether the geography is a line that is closed and simple.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIsRing =
            Function("CLR_ST_GEOG_ISRING", nameof(GeographyFunctions.IsRing), GeographyReturnTypes.Boolean,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_BUFFER(GEOGRAPHY, DOUBLE)</c>. Returns the region within the given distance in metres of the geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeogBuffer =
            Function("CLR_ST_GEOG_BUFFER", nameof(GeographyFunctions.Buffer), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog", "distance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_CENTROID(GEOGRAPHY)</c>. Returns the centre of the geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeogCentroid =
            Function("CLR_ST_GEOG_CENTROID", nameof(GeographyFunctions.Centroid), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_CONVEXHULL(GEOGRAPHY)</c>. Returns the smallest convex geography containing this one.
        /// </summary>
        public static readonly SqlFunction ClrStGeogConvexHull =
            Function("CLR_ST_GEOG_CONVEXHULL", nameof(GeographyFunctions.ConvexHull), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_SIMPLIFY(GEOGRAPHY, DOUBLE)</c>. Returns the geography with vertices removed that move its boundary by no more than the given distance in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogSimplify =
            Function("CLR_ST_GEOG_SIMPLIFY", nameof(GeographyFunctions.Simplify), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog", "tolerance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_INTERSECTION(GEOGRAPHY, GEOGRAPHY)</c>. Returns the area common to two geographies.
        /// </summary>
        public static readonly SqlFunction ClrStGeogIntersection =
            Function("CLR_ST_GEOG_INTERSECTION", nameof(GeographyFunctions.Intersection), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_DIFFERENCE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the part of the first geography that is not in the second.
        /// </summary>
        public static readonly SqlFunction ClrStGeogDifference =
            Function("CLR_ST_GEOG_DIFFERENCE", nameof(GeographyFunctions.Difference), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_SYMDIFFERENCE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the parts of two geographies that are in one and not the other.
        /// </summary>
        public static readonly SqlFunction ClrStGeogSymDifference =
            Function("CLR_ST_GEOG_SYMDIFFERENCE", nameof(GeographyFunctions.SymDifference), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_UNARYUNION(GEOGRAPHY)</c>. Returns the geography with its overlapping parts merged.
        /// </summary>
        public static readonly SqlFunction ClrStGeogUnaryUnion =
            Function("CLR_ST_GEOG_UNARYUNION", nameof(GeographyFunctions.UnaryUnion), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_DENSIFY(GEOGRAPHY, DOUBLE)</c>. Returns the geography with vertices inserted along its geodesics so that no edge is longer than the given distance in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogDensify =
            Function("CLR_ST_GEOG_DENSIFY", nameof(GeographyFunctions.Densify), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog", "tolerance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_PROJECTPOINT(GEOGRAPHY, GEOGRAPHY)</c>. Returns the point of the line nearest the given point.
        /// </summary>
        public static readonly SqlFunction ClrStGeogProjectPoint =
            Function("CLR_ST_GEOG_PROJECTPOINT", nameof(GeographyFunctions.ProjectPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["point", "line"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_ENVELOPE(GEOGRAPHY)</c>. Returns the smallest latitude-longitude rectangle containing the geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeogEnvelope =
            Function("CLR_ST_GEOG_ENVELOPE", nameof(GeographyFunctions.Envelope), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_EXTENT(GEOGRAPHY)</c>. Returns the smallest latitude-longitude rectangle containing the geography.
        /// </summary>
        public static readonly SqlFunction ClrStGeogExtent =
            Function("CLR_ST_GEOG_EXTENT", nameof(GeographyFunctions.Extent), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_EXPAND(GEOGRAPHY, DOUBLE)</c>. Returns the geography's rectangle grown by a distance in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogExpand =
            Function("CLR_ST_GEOG_EXPAND", nameof(GeographyFunctions.Expand), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Fractional], ["geog", "distance"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_CLOSESTCOORDINATE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the coordinate or coordinates of the geography nearest the given point.
        /// </summary>
        public static readonly SqlFunction ClrStGeogClosestCoordinate =
            Function("CLR_ST_GEOG_CLOSESTCOORDINATE", nameof(GeographyFunctions.ClosestCoordinate), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["point", "geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_FURTHESTCOORDINATE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the coordinate or coordinates of the geography furthest from the given point.
        /// </summary>
        public static readonly SqlFunction ClrStGeogFurthestCoordinate =
            Function("CLR_ST_GEOG_FURTHESTCOORDINATE", nameof(GeographyFunctions.FurthestCoordinate), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["point", "geog"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_CLOSESTPOINT(GEOGRAPHY, GEOGRAPHY)</c>. Returns the point of the first geography nearest the second.
        /// </summary>
        public static readonly SqlFunction ClrStGeogClosestPoint =
            Function("CLR_ST_GEOG_CLOSESTPOINT", nameof(GeographyFunctions.ClosestPoint), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_LONGESTLINE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the line between the two coordinates, one from each geography, that are furthest apart.
        /// </summary>
        public static readonly SqlFunction ClrStGeogLongestLine =
            Function("CLR_ST_GEOG_LONGESTLINE", nameof(GeographyFunctions.LongestLine), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

        /// <summary>
        /// <c>CLR_ST_GEOG_MAXDISTANCE(GEOGRAPHY, GEOGRAPHY)</c>. Returns the greatest distance between a coordinate of one geography and a coordinate of the other, in metres.
        /// </summary>
        public static readonly SqlFunction ClrStGeogMaxDistance =
            Function("CLR_ST_GEOG_MAXDISTANCE", nameof(GeographyFunctions.MaxDistance), GeographyReturnTypes.Double,
                [GeographyOperand.Geometry, GeographyOperand.Geometry], ["geog1", "geog2"]);

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

            return new GeographyFunction(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                returnType,
                new GeographyOperandTypeChecker(operands, names),
                ScalarFunctionImpl.create(found),
                IsStrict(name),
                IsSymmetrical(name));
        }

        /// <summary>
        /// Determines whether an operator answers null whenever an argument is null, and only then.
        /// </summary>
        /// <param name="name">The SQL name.</param>
        /// <returns><c>true</c> where the operator is strict.</returns>
        /// <remarks>
        /// <para>This is <c>Strong.Policy.ANY</c>, and Calcite reads it <b>both ways</b>:
        /// <c>RexSimplify.simplifyIsNull</c> rewrites <c>f(a, b) IS NULL</c> to <c>a IS NULL OR b IS NULL</c>,
        /// which asserts that a call over non-null arguments cannot be null. So the list is the operators
        /// whose body wraps a primitive — <c>java.lang.Boolean.valueOf(…)</c>, <c>Double.valueOf(…)</c>,
        /// <c>Integer.valueOf(…)</c> — behind a guard that answers null for a null argument, which is a
        /// mechanical proof of both directions at once.</para>
        ///
        /// <para><b>Most of the surface is not strict and must not say it is.</b> A reader answers null for
        /// text naming a different shape, so <c>CLR_ST_GEOG_POINTFROMTEXT('LINESTRING(0 0, 1 1)')</c> is null
        /// over a non-null argument; so is <c>CLR_ST_GEOG_X</c> of anything but a point, <c>ST_X</c> being
        /// <c>geom instanceof Point ? … : null</c>; so are <c>POINTN</c> and <c>INTERIORRING</c> out of range,
        /// and <c>STARTPOINT</c> of a polygon. A remark on <c>GeographyReturnTypes</c> used to say every body
        /// here answers null exactly when an argument is null, which is why this is written out rather than
        /// derived from the return type.</para>
        /// </remarks>
        static bool IsStrict(string name)
        {
            return name switch
            {
                "CLR_ST_GEOG_AREA" or "CLR_ST_GEOG_LENGTH" or "CLR_ST_GEOG_PERIMETER" => true,
                "CLR_ST_GEOG_DISTANCE" or "CLR_ST_GEOG_MAXDISTANCE" or "CLR_ST_GEOG_DWITHIN" => true,
                "CLR_ST_GEOG_WITHIN" or "CLR_ST_GEOG_CONTAINS" => true,
                "CLR_ST_GEOG_COVERS" or "CLR_ST_GEOG_COVEREDBY" => true,
                "CLR_ST_GEOG_INTERSECTS" or "CLR_ST_GEOG_DISJOINT" => true,
                "CLR_ST_GEOG_EQUALS" or "CLR_ST_GEOG_ORDERINGEQUALS" or "CLR_ST_GEOG_ENVELOPESINTERSECT" => true,
                "CLR_ST_GEOG_ISVALID" or "CLR_ST_GEOG_ISSIMPLE" or "CLR_ST_GEOG_ISRING" => true,
                "CLR_ST_GEOG_ISEMPTY" or "CLR_ST_GEOG_IS3D" or "CLR_ST_GEOG_ISCLOSED" => true,
                "CLR_ST_GEOG_DIMENSION" or "CLR_ST_GEOG_COORDDIM" or "CLR_ST_GEOG_GEOMETRYTYPECODE" or "CLR_ST_GEOG_SRID" => true,
                "CLR_ST_GEOG_NPOINTS" or "CLR_ST_GEOG_NUMPOINTS" or "CLR_ST_GEOG_NUMGEOMETRIES" => true,
                "CLR_ST_GEOG_NUMINTERIORRING" or "CLR_ST_GEOG_NUMINTERIORRINGS" => true,
                _ => false,
            };
        }

        /// <summary>
        /// Determines whether an operator answers the same with its two operands the other way round.
        /// </summary>
        /// <param name="name">The SQL name.</param>
        /// <returns><c>true</c> where the operator is symmetrical.</returns>
        /// <remarks>
        /// <para>What this buys is one digest for two spellings, so <c>CLR_ST_GEOG_DISTANCE(a, b)</c> and
        /// <c>CLR_ST_GEOG_DISTANCE(b, a)</c> in one statement are one expression rather than two.
        /// <c>RexNormalize</c> is where it is read, and only for a call of exactly two operands — which is why
        /// <c>CLR_ST_GEOG_DWITHIN</c>, symmetrical in the two it measures between, is not here.</para>
        ///
        /// <para>Each is symmetrical in <c>S2Geographies</c> by construction rather than by inspection:
        /// <c>Disjoint</c> is <c>Intersects</c> negated, <c>Intersects</c> is <c>Angle</c> which considers
        /// both enclosures, <c>Equals</c> is <c>Covers</c> both ways, and the two distances are over a pair
        /// chosen by extremum.</para>
        /// </remarks>
        static bool IsSymmetrical(string name)
        {
            return name switch
            {
                "CLR_ST_GEOG_DISTANCE" or "CLR_ST_GEOG_MAXDISTANCE" => true,
                "CLR_ST_GEOG_INTERSECTS" or "CLR_ST_GEOG_DISJOINT" => true,
                "CLR_ST_GEOG_EQUALS" or "CLR_ST_GEOG_ENVELOPESINTERSECT" => true,
                _ => false,
            };
        }

        /// <summary>
        /// Determines whether an operator is the named one, whichever route resolved it.
        /// </summary>
        /// <remarks>
        /// <b>By name, and never by identity.</b> A call resolved through <see cref="Schema.GeographySchema"/>
        /// carries a <c>SqlUserDefinedFunction</c> Calcite built around the declaration — same name, different
        /// object — so an identity test recognises a call reached through a chained operator table and
        /// silently fails to recognise the same call reached through a connection.
        /// </remarks>
        /// <param name="op">The operator to test.</param>
        /// <param name="function">The operator it should be.</param>
        /// <returns><c>true</c> where the operator is that one.</returns>
        public static bool Matches(SqlOperator? op, SqlFunction function)
        {
            return op is not null && function is not null && op.getName() == function.getName();
        }

        /// <summary>
        /// Determines whether an operator is one of these, whichever route resolved it.
        /// </summary>
        /// <param name="op">The operator to test.</param>
        /// <returns><c>true</c> where the operator is a geography operator.</returns>
        public static bool IsGeography(SqlOperator? op)
        {
            return op?.getName().StartsWith("CLR_ST_GEO", StringComparison.Ordinal) == true && Lookup(op) is not null;
        }

        /// <summary>
        /// Returns this table's declaration of the given operator, where the two are the same operator.
        /// </summary>
        /// <param name="op">The operator a plan carries.</param>
        /// <returns>The declaration, or <c>null</c> where it is not one of these.</returns>
        /// <remarks>
        /// <para><b>What this is for.</b> The facts <see cref="GeographyFunction"/> declares — strictness,
        /// symmetry — live on the operator object, and a name resolved through
        /// <see cref="Schema.GeographySchema"/> arrives as something Calcite built:
        /// <c>CalciteCatalogReader.toOp</c> takes the bare <c>Function</c> out of the schema and wraps it in a
        /// plain <c>SqlUserDefinedFunction</c>. Putting this table's operator back in its place is what makes
        /// those facts reach a plan that came in that way.</para>
        ///
        /// <para><b>Matched by name and confirmed by the body.</b> The name finds the candidate and the
        /// <c>Function</c> being the same object is what says the two are the same operator — which it is,
        /// because <c>GeographySchema.AddTo</c> registers the very <c>ScalarFunctionImpl</c> this table
        /// built. A host that declared a <c>CLR_ST_GEOG_</c> name over a body of its own is left alone, which
        /// a name test would not do.</para>
        /// </remarks>
        public static SqlFunction? Rebind(SqlOperator? op)
        {
            if (op is not SqlUserDefinedFunction udf)
                return null;

            var mine = Lookup(op);
            if (mine is null)
                return null;

            return ReferenceEquals(((SqlUserDefinedFunction)mine).function, udf.function) ? mine : null;
        }

        /// <summary>
        /// Returns this table's operator of the given one's name and arity.
        /// </summary>
        /// <param name="op"></param>
        /// <returns></returns>
        /// <remarks>
        /// Keyed by name and arity together, because the overloads of one name here differ only by arity —
        /// <c>CLR_ST_GEOG_GEOMFROMTEXT</c> with and without an SRID, <c>CLR_ST_GEOG_MAKELINE</c> five ways —
        /// and a call in a plan has settled on one.
        /// </remarks>
        static SqlFunction? Lookup(SqlOperator op)
        {
            return Instance().index.TryGetValue((op.getName(), op.getOperandCountRange().getMin()), out var found) ? found : null;
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
                GeographyOperand.Geometry => (java.lang.Class)typeof(org.locationtech.jts.geom.Geometry),
                GeographyOperand.Character => (java.lang.Class)typeof(string),
                GeographyOperand.Integral => (java.lang.Class)typeof(java.lang.Integer),
                GeographyOperand.Fractional => (java.lang.Class)typeof(java.lang.Object),
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
        /// Each operator by its name and arity, for <see cref="Lookup"/>.
        /// </summary>
        readonly Dictionary<(string Name, int Arity), SqlFunction> index = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        GeographyOperatorTable()
        {
            operators = SqlOperatorTables.of([
                ClrStGeogGeomFromGeoJson,
                ClrStGeogGeomFromText,
                ClrStGeogGeomFromWkt,
                ClrStGeogGeomFromTextWithSrid,
                ClrStGeogGeomFromWktWithSrid,
                ClrStGeogAsGeom,
                ClrStGeomAsGeog,
                ClrStGeogDistance,
                ClrStGeogDWithin,
                ClrStGeogWithin,
                ClrStGeogIntersects,
                ClrStGeogIsValid,
                ClrStGeogContains,
                ClrStGeogCovers,
                ClrStGeogCoveredBy,
                ClrStGeogDisjoint,
                ClrStGeogEquals,
                ClrStGeogEnvelopesIntersect,
                ClrStGeogArea,
                ClrStGeogLength,
                ClrStGeogPerimeter,
                ClrStGeogMaxDistance,
                ClrStGeogOffsetCurve,
                ClrStGeogMakeEllipse,
                ClrStGeogLocateAlong,
                ClrStGeogMinimumDiameter,
                ClrStGeogBoundingCircle,
                ClrStGeogIsSimple,
                ClrStGeogIsRing,
                ClrStGeogBuffer,
                ClrStGeogCentroid,
                ClrStGeogConvexHull,
                ClrStGeogSimplify,
                ClrStGeogIntersection,
                ClrStGeogDifference,
                ClrStGeogSymDifference,
                ClrStGeogUnaryUnion,
                ClrStGeogDensify,
                ClrStGeogProjectPoint,
                ClrStGeogEnvelope,
                ClrStGeogExtent,
                ClrStGeogExpand,
                ClrStGeogClosestCoordinate,
                ClrStGeogFurthestCoordinate,
                ClrStGeogClosestPoint,
                ClrStGeogLongestLine,
                ClrStGeogX,
                ClrStGeogY,
                ClrStGeogZ,
                ClrStGeogXMin,
                ClrStGeogXMax,
                ClrStGeogYMin,
                ClrStGeogYMax,
                ClrStGeogZMin,
                ClrStGeogZMax,
                ClrStGeogCoordDim,
                ClrStGeogDimension,
                ClrStGeogGeometryType,
                ClrStGeogGeometryTypeCode,
                ClrStGeogNPoints,
                ClrStGeogNumPoints,
                ClrStGeogNumGeometries,
                ClrStGeogNumInteriorRing,
                ClrStGeogNumInteriorRings,
                ClrStGeogStartPoint,
                ClrStGeogEndPoint,
                ClrStGeogExteriorRing,
                ClrStGeogBoundary,
                ClrStGeogHoles,
                ClrStGeogIsEmpty,
                ClrStGeogIs3D,
                ClrStGeogIsClosed,
                ClrStGeogSrid,
                ClrStGeogAsText,
                ClrStGeogAsWkt,
                ClrStGeogAsEwkt,
                ClrStGeogAsGeoJson,
                ClrStGeogAsGml,
                ClrStGeogAsBinary,
                ClrStGeogAsWkb,
                ClrStGeogAsEwkb,
                ClrStGeogPointN,
                ClrStGeogGeometryN,
                ClrStGeogInteriorRing,
                ClrStGeogOrderingEquals,
                ClrStGeogGeomFromEwkt,
                ClrStGeogGeomFromWkb,
                ClrStGeogGeomFromWkbWithSrid,
                ClrStGeogGeomFromEwkb,
                ClrStGeogGeomFromGml,
                ClrStGeogGeomFromGmlWithSrid,
                ClrStGeogFlipCoordinates,
                ClrStGeogForce2D,
                ClrStGeogForce3D,
                ClrStGeogNormalize,
                ClrStGeogRemoveHoles,
                ClrStGeogRemoveRepeatedPoints,
                ClrStGeogReverse,
                ClrStGeogToMultiLine,
                ClrStGeogToMultiPoint,
                ClrStGeogToMultiSegments,
                ClrStGeogAddPoint,
                ClrStGeogAddPointAtIndex,
                ClrStGeogRemovePoint,
                ClrStGeogAddZ,
                ClrStGeogRemoveRepeatedPointsWithTolerance,
                ClrStGeogPoint,
                ClrStGeogPoint3D,
                ClrStGeogMakePoint,
                ClrStGeogMakePoint3D,
                ClrStGeogMakeLine2,
                ClrStGeogMakeLine3,
                ClrStGeogMakeLine4,
                ClrStGeogMakeLine5,
                ClrStGeogMakeLine6,
                ClrStGeogMakePolygon1,
                ClrStGeogMakePolygon2,
                ClrStGeogMakePolygon3,
                ClrStGeogMakePolygon4,
                ClrStGeogMakePolygon5,
                ClrStGeogMakePolygon6,
                ClrStGeogMakePolygon7,
                ClrStGeogMakePolygon8,
                ClrStGeogMakePolygon9,
                ClrStGeogMakePolygon10,
                ClrStGeogMakePolygon11,
                ClrStGeogLineFromText,
                ClrStGeogLineFromTextWithSrid,
                ClrStGeogLineFromWkb,
                ClrStGeogLineFromWkbWithSrid,
                ClrStGeogMLineFromText,
                ClrStGeogMLineFromTextWithSrid,
                ClrStGeogMPointFromText,
                ClrStGeogMPointFromTextWithSrid,
                ClrStGeogMPolyFromText,
                ClrStGeogMPolyFromTextWithSrid,
                ClrStGeogPointFromText,
                ClrStGeogPointFromTextWithSrid,
                ClrStGeogPointFromWkb,
                ClrStGeogPointFromWkbWithSrid,
                ClrStGeogPolyFromText,
                ClrStGeogPolyFromTextWithSrid,
                ClrStGeogPolyFromWkb,
                ClrStGeogPolyFromWkbWithSrid,
            ]);

            var list = operators.getOperatorList();
            for (var i = 0; i < list.size(); i++)
                if (list.get(i) is SqlFunction function)
                    index[(function.getName(), function.getOperandCountRange().getMin())] = function;
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
