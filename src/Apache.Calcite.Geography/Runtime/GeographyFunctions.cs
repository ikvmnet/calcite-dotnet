using System;
using System.Collections.Generic;

using org.apache.calcite.runtime;

// the class and this class's Wgs84 constant, which is the SRID, are two different things with one name;
// inside here the constant wins, so the ellipsoid is reached under a name that says what it is
using Ellipsoid = Apache.Calcite.Geography.Runtime.Wgs84;
using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// The bodies behind the <c>ST_GEOG_*</c> operators.
    /// </summary>
    /// <remarks>
    /// Bound by reflection, the way <c>SpatialTypeFunctions</c> is: <c>GeographyOperatorTable</c> resolves
    /// each of these to a <c>java.lang.reflect.Method</c>, wraps it in a <c>ScalarFunctionImpl</c> and hands
    /// that to a <c>SqlUserDefinedFunction</c>, which is how the call gets an implementor without a hook into
    /// <c>RexImpTable</c> — that table's map is private and 1.42 has no <c>RexImplementorTable</c>.
    ///
    /// <para>Every parameter and every result is a reference type and every method tolerates a null argument.
    /// <c>ScalarFunctionImpl</c> reads a null policy off the method's annotations and answers
    /// <c>NullPolicy.NONE</c> when there are none, so no null check is generated around the call and a null
    /// argument arrives here. A method returning a primitive would throw on one.</para>
    ///
    /// <para>The values are ordinary JTS geometries. That is the whole of what makes a geography free at
    /// runtime, and the whole of why the two readings cannot be told apart once the type is gone.</para>
    /// </remarks>
    public static class GeographyFunctions
    {

        /// <summary>
        /// The reference system a geography is in, always.
        /// </summary>
        /// <remarks>
        /// There is no second one to reproject into, which is why <c>ST_SETSRID</c> and <c>ST_TRANSFORM</c>
        /// have no <c>ST_GEOG_</c> counterpart. Calcite's own constructors leave a geometry on
        /// <c>NO_SRID</c>, which is zero.
        /// </remarks>
        public const int Wgs84 = 4326;

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGEOJSON</c>. Reads a geography from GeoJSON.
        /// </summary>
        /// <param name="geoJson"></param>
        /// <returns></returns>
        public static Geometry? FromGeoJson(string? geoJson)
        {
            if (geoJson is null)
                return null;

            return Wgs84Of(SpatialTypeUtils.fromGeoJson(geoJson));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMTEXT</c> and <c>ST_GEOG_GEOMFROMWKT</c>. Reads a geography from WKT.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? FromWkt(string? wkt)
        {
            if (wkt is null)
                return null;

            return Wgs84Of(SpatialTypeUtils.fromWkt(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMTEXT</c> and <c>ST_GEOG_GEOMFROMWKT</c>, with the SRID Calcite lets a caller
        /// name. Reads a geography from WKT.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        /// <remarks>
        /// The arity is Calcite's, and it is declared so that the mirror is complete rather than because
        /// there is a choice to make: a geography is WGS84 and there is no second reference system to be in,
        /// which is the same reason <c>ST_SETSRID</c> and <c>ST_TRANSFORM</c> have no counterpart at all.
        /// Anything but 4326 is refused rather than ignored — a caller who names one is asking for a
        /// reprojection that will not happen, and silence would hand them coordinates read as something they
        /// are not.
        /// </remarks>
        public static Geometry? FromWkt(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromWkt(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_ASGEOM</c>. Reads a geography as a geometry.
        /// </summary>
        /// <param name="geography"></param>
        /// <returns></returns>
        /// <remarks>
        /// Nothing happens. The two types are carried by the same class, so the crossing is a re-typing and
        /// the object goes through untouched — deliberately not copied, and deliberately not restamped with
        /// an SRID, since the caller's object is not this function's to change. What it costs is the geodesic
        /// reading: from here on the coordinates are a plane's and Calcite's <c>ST_*</c> will take them.
        /// </remarks>
        public static Geometry? AsGeometry(Geometry? geography)
        {
            return geography;
        }

        /// <summary>
        /// <c>ST_GEOM_ASGEOG</c>. Reads a geometry as a geography.
        /// </summary>
        /// <param name="geometry"></param>
        /// <returns></returns>
        /// <remarks>
        /// The other half of <see cref="AsGeometry"/>, and the assertion that the coordinates are WGS84. It
        /// is an assertion and not a conversion: nothing checks, and nothing can, a geometry carrying no
        /// record of what its coordinates mean.
        /// </remarks>
        public static Geometry? AsGeography(Geometry? geometry)
        {
            return geometry;
        }

        /// <summary>
        /// <c>ST_GEOG_DISTANCE</c>. The distance between two geographies, in metres.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Double? Distance(Geometry? a, Geometry? b)
        {
            if (a is null || b is null)
                return null;

            return java.lang.Double.valueOf(S2Geographies.Distance(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_DWITHIN</c>. Whether two geographies are within the given distance in metres.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <param name="distance"></param>
        /// <returns></returns>
        /// <remarks>
        /// The distance is a <c>Number</c> rather than a <c>Double</c> because the argument arrives as
        /// whatever type the literal had. <c>2.0</c> is <c>DECIMAL(2, 1)</c> and reaches the call as a
        /// <c>BigDecimal</c>: nothing converts it on the way, since
        /// <c>ReflectiveCallNotNullImplementor</c> runs <c>EnumUtils.convertAssignableTypes</c>, which
        /// converts <em>to</em> a decimal and not from one. Calcite's own <c>ST_DWITHIN</c> takes a
        /// <c>double</c> and fails on the same literal — measured, both under Janino — so this is a
        /// divergence and a deliberate one: a caller should not have to write
        /// <c>CAST(2.0 AS DOUBLE)</c> to call a function that takes a distance.
        /// </remarks>
        public static java.lang.Boolean? DWithin(Geometry? a, Geometry? b, java.lang.Object? distance)
        {
            if (a is null || b is null || distance is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.DWithin(S2Geographies.Of(a), S2Geographies.Of(b), Double(distance)));
        }

        /// <summary>
        /// <c>ST_GEOG_WITHIN</c>. Whether the first geography lies within the second.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Within(Geometry? a, Geometry? b)
        {
            if (a is null || b is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.Within(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_INTERSECTS</c>. Whether two geographies have any point in common.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Intersects(Geometry? a, Geometry? b)
        {
            if (a is null || b is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.Intersects(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_CONTAINS</c>. Returns whether the first geography contains the second.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Contains(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Contains(S2Geographies.Of(a), S2Geographies.Of(b)));
        }



        /// <summary>
        /// <c>ST_GEOG_COVERS</c>. Returns whether no point of the second geography is outside the first.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Covers(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Covers(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_COVEREDBY</c>. Returns whether no point of the first geography is outside the second.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? CoveredBy(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.CoveredBy(S2Geographies.Of(a), S2Geographies.Of(b)));
        }



        /// <summary>
        /// <c>ST_GEOG_DISJOINT</c>. Returns whether two geographies have no point in common.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Disjoint(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Disjoint(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_EQUALS</c>. Returns whether two geographies are the same set of places.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Equals(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Equals(S2Geographies.Of(a), S2Geographies.Of(b)));
        }





        /// <summary>
        /// <c>ST_GEOG_ENVELOPESINTERSECT</c>. Returns whether the bounding boxes of two geographies meet.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Boolean? EnvelopesIntersect(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.EnvelopesIntersect(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_AREA</c>. Returns the area of the geography in square metres.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? Area(Geometry? g)
        {
            return g is null ? null : java.lang.Double.valueOf(S2Geographies.Area(S2Geographies.Of(g)));
        }

        /// <summary>
        /// <c>ST_GEOG_LENGTH</c>. Returns the length of the geography in metres.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? Length(Geometry? g)
        {
            return g is null ? null : java.lang.Double.valueOf(S2Geographies.Length(S2Geographies.Of(g)));
        }

        /// <summary>
        /// <c>ST_GEOG_PERIMETER</c>. Returns the perimeter of the areal part of the geography in metres.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? Perimeter(Geometry? g)
        {
            return g is null ? null : java.lang.Double.valueOf(S2Geographies.Perimeter(S2Geographies.Of(g)));
        }

        /// <summary>
        /// <c>ST_GEOG_MAXDISTANCE</c>. Returns the greatest distance between a coordinate of one geography and a coordinate of the other, in metres.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static java.lang.Double? MaxDistance(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Double.valueOf(S2Geographies.MaxDistance(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>ST_GEOG_ISVALID</c>. Whether the geography is valid on the sphere.
        /// </summary>
        /// <param name="geography"></param>
        /// <returns></returns>
        public static java.lang.Boolean? IsValid(Geometry? geography)
        {
            if (geography is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.IsValid(geography));
        }

        /// <summary>
        /// <c>ST_GEOG_X</c>. Returns the longitude of a point.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? X(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_X(g);
        }

        /// <summary>
        /// <c>ST_GEOG_Y</c>. Returns the latitude of a point.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? Y(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Y(g);
        }

        /// <summary>
        /// <c>ST_GEOG_Z</c>. Returns the third ordinate of a point.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? Z(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Z(g);
        }

        /// <summary>
        /// <c>ST_GEOG_XMIN</c>. Returns the least longitude.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? XMin(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_XMin(g);
        }

        /// <summary>
        /// <c>ST_GEOG_XMAX</c>. Returns the greatest longitude.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? XMax(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_XMax(g);
        }

        /// <summary>
        /// <c>ST_GEOG_YMIN</c>. Returns the least latitude.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? YMin(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_YMin(g);
        }

        /// <summary>
        /// <c>ST_GEOG_YMAX</c>. Returns the greatest latitude.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? YMax(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_YMax(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ZMIN</c>. Returns the least third ordinate.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? ZMin(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_ZMin(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ZMAX</c>. Returns the greatest third ordinate.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Double? ZMax(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_ZMax(g);
        }

        /// <summary>
        /// <c>ST_GEOG_COORDDIM</c>. Returns how many ordinates a coordinate carries.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? CoordDim(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_CoordDim(g));
        }

        /// <summary>
        /// <c>ST_GEOG_DIMENSION</c>. Returns the dimension: 0, 1 or 2.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? Dimension(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_Dimension(g));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYTYPE</c>. Returns the name of the kind of shape.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static string? GeometryType(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_GeometryType(g);
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYTYPECODE</c>. Returns the number of the kind of shape.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? GeometryTypeCode(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_GeometryTypeCode(g));
        }

        /// <summary>
        /// <c>ST_GEOG_NPOINTS</c>. Returns how many coordinates the shape names.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? NPoints(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NPoints(g));
        }

        /// <summary>
        /// <c>ST_GEOG_NUMPOINTS</c>. Returns how many coordinates a line names.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? NumPoints(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumPoints(g));
        }

        /// <summary>
        /// <c>ST_GEOG_NUMGEOMETRIES</c>. Returns how many parts the shape has.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? NumGeometries(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumGeometries(g));
        }

        /// <summary>
        /// <c>ST_GEOG_NUMINTERIORRING</c>. Returns how many holes a polygon has.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? NumInteriorRing(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumInteriorRing(g));
        }

        /// <summary>
        /// <c>ST_GEOG_NUMINTERIORRINGS</c>. Returns how many holes a polygon has, under Calcite's other spelling.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? NumInteriorRings(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumInteriorRings(g));
        }

        /// <summary>
        /// <c>ST_GEOG_STARTPOINT</c>. Returns the first coordinate of a line.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? StartPoint(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_StartPoint(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ENDPOINT</c>. Returns the last coordinate of a line.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? EndPoint(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_EndPoint(g);
        }

        /// <summary>
        /// <c>ST_GEOG_EXTERIORRING</c>. Returns the shell of a polygon.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? ExteriorRing(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_ExteriorRing(g);
        }

        /// <summary>
        /// <c>ST_GEOG_BOUNDARY</c>. Returns the boundary of the shape.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? Boundary(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Boundary(g);
        }

        /// <summary>
        /// <c>ST_GEOG_HOLES</c>. Returns the holes of a polygon.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? Holes(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Holes(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ISEMPTY</c>. Returns whether the shape names nothing.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Boolean? IsEmpty(Geometry? g)
        {
            return g is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_IsEmpty(g));
        }

        /// <summary>
        /// <c>ST_GEOG_IS3D</c>. Returns whether the coordinates carry a third ordinate.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Boolean? Is3D(Geometry? g)
        {
            return g is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_Is3D(g));
        }

        /// <summary>
        /// <c>ST_GEOG_ISCLOSED</c>. Returns whether a line ends where it began.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Boolean? IsClosed(Geometry? g)
        {
            return g is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_IsClosed(g));
        }

        /// <summary>
        /// <c>ST_GEOG_SRID</c>. Returns the reference system the coordinates are stamped with.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static java.lang.Integer? Srid(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_SRID(g));
        }

        /// <summary>
        /// <c>ST_GEOG_ASTEXT</c>. Writes the geography as WKT.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static string? AsText(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsText(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASWKT</c>. Writes the geography as WKT, under Calcite's other spelling.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static string? AsWkt(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsWKT(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASEWKT</c>. Writes the geography as EWKT, which carries the SRID.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static string? AsEwkt(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsEWKT(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASGEOJSON</c>. Writes the geography as GeoJSON.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static string? AsGeoJson(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsGeoJSON(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASGML</c>. Writes the geography as GML.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static string? AsGml(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsGML(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASBINARY</c>. Writes the geography as WKB.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static org.apache.calcite.avatica.util.ByteString? AsBinary(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsBinary(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASWKB</c>. Writes the geography as WKB, under Calcite's other spelling.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static org.apache.calcite.avatica.util.ByteString? AsWkb(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsWKB(g);
        }

        /// <summary>
        /// <c>ST_GEOG_ASEWKB</c>. Writes the geography as EWKB.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static org.apache.calcite.avatica.util.ByteString? AsEwkb(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsEWKB(g);
        }

        /// <summary>
        /// <c>ST_GEOG_POINTN</c>. Returns the <c>n</c>th coordinate of a line, counting from one.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        public static Geometry? PointN(Geometry? g, java.lang.Integer? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_PointN(g, n.intValue());
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYN</c>. Returns the <c>n</c>th part of the geography, counting from one.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        public static Geometry? GeometryN(Geometry? g, java.lang.Integer? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_GeometryN(g, n.intValue());
        }

        /// <summary>
        /// <c>ST_GEOG_INTERIORRING</c>. Returns the <c>n</c>th hole of a polygon, counting from one.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        public static Geometry? InteriorRing(Geometry? g, java.lang.Integer? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_InteriorRing(g, n.intValue());
        }

        /// <summary>
        /// <c>ST_GEOG_ORDERINGEQUALS</c>. Whether two geographies name the same coordinates in the same
        /// order.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        /// <remarks>
        /// The one comparison in this package that reads the coordinates as a list rather than as places, so
        /// it means the same thing on a sphere as it does on a plane.
        /// </remarks>
        public static java.lang.Boolean? OrderingEquals(Geometry? a, Geometry? b)
        {
            return a is null || b is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_OrderingEquals(a, b));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMEWKT</c>. Reads a geography from EWKT.
        /// </summary>
        /// <param name="ewkt"></param>
        /// <returns></returns>
        /// <remarks>
        /// EWKT carries its own SRID, so unlike the plain WKT form there is nothing for a caller to pass and
        /// the text itself can name a reference system a geography cannot be in. It is refused there for the
        /// same reason it is refused as an argument.
        /// </remarks>
        public static Geometry? FromEwkt(string? ewkt)
        {
            return ewkt is null ? null : Wgs84Of(Stamped(SpatialTypeFunctions.ST_GeomFromEWKT(ewkt)));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKB</c>. Reads a geography from WKB.
        /// </summary>
        /// <param name="wkb"></param>
        /// <returns></returns>
        public static Geometry? FromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_GeomFromWKB(wkb));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKB</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkb"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? FromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromWkb(wkb);
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMEWKB</c>. Reads a geography from EWKB.
        /// </summary>
        /// <param name="ewkb"></param>
        /// <returns></returns>
        public static Geometry? FromEwkb(org.apache.calcite.avatica.util.ByteString? ewkb)
        {
            return ewkb is null ? null : Wgs84Of(Stamped(SpatialTypeFunctions.ST_GeomFromEWKB(ewkb)));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGML</c>. Reads a geography from GML.
        /// </summary>
        /// <param name="gml"></param>
        /// <returns></returns>
        public static Geometry? FromGml(string? gml)
        {
            return gml is null ? null : Wgs84Of(SpatialTypeFunctions.ST_GeomFromGML(gml));
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGML</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="gml"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? FromGml(string? gml, java.lang.Integer? srid)
        {
            if (gml is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromGml(gml);
        }

        /// <summary>
        /// <c>ST_GEOG_FLIPCOORDINATES</c>. Returns the geography with longitude and latitude swapped.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? FlipCoordinates(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_FlipCoordinates(g));
        }

        /// <summary>
        /// <c>ST_GEOG_FORCE2D</c>. Returns the geography with any third ordinate dropped.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? Force2D(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Force2D(g));
        }

        /// <summary>
        /// <c>ST_GEOG_FORCE3D</c>. Returns the geography with a third ordinate on every coordinate.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? Force3D(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Force3D(g));
        }

        /// <summary>
        /// <c>ST_GEOG_NORMALIZE</c>. Returns the geography in its canonical form.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? Normalize(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Normalize(g));
        }

        /// <summary>
        /// <c>ST_GEOG_REMOVEHOLES</c>. Returns the geography with the holes taken out of its polygons.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? RemoveHoles(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_RemoveHoles(g));
        }

        /// <summary>
        /// <c>ST_GEOG_REMOVEREPEATEDPOINTS</c>. Returns the geography with repeated coordinates dropped.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? RemoveRepeatedPoints(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_RemoveRepeatedPoints(g));
        }

        /// <summary>
        /// <c>ST_GEOG_REVERSE</c>. Returns the geography with its coordinates in the opposite order.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? Reverse(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Reverse(g));
        }

        /// <summary>
        /// <c>ST_GEOG_TOMULTILINE</c>. Returns the lines of the geography as a multi-line.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? ToMultiLine(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_ToMultiLine(g));
        }

        /// <summary>
        /// <c>ST_GEOG_TOMULTIPOINT</c>. Returns the coordinates of the geography as a multi-point.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? ToMultiPoint(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_ToMultiPoint(g));
        }

        /// <summary>
        /// <c>ST_GEOG_TOMULTISEGMENTS</c>. Returns the edges of the geography as a multi-line.
        /// </summary>
        /// <param name="g"></param>
        /// <returns></returns>
        public static Geometry? ToMultiSegments(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_ToMultiSegments(g));
        }

        /// <summary>
        /// <c>ST_GEOG_ADDPOINT</c>. Returns the line with the coordinate added at its end.
        /// </summary>
        /// <param name="line"></param>
        /// <param name="point"></param>
        /// <returns></returns>
        public static Geometry? AddPoint(Geometry? line, Geometry? point)
        {
            return line is null || point is null ? null : Wgs84Of(SpatialTypeFunctions.ST_AddPoint(line, point));
        }

        /// <summary>
        /// <c>ST_GEOG_ADDPOINT</c>. Returns the line with the coordinate added at the given index.
        /// </summary>
        /// <param name="line"></param>
        /// <param name="point"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static Geometry? AddPoint(Geometry? line, Geometry? point, java.lang.Integer? index)
        {
            return line is null || point is null || index is null
                ? null
                : Wgs84Of(SpatialTypeFunctions.ST_AddPoint(line, point, index.intValue()));
        }

        /// <summary>
        /// <c>ST_GEOG_REMOVEPOINT</c>. Returns the line with the coordinate at the given index taken out.
        /// </summary>
        /// <param name="line"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static Geometry? RemovePoint(Geometry? line, java.lang.Integer? index)
        {
            return line is null || index is null ? null : Wgs84Of(SpatialTypeFunctions.ST_RemovePoint(line, index.intValue()));
        }

        /// <summary>
        /// <c>ST_GEOG_ADDZ</c>. Returns the geography with the given amount added to every third ordinate.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="z"></param>
        /// <returns></returns>
        public static Geometry? AddZ(Geometry? g, java.lang.Object? z)
        {
            return g is null || z is null ? null : Wgs84Of(SpatialTypeFunctions.ST_AddZ(g, Decimal(z)));
        }

        /// <summary>
        /// <c>ST_GEOG_REMOVEREPEATEDPOINTS</c>. Returns the geography with coordinates closer together than
        /// the given tolerance dropped.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="tolerance"></param>
        /// <returns></returns>
        /// <remarks>
        /// The tolerance is in the units of the coordinates and not in metres, because what this does is drop
        /// coordinates rather than measure between places. Calcite's is the same number.
        /// </remarks>
        public static Geometry? RemoveRepeatedPoints(Geometry? g, java.lang.Object? tolerance)
        {
            return g is null || tolerance is null
                ? null
                : Wgs84Of(SpatialTypeFunctions.ST_RemoveRepeatedPoints(g, Decimal(tolerance)));
        }

        /// <summary>
        /// Reads a number as the decimal Calcite's own signature asks for.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        /// <remarks>
        /// The parameter is <c>Object</c> rather than <c>Number</c> for a reason that is not about this
        /// method: a schema function's parameter type is derived from the declared class, and a fractional
        /// operand has to be declared as one that accepts whatever a SQL literal arrives as.
        /// <c>200000.0</c> is a <c>DECIMAL</c> and reaches the body as a <c>BigDecimal</c>; <c>200000</c> is
        /// an <c>INTEGER</c> and reaches it as an <c>Integer</c>. Declaring <c>Double</c> makes routine
        /// resolution succeed on the assignment rules and then the generated call fail to compile, since
        /// nothing inserted the cast the rules implied. <c>Object</c> is the one declaration that always
        /// compiles, and <c>CalciteCatalogReader.toSql</c> reads it as <c>ANY</c>, which has assignment rules.
        /// </remarks>
        static java.math.BigDecimal Decimal(java.lang.Object number)
        {
            return number as java.math.BigDecimal ?? java.math.BigDecimal.valueOf(Double(number));
        }

        /// <summary>
        /// Reads a number as a <see cref="double"/>.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        /// <inheritdoc cref="Decimal" />
        static double Double(java.lang.Object number)
        {
            return ((java.lang.Number)number).doubleValue();
        }

        /// <summary>
        /// <c>ST_GEOG_POINT</c> and <c>ST_GEOG_MAKEPOINT</c>. Returns the place at the given longitude and
        /// latitude.
        /// </summary>
        /// <param name="x">The longitude.</param>
        /// <param name="y">The latitude.</param>
        /// <returns></returns>
        public static Geometry? Point(java.lang.Object? x, java.lang.Object? y)
        {
            return x is null || y is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Point(Decimal(x), Decimal(y)));
        }

        /// <summary>
        /// <c>ST_GEOG_POINT</c> and <c>ST_GEOG_MAKEPOINT</c>, with a third ordinate.
        /// </summary>
        /// <param name="x">The longitude.</param>
        /// <param name="y">The latitude.</param>
        /// <param name="z"></param>
        /// <returns></returns>
        public static Geometry? Point(java.lang.Object? x, java.lang.Object? y, java.lang.Object? z)
        {
            return x is null || y is null || z is null
                ? null
                : Wgs84Of(SpatialTypeFunctions.ST_Point(Decimal(x), Decimal(y), Decimal(z)));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKELINE</c>. Returns the line through 2 places.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2)
        {
            return g1 is null || g2 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKELINE</c>. Returns the line through 3 places.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3)
        {
            return g1 is null || g2 is null || g3 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKELINE</c>. Returns the line through 4 places.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3, Geometry? g4)
        {
            return g1 is null || g2 is null || g3 is null || g4 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3, g4));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKELINE</c>. Returns the line through 5 places.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3, Geometry? g4, Geometry? g5)
        {
            return g1 is null || g2 is null || g3 is null || g4 is null || g5 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3, g4, g5));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKELINE</c>. Returns the line through 6 places.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3, Geometry? g4, Geometry? g5, Geometry? g6)
        {
            return g1 is null || g2 is null || g3 is null || g4 is null || g5 is null || g6 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3, g4, g5, g6));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and no holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell)
        {
            return shell is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and one hole.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0)
        {
            return shell is null || hole0 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 2 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1)
        {
            return shell is null || hole0 is null || hole1 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 3 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 4 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 5 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 6 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 7 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 8 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6, Geometry? hole7)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null || hole7 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6, hole7));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 9 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6, Geometry? hole7, Geometry? hole8)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null || hole7 is null || hole8 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8));
        }

        /// <summary>
        /// <c>ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 10 holes.
        /// </summary>
        /// <returns></returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6, Geometry? hole7, Geometry? hole8, Geometry? hole9)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null || hole7 is null || hole8 is null || hole9 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8, hole9));
        }

        /// <summary>
        /// <c>ST_GEOG_LINEFROMTEXT</c>. Returns a line read from WKT, or null if the text does not name one.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? LineFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_LineFromText(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_LINEFROMTEXT</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? LineFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return LineFromText(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_LINEFROMWKB</c>. Returns a line read from WKB, or null if the text does not name one.
        /// </summary>
        /// <param name="wkb"></param>
        /// <returns></returns>
        public static Geometry? LineFromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_LineFromWKB(wkb));
        }

        /// <summary>
        /// <c>ST_GEOG_LINEFROMWKB</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkb"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? LineFromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return LineFromWkb(wkb);
        }

        /// <summary>
        /// <c>ST_GEOG_MLINEFROMTEXT</c>. Returns a multi-line read from WKT, or null if the text does not name one.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? MLineFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MLineFromText(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_MLINEFROMTEXT</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? MLineFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return MLineFromText(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_MPOINTFROMTEXT</c>. Returns a multi-point read from WKT, or null if the text does not name one.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? MPointFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MPointFromText(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_MPOINTFROMTEXT</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? MPointFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return MPointFromText(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_MPOLYFROMTEXT</c>. Returns a multi-polygon read from WKT, or null if the text does not name one.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? MPolyFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MPolyFromText(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_MPOLYFROMTEXT</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? MPolyFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return MPolyFromText(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_POINTFROMTEXT</c>. Returns a point read from WKT, or null if the text does not name one.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? PointFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PointFromText(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_POINTFROMTEXT</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? PointFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PointFromText(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_POINTFROMWKB</c>. Returns a point read from WKB, or null if the text does not name one.
        /// </summary>
        /// <param name="wkb"></param>
        /// <returns></returns>
        public static Geometry? PointFromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PointFromWKB(wkb));
        }

        /// <summary>
        /// <c>ST_GEOG_POINTFROMWKB</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkb"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? PointFromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PointFromWkb(wkb);
        }

        /// <summary>
        /// <c>ST_GEOG_POLYFROMTEXT</c>. Returns a polygon read from WKT, or null if the text does not name one.
        /// </summary>
        /// <param name="wkt"></param>
        /// <returns></returns>
        public static Geometry? PolyFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PolyFromText(wkt));
        }

        /// <summary>
        /// <c>ST_GEOG_POLYFROMTEXT</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkt"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? PolyFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PolyFromText(wkt);
        }

        /// <summary>
        /// <c>ST_GEOG_POLYFROMWKB</c>. Returns a polygon read from WKB, or null if the text does not name one.
        /// </summary>
        /// <param name="wkb"></param>
        /// <returns></returns>
        public static Geometry? PolyFromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PolyFromWKB(wkb));
        }

        /// <summary>
        /// <c>ST_GEOG_POLYFROMWKB</c>, with the SRID Calcite lets a caller name.
        /// </summary>
        /// <param name="wkb"></param>
        /// <param name="srid"></param>
        /// <returns></returns>
        public static Geometry? PolyFromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PolyFromWkb(wkb);
        }

        /// <summary>
        /// Refuses an SRID a geography cannot be in.
        /// </summary>
        /// <param name="srid"></param>
        static void RequireWgs84(int srid)
        {
            if (srid != Wgs84)
                throw new java.lang.IllegalArgumentException($"A geography is WGS84; SRID {srid} is not a reference system it can be in.");
        }

        /// <summary>
        /// <c>ST_GEOG_BOUNDINGCIRCLE</c>. Returns the smallest circle containing the geography.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// A circle on the Earth rather than on a map, which is a different shape and a different centre. A
        /// planar smallest circle measures its radius in degrees, so the circle it draws is an ellipse on the
        /// ground everywhere off the equator, and the centre it picks is the one that minimises a distance
        /// nobody travels.
        ///
        /// <para>The centre is found by walking toward whichever vertex is furthest, in steps that shrink as
        /// the walk goes on. That converges on the point whose greatest distance to the shape is least, and
        /// it converges from any start; what it does not do is arrive exactly. So the radius is taken
        /// afterwards as the true greatest distance from the centre it settled on, which makes containment
        /// exact and minimality approximate — the circle certainly holds the shape, and may be a fraction of
        /// a percent wider than the smallest one that would.</para>
        ///
        /// <para>Only the vertices are walked, which is enough: a cap is convex and a geodesic between two
        /// points inside one stays inside it, so a circle holding every vertex holds every edge.</para>
        ///
        /// <para>The ring is drawn a little wide — by <c>1 / cos(π / sides)</c> — because it is a polygon of
        /// thirty-two sides rather than a circle, and an inscribed polygon would cut inside the radius
        /// between its vertices and leave the shape sticking out.</para>
        /// </remarks>
        public static Geometry? BoundingCircle(Geometry? geog)
        {
            if (geog is null)
                return null;

            var vertices = geog.getCoordinates();
            if (vertices.Length == 0)
                return Wgs84Of(Factory.createPolygon());

            var centre = Centre(vertices);
            var radius = 0.0;

            foreach (var vertex in vertices)
                radius = System.Math.Max(radius, Ellipsoid.Distance(centre, vertex));

            if (radius == 0)
                return Wgs84Of(Factory.createPoint(centre));

            return Wgs84Of(Areal(Circle(centre, radius / System.Math.Cos(System.Math.PI / CircleSides))));
        }

        /// <summary>
        /// The place whose greatest distance to any of the given coordinates is least, near enough.
        /// </summary>
        /// <param name="vertices"></param>
        /// <returns></returns>
        /// <remarks>
        /// Steps of <c>d / (i + 1)</c> toward the furthest vertex, which is the shrinking-step walk that
        /// converges on the one-centre from any start. The count is what decides how close it gets; a
        /// thousand puts it within a small fraction of a percent, and the radius is measured afterwards so
        /// that being short of the true centre widens the circle rather than letting anything escape it.
        /// </remarks>
        static org.locationtech.jts.geom.Coordinate Centre(org.locationtech.jts.geom.Coordinate[] vertices)
        {
            const int steps = 1000;

            var centre = vertices[0];

            for (var i = 1; i <= steps; i++)
            {
                var furthest = vertices[0];
                var distance = 0.0;

                foreach (var vertex in vertices)
                {
                    var candidate = Ellipsoid.Distance(centre, vertex);

                    if (candidate > distance)
                    {
                        distance = candidate;
                        furthest = vertex;
                    }
                }

                if (distance == 0)
                    break;

                centre = Ellipsoid.Offset(centre, Ellipsoid.Azimuth(centre, furthest), distance / (i + 1));
            }

            return centre;
        }

        /// <summary>
        /// <c>ST_GEOG_ISSIMPLE</c>. Returns whether the geography touches itself nowhere it should not.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// The edges are geodesics, which is what makes this a different question from Calcite's. Two edges a
        /// planar reading draws as straight lines in degrees may cross on the Earth and not on the map,
        /// because a geodesic between two points on a parallel bows poleward and can reach over a line drawn
        /// north of it.
        /// </remarks>
        public static java.lang.Boolean? IsSimple(Geometry? geog)
        {
            return geog is null ? null : java.lang.Boolean.valueOf(S2Geographies.IsSimple(geog));
        }

        /// <summary>
        /// <c>ST_GEOG_ISRING</c>. Returns whether the geography is a line that is closed and simple.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <inheritdoc cref="IsSimple" />
        public static java.lang.Boolean? IsRing(Geometry? geog)
        {
            return geog is null ? null : java.lang.Boolean.valueOf(S2Geographies.IsRing(geog));
        }

        /// <summary>
        /// <c>ST_GEOG_BUFFER</c>. Returns the region within the given distance in metres of the geography.
        /// </summary>
        /// <param name="geog"></param>
        /// <param name="distance"></param>
        /// <returns></returns>
        /// <remarks>
        /// The one operation here that S2 does not have. Its Java release has no buffer, so this is built
        /// rather than called, and what it is built from is the definition: the set of places within the
        /// distance of any part of the shape. Every vertex contributes a ring of points at exactly that
        /// distance, traced with <c>Wgs84.Offset</c> so the ring is the true geodesic circle rather than a
        /// circle of constant angular radius; the rings are unioned, and an areal shape is unioned with its
        /// own interior so the buffer grows outward rather than only skinning the boundary.
        ///
        /// <para>An edge is longer than the gaps between the rings its two ends make, so the edges are
        /// divided first — every part of the boundary gets a ring within a quarter of the distance of it.
        /// That is what bounds the error: the result is contained in the true buffer and contains everything
        /// more than a small fraction of the distance inside it, and the fraction falls as the division
        /// tightens.</para>
        ///
        /// <para>An inscribed polygon is used for each ring, as JTS uses one, so the answer is a little
        /// inside the true circle rather than straddling it. Thirty-two sides, which is what JTS's default of
        /// eight per quadrant comes to.</para>
        ///
        /// <para>The work is bounded rather than unbounded: a shape with a great many vertices, or one
        /// enormous beside the distance, would otherwise trace millions of rings. Past a limit the division
        /// coarsens instead, which loses accuracy and keeps the answer finite, and is the honest trade for an
        /// operation with no exact form.</para>
        /// </remarks>
        public static Geometry? Buffer(Geometry? geog, java.lang.Object? distance)
        {
            if (geog is null || distance is null)
                return null;

            var metres = Double(distance);
            if (metres <= 0 || geog.isEmpty())
                return Wgs84Of(Factory.createPolygon());

            var seeds = Seeds(geog, metres);
            if (seeds.Count == 0)
                return Wgs84Of(Factory.createPolygon());

            var union = S2Geographies.Of(geog).Polygon ?? new com.google.common.geometry.S2Polygon();

            foreach (var seed in seeds)
            {
                var ring = Circle(seed, metres);
                var merged = new com.google.common.geometry.S2Polygon();

                merged.initToUnion(union, ring);
                union = merged;
            }

            return Wgs84Of(Areal(union));
        }

        /// <summary>
        /// The places a ring is drawn around, which is every vertex plus enough of every edge.
        /// </summary>
        /// <param name="geog"></param>
        /// <param name="metres"></param>
        /// <returns></returns>
        static List<org.locationtech.jts.geom.Coordinate> Seeds(Geometry geog, double metres)
        {
            var coordinates = geog.getCoordinates();
            var seeds = new List<org.locationtech.jts.geom.Coordinate>();

            if (coordinates.Length == 0)
                return seeds;

            // a quarter of the distance keeps the gap between neighbouring rings small beside their radius;
            // the limit is what keeps a large shape from tracing more rings than anyone wants to wait for
            const int most = 512;

            var step = metres / 4;
            var length = Ellipsoid.Length(S2Geographies.Of(geog).Edges);

            if (length / step > most)
                step = length / most;

            seeds.Add(coordinates[0]);

            for (var i = 0; i < coordinates.Length - 1; i++)
            {
                foreach (var between in Ellipsoid.Divide(coordinates[i], coordinates[i + 1], step))
                    seeds.Add(between);

                seeds.Add(coordinates[i + 1]);
            }

            return seeds;
        }

        /// <summary>
        /// How many sides a circle is drawn with, which is what JTS's default of eight per quadrant comes to.
        /// </summary>
        const int CircleSides = 32;

        /// <summary>
        /// The ring of places at exactly the given distance from one coordinate.
        /// </summary>
        /// <param name="centre"></param>
        /// <param name="metres"></param>
        /// <returns></returns>
        static com.google.common.geometry.S2Polygon Circle(org.locationtech.jts.geom.Coordinate centre, double metres)
        {
            const int sides = CircleSides;

            var vertices = new java.util.ArrayList();

            for (var i = 0; i < sides; i++)
            {
                var point = Ellipsoid.Offset(centre, i * 360.0 / sides, metres);

                vertices.add(com.google.common.geometry.S2LatLng.fromDegrees(point.getY(), point.getX()).toPoint());
            }

            var loop = new com.google.common.geometry.S2Loop(vertices);
            loop.normalize();

            return new com.google.common.geometry.S2Polygon(loop);
        }

        /// <summary>
        /// <c>ST_GEOG_CENTROID</c>. Returns the centre of the geography.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// The same dimensional rule Calcite's follows — an area outranks a line and a line outranks a point
        /// — computed on the sphere. The difference is not a refinement. A planar centroid averages
        /// longitudes, so the centre of a shape straddling the antimeridian lands on the far side of the
        /// planet; this sums directions from the Earth's centre, and answers a point in the shape.
        ///
        /// <para>Null where there is no centre to name: an empty geography, or one symmetric about the
        /// Earth's centre, where every direction is as good as its opposite.</para>
        /// </remarks>
        public static Geometry? Centroid(Geometry? geog)
        {
            if (geog is null)
                return null;

            var centre = S2Geographies.Centroid(S2Geographies.Of(geog));

            return centre is null ? Wgs84Of(Factory.createPoint()) : Wgs84Of(Factory.createPoint(Coordinate(centre)));
        }

        /// <summary>
        /// <c>ST_GEOG_CONVEXHULL</c>. Returns the smallest convex geography containing this one.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// Convex on the sphere, which is a different region from convex on a plane: the hull's edges are
        /// geodesics, so away from the equator they bow poleward of the straight lines a planar hull draws
        /// between the same vertices. A point can be inside one and outside the other.
        ///
        /// <para>Only the vertices are offered to the query, which is enough — every edge of the input is a
        /// geodesic between two of them, and a convex region containing the ends of a geodesic contains the
        /// geodesic.</para>
        /// </remarks>
        public static Geometry? ConvexHull(Geometry? geog)
        {
            if (geog is null)
                return null;

            var query = new com.google.common.geometry.S2ConvexHullQuery();
            var any = false;

            foreach (var vertex in S2Geographies.Of(geog).Vertices)
            {
                query.addPoint(vertex);
                any = true;
            }

            if (any == false)
                return Wgs84Of(Factory.createPolygon());

            return Wgs84Of(Areal(new com.google.common.geometry.S2Polygon(query.getConvexHull())));
        }

        /// <summary>
        /// <c>ST_GEOG_SIMPLIFY</c>. Returns the geography with vertices removed that move its boundary by no
        /// more than the given distance in metres.
        /// </summary>
        /// <param name="geog"></param>
        /// <param name="tolerance"></param>
        /// <returns></returns>
        /// <remarks>
        /// Metres rather than degrees, and the boundary the tolerance is measured against is made of
        /// geodesics. Areas only, as the overlay operations are, and for the same reason: S2 simplifies a
        /// polygon and answering a line by falling back to the plane would put two models in one expression.
        ///
        /// <para>There is no <c>ST_GEOG_SIMPLIFYPRESERVETOPOLOGY</c>. Calcite has both because JTS has both,
        /// the second promising the result is still valid and still disjoint from what it was disjoint from.
        /// S2's simplification makes no such promise, and a function that claimed it without keeping it would
        /// be worse than one that is missing.</para>
        /// </remarks>
        public static Geometry? Simplify(Geometry? geog, java.lang.Object? tolerance)
        {
            if (geog is null || tolerance is null)
                return null;

            var polygon = S2Geographies.Of(geog).Polygon;
            if (polygon is null)
                return null;

            var simplified = new com.google.common.geometry.S2Polygon();
            simplified.initToSimplified(polygon, Ellipsoid.AngleFor(Double(tolerance)), false);

            return Wgs84Of(Areal(simplified));
        }

        /// <summary>
        /// <c>ST_GEOG_INTERSECTION</c>. Returns the area common to two geographies.
        /// </summary>
        /// <param name="geog1"></param>
        /// <param name="geog2"></param>
        /// <returns></returns>
        /// <inheritdoc cref="Overlay" />
        public static Geometry? Intersection(Geometry? geog1, Geometry? geog2)
        {
            return Overlay(geog1, geog2, (result, a, b) => result.initToIntersection(a, b));
        }

        /// <summary>
        /// <c>ST_GEOG_DIFFERENCE</c>. Returns the part of the first geography that is not in the second.
        /// </summary>
        /// <param name="geog1"></param>
        /// <param name="geog2"></param>
        /// <returns></returns>
        /// <inheritdoc cref="Overlay" />
        public static Geometry? Difference(Geometry? geog1, Geometry? geog2)
        {
            return Overlay(geog1, geog2, (result, a, b) => result.initToDifference(a, b));
        }

        /// <summary>
        /// <c>ST_GEOG_SYMDIFFERENCE</c>. Returns the parts of two geographies that are in one and not the
        /// other.
        /// </summary>
        /// <param name="geog1"></param>
        /// <param name="geog2"></param>
        /// <returns></returns>
        /// <remarks>
        /// Built from two differences and a union, S2 having no symmetric difference of its own. That is what
        /// the operation is: everything in one and not the other, either way round.
        /// </remarks>
        /// <inheritdoc cref="Overlay" />
        public static Geometry? SymDifference(Geometry? geog1, Geometry? geog2)
        {
            return Overlay(geog1, geog2, (result, a, b) =>
            {
                var left = new com.google.common.geometry.S2Polygon();
                var right = new com.google.common.geometry.S2Polygon();

                left.initToDifference(a, b);
                right.initToDifference(b, a);

                result.initToUnion(left, right);
            });
        }

        /// <summary>
        /// <c>ST_GEOG_UNARYUNION</c>. Returns the geography with its overlapping parts merged.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <inheritdoc cref="Overlay" />
        public static Geometry? UnaryUnion(Geometry? geog)
        {
            return Overlay(geog, geog, (result, a, b) => result.initToUnion(a, b));
        }

        /// <summary>
        /// Runs one of S2's overlay operations over the areal parts of two geographies.
        /// </summary>
        /// <param name="geog1"></param>
        /// <param name="geog2"></param>
        /// <param name="operation"></param>
        /// <returns></returns>
        /// <remarks>
        /// These are areal operations and this answers them for areas, declining anything else with null
        /// rather than guessing. Calcite's take any pair, JTS overlaying whatever it is handed; the reason
        /// not to follow it there is that a line clipped by a polygon is a different computation from an area
        /// intersected with one, and S2 has the second. Answering the first by falling back to the plane
        /// would put two models in one expression, which is the thing this package exists to prevent.
        ///
        /// <para>What is on offer instead is exact where it applies. An intersection of two areas on the
        /// sphere is bounded by geodesics, and the planar answer is bounded by straight lines in degrees —
        /// which is a different region, not a rounding of the same one.</para>
        /// </remarks>
        static Geometry? Overlay(Geometry? geog1, Geometry? geog2, Action<com.google.common.geometry.S2Polygon, com.google.common.geometry.S2Polygon, com.google.common.geometry.S2Polygon> operation)
        {
            if (geog1 is null || geog2 is null)
                return null;

            var a = S2Geographies.Of(geog1).Polygon;
            var b = S2Geographies.Of(geog2).Polygon;

            if (a is null || b is null)
                return null;

            var result = new com.google.common.geometry.S2Polygon();
            operation(result, a, b);

            return Wgs84Of(Areal(result));
        }

        /// <summary>
        /// Writes an S2 polygon as a geography.
        /// </summary>
        /// <param name="polygon"></param>
        /// <returns></returns>
        /// <remarks>
        /// S2 records nesting as a loop's depth — even is a shell and odd is a hole — and orders a shell's
        /// holes after it, so one pass builds the rings. A hole is stored wound the other way round from the
        /// shell that contains it, so its vertices are reversed on the way out; and an S2 loop does not repeat
        /// its first vertex where a JTS ring must.
        /// </remarks>
        static Geometry Areal(com.google.common.geometry.S2Polygon polygon)
        {
            if (polygon.numLoops() == 0)
                return Factory.createPolygon();

            var polygons = new java.util.ArrayList();
            org.locationtech.jts.geom.LinearRing? shell = null;
            var holes = new java.util.ArrayList();

            void Close()
            {
                if (shell is null)
                    return;

                var rings = new org.locationtech.jts.geom.LinearRing[holes.size()];
                for (var i = 0; i < holes.size(); i++)
                    rings[i] = (org.locationtech.jts.geom.LinearRing)holes.get(i);

                polygons.add(Factory.createPolygon(shell, rings));
                holes.clear();
            }

            for (var i = 0; i < polygon.numLoops(); i++)
            {
                var loop = polygon.loop(i);
                var ring = Ring(loop, reversed: loop.depth() % 2 != 0);

                if (loop.depth() % 2 == 0)
                {
                    Close();
                    shell = ring;
                }
                else
                {
                    holes.add(ring);
                }
            }

            Close();

            return Factory.buildGeometry(polygons);
        }

        /// <summary>
        /// Writes one S2 loop as a closed ring.
        /// </summary>
        /// <param name="loop"></param>
        /// <param name="reversed"></param>
        /// <returns></returns>
        static org.locationtech.jts.geom.LinearRing Ring(com.google.common.geometry.S2Loop loop, bool reversed)
        {
            var count = loop.numVertices();
            var coordinates = new org.locationtech.jts.geom.Coordinate[count + 1];

            for (var i = 0; i < count; i++)
                coordinates[i] = Coordinate(loop.vertex(reversed ? count - 1 - i : i));

            coordinates[count] = coordinates[0];

            return Factory.createLinearRing(coordinates);
        }

        /// <summary>
        /// <c>ST_GEOG_DENSIFY</c>. Returns the geography with vertices inserted so that no edge is longer
        /// than the given distance in metres.
        /// </summary>
        /// <param name="geog"></param>
        /// <param name="longest"></param>
        /// <returns></returns>
        /// <remarks>
        /// Metres and a geodesic, where Calcite's is degrees and a straight line in them. Both differences
        /// matter and the second is the point of the function: densifying is usually done to hand a planar
        /// consumer something that follows the true path, and a straight line in degrees is exactly what it
        /// would have drawn anyway. Between two points on a parallel away from the equator the geodesic bows
        /// poleward, and these vertices bow with it.
        ///
        /// <para>Every part of the geography is walked, a polygon's rings included, which is what
        /// <c>GeometryTransformer</c> is for — the alternative is a case for each of the seven types.</para>
        /// </remarks>
        public static Geometry? Densify(Geometry? geog, java.lang.Object? longest)
        {
            if (geog is null || longest is null)
                return null;

            return Wgs84Of(new Densifier(Double(longest)).transform(geog));
        }

        /// <summary>
        /// <c>ST_GEOG_PROJECTPOINT</c>. Returns the point of the line nearest the given point.
        /// </summary>
        /// <param name="point"></param>
        /// <param name="line"></param>
        /// <returns></returns>
        /// <remarks>
        /// Null for anything of more than one dimension, as Calcite's is: a projection onto an area is not
        /// defined and it declines rather than guessing. The point lands on a geodesic and so is not where a
        /// planar projection puts it.
        /// </remarks>
        public static Geometry? ProjectPoint(Geometry? point, Geometry? line)
        {
            if (point is null || line is null || line.getDimension() > 1)
                return null;

            var pair = S2Geographies.ClosestPair(S2Geographies.Of(line), S2Geographies.Of(point));

            return pair is null ? null : Wgs84Of(Factory.createPoint(Coordinate(pair.Value.A)));
        }

        /// <summary>
        /// Inserts vertices along every edge of whatever it is handed.
        /// </summary>
        /// <param name="longest">The greatest edge length in metres.</param>
        sealed class Densifier(double longest) : org.locationtech.jts.geom.util.GeometryTransformer
        {

            protected override org.locationtech.jts.geom.CoordinateSequence transformCoordinates(
                org.locationtech.jts.geom.CoordinateSequence coords,
                Geometry parent)
            {
                if (coords.size() < 2)
                    return coords;

                var built = new java.util.ArrayList();

                for (var i = 0; i < coords.size() - 1; i++)
                {
                    var from = coords.getCoordinate(i);
                    var to = coords.getCoordinate(i + 1);

                    built.add(from);

                    foreach (var between in Ellipsoid.Divide(from, to, longest))
                        built.add(between);
                }

                built.add(coords.getCoordinate(coords.size() - 1));

                var array = new org.locationtech.jts.geom.Coordinate[built.size()];
                for (var i = 0; i < built.size(); i++)
                    array[i] = (org.locationtech.jts.geom.Coordinate)built.get(i);

                return createCoordinateSequence(array);
            }

        }

        /// <summary>
        /// <c>ST_GEOG_ENVELOPE</c>. Returns the smallest latitude-longitude rectangle containing the
        /// geography.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// The reason this is not <c>ST_ENVELOPE</c> is the antimeridian. A planar envelope is the minimum
        /// and maximum of the coordinates, so a shape with a vertex at 179 and another at -179 gets a
        /// rectangle 358 degrees wide — very nearly the whole globe, for a shape two degrees across. S2's
        /// rectangle knows a longitude interval may wrap, and answers the two-degree band that is actually
        /// there. Where the interval does wrap the answer is a multi-polygon of the two halves either side of
        /// the antimeridian, there being no way to write a wrapped box as one ring in longitude and latitude.
        ///
        /// <para>A degenerate rectangle answers what JTS answers for one: a point where the shape is a point,
        /// a line where it has no width or no height.</para>
        /// </remarks>
        public static Geometry? Envelope(Geometry? geog)
        {
            return geog is null ? null : Wgs84Of(Rectangle(S2Geographies.Of(geog).Bound()));
        }

        /// <summary>
        /// <c>ST_GEOG_EXTENT</c>. Returns the smallest latitude-longitude rectangle containing the geography.
        /// </summary>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// The same rectangle <see cref="Envelope"/> answers. Calcite's two are the same call as well —
        /// <c>ST_Extent</c> is <c>geom.getEnvelope()</c>, with a comment wondering whether they differ — and
        /// this mirrors that rather than inventing a difference.
        /// </remarks>
        public static Geometry? Extent(Geometry? geog)
        {
            return Envelope(geog);
        }

        /// <summary>
        /// <c>ST_GEOG_EXPAND</c>. Returns the geography's rectangle grown by a distance in metres.
        /// </summary>
        /// <param name="geog"></param>
        /// <param name="distance"></param>
        /// <returns></returns>
        /// <remarks>
        /// Metres, where Calcite's grows by degrees. Growing a box by a degree moves its northern edge
        /// further than its eastern one everywhere off the equator, and by a factor that reaches two by 60
        /// degrees of latitude, so the planar reading of this function has no fixed meaning on the Earth at
        /// all. S2 grows the rectangle by an angle and widens the longitude interval by more than that as the
        /// latitude rises, which is what keeps every point within the distance actually inside.
        /// </remarks>
        public static Geometry? Expand(Geometry? geog, java.lang.Object? distance)
        {
            if (geog is null || distance is null)
                return null;

            return Wgs84Of(Rectangle(S2Geographies.Of(geog).Bound().expandedByDistance(Ellipsoid.AngleFor(Double(distance)))));
        }

        /// <summary>
        /// Writes a latitude-longitude rectangle as a geography.
        /// </summary>
        /// <param name="rect"></param>
        /// <returns></returns>
        static Geometry Rectangle(com.google.common.geometry.S2LatLngRect rect)
        {
            if (rect.isEmpty())
                return Factory.createPolygon();

            var latLo = rect.lat().lo() * 180 / System.Math.PI;
            var latHi = rect.lat().hi() * 180 / System.Math.PI;
            var lngLo = rect.lng().lo() * 180 / System.Math.PI;
            var lngHi = rect.lng().hi() * 180 / System.Math.PI;

            // a wrapped interval has no single ring in these coordinates, so it is written as the two halves
            if (rect.lng().isInverted())
            {
                // buildGeometry rather than createMultiPolygon, because either half degenerates to a line or
                // a point exactly as one box does, and a shape on the equator makes both of them lines
                var halves = new java.util.ArrayList();
                halves.add(Box(latLo, latHi, lngLo, 180));
                halves.add(Box(latLo, latHi, -180, lngHi));

                return Factory.buildGeometry(halves);
            }

            return Box(latLo, latHi, lngLo, lngHi);
        }

        /// <summary>
        /// Writes one box, degenerating to a line or a point as JTS does.
        /// </summary>
        /// <param name="latLo"></param>
        /// <param name="latHi"></param>
        /// <param name="lngLo"></param>
        /// <param name="lngHi"></param>
        /// <returns></returns>
        static Geometry Box(double latLo, double latHi, double lngLo, double lngHi)
        {
            // a tolerance rather than equality: a coordinate reaches the rectangle as a unit vector and
            // comes back a few bits shy, so a shape that lies exactly on a parallel has a latitude interval
            // that is degenerate in fact and not in the last digit. This is a thousandth of a millimetre.
            const double flat = 1e-11;

            if (System.Math.Abs(latHi - latLo) < flat && System.Math.Abs(lngHi - lngLo) < flat)
                return Factory.createPoint(new org.locationtech.jts.geom.Coordinate(lngLo, latLo));

            if (System.Math.Abs(latHi - latLo) < flat || System.Math.Abs(lngHi - lngLo) < flat)
                return Factory.createLineString([
                    new org.locationtech.jts.geom.Coordinate(lngLo, latLo),
                    new org.locationtech.jts.geom.Coordinate(lngHi, latHi)]);

            return Factory.createPolygon([
                new org.locationtech.jts.geom.Coordinate(lngLo, latLo),
                new org.locationtech.jts.geom.Coordinate(lngHi, latLo),
                new org.locationtech.jts.geom.Coordinate(lngHi, latHi),
                new org.locationtech.jts.geom.Coordinate(lngLo, latHi),
                new org.locationtech.jts.geom.Coordinate(lngLo, latLo)]);
        }

        /// <summary>
        /// <c>ST_GEOG_CLOSESTCOORDINATE</c>. Returns the coordinate or coordinates of the geography nearest
        /// the given point.
        /// </summary>
        /// <param name="point"></param>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <remarks>
        /// A coordinate of the geography rather than a point on it, which is what Calcite's own answers: it
        /// walks the coordinate array and never looks at the space between two of them. Ties answer a
        /// multi-point, as Calcite's does.
        ///
        /// <para>The ranking is geodesic and Calcite's is planar, which is the whole of the difference and is
        /// not cosmetic: a candidate one degree east and a candidate one degree north are equidistant in
        /// degrees and 745 metres apart in metres, so the two disagree about which is nearer whenever the
        /// candidates lie in different directions.</para>
        /// </remarks>
        public static Geometry? ClosestCoordinate(Geometry? point, Geometry? geog)
        {
            return ExtremeCoordinate(point, geog, furthest: false);
        }

        /// <summary>
        /// <c>ST_GEOG_FURTHESTCOORDINATE</c>. Returns the coordinate or coordinates of the geography furthest
        /// from the given point.
        /// </summary>
        /// <param name="point"></param>
        /// <param name="geog"></param>
        /// <returns></returns>
        /// <inheritdoc cref="ClosestCoordinate" />
        public static Geometry? FurthestCoordinate(Geometry? point, Geometry? geog)
        {
            return ExtremeCoordinate(point, geog, furthest: true);
        }

        /// <summary>
        /// <c>ST_GEOG_CLOSESTPOINT</c>. Returns the point of the first geography nearest the second.
        /// </summary>
        /// <param name="geog1"></param>
        /// <param name="geog2"></param>
        /// <returns></returns>
        /// <remarks>
        /// A point on the geography rather than one of its coordinates — it may fall part way along an edge,
        /// which is why this is a different function from <see cref="ClosestCoordinate"/> and why S2 answers
        /// it. The edge it falls on is a geodesic, so the point is not the one a planar reading finds: a
        /// chord and an arc between the same two ends meet a third point at different places.
        /// </remarks>
        public static Geometry? ClosestPoint(Geometry? geog1, Geometry? geog2)
        {
            if (geog1 is null || geog2 is null)
                return null;

            var pair = S2Geographies.ClosestPair(S2Geographies.Of(geog1), S2Geographies.Of(geog2));

            return pair is null ? null : Wgs84Of(Factory.createPoint(Coordinate(pair.Value.A)));
        }

        /// <summary>
        /// <c>ST_GEOG_LONGESTLINE</c>. Returns the line between the two coordinates, one from each geography,
        /// that are furthest apart.
        /// </summary>
        /// <param name="geog1"></param>
        /// <param name="geog2"></param>
        /// <returns></returns>
        /// <remarks>
        /// Between coordinates and not between shapes, which is what Calcite measures, and the same pair
        /// <c>ST_GEOG_MAXDISTANCE</c> measures the length of.
        /// </remarks>
        public static Geometry? LongestLine(Geometry? geog1, Geometry? geog2)
        {
            if (geog1 is null || geog2 is null)
                return null;

            var max = double.NaN;
            org.locationtech.jts.geom.Coordinate? left = null;
            org.locationtech.jts.geom.Coordinate? right = null;

            foreach (var a in geog1.getCoordinates())
            {
                foreach (var b in geog2.getCoordinates())
                {
                    var distance = Ellipsoid.Distance(a, b);

                    if (double.IsNaN(max) || distance > max)
                    {
                        max = distance;
                        left = a;
                        right = b;
                    }
                }
            }

            if (left is null || right is null)
                return null;

            return Wgs84Of(Factory.createLineString([left, right]));
        }

        /// <summary>
        /// The coordinate or coordinates of the geography at the extreme geodesic distance from the point.
        /// </summary>
        /// <param name="point"></param>
        /// <param name="geog"></param>
        /// <param name="furthest"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite reads a single coordinate off the point argument and compares every coordinate of the
        /// other geography against it, so this does too — the argument is a point in the signature and only
        /// its first coordinate in the behaviour.
        /// </remarks>
        static Geometry? ExtremeCoordinate(Geometry? point, Geometry? geog, bool furthest)
        {
            if (point is null || geog is null)
                return null;

            var origin = point.getCoordinate();
            if (origin is null)
                return null;

            var found = new List<org.locationtech.jts.geom.Coordinate>();
            var best = double.NaN;

            foreach (var candidate in geog.getCoordinates())
            {
                var distance = Ellipsoid.Distance(origin, candidate);

                if (double.IsNaN(best) || (furthest ? distance > best : distance < best))
                {
                    best = distance;
                    found.Clear();
                    found.Add(candidate);
                }
                else if (distance == best && found.Contains(candidate) == false)
                {
                    found.Add(candidate);
                }
            }

            if (found.Count == 0)
                return null;

            return Wgs84Of(found.Count == 1
                ? Factory.createPoint(found[0])
                : Factory.createMultiPointFromCoords([.. found]));
        }

        /// <summary>
        /// The factory the answers above are built with.
        /// </summary>
        static readonly org.locationtech.jts.geom.GeometryFactory Factory = new();

        static org.locationtech.jts.geom.Coordinate Coordinate(com.google.common.geometry.S2Point p)
        {
            var ll = new com.google.common.geometry.S2LatLng(p);

            return new org.locationtech.jts.geom.Coordinate(ll.lngDegrees(), ll.latDegrees());
        }

        /// <summary>
        /// Refuses a geometry whose own SRID says it is in a reference system a geography cannot be in.
        /// </summary>
        /// <param name="geometry"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite leaves a geometry with no SRID on zero, which says nothing rather than says the wrong
        /// thing, so that one is stamped rather than refused.
        /// </remarks>
        static Geometry? Stamped(Geometry? geometry)
        {
            if (geometry is not null && geometry.getSRID() != 0)
                RequireWgs84(geometry.getSRID());

            return geometry;
        }

        static Geometry? Wgs84Of(Geometry? geometry)
        {
            geometry?.setSRID(Wgs84);
            return geometry;
        }

    }

}
