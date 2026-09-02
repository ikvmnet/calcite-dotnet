using org.apache.calcite.runtime;

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
        public static Geometry? FromWkt(string? wkt, java.lang.Number? srid)
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
        public static java.lang.Boolean? DWithin(Geometry? a, Geometry? b, java.lang.Number? distance)
        {
            if (a is null || b is null || distance is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.DWithin(S2Geographies.Of(a), S2Geographies.Of(b), distance.doubleValue()));
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
        public static Geometry? PointN(Geometry? g, java.lang.Number? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_PointN(g, n.intValue());
        }

        /// <summary>
        /// <c>ST_GEOG_GEOMETRYN</c>. Returns the <c>n</c>th part of the geography, counting from one.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        public static Geometry? GeometryN(Geometry? g, java.lang.Number? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_GeometryN(g, n.intValue());
        }

        /// <summary>
        /// <c>ST_GEOG_INTERIORRING</c>. Returns the <c>n</c>th hole of a polygon, counting from one.
        /// </summary>
        /// <param name="g"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        public static Geometry? InteriorRing(Geometry? g, java.lang.Number? n)
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
        public static Geometry? FromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Number? srid)
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
        public static Geometry? FromGml(string? gml, java.lang.Number? srid)
        {
            if (gml is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromGml(gml);
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
