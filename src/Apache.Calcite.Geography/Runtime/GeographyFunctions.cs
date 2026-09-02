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

            if (srid.intValue() != Wgs84)
                throw new java.lang.IllegalArgumentException($"A geography is WGS84; SRID {srid.intValue()} is not a reference system it can be in.");

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

        static Geometry? Wgs84Of(Geometry? geometry)
        {
            geometry?.setSRID(Wgs84);
            return geometry;
        }

    }

}
