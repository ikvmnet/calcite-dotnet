using System;
using System.Collections.Generic;

using org.apache.calcite.runtime;

// inside GeographyFunctions the name Wgs84 means its SRID constant, so the Wgs84 class is reached by this alias
using Ellipsoid = Apache.Calcite.Geography.Runtime.Wgs84;
using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// The implementations of the <c>CLR_ST_GEOG_*</c> operators.
    /// </summary>
    /// <remarks>
    /// <see cref="Sql.GeographyOperatorTable"/> binds each operator to one of these methods through a
    /// <c>ScalarFunctionImpl</c>, the way Calcite binds its <c>ST_*</c> functions to <c>SpatialTypeFunctions</c>.
    /// Values are JTS geometries whose coordinates are read as WGS84 longitude (x) and latitude (y) in degrees;
    /// distances are in metres and areas in square metres.
    ///
    /// <para>Every parameter and result is a reference type, and every method returns <c>null</c> when any
    /// argument is <c>null</c>. <c>ScalarFunctionImpl</c> generates no null check around a method that declares no
    /// null policy, so the null reaches the method and is handled here.</para>
    ///
    /// <para>Numeric parameters that take a distance, tolerance or ordinate are declared as <c>Object</c> and accept
    /// any <c>java.lang.Number</c>, because a SQL literal reaches the method as whatever type it has: <c>2.0</c>
    /// arrives as a <c>BigDecimal</c> and <c>2</c> as an <c>Integer</c>.</para>
    /// </remarks>
    public static class GeographyFunctions
    {

        /// <summary>
        /// The SRID of WGS84, which the constructors and editing functions stamp on the geographies they return.
        /// </summary>
        /// <remarks>
        /// It is the only reference system a geography can be in, so there is no counterpart to <c>ST_SETSRID</c> or
        /// <c>ST_TRANSFORM</c>.
        /// </remarks>
        public const int Wgs84 = 4326;

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMGEOJSON</c>. Reads a geography from GeoJSON.
        /// </summary>
        /// <param name="geoJson">The GeoJSON text.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        public static Geometry? FromGeoJson(string? geoJson)
        {
            if (geoJson is null)
                return null;

            return Wgs84Of(SpatialTypeUtils.fromGeoJson(geoJson));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMTEXT</c> and <c>CLR_ST_GEOG_GEOMFROMWKT</c>. Reads a geography from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        public static Geometry? FromWkt(string? wkt)
        {
            if (wkt is null)
                return null;

            return Wgs84Of(SpatialTypeUtils.fromWkt(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMTEXT</c> and <c>CLR_ST_GEOG_GEOMFROMWKT</c> with an SRID. Reads a geography from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        /// <remarks>
        /// The overload exists because Calcite's <c>ST_GEOMFROMTEXT</c> has it. Any SRID other than 4326 is refused
        /// rather than ignored, since no reprojection takes place.
        /// </remarks>
        public static Geometry? FromWkt(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromWkt(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASGEOM</c>. Marks a geography as a geometry to be read on the plane.
        /// </summary>
        /// <param name="geography">The geography.</param>
        /// <returns>The same object, unchanged.</returns>
        /// <remarks>
        /// Geographies and geometries share one type, so this converts nothing. It exists so that a query says where
        /// it stops reading coordinates geodesically and starts passing them to Calcite's <c>ST_*</c> functions.
        /// </remarks>
        public static Geometry? AsGeometry(Geometry? geography)
        {
            return geography;
        }

        /// <summary>
        /// <c>CLR_ST_GEOM_ASGEOG</c>. Marks a geometry as a geography to be read geodesically.
        /// </summary>
        /// <param name="geometry">The geometry, whose coordinates the caller asserts are WGS84.</param>
        /// <returns>The same object, unchanged.</returns>
        /// <remarks>
        /// Nothing is converted or checked; a geometry carries no reliable record of what its coordinates mean.
        /// </remarks>
        public static Geometry? AsGeography(Geometry? geometry)
        {
            return geometry;
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_DISTANCE</c>. Returns the geodesic distance between two geographies in metres.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>
        /// The distance on the WGS84 ellipsoid; zero where the two intersect, one encloses the other, or either is
        /// empty.
        /// </returns>
        public static java.lang.Double? Distance(Geometry? a, Geometry? b)
        {
            if (a is null || b is null)
                return null;

            return java.lang.Double.valueOf(S2Geographies.Distance(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_DWITHIN</c>. Returns whether two geographies are within a distance of one another.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <param name="distance">The distance in metres, as any <c>java.lang.Number</c>.</param>
        /// <returns>Whether <see cref="Distance"/> of the two is at most <paramref name="distance"/>.</returns>
        /// <remarks>
        /// The distance is declared as <c>Object</c> so that a decimal literal such as <c>2.0</c>, which arrives as a
        /// <c>BigDecimal</c>, is accepted without a <c>CAST</c>.
        /// </remarks>
        public static java.lang.Boolean? DWithin(Geometry? a, Geometry? b, java.lang.Object? distance)
        {
            if (a is null || b is null || distance is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.DWithin(S2Geographies.Of(a), S2Geographies.Of(b), Double(distance)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_WITHIN</c>. Returns whether the first geography lies within the second.
        /// </summary>
        /// <param name="a">The geography that may be inside.</param>
        /// <param name="b">The geography that may hold it.</param>
        /// <returns>
        /// Whether every point of <paramref name="a"/> lies in <paramref name="b"/> and their interiors meet, as JTS
        /// defines <c>within</c>; <c>false</c> where either is empty.
        /// </returns>
        public static java.lang.Boolean? Within(Geometry? a, Geometry? b)
        {
            if (a is null || b is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.Within(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_INTERSECTS</c>. Returns whether two geographies have any point in common.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>Whether the two meet; <c>false</c> where either is empty.</returns>
        public static java.lang.Boolean? Intersects(Geometry? a, Geometry? b)
        {
            if (a is null || b is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.Intersects(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_CONTAINS</c>. Returns whether the first geography contains the second.
        /// </summary>
        /// <param name="a">The geography that may hold the other.</param>
        /// <param name="b">The geography that may be inside.</param>
        /// <returns><see cref="Within"/> with the arguments reversed.</returns>
        public static java.lang.Boolean? Contains(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Contains(S2Geographies.Of(a), S2Geographies.Of(b)));
        }



        /// <summary>
        /// <c>CLR_ST_GEOG_COVERS</c>. Returns whether no point of the second geography lies outside the first.
        /// </summary>
        /// <param name="a">The geography that may cover the other.</param>
        /// <param name="b">The geography that may be covered.</param>
        /// <returns>
        /// Whether every point of <paramref name="b"/>, boundary included, lies in <paramref name="a"/>; <c>false</c>
        /// where either is empty.
        /// </returns>
        public static java.lang.Boolean? Covers(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Covers(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_COVEREDBY</c>. Returns whether no point of the first geography lies outside the second.
        /// </summary>
        /// <param name="a">The geography that may be covered.</param>
        /// <param name="b">The geography that may cover it.</param>
        /// <returns><see cref="Covers"/> with the arguments reversed.</returns>
        public static java.lang.Boolean? CoveredBy(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.CoveredBy(S2Geographies.Of(a), S2Geographies.Of(b)));
        }



        /// <summary>
        /// <c>CLR_ST_GEOG_DISJOINT</c>. Returns whether two geographies have no point in common.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>The negation of <see cref="Intersects"/>.</returns>
        public static java.lang.Boolean? Disjoint(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Disjoint(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_EQUALS</c>. Returns whether two geographies are the same set of places.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>Whether each covers the other; <c>false</c> where either is empty.</returns>
        /// <remarks>
        /// This is topological equality: a line and the same line reversed are equal. <see cref="OrderingEquals"/>
        /// compares the coordinates instead.
        /// </remarks>
        public static java.lang.Boolean? Equals(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.Equals(S2Geographies.Of(a), S2Geographies.Of(b)));
        }





        /// <summary>
        /// <c>CLR_ST_GEOG_ENVELOPESINTERSECT</c>. Returns whether the bounding rectangles of two geographies meet.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>
        /// Whether the latitude-longitude rectangles <see cref="Envelope"/> describes intersect; <c>false</c> where
        /// either geography is empty.
        /// </returns>
        public static java.lang.Boolean? EnvelopesIntersect(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Boolean.valueOf(S2Geographies.EnvelopesIntersect(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_AREA</c>. Returns the area of the geography on the WGS84 ellipsoid.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The area in square metres; zero for a geography with no polygon.</returns>
        public static java.lang.Double? Area(Geometry? g)
        {
            return g is null ? null : java.lang.Double.valueOf(S2Geographies.Area(S2Geographies.Of(g)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LENGTH</c>. Returns the geodesic length of every edge of the geography.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The length in metres, including the rings of polygons, as JTS <c>getLength</c> does.</returns>
        public static java.lang.Double? Length(Geometry? g)
        {
            return g is null ? null : java.lang.Double.valueOf(S2Geographies.Length(S2Geographies.Of(g)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_PERIMETER</c>. Returns the geodesic length of the rings of the geography's polygons.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The perimeter in metres; zero for a geography with no polygon.</returns>
        public static java.lang.Double? Perimeter(Geometry? g)
        {
            return g is null ? null : java.lang.Double.valueOf(S2Geographies.Perimeter(S2Geographies.Of(g)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAXDISTANCE</c>. Returns the greatest geodesic distance between a coordinate of one geography
        /// and a coordinate of the other.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>The distance in metres; zero where either is empty.</returns>
        /// <remarks>
        /// Only coordinates are compared, not points along edges, as Calcite's <c>ST_MAXDISTANCE</c> does.
        /// </remarks>
        public static java.lang.Double? MaxDistance(Geometry? a, Geometry? b)
        {
            return a is null || b is null
                ? null
                : java.lang.Double.valueOf(S2Geographies.MaxDistance(S2Geographies.Of(a), S2Geographies.Of(b)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ISVALID</c>. Returns whether the geography is valid on the sphere.
        /// </summary>
        /// <param name="geography">The geography.</param>
        /// <returns>
        /// Whether every coordinate is a valid latitude and longitude and S2 accepts every line, ring and polygon;
        /// this is not the planar validity <c>ST_ISVALID</c> checks.
        /// </returns>
        public static java.lang.Boolean? IsValid(Geometry? geography)
        {
            if (geography is null)
                return null;

            return java.lang.Boolean.valueOf(S2Geographies.IsValid(geography));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_X</c>. Returns the longitude of a point.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The longitude in degrees, or <c>null</c> if <paramref name="g"/> is not a point.</returns>
        public static java.lang.Double? X(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_X(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_Y</c>. Returns the latitude of a point.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The latitude in degrees, or <c>null</c> if <paramref name="g"/> is not a point.</returns>
        public static java.lang.Double? Y(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Y(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_Z</c>. Returns the third ordinate of a point.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_Z</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Double? Z(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Z(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_XMIN</c>. Returns the least longitude among the geography's coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_XMin</c> returns for <paramref name="g"/>.</returns>
        /// <remarks>
        /// This and the other minimum and maximum functions read the coordinates as numbers, so for a shape that
        /// crosses the antimeridian they do not give its western or eastern extent. <see cref="Envelope"/> does.
        /// </remarks>
        public static java.lang.Double? XMin(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_XMin(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_XMAX</c>. Returns the greatest longitude among the geography's coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_XMax</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Double? XMax(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_XMax(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_YMIN</c>. Returns the least latitude among the geography's coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_YMin</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Double? YMin(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_YMin(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_YMAX</c>. Returns the greatest latitude among the geography's coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_YMax</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Double? YMax(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_YMax(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ZMIN</c>. Returns the least third ordinate among the geography's coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_ZMin</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Double? ZMin(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_ZMin(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ZMAX</c>. Returns the greatest third ordinate among the geography's coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_ZMax</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Double? ZMax(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_ZMax(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_COORDDIM</c>. Returns how many ordinates each coordinate carries.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_CoordDim</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Integer? CoordDim(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_CoordDim(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_DIMENSION</c>. Returns the topological dimension of the geography.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>0 for points, 1 for lines and 2 for polygons, as Calcite's <c>ST_Dimension</c> returns.</returns>
        public static java.lang.Integer? Dimension(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_Dimension(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMETRYTYPE</c>. Returns the name of the kind of shape.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_GeometryType</c> returns for <paramref name="g"/>.</returns>
        public static string? GeometryType(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_GeometryType(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMETRYTYPECODE</c>. Returns the numeric code of the kind of shape.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_GeometryTypeCode</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Integer? GeometryTypeCode(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_GeometryTypeCode(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_NPOINTS</c>. Returns how many coordinates the geography has.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The same count as <see cref="NumPoints"/>, of which this is an alias.</returns>
        public static java.lang.Integer? NPoints(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NPoints(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMPOINTS</c>. Returns how many coordinates the geography has.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_NumPoints</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Integer? NumPoints(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumPoints(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMGEOMETRIES</c>. Returns how many parts the geography has.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_NumGeometries</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Integer? NumGeometries(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumGeometries(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMINTERIORRING</c>. Returns how many holes the geography's polygons have.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_NumInteriorRing</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Integer? NumInteriorRing(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumInteriorRing(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_NUMINTERIORRINGS</c>. An alias of <see cref="NumInteriorRing"/>.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_NumInteriorRings</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Integer? NumInteriorRings(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_NumInteriorRings(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_STARTPOINT</c>. Returns the first coordinate of a line as a point.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_StartPoint</c> returns for <paramref name="g"/>.</returns>
        public static Geometry? StartPoint(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_StartPoint(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ENDPOINT</c>. Returns the last coordinate of a line as a point.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_EndPoint</c> returns for <paramref name="g"/>.</returns>
        public static Geometry? EndPoint(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_EndPoint(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_EXTERIORRING</c>. Returns the shell of a polygon.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_ExteriorRing</c> returns for <paramref name="g"/>.</returns>
        public static Geometry? ExteriorRing(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_ExteriorRing(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_BOUNDARY</c>. Returns the boundary of the geography.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_Boundary</c> returns for <paramref name="g"/>.</returns>
        public static Geometry? Boundary(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Boundary(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_HOLES</c>. Returns the holes of the geography's polygons.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_Holes</c> returns for <paramref name="g"/>.</returns>
        public static Geometry? Holes(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_Holes(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ISEMPTY</c>. Returns whether the geography has no coordinates.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_IsEmpty</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Boolean? IsEmpty(Geometry? g)
        {
            return g is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_IsEmpty(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_IS3D</c>. Returns whether the coordinates carry a third ordinate.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_Is3D</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Boolean? Is3D(Geometry? g)
        {
            return g is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_Is3D(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ISCLOSED</c>. Returns whether a line ends where it begins.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>What Calcite's <c>ST_IsClosed</c> returns for <paramref name="g"/>.</returns>
        public static java.lang.Boolean? IsClosed(Geometry? g)
        {
            return g is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_IsClosed(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_SRID</c>. Returns the SRID the geography is stamped with.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>
        /// The SRID. Calcite's own spatial functions often return a geometry with an SRID of zero, so this does not
        /// reliably say whether a value is a geography.
        /// </returns>
        public static java.lang.Integer? Srid(Geometry? g)
        {
            return g is null ? null : java.lang.Integer.valueOf(SpatialTypeFunctions.ST_SRID(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASTEXT</c>. Writes the geography as WKT.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The WKT text.</returns>
        public static string? AsText(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsText(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASWKT</c>. An alias of <see cref="AsText"/>.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The WKT text.</returns>
        public static string? AsWkt(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsWKT(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASEWKT</c>. Writes the geography as EWKT, which carries the SRID.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The EWKT text.</returns>
        public static string? AsEwkt(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsEWKT(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASGEOJSON</c>. Writes the geography as GeoJSON.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The GeoJSON text.</returns>
        public static string? AsGeoJson(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsGeoJSON(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASGML</c>. Writes the geography as GML.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The GML text.</returns>
        public static string? AsGml(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsGML(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASBINARY</c>. Writes the geography as WKB.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The WKB bytes.</returns>
        public static org.apache.calcite.avatica.util.ByteString? AsBinary(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsBinary(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASWKB</c>. An alias of <see cref="AsBinary"/>.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The WKB bytes.</returns>
        public static org.apache.calcite.avatica.util.ByteString? AsWkb(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsWKB(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ASEWKB</c>. Writes the geography as Calcite's <c>ST_AsEWKB</c> does.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The bytes.</returns>
        /// <remarks>
        /// Calcite's <c>ST_AsEWKB</c> delegates to <c>ST_AsWKB</c> and so writes no SRID; this returns the same
        /// bytes.
        /// </remarks>
        public static org.apache.calcite.avatica.util.ByteString? AsEwkb(Geometry? g)
        {
            return g is null ? null : SpatialTypeFunctions.ST_AsEWKB(g);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTN</c>. Returns the <paramref name="n"/>th coordinate of a line as a point.
        /// </summary>
        /// <param name="g">The line.</param>
        /// <param name="n">The position, counting from one.</param>
        /// <returns>What Calcite's <c>ST_PointN</c> returns for these arguments.</returns>
        public static Geometry? PointN(Geometry? g, java.lang.Integer? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_PointN(g, n.intValue());
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMETRYN</c>. Returns the <paramref name="n"/>th part of the geography.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <param name="n">The position.</param>
        /// <returns>What Calcite's <c>ST_GeometryN</c> returns for these arguments.</returns>
        public static Geometry? GeometryN(Geometry? g, java.lang.Integer? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_GeometryN(g, n.intValue());
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_INTERIORRING</c>. Returns the <paramref name="n"/>th hole of a polygon.
        /// </summary>
        /// <param name="g">The polygon.</param>
        /// <param name="n">The position.</param>
        /// <returns>What Calcite's <c>ST_InteriorRing</c> returns for these arguments.</returns>
        public static Geometry? InteriorRing(Geometry? g, java.lang.Integer? n)
        {
            return g is null || n is null ? null : SpatialTypeFunctions.ST_InteriorRing(g, n.intValue());
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ORDERINGEQUALS</c>. Returns whether two geographies have the same coordinates in the same
        /// order.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>What Calcite's <c>ST_OrderingEquals</c> returns for these arguments.</returns>
        /// <remarks>
        /// This compares coordinate lists rather than places, so it means the same on the sphere as on the plane.
        /// </remarks>
        public static java.lang.Boolean? OrderingEquals(Geometry? a, Geometry? b)
        {
            return a is null || b is null ? null : java.lang.Boolean.valueOf(SpatialTypeFunctions.ST_OrderingEquals(a, b));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMEWKT</c>. Reads a geography from EWKT.
        /// </summary>
        /// <param name="ewkt">The EWKT text.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        /// <exception cref="java.lang.IllegalArgumentException">The text names an SRID other than 0 or 4326.</exception>
        public static Geometry? FromEwkt(string? ewkt)
        {
            return ewkt is null ? null : Wgs84Of(Stamped(SpatialTypeFunctions.ST_GeomFromEWKT(ewkt)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMWKB</c>. Reads a geography from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        public static Geometry? FromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_GeomFromWKB(wkb));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMWKB</c> with an SRID. Reads a geography from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? FromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromWkb(wkb);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMEWKB</c>. Reads a geography from EWKB.
        /// </summary>
        /// <param name="ewkb">The EWKB bytes.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        /// <exception cref="java.lang.IllegalArgumentException">The bytes name an SRID other than 0 or 4326.</exception>
        public static Geometry? FromEwkb(org.apache.calcite.avatica.util.ByteString? ewkb)
        {
            return ewkb is null ? null : Wgs84Of(Stamped(SpatialTypeFunctions.ST_GeomFromEWKB(ewkb)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMGML</c>. Reads a geography from GML.
        /// </summary>
        /// <param name="gml">The GML text.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        public static Geometry? FromGml(string? gml)
        {
            return gml is null ? null : Wgs84Of(SpatialTypeFunctions.ST_GeomFromGML(gml));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_GEOMFROMGML</c> with an SRID. Reads a geography from GML.
        /// </summary>
        /// <param name="gml">The GML text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>The geography, stamped with SRID 4326.</returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? FromGml(string? gml, java.lang.Integer? srid)
        {
            if (gml is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return FromGml(gml);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_FLIPCOORDINATES</c>. Returns the geography with longitude and latitude swapped.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_FlipCoordinates</c>, stamped with SRID 4326.</returns>
        public static Geometry? FlipCoordinates(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_FlipCoordinates(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_FORCE2D</c>. Returns the geography with any third ordinate dropped.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_Force2D</c>, stamped with SRID 4326.</returns>
        public static Geometry? Force2D(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Force2D(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_FORCE3D</c>. Returns the geography with a third ordinate on every coordinate.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_Force3D</c>, stamped with SRID 4326.</returns>
        public static Geometry? Force3D(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Force3D(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_NORMALIZE</c>. Returns the geography in JTS's canonical form.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_Normalize</c>, stamped with SRID 4326.</returns>
        public static Geometry? Normalize(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Normalize(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEHOLES</c>. Returns the geography with the holes removed from its polygons.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_RemoveHoles</c>, stamped with SRID 4326.</returns>
        public static Geometry? RemoveHoles(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_RemoveHoles(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEREPEATEDPOINTS</c>. Returns the geography with repeated coordinates removed.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_RemoveRepeatedPoints</c>, stamped with SRID 4326.</returns>
        public static Geometry? RemoveRepeatedPoints(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_RemoveRepeatedPoints(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_REVERSE</c>. Returns the geography with its coordinates in the opposite order.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_Reverse</c>, stamped with SRID 4326.</returns>
        public static Geometry? Reverse(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Reverse(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_TOMULTILINE</c>. Returns the lines of the geography as a multi-line.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_ToMultiLine</c>, stamped with SRID 4326.</returns>
        public static Geometry? ToMultiLine(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_ToMultiLine(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_TOMULTIPOINT</c>. Returns the coordinates of the geography as a multi-point.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_ToMultiPoint</c>, stamped with SRID 4326.</returns>
        public static Geometry? ToMultiPoint(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_ToMultiPoint(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_TOMULTISEGMENTS</c>. Returns the edges of the geography as a multi-line.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The result of Calcite's <c>ST_ToMultiSegments</c>, stamped with SRID 4326.</returns>
        public static Geometry? ToMultiSegments(Geometry? g)
        {
            return g is null ? null : Wgs84Of(SpatialTypeFunctions.ST_ToMultiSegments(g));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ADDPOINT</c>. Returns the line with a point appended.
        /// </summary>
        /// <param name="line">The line.</param>
        /// <param name="point">The point to append.</param>
        /// <returns>The result of Calcite's <c>ST_AddPoint</c>, stamped with SRID 4326.</returns>
        public static Geometry? AddPoint(Geometry? line, Geometry? point)
        {
            return line is null || point is null ? null : Wgs84Of(SpatialTypeFunctions.ST_AddPoint(line, point));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ADDPOINT</c>. Returns the line with a point inserted at the given index.
        /// </summary>
        /// <param name="line">The line.</param>
        /// <param name="point">The point to insert.</param>
        /// <param name="index">Where to insert it.</param>
        /// <returns>The result of Calcite's <c>ST_AddPoint</c>, stamped with SRID 4326.</returns>
        public static Geometry? AddPoint(Geometry? line, Geometry? point, java.lang.Integer? index)
        {
            return line is null || point is null || index is null
                ? null
                : Wgs84Of(SpatialTypeFunctions.ST_AddPoint(line, point, index.intValue()));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEPOINT</c>. Returns the line with the coordinate at the given index removed.
        /// </summary>
        /// <param name="line">The line.</param>
        /// <param name="index">The index of the coordinate to remove.</param>
        /// <returns>The result of Calcite's <c>ST_RemovePoint</c>, stamped with SRID 4326.</returns>
        public static Geometry? RemovePoint(Geometry? line, java.lang.Integer? index)
        {
            return line is null || index is null ? null : Wgs84Of(SpatialTypeFunctions.ST_RemovePoint(line, index.intValue()));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ADDZ</c>. Returns the geography with an amount added to every third ordinate.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <param name="z">The amount to add, as any <c>java.lang.Number</c>.</param>
        /// <returns>The result of Calcite's <c>ST_AddZ</c>, stamped with SRID 4326.</returns>
        public static Geometry? AddZ(Geometry? g, java.lang.Object? z)
        {
            return g is null || z is null ? null : Wgs84Of(SpatialTypeFunctions.ST_AddZ(g, Decimal(z)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_REMOVEREPEATEDPOINTS</c> with a tolerance. Returns the geography with coordinates closer
        /// together than the tolerance removed.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <param name="tolerance">
        /// The tolerance in degrees, as any <c>java.lang.Number</c>. It is passed to Calcite unchanged, so unlike
        /// the other distances in this class it is not in metres.
        /// </param>
        /// <returns>The result of Calcite's <c>ST_RemoveRepeatedPoints</c>, stamped with SRID 4326.</returns>
        public static Geometry? RemoveRepeatedPoints(Geometry? g, java.lang.Object? tolerance)
        {
            return g is null || tolerance is null
                ? null
                : Wgs84Of(SpatialTypeFunctions.ST_RemoveRepeatedPoints(g, Decimal(tolerance)));
        }

        /// <summary>
        /// Converts a numeric argument to the <c>BigDecimal</c> Calcite's own functions take.
        /// </summary>
        /// <remarks>
        /// Numeric parameters are declared as <c>Object</c> because a schema function's SQL parameter type is derived
        /// from the declared class. A literal reaches the method as the type it has (<c>200000.0</c> as a
        /// <c>BigDecimal</c>, <c>200000</c> as an <c>Integer</c>), and declaring <c>Double</c> lets routine resolution
        /// succeed and the generated call then fail to compile, since nothing inserts the cast. <c>Object</c> maps to
        /// <c>ANY</c>, which Calcite's assignment rules accept.
        /// </remarks>
        /// <param name="number">A <c>java.lang.Number</c>.</param>
        /// <returns><paramref name="number"/> itself if it is a <c>BigDecimal</c>, otherwise its <c>double</c> value as a
        /// <c>BigDecimal</c>.</returns>
        static java.math.BigDecimal Decimal(java.lang.Object number)
        {
            return number as java.math.BigDecimal ?? java.math.BigDecimal.valueOf(Double(number));
        }

        /// <summary>
        /// Converts a numeric argument to a <see cref="double"/>.
        /// </summary>
        /// <inheritdoc cref="Decimal" path="/remarks" />
        static double Double(java.lang.Object number)
        {
            return ((java.lang.Number)number).doubleValue();
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINT</c> and <c>CLR_ST_GEOG_MAKEPOINT</c>. Returns the point at a longitude and latitude.
        /// </summary>
        /// <param name="x">The longitude in degrees, as any <c>java.lang.Number</c>.</param>
        /// <param name="y">The latitude in degrees, as any <c>java.lang.Number</c>.</param>
        /// <returns>The point, stamped with SRID 4326.</returns>
        public static Geometry? Point(java.lang.Object? x, java.lang.Object? y)
        {
            return x is null || y is null ? null : Wgs84Of(SpatialTypeFunctions.ST_Point(Decimal(x), Decimal(y)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINT</c> and <c>CLR_ST_GEOG_MAKEPOINT</c> with a third ordinate.
        /// </summary>
        /// <param name="x">The longitude in degrees, as any <c>java.lang.Number</c>.</param>
        /// <param name="y">The latitude in degrees, as any <c>java.lang.Number</c>.</param>
        /// <param name="z">The third ordinate, as any <c>java.lang.Number</c>.</param>
        /// <returns>The point, stamped with SRID 4326.</returns>
        public static Geometry? Point(java.lang.Object? x, java.lang.Object? y, java.lang.Object? z)
        {
            return x is null || y is null || z is null
                ? null
                : Wgs84Of(SpatialTypeFunctions.ST_Point(Decimal(x), Decimal(y), Decimal(z)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE</c>. Returns the line through 2 points, in order.
        /// </summary>
        /// <param name="g1">Point 1.</param>
        /// <param name="g2">Point 2.</param>
        /// <returns>The result of Calcite's <c>ST_MakeLine</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2)
        {
            return g1 is null || g2 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE</c>. Returns the line through 3 points, in order.
        /// </summary>
        /// <param name="g1">Point 1.</param>
        /// <param name="g2">Point 2.</param>
        /// <param name="g3">Point 3.</param>
        /// <returns>The result of Calcite's <c>ST_MakeLine</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3)
        {
            return g1 is null || g2 is null || g3 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE</c>. Returns the line through 4 points, in order.
        /// </summary>
        /// <param name="g1">Point 1.</param>
        /// <param name="g2">Point 2.</param>
        /// <param name="g3">Point 3.</param>
        /// <param name="g4">Point 4.</param>
        /// <returns>The result of Calcite's <c>ST_MakeLine</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3, Geometry? g4)
        {
            return g1 is null || g2 is null || g3 is null || g4 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3, g4));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE</c>. Returns the line through 5 points, in order.
        /// </summary>
        /// <param name="g1">Point 1.</param>
        /// <param name="g2">Point 2.</param>
        /// <param name="g3">Point 3.</param>
        /// <param name="g4">Point 4.</param>
        /// <param name="g5">Point 5.</param>
        /// <returns>The result of Calcite's <c>ST_MakeLine</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3, Geometry? g4, Geometry? g5)
        {
            return g1 is null || g2 is null || g3 is null || g4 is null || g5 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3, g4, g5));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKELINE</c>. Returns the line through 6 points, in order.
        /// </summary>
        /// <param name="g1">Point 1.</param>
        /// <param name="g2">Point 2.</param>
        /// <param name="g3">Point 3.</param>
        /// <param name="g4">Point 4.</param>
        /// <param name="g5">Point 5.</param>
        /// <param name="g6">Point 6.</param>
        /// <returns>The result of Calcite's <c>ST_MakeLine</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakeLine(Geometry? g1, Geometry? g2, Geometry? g3, Geometry? g4, Geometry? g5, Geometry? g6)
        {
            return g1 is null || g2 is null || g3 is null || g4 is null || g5 is null || g6 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakeLine(g1, g2, g3, g4, g5, g6));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and no holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell)
        {
            return shell is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and one hole.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0)
        {
            return shell is null || hole0 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 2 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1)
        {
            return shell is null || hole0 is null || hole1 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 3 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 4 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 5 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <param name="hole4">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 6 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <param name="hole4">A closed line bounding a hole.</param>
        /// <param name="hole5">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 7 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <param name="hole4">A closed line bounding a hole.</param>
        /// <param name="hole5">A closed line bounding a hole.</param>
        /// <param name="hole6">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 8 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <param name="hole4">A closed line bounding a hole.</param>
        /// <param name="hole5">A closed line bounding a hole.</param>
        /// <param name="hole6">A closed line bounding a hole.</param>
        /// <param name="hole7">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6, Geometry? hole7)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null || hole7 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6, hole7));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 9 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <param name="hole4">A closed line bounding a hole.</param>
        /// <param name="hole5">A closed line bounding a hole.</param>
        /// <param name="hole6">A closed line bounding a hole.</param>
        /// <param name="hole7">A closed line bounding a hole.</param>
        /// <param name="hole8">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6, Geometry? hole7, Geometry? hole8)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null || hole7 is null || hole8 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEPOLYGON</c>. Returns the polygon with the given shell and 10 holes.
        /// </summary>
        /// <param name="shell">The closed line bounding the polygon.</param>
        /// <param name="hole0">A closed line bounding a hole.</param>
        /// <param name="hole1">A closed line bounding a hole.</param>
        /// <param name="hole2">A closed line bounding a hole.</param>
        /// <param name="hole3">A closed line bounding a hole.</param>
        /// <param name="hole4">A closed line bounding a hole.</param>
        /// <param name="hole5">A closed line bounding a hole.</param>
        /// <param name="hole6">A closed line bounding a hole.</param>
        /// <param name="hole7">A closed line bounding a hole.</param>
        /// <param name="hole8">A closed line bounding a hole.</param>
        /// <param name="hole9">A closed line bounding a hole.</param>
        /// <returns>The result of Calcite's <c>ST_MakePolygon</c>, stamped with SRID 4326.</returns>
        public static Geometry? MakePolygon(Geometry? shell, Geometry? hole0, Geometry? hole1, Geometry? hole2, Geometry? hole3, Geometry? hole4, Geometry? hole5, Geometry? hole6, Geometry? hole7, Geometry? hole8, Geometry? hole9)
        {
            return shell is null || hole0 is null || hole1 is null || hole2 is null || hole3 is null || hole4 is null || hole5 is null || hole6 is null || hole7 is null || hole8 is null || hole9 is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MakePolygon(shell, hole0, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8, hole9));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMTEXT</c>. Reads a line from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        public static Geometry? LineFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_LineFromText(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMTEXT</c> with an SRID. Reads a line from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? LineFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return LineFromText(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMWKB</c>. Reads a line from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKB describes another kind of shape.
        /// </returns>
        public static Geometry? LineFromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_LineFromWKB(wkb));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LINEFROMWKB</c> with an SRID. Reads a line from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKB describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? LineFromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return LineFromWkb(wkb);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MLINEFROMTEXT</c>. Reads a multi-line from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        public static Geometry? MLineFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MLineFromText(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MLINEFROMTEXT</c> with an SRID. Reads a multi-line from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? MLineFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return MLineFromText(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOINTFROMTEXT</c>. Reads a multi-point from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        public static Geometry? MPointFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MPointFromText(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOINTFROMTEXT</c> with an SRID. Reads a multi-point from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? MPointFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return MPointFromText(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOLYFROMTEXT</c>. Reads a multi-polygon from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        public static Geometry? MPolyFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_MPolyFromText(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MPOLYFROMTEXT</c> with an SRID. Reads a multi-polygon from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? MPolyFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return MPolyFromText(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMTEXT</c>. Reads a point from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        public static Geometry? PointFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PointFromText(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMTEXT</c> with an SRID. Reads a point from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? PointFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PointFromText(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMWKB</c>. Reads a point from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKB describes another kind of shape.
        /// </returns>
        public static Geometry? PointFromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PointFromWKB(wkb));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POINTFROMWKB</c> with an SRID. Reads a point from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKB describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? PointFromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PointFromWkb(wkb);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMTEXT</c>. Reads a polygon from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        public static Geometry? PolyFromText(string? wkt)
        {
            return wkt is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PolyFromText(wkt));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMTEXT</c> with an SRID. Reads a polygon from WKT.
        /// </summary>
        /// <param name="wkt">The WKT text.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKT describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? PolyFromText(string? wkt, java.lang.Integer? srid)
        {
            if (wkt is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PolyFromText(wkt);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMWKB</c>. Reads a polygon from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKB describes another kind of shape.
        /// </returns>
        public static Geometry? PolyFromWkb(org.apache.calcite.avatica.util.ByteString? wkb)
        {
            return wkb is null ? null : Wgs84Of(SpatialTypeFunctions.ST_PolyFromWKB(wkb));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_POLYFROMWKB</c> with an SRID. Reads a polygon from WKB.
        /// </summary>
        /// <param name="wkb">The WKB bytes.</param>
        /// <param name="srid">The SRID, which must be 4326.</param>
        /// <returns>
        /// The geography, stamped with SRID 4326, or <c>null</c> if the WKB describes another kind of shape.
        /// </returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        public static Geometry? PolyFromWkb(org.apache.calcite.avatica.util.ByteString? wkb, java.lang.Integer? srid)
        {
            if (wkb is null || srid is null)
                return null;

            RequireWgs84(srid.intValue());
            return PolyFromWkb(wkb);
        }

        /// <summary>
        /// Throws unless the SRID is WGS84's.
        /// </summary>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="srid"/> is not 4326.</exception>
        /// <param name="srid">The SRID to check.</param>
        static void RequireWgs84(int srid)
        {
            if (srid != Wgs84)
                throw new java.lang.IllegalArgumentException($"A geography is WGS84; SRID {srid} is not a reference system it can be in.");
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_OFFSETCURVE</c>. Returns the line drawn a geodesic distance to one side of a line.
        /// </summary>
        /// <param name="line">The line.</param>
        /// <param name="distance">
        /// Metres to the left of the direction of travel, negative for the right, as any <c>java.lang.Number</c>.
        /// </param>
        /// <returns>
        /// The offset line, stamped with SRID 4326; an empty line if <paramref name="line"/> has fewer than two
        /// coordinates; <c>null</c> if <paramref name="line"/> is not a line.
        /// </returns>
        /// <remarks>
        /// Each vertex is moved perpendicular to the direction from the vertex before it to the vertex after it, so
        /// corners are smoothed rather than mitred, and a closed line gives a closed result. Calcite's
        /// <c>ST_OFFSETCURVE</c> takes a third argument naming a JTS buffer style; this takes two, and the offset is in
        /// metres rather than degrees.
        /// </remarks>
        public static Geometry? OffsetCurve(Geometry? line, java.lang.Object? distance)
        {
            if (line is null || distance is null || line is not org.locationtech.jts.geom.LineString path)
                return null;

            var metres = Double(distance);
            var vertices = path.getCoordinates();

            if (vertices.Length < 2)
                return Wgs84Of(Factory.createLineString([]));

            // a closed line has no ends: the vertex before the first is the one before the repeated last, and
            // the vertex after the last is the second, so the shared vertex moves the same way at both ends and
            // the result stays closed
            var closed = path.isClosed() && vertices.Length >= 4;
            var last = vertices.Length - 1;

            var moved = new org.locationtech.jts.geom.Coordinate[vertices.Length];

            for (var i = 0; i < vertices.Length; i++)
            {
                // the direction of travel at a vertex is taken from the vertex before to the vertex after, which
                // smooths a corner
                var before = vertices[i == 0 ? (closed ? last - 1 : 0) : i - 1];
                var after = vertices[i == last ? (closed ? 1 : i) : i + 1];

                moved[i] = Ellipsoid.Offset(vertices[i], Ellipsoid.Azimuth(before, after) - 90, metres);
            }

            return Wgs84Of(Factory.createLineString(moved));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MAKEELLIPSE</c>. Returns an ellipse of a given width and height in metres centred on a point.
        /// </summary>
        /// <param name="point">The centre.</param>
        /// <param name="width">The full east-west extent in metres, as any <c>java.lang.Number</c>.</param>
        /// <param name="height">The full north-south extent in metres, as any <c>java.lang.Number</c>.</param>
        /// <returns>
        /// A 32-sided polygon stamped with SRID 4326; an empty polygon if either extent is not positive or the point is
        /// empty; <c>null</c> if <paramref name="point"/> is not a point.
        /// </returns>
        /// <remarks>
        /// Calcite's <c>ST_MAKEELLIPSE</c> takes degrees. Because the extents here are metres, equal width and height
        /// give a circle on the ground rather than on the map.
        /// </remarks>
        public static Geometry? MakeEllipse(Geometry? point, java.lang.Object? width, java.lang.Object? height)
        {
            if (point is null || width is null || height is null || point is not org.locationtech.jts.geom.Point centre)
                return null;

            var east = Double(width) / 2;
            var north = Double(height) / 2;

            if (east <= 0 || north <= 0 || centre.isEmpty())
                return Wgs84Of(Factory.createPolygon());

            var origin = centre.getCoordinate();
            var ring = new org.locationtech.jts.geom.Coordinate[CircleSides + 1];

            for (var i = 0; i < CircleSides; i++)
            {
                // walked by the ellipse's parameter t, as JTS does: the point at t lies a·cos t east and b·sin t
                // north of the centre, which gives both the azimuth and the distance to travel
                var t = 2 * System.Math.PI * i / CircleSides;
                var sideways = east * System.Math.Cos(t);
                var forward = north * System.Math.Sin(t);

                var azimuth = System.Math.Atan2(sideways, forward) * 180 / System.Math.PI;
                var reach = System.Math.Sqrt(sideways * sideways + forward * forward);

                ring[i] = Ellipsoid.Offset(origin, azimuth, reach);
            }

            ring[CircleSides] = ring[0];

            return Wgs84Of(Factory.createPolygon(ring));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LOCATEALONG</c>. Returns a point on every segment of the geography, a fraction of the way
        /// along it and offset sideways.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <param name="fraction">How far along each segment, from 0 to 1, as any <c>java.lang.Number</c>.</param>
        /// <param name="offset">
        /// Metres to the left of the direction of travel at that point, negative for the right, as any
        /// <c>java.lang.Number</c>.
        /// </param>
        /// <returns>A multi-point of one point per segment of every part, stamped with SRID 4326.</returns>
        /// <remarks>
        /// Each segment is a geodesic, so the point a fraction along it is not the point that fraction along a straight
        /// line in degrees, and the sideways direction is taken where the point lands.
        /// </remarks>
        public static Geometry? LocateAlong(Geometry? geog, java.lang.Object? fraction, java.lang.Object? offset)
        {
            if (geog is null || fraction is null || offset is null)
                return null;

            var along = Double(fraction);
            var aside = Double(offset);
            var found = new List<org.locationtech.jts.geom.Coordinate>();

            for (var i = 0; i < geog.getNumGeometries(); i++)
            {
                var part = geog.getGeometryN(i).getCoordinates();

                for (var j = 0; j < part.Length - 1; j++)
                    found.Add(Ellipsoid.Along(part[j], part[j + 1], along, aside));
            }

            return Wgs84Of(Factory.createMultiPointFromCoords([.. found]));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_MINIMUMDIAMETER</c>. Returns the shortest line across the geography's width.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>
        /// A two-point line stamped with SRID 4326, or an empty line if the geography has fewer than three distinct
        /// coordinates or no width.
        /// </returns>
        /// <remarks>
        /// For each edge of the convex hull, the width is the greatest geodesic distance from a vertex to that edge's
        /// great circle; the narrowest such width wins. The result runs from the furthest vertex to its nearest point
        /// on the great circle.
        /// </remarks>
        public static Geometry? MinimumDiameter(Geometry? geog)
        {
            if (geog is null)
                return null;

            // counted on the input rather than on the hull: S2's hull of a single point is a degenerate loop
            // with several vertices, which would pass a count on the hull
            var distinct = new java.util.HashSet();
            foreach (var coordinate in geog.getCoordinates())
                distinct.add(coordinate.toString());

            if (distinct.size() < 3)
                return Wgs84Of(Factory.createLineString([]));

            var hull = ConvexHull(geog);
            var ring = hull is null || hull.isEmpty() ? geog.getCoordinates() : hull.getCoordinates();

            if (ring.Length < 3)
                return Wgs84Of(Factory.createLineString([]));

            var best = double.MaxValue;
            org.locationtech.jts.geom.Coordinate? from = null;
            org.locationtech.jts.geom.Coordinate? to = null;

            for (var i = 0; i < ring.Length - 1; i++)
            {
                var normal = Normal(ring[i], ring[i + 1]);
                if (normal is null)
                    continue;

                var width = 0.0;
                org.locationtech.jts.geom.Coordinate? outer = null;
                org.locationtech.jts.geom.Coordinate? foot = null;

                foreach (var vertex in ring)
                {
                    var landing = Project(vertex, normal);
                    var distance = Ellipsoid.Distance(vertex, landing);

                    if (distance > width)
                    {
                        width = distance;
                        outer = vertex;
                        foot = landing;
                    }
                }

                if (outer is not null && width < best)
                {
                    best = width;
                    from = outer;
                    to = foot;
                }
            }

            return from is null || to is null
                ? Wgs84Of(Factory.createLineString([]))
                : Wgs84Of(Factory.createLineString([from, to]));
        }

        /// <summary>
        /// Returns the pole of the great circle through two coordinates, or <see langword="null"/> where they
        /// determine no circle.
        /// </summary>
        /// <param name="a">One coordinate on the circle, longitude in x and latitude in y.</param>
        /// <param name="b">Another coordinate on the circle.</param>
        /// <returns>The pole as a unit vector, or <see langword="null"/> where the coordinates are the same place or
        /// antipodal.</returns>
        static com.google.common.geometry.S2Point? Normal(org.locationtech.jts.geom.Coordinate a, org.locationtech.jts.geom.Coordinate b)
        {
            var p = com.google.common.geometry.S2LatLng.fromDegrees(a.getY(), a.getX()).toPoint();
            var q = com.google.common.geometry.S2LatLng.fromDegrees(b.getY(), b.getX()).toPoint();
            var cross = com.google.common.geometry.S2Point.crossProd(p, q);

            return cross.norm() == 0 ? null : cross.normalize();
        }

        /// <summary>
        /// Returns the point of a great circle nearest a coordinate.
        /// </summary>
        /// <param name="vertex">The coordinate.</param>
        /// <param name="normal">The pole of the circle.</param>
        /// <returns>The nearest point of the circle, or <paramref name="vertex"/> itself where it is a pole of the
        /// circle.</returns>
        static org.locationtech.jts.geom.Coordinate Project(org.locationtech.jts.geom.Coordinate vertex, com.google.common.geometry.S2Point normal)
        {
            var p = com.google.common.geometry.S2LatLng.fromDegrees(vertex.getY(), vertex.getX()).toPoint();
            var lifted = com.google.common.geometry.S2Point.sub(p, com.google.common.geometry.S2Point.mul(normal, p.dotProd(normal)));

            return Coordinate(lifted.norm() == 0 ? p : lifted.normalize());
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_BOUNDINGCIRCLE</c>. Returns a circle on the ground that contains the geography.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>
        /// A 32-sided polygon stamped with SRID 4326; a point if every coordinate is the same place; an empty polygon
        /// if the geography has no coordinates.
        /// </returns>
        /// <remarks>
        /// The centre is found iteratively and is close to, but not exactly, the one that minimises the radius. The
        /// radius is then the greatest geodesic distance from that centre to any vertex, so the circle always contains
        /// every vertex and may be slightly larger than the smallest possible. The polygon is drawn outside the circle,
        /// its vertices at <c>radius / cos(π / 32)</c>, so its edges do not cut inside it.
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
        /// Returns approximately the place whose greatest geodesic distance to any of the coordinates is least.
        /// </summary>
        /// <remarks>
        /// Starting from the first coordinate, each of 1000 steps moves <c>d / (i + 1)</c> toward the furthest
        /// coordinate, where <c>d</c> is the distance to it. This converges on the one-centre from any start.
        /// </remarks>
        /// <param name="vertices">The coordinates; there must be at least one.</param>
        /// <returns>The approximate centre.</returns>
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
        /// <c>CLR_ST_GEOG_ISSIMPLE</c>. Returns whether the geography touches itself only where JTS allows.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>
        /// JTS's rule applied to geodesic edges: points are simple, a multi-point is simple when no point repeats, a
        /// line when its edges meet only where they join (its ends may coincide), a polygon always, and a collection
        /// when all its parts are.
        /// </returns>
        /// <remarks>
        /// Because edges are geodesics, two edges can cross here that do not cross as straight lines in degrees.
        /// </remarks>
        public static java.lang.Boolean? IsSimple(Geometry? geog)
        {
            return geog is null ? null : java.lang.Boolean.valueOf(S2Geographies.IsSimple(geog));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ISRING</c>. Returns whether the geography is a closed, simple line.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>Whether it is a non-empty line that ends where it begins and is simple on geodesic edges.</returns>
        public static java.lang.Boolean? IsRing(Geometry? geog)
        {
            return geog is null ? null : java.lang.Boolean.valueOf(S2Geographies.IsRing(geog));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_BUFFER</c>. Returns an approximation of the region within a distance of the geography.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <param name="distance">The distance in metres, as any <c>java.lang.Number</c>.</param>
        /// <returns>
        /// The region, stamped with SRID 4326; an empty polygon if the distance is not positive or the geography is
        /// empty.
        /// </returns>
        /// <remarks>
        /// S2's Java library has no buffer, so this builds one: a 32-sided polygon of points at exactly the distance
        /// is drawn around every vertex and around points spaced along each edge, and these are unioned with the
        /// geography's own polygons. The rings are inscribed, so the result lies slightly inside the true buffer.
        /// Edge points are spaced at a quarter of the distance, or more widely where that would need more than 512 of
        /// them, which reduces accuracy for shapes that are long relative to the distance.
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
        /// Returns the centres of the rings <see cref="Buffer"/> draws: every coordinate, plus points dividing the
        /// geodesic between each pair of consecutive coordinates.
        /// </summary>
        /// <param name="geog">The geography being buffered.</param>
        /// <param name="metres">The buffer distance, which sets the spacing between centres.</param>
        /// <returns>The centres in order along the geography; empty where it has no coordinates.</returns>
        static List<org.locationtech.jts.geom.Coordinate> Seeds(Geometry geog, double metres)
        {
            var coordinates = geog.getCoordinates();
            var seeds = new List<org.locationtech.jts.geom.Coordinate>();

            if (coordinates.Length == 0)
                return seeds;

            // a quarter of the distance keeps the gap between neighbouring rings small beside their radius; the
            // limit bounds the number of rings for a shape that is long relative to the distance
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
        /// The number of sides a circle is drawn with, which matches JTS's default of eight per quadrant.
        /// </summary>
        const int CircleSides = 32;

        /// <summary>
        /// Returns a polygon whose vertices are the given geodesic distance from a centre.
        /// </summary>
        /// <param name="centre">The centre, longitude in x and latitude in y.</param>
        /// <param name="metres">The geodesic distance from the centre to each vertex.</param>
        /// <returns>A normalized polygon of <see cref="CircleSides"/> vertices.</returns>
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
        /// <c>CLR_ST_GEOG_CENTROID</c>. Returns the centre of the geography, computed on the sphere.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>
        /// A point stamped with SRID 4326, or an empty point where there is no centre: an empty geography, or one
        /// whose parts balance exactly about the Earth's centre, such as two antipodal points.
        /// </returns>
        /// <remarks>
        /// As in Calcite, polygons outrank lines and lines outrank points, so only the parts of the highest dimension
        /// count. The centre is a mean of directions from the Earth's centre rather than of longitudes and latitudes,
        /// so a shape crossing the antimeridian gets a centre inside it.
        /// </remarks>
        public static Geometry? Centroid(Geometry? geog)
        {
            if (geog is null)
                return null;

            var centre = S2Geographies.Centroid(S2Geographies.Of(geog));

            return centre is null ? Wgs84Of(Factory.createPoint()) : Wgs84Of(Factory.createPoint(Coordinate(centre)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_CONVEXHULL</c>. Returns the smallest region that contains the geography and is convex on the
        /// sphere.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>The hull, stamped with SRID 4326; an empty polygon if the geography has no vertices.</returns>
        /// <remarks>
        /// The hull's edges are great-circle arcs, so away from the equator they bow poleward of the edges a planar
        /// hull draws between the same vertices.
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
        /// <c>CLR_ST_GEOG_SIMPLIFY</c>. Returns the geography's polygons with vertices removed that move the boundary
        /// by no more than a distance.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <param name="tolerance">The distance in metres, as any <c>java.lang.Number</c>.</param>
        /// <returns>
        /// The simplified polygons, stamped with SRID 4326, or <c>null</c> if the geography has no polygon.
        /// </returns>
        /// <remarks>
        /// Only polygons are simplified, using S2. There is no <c>CLR_ST_GEOG_SIMPLIFYPRESERVETOPOLOGY</c>, because
        /// S2's simplification does not guarantee what JTS's topology-preserving form does.
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
        /// <c>CLR_ST_GEOG_INTERSECTION</c>. Returns the area two geographies share.
        /// </summary>
        /// <param name="geog1">The first geography.</param>
        /// <param name="geog2">The second geography.</param>
        /// <returns>
        /// The intersection of their polygons, stamped with SRID 4326, or <c>null</c> if either has no polygon.
        /// </returns>
        /// <inheritdoc cref="Overlay" path="/remarks" />
        public static Geometry? Intersection(Geometry? geog1, Geometry? geog2)
        {
            return Overlay(geog1, geog2, (result, a, b) => result.initToIntersection(a, b));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_DIFFERENCE</c>. Returns the area of the first geography that is not in the second.
        /// </summary>
        /// <param name="geog1">The geography to subtract from.</param>
        /// <param name="geog2">The geography to subtract.</param>
        /// <returns>
        /// The difference of their polygons, stamped with SRID 4326, or <c>null</c> if either has no polygon.
        /// </returns>
        /// <inheritdoc cref="Overlay" path="/remarks" />
        public static Geometry? Difference(Geometry? geog1, Geometry? geog2)
        {
            return Overlay(geog1, geog2, (result, a, b) => result.initToDifference(a, b));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_SYMDIFFERENCE</c>. Returns the area that is in one geography and not the other.
        /// </summary>
        /// <param name="geog1">The first geography.</param>
        /// <param name="geog2">The second geography.</param>
        /// <returns>
        /// The symmetric difference of their polygons, stamped with SRID 4326, or <c>null</c> if either has no polygon.
        /// </returns>
        /// <inheritdoc cref="Overlay" path="/remarks" />
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
        /// <c>CLR_ST_GEOG_UNARYUNION</c>. Returns the geography's polygons merged into one region.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>
        /// The union of its polygons, stamped with SRID 4326, or <c>null</c> if it has no polygon.
        /// </returns>
        /// <inheritdoc cref="Overlay" path="/remarks" />
        public static Geometry? UnaryUnion(Geometry? geog)
        {
            return Overlay(geog, geog, (result, a, b) => result.initToUnion(a, b));
        }

        /// <summary>
        /// Runs an S2 overlay operation over the polygons of two geographies.
        /// </summary>
        /// <param name="geog1">The first geography.</param>
        /// <param name="geog2">The second geography.</param>
        /// <param name="operation">Fills its first argument with the result of combining the other two.</param>
        /// <returns>The result, or <c>null</c> if either argument is <c>null</c> or has no polygon.</returns>
        /// <remarks>
        /// Only polygons take part; points and lines are ignored, and a geography with no polygon gives <c>null</c>.
        /// Calcite's overlay functions accept any shapes, but clipping a line is not an operation S2's Java library
        /// provides, and falling back to JTS would mix planar and geodesic results. The result is bounded by geodesics.
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
        /// Converts an S2 polygon to a JTS polygon or multi-polygon.
        /// </summary>
        /// <remarks>
        /// S2 gives each loop a depth, even for a shell and odd for a hole, and orders each shell's holes after it, so
        /// one pass builds the rings. Holes are wound opposite to their shell in S2 and are reversed on the way out.
        /// </remarks>
        /// <param name="polygon">The S2 polygon.</param>
        /// <returns>An empty polygon where it has no loops, a polygon for one shell, and a multi-polygon for
        /// several.</returns>
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
        /// Converts an S2 loop to a JTS ring, repeating the first coordinate at the end as JTS requires.
        /// </summary>
        /// <param name="loop">The loop.</param>
        /// <param name="reversed">Whether to write the vertices in reverse order.</param>
        /// <returns>A closed ring.</returns>
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
        /// <c>CLR_ST_GEOG_DENSIFY</c>. Returns the geography with vertices inserted along its geodesic edges so that no
        /// edge is longer than a distance.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <param name="longest">The greatest edge length in metres, as any <c>java.lang.Number</c>.</param>
        /// <returns>The densified geography, stamped with SRID 4326.</returns>
        /// <remarks>
        /// The inserted vertices lie on the geodesic, so a consumer that joins vertices with straight lines in degrees
        /// draws something close to the true path. Every part is densified, including polygon rings.
        /// </remarks>
        public static Geometry? Densify(Geometry? geog, java.lang.Object? longest)
        {
            if (geog is null || longest is null)
                return null;

            return Wgs84Of(new Densifier(Double(longest)).transform(geog));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_PROJECTPOINT</c>. Returns the point of a line nearest a given point.
        /// </summary>
        /// <param name="point">The point to project.</param>
        /// <param name="line">The line to project onto.</param>
        /// <returns>
        /// The nearest point on the line's geodesic edges, stamped with SRID 4326; <c>null</c> if
        /// <paramref name="line"/> is a polygon or either geography is empty.
        /// </returns>
        public static Geometry? ProjectPoint(Geometry? point, Geometry? line)
        {
            if (point is null || line is null || line.getDimension() > 1)
                return null;

            var pair = S2Geographies.ClosestPair(S2Geographies.Of(line), S2Geographies.Of(point));

            return pair is null ? null : Wgs84Of(Factory.createPoint(Coordinate(pair.Value.A)));
        }

        /// <summary>
        /// Inserts vertices along every edge of a geometry, including polygon rings.
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
        /// <c>CLR_ST_GEOG_ENVELOPE</c>. Returns the smallest latitude-longitude rectangle containing the geography.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>
        /// The rectangle, stamped with SRID 4326. It is a point or a line where the rectangle has no width or height,
        /// an empty polygon for an empty geography, and a multi-polygon of the two halves where the rectangle crosses
        /// the antimeridian.
        /// </returns>
        /// <remarks>
        /// The rectangle comes from S2, whose longitude interval can wrap, so a shape spanning 179° to -179° gets a
        /// two-degree rectangle rather than a 358-degree one. It also includes the poleward bulge of geodesic edges.
        /// </remarks>
        public static Geometry? Envelope(Geometry? geog)
        {
            return geog is null ? null : Wgs84Of(Rectangle(S2Geographies.Of(geog).Bound()));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_EXTENT</c>. An alias of <see cref="Envelope"/>, as Calcite's <c>ST_EXTENT</c> is of
        /// <c>ST_ENVELOPE</c>.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <returns>The same rectangle as <see cref="Envelope"/>.</returns>
        public static Geometry? Extent(Geometry? geog)
        {
            return Envelope(geog);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_EXPAND</c>. Returns the geography's latitude-longitude rectangle grown by a distance.
        /// </summary>
        /// <param name="geog">The geography.</param>
        /// <param name="distance">The distance in metres, as any <c>java.lang.Number</c>.</param>
        /// <returns>The grown rectangle, written as <see cref="Envelope"/> writes one.</returns>
        /// <remarks>
        /// S2 grows the rectangle so that every point within the distance is inside it, widening the longitude
        /// interval more at higher latitudes. The distance is turned into an angle using the mean radius of the WGS84
        /// ellipsoid. Calcite's <c>ST_EXPAND</c> grows by degrees.
        /// </remarks>
        public static Geometry? Expand(Geometry? geog, java.lang.Object? distance)
        {
            if (geog is null || distance is null)
                return null;

            return Wgs84Of(Rectangle(S2Geographies.Of(geog).Bound().expandedByDistance(Ellipsoid.AngleFor(Double(distance)))));
        }

        /// <summary>
        /// Converts an S2 latitude-longitude rectangle to a geometry.
        /// </summary>
        /// <param name="rect">The rectangle.</param>
        /// <returns>An empty polygon for an empty rectangle; otherwise the box <see cref="Box"/> builds, or two of them
        /// where the longitude interval crosses the antimeridian.</returns>
        static Geometry Rectangle(com.google.common.geometry.S2LatLngRect rect)
        {
            if (rect.isEmpty())
                return Factory.createPolygon();

            var latLo = rect.lat().lo() * 180 / System.Math.PI;
            var latHi = rect.lat().hi() * 180 / System.Math.PI;
            var lngLo = rect.lng().lo() * 180 / System.Math.PI;
            var lngHi = rect.lng().hi() * 180 / System.Math.PI;

            // a longitude interval that wraps the antimeridian cannot be one ring, so it is written as two halves
            if (rect.lng().isInverted())
            {
                // buildGeometry rather than createMultiPolygon, because either half can degenerate to a line
                // or a point as a single box does
                var halves = new java.util.ArrayList();
                halves.add(Box(latLo, latHi, lngLo, 180));
                halves.add(Box(latLo, latHi, -180, lngHi));

                return Factory.buildGeometry(halves);
            }

            return Box(latLo, latHi, lngLo, lngHi);
        }

        /// <summary>
        /// Returns a box, or a line or point where it has no width or height, as JTS does for a degenerate envelope.
        /// </summary>
        /// <param name="latLo">The southern latitude in degrees.</param>
        /// <param name="latHi">The northern latitude in degrees.</param>
        /// <param name="lngLo">The western longitude in degrees.</param>
        /// <param name="lngHi">The eastern longitude in degrees.</param>
        /// <returns>A polygon, or a line or point where the box has no width or height.</returns>
        static Geometry Box(double latLo, double latHi, double lngLo, double lngHi)
        {
            // a tolerance rather than equality: a coordinate goes through a unit vector on its way into the
            // rectangle and loses its last bits, so a shape lying on a parallel has a latitude interval that is
            // only nearly empty. 1e-11 degrees is about a micrometre.
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
        /// <c>CLR_ST_GEOG_CLOSESTCOORDINATE</c>. Returns the coordinate or coordinates of a geography nearest a point.
        /// </summary>
        /// <param name="point">The point. Only its first coordinate is used.</param>
        /// <param name="geog">The geography whose coordinates are searched.</param>
        /// <returns>
        /// A point, or a multi-point where several coordinates are equally near, stamped with SRID 4326; <c>null</c>
        /// if either geography has no coordinates.
        /// </returns>
        /// <remarks>
        /// As in Calcite's <c>ST_CLOSESTCOORDINATE</c>, only the geography's coordinates are candidates, not points
        /// along its edges. They are ranked by geodesic distance rather than by distance in degrees, so the two can
        /// choose differently.
        /// </remarks>
        public static Geometry? ClosestCoordinate(Geometry? point, Geometry? geog)
        {
            return ExtremeCoordinate(point, geog, furthest: false);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_FURTHESTCOORDINATE</c>. Returns the coordinate or coordinates of a geography furthest from a
        /// point.
        /// </summary>
        /// <param name="point">The point. Only its first coordinate is used.</param>
        /// <param name="geog">The geography whose coordinates are searched.</param>
        /// <returns>
        /// A point, or a multi-point where several coordinates are equally far, stamped with SRID 4326; <c>null</c>
        /// if either geography has no coordinates.
        /// </returns>
        /// <inheritdoc cref="ClosestCoordinate" path="/remarks" />
        public static Geometry? FurthestCoordinate(Geometry? point, Geometry? geog)
        {
            return ExtremeCoordinate(point, geog, furthest: true);
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_CLOSESTPOINT</c>. Returns the point of the first geography nearest the second.
        /// </summary>
        /// <param name="geog1">The geography the point is taken from.</param>
        /// <param name="geog2">The geography it is nearest to.</param>
        /// <returns>
        /// The point, which may lie part way along a geodesic edge, stamped with SRID 4326; <c>null</c> if either
        /// geography is empty.
        /// </returns>
        public static Geometry? ClosestPoint(Geometry? geog1, Geometry? geog2)
        {
            if (geog1 is null || geog2 is null)
                return null;

            var pair = S2Geographies.ClosestPair(S2Geographies.Of(geog1), S2Geographies.Of(geog2));

            return pair is null ? null : Wgs84Of(Factory.createPoint(Coordinate(pair.Value.A)));
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_LONGESTLINE</c>. Returns the line between the two coordinates, one from each geography, that
        /// are furthest apart.
        /// </summary>
        /// <param name="geog1">The first geography.</param>
        /// <param name="geog2">The second geography.</param>
        /// <returns>
        /// A two-point line stamped with SRID 4326, or <c>null</c> if either geography has no coordinates. Its length
        /// is what <see cref="MaxDistance"/> returns.
        /// </returns>
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
        /// Returns the coordinate or coordinates of <paramref name="geog"/> at the least or greatest geodesic
        /// distance from the first coordinate of <paramref name="point"/>.
        /// </summary>
        /// <remarks>
        /// Calcite's functions also read a single coordinate off the point argument.
        /// </remarks>
        /// <param name="point">The geometry whose first coordinate distances are measured from.</param>
        /// <param name="geog">The geography whose coordinates are the candidates.</param>
        /// <param name="furthest">Whether to find the greatest distance rather than the least.</param>
        /// <returns>A point, or a multi-point where several coordinates tie, stamped with SRID 4326; <c>null</c> where
        /// an argument is <c>null</c> or has no coordinates.</returns>
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
        /// The factory the geometries this class builds are made with.
        /// </summary>
        static readonly org.locationtech.jts.geom.GeometryFactory Factory = new();

        static org.locationtech.jts.geom.Coordinate Coordinate(com.google.common.geometry.S2Point p)
        {
            var ll = new com.google.common.geometry.S2LatLng(p);

            return new org.locationtech.jts.geom.Coordinate(ll.lngDegrees(), ll.latDegrees());
        }

        /// <summary>
        /// Throws if a geometry carries an SRID other than 0 or 4326.
        /// </summary>
        /// <remarks>
        /// Calcite leaves a geometry with no SRID on 0, which is accepted and later stamped with 4326.
        /// </remarks>
        /// <exception cref="java.lang.IllegalArgumentException">The geometry's SRID is not 0 or 4326.</exception>
        /// <param name="geometry">The geometry to check; may be <c>null</c>.</param>
        /// <returns><paramref name="geometry"/>, unchanged.</returns>
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
