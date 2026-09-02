using System;
using System.Collections.Generic;

using com.google.common.geometry;

using org.locationtech.jts.geom;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// A JTS geometry read as WGS84 and carried as S2 shapes, and the geodesic questions asked of a pair of
    /// them.
    /// </summary>
    /// <remarks>
    /// The engine is Google's S2, which models the Earth as a sphere and joins two vertices with a
    /// great-circle arc. That is the choice this package makes, and it is one model rather than two: an
    /// ellipsoidal distance from GeographicLib next to a spherical containment from S2 would put two
    /// readings of the same coordinates in one plan. It costs a few tenths of a percent against an
    /// ellipsoidal distance, and it is what BigQuery and Snowflake compute in.
    ///
    /// <para>A JTS coordinate is <c>(x, y)</c> and WGS84 order is longitude then latitude, so <c>x</c> is the
    /// longitude and <c>y</c> is the latitude — the order <c>POINT(lng lat)</c> in WKT and GeoJSON.</para>
    ///
    /// <para>The pairwise operations are quadratic in the vertex counts: every edge of one against every edge
    /// of the other. S2 has an indexed form of these queries and this does not use it. That is deliberate for
    /// now — the shapes a store pushes a predicate over are small, and an index is a thing to add against a
    /// measurement rather than a guess.</para>
    /// </remarks>
    sealed class S2Geographies
    {

        /// <summary>
        /// The radius S2 uses for the Earth, in metres, and therefore the radius every distance this package
        /// answers is measured on.
        /// </summary>
        public const double EarthRadiusMeters = 6371010.0;

        /// <summary>
        /// Reads a JTS geometry as WGS84 and builds its S2 shapes.
        /// </summary>
        /// <param name="geometry"></param>
        /// <returns></returns>
        public static S2Geographies Of(Geometry geometry)
        {
            ArgumentNullException.ThrowIfNull(geometry);

            var self = new S2Geographies();
            self.Add(geometry);
            self.Close();
            return self;
        }

        /// <summary>
        /// Returns whether the given geometry is a valid geography.
        /// </summary>
        /// <param name="geometry"></param>
        /// <returns></returns>
        /// <remarks>
        /// Not what <c>ST_ISVALID</c> answers. JTS validity is planar and says nothing about whether a
        /// coordinate names a place on the Earth; a ring whose edges do not cross as straight lines in
        /// longitude and latitude may still cross as great-circle arcs. This asks S2: every coordinate a
        /// valid latitude and longitude, every line a valid polyline, every ring a valid loop, and the rings
        /// of a polygon a valid set.
        /// </remarks>
        public static bool IsValid(Geometry geometry)
        {
            ArgumentNullException.ThrowIfNull(geometry);

            switch (geometry)
            {
                case Point point:
                    return point.isEmpty() || IsValidCoordinate(point.getCoordinate());
                case LineString line:
                    return line.isEmpty() || IsValidLine(line);
                case Polygon polygon:
                    return polygon.isEmpty() || IsValidPolygon(polygon);
                case GeometryCollection collection:
                    for (var i = 0; i < collection.getNumGeometries(); i++)
                        if (IsValid(collection.getGeometryN(i)) == false)
                            return false;

                    return true;
                default:
                    return false;
            }
        }

        static bool IsValidCoordinate(Coordinate coordinate)
        {
            return S2LatLng.fromDegrees(coordinate.getY(), coordinate.getX()).isValid();
        }

        static bool IsValidLine(LineString line)
        {
            var coordinates = line.getCoordinates();
            var vertices = ToPoints(coordinates, coordinates.Length);
            if (vertices is null)
                return false;

            // the list overload is an instance method on this S2 release, so the polyline has to exist first;
            // its constructor stores the vertices without checking them
            return new S2Polyline(ToList(vertices)).isValid();
        }

        static bool IsValidPolygon(Polygon polygon)
        {
            var loops = new java.util.ArrayList();

            var shell = ToLoop(polygon.getExteriorRing());
            if (shell is null)
                return false;

            loops.add(shell);

            for (var i = 0; i < polygon.getNumInteriorRing(); i++)
            {
                var hole = ToLoop(polygon.getInteriorRingN(i));
                if (hole is null)
                    return false;

                loops.add(hole);
            }

            return S2Polygon.isValid(loops);
        }

        readonly List<S2Point> points = [];
        readonly List<S2Point[]> paths = [];
        readonly java.util.ArrayList loops = new();
        S2Polygon? polygon;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        S2Geographies()
        {

        }

        /// <summary>
        /// The areal part, as one polygon, or <c>null</c> when the geography has no area.
        /// </summary>
        public S2Polygon? Polygon => polygon;

        /// <summary>
        /// Every vertex the geography names, whatever it belongs to.
        /// </summary>
        /// <remarks>
        /// The first vertex of a ring is named twice, since it arrives from JTS repeated at the end. Nothing
        /// that reads this cares: every use is a minimum or a containment test over the whole sequence.
        /// </remarks>
        public IEnumerable<S2Point> Vertices
        {
            get
            {
                foreach (var point in points)
                    yield return point;

                foreach (var path in paths)
                    foreach (var vertex in path)
                        yield return vertex;
            }
        }

        /// <summary>
        /// Every edge the geography names, as a pair of endpoints.
        /// </summary>
        /// <remarks>
        /// A line and a ring are the same thing here, because a ring arrives from JTS with its first
        /// coordinate repeated at the end: walking consecutive pairs closes it, and the closing edge is a
        /// real edge that a distance or an intersection can be nearest to.
        /// </remarks>
        public IEnumerable<(S2Point, S2Point)> Edges
        {
            get
            {
                foreach (var path in paths)
                    for (var i = 1; i < path.Length; i++)
                        yield return (path[i - 1], path[i]);
            }
        }

        void Add(Geometry geometry)
        {
            switch (geometry)
            {
                case Point point:
                    if (point.isEmpty() == false)
                        points.Add(ToPoint(point.getCoordinate()));

                    break;
                case LineString line:
                    if (line.isEmpty() == false)
                        AddPath(line);

                    break;
                case Polygon polygon:
                    if (polygon.isEmpty() == false)
                        Add(polygon);

                    break;
                case GeometryCollection collection:
                    for (var i = 0; i < collection.getNumGeometries(); i++)
                        Add(collection.getGeometryN(i));

                    break;
                default:
                    throw new NotSupportedException($"Cannot read '{geometry.getGeometryType()}' as a geography.");
            }
        }

        void Add(Polygon polygon)
        {
            AddRing(polygon.getExteriorRing());

            for (var i = 0; i < polygon.getNumInteriorRing(); i++)
                AddRing(polygon.getInteriorRingN(i));
        }

        void AddRing(LineString ring)
        {
            AddPath(ring);

            var loop = ToLoop(ring);
            if (loop is not null)
                loops.add(loop);
        }

        void AddPath(LineString path)
        {
            var coordinates = path.getCoordinates();
            var vertices = ToPoints(coordinates, coordinates.Length);
            if (vertices is not null)
                paths.Add(vertices);
        }

        /// <summary>
        /// Builds the areal part, once every ring has been read.
        /// </summary>
        /// <remarks>
        /// <c>S2Polygon.init</c> works out the nesting itself and reorders the loops, so a shell and its holes
        /// go in together in whatever order they arrived; what it wants is that each loop is normalized, which
        /// <see cref="ToLoop"/> has already done.
        /// </remarks>
        void Close()
        {
            if (loops.isEmpty())
                return;

            var built = new S2Polygon();
            built.init(loops);
            polygon = built;
        }

        /// <summary>
        /// Returns the distance between two geographies, in metres, or <see cref="double.NaN"/> if either is
        /// empty.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static double Distance(S2Geographies a, S2Geographies b)
        {
            var angle = Angle(a, b);
            return double.IsNaN(angle) ? double.NaN : angle * EarthRadiusMeters;
        }

        /// <summary>
        /// Returns whether two geographies are within the given distance in metres of one another.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <param name="distance"></param>
        /// <returns></returns>
        /// <remarks>
        /// The distance is computed rather than compared against a bound, so this is
        /// <c>Distance(a, b) &lt;= distance</c> and nothing cheaper. An early exit would want the indexed
        /// query this does not yet use.
        /// </remarks>
        public static bool DWithin(S2Geographies a, S2Geographies b, double distance)
        {
            var d = Distance(a, b);
            return double.IsNaN(d) == false && d <= distance;
        }

        /// <summary>
        /// Returns whether two geographies have any point in common.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static bool Intersects(S2Geographies a, S2Geographies b)
        {
            if (a.Polygon is not null && b.Polygon is not null && a.Polygon.intersects(b.Polygon))
                return true;

            if (Covers(a, b) || Covers(b, a))
                return true;

            foreach (var (a0, a1) in a.Edges)
                foreach (var (b0, b1) in b.Edges)
                    if (S2EdgeUtil.edgeOrVertexCrossing(a0, a1, b0, b1))
                        return true;

            // two point geographies, or a point on another's vertex, have no edges to cross
            foreach (var va in a.Vertices)
                foreach (var vb in b.Vertices)
                    if (va.equals(vb))
                        return true;

            return false;
        }

        /// <summary>
        /// Returns whether every point of <paramref name="a"/> lies in <paramref name="b"/>.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        /// <remarks>
        /// Containment against <paramref name="b"/>'s areal part: every vertex of <paramref name="a"/> inside
        /// it, and no edge of <paramref name="a"/> crossing its boundary. A geography with no area contains
        /// nothing, so <c>ST_GEOG_WITHIN</c> over two lines is false — which is what <c>ST_WITHIN</c> answers
        /// for two lines that are not equal, and not what it answers for two that are.
        ///
        /// <para>The boundary is where this and a store will disagree if they disagree at all, and it is
        /// exactly what the agreement measurement in the design issue has to settle before any of this may
        /// recheck a pushed-down predicate.</para>
        /// </remarks>
        public static bool Within(S2Geographies a, S2Geographies b)
        {
            if (b.Polygon is null)
                return false;

            var empty = true;

            foreach (var vertex in a.Vertices)
            {
                empty = false;

                if (b.Polygon.contains(vertex) == false)
                    return false;
            }

            if (empty)
                return false;

            foreach (var (a0, a1) in a.Edges)
                foreach (var (b0, b1) in b.Edges)
                    if (S2EdgeUtil.robustCrossing(a0, a1, b0, b1) > 0)
                        return false;

            return true;
        }

        /// <summary>
        /// Returns the angle between two geographies in radians, or <see cref="double.NaN"/> if either is
        /// empty.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        static double Angle(S2Geographies a, S2Geographies b)
        {
            if (Intersects(a, b))
                return 0;

            var min = double.NaN;

            foreach (var (a0, a1) in a.Edges)
                foreach (var (b0, b1) in b.Edges)
                    min = Least(min, S2EdgeUtil.getEdgePairDistance(a0, a1, b0, b1).toAngle().radians());

            foreach (var vertex in a.Vertices)
                foreach (var (b0, b1) in b.Edges)
                    min = Least(min, S2EdgeUtil.getDistance(vertex, b0, b1).radians());

            foreach (var vertex in b.Vertices)
                foreach (var (a0, a1) in a.Edges)
                    min = Least(min, S2EdgeUtil.getDistance(vertex, a0, a1).radians());

            // a pair of point geographies names no edge at all, so nothing above reaches them
            foreach (var va in a.Vertices)
                foreach (var vb in b.Vertices)
                    min = Least(min, new S1Angle(va, vb).radians());

            return min;
        }

        static double Least(double min, double candidate)
        {
            return double.IsNaN(min) || candidate < min ? candidate : min;
        }

        /// <summary>
        /// Returns whether <paramref name="b"/>'s areal part holds any vertex of <paramref name="a"/>.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        static bool Covers(S2Geographies a, S2Geographies b)
        {
            if (b.Polygon is null)
                return false;

            foreach (var vertex in a.Vertices)
                if (b.Polygon.contains(vertex))
                    return true;

            return false;
        }

        static S2Point ToPoint(Coordinate coordinate)
        {
            return S2LatLng.fromDegrees(coordinate.getY(), coordinate.getX()).toPoint();
        }

        /// <summary>
        /// Converts the first <paramref name="count"/> of a JTS coordinate sequence to S2 points, or
        /// <c>null</c> if any of them is not a place on the Earth.
        /// </summary>
        /// <param name="coordinates"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        static S2Point[]? ToPoints(Coordinate[] coordinates, int count)
        {
            var points = new S2Point[count];

            for (var i = 0; i < count; i++)
            {
                var latLng = S2LatLng.fromDegrees(coordinates[i].getY(), coordinates[i].getX());
                if (latLng.isValid() == false)
                    return null;

                points[i] = latLng.toPoint();
            }

            return points;
        }

        /// <summary>
        /// Hands S2 the vertex list its constructors take.
        /// </summary>
        /// <param name="points"></param>
        /// <returns></returns>
        static java.util.List ToList(S2Point[] points)
        {
            var list = new java.util.ArrayList(points.Length);

            foreach (var point in points)
                list.add(point);

            return list;
        }

        /// <summary>
        /// Converts a JTS ring to a normalized S2 loop, or <c>null</c> if it is not one.
        /// </summary>
        /// <param name="ring"></param>
        /// <returns></returns>
        /// <remarks>
        /// A loop's orientation is what says which side of it is the interior, and JTS carries no orientation
        /// S2 can trust — a shell and a hole are told apart by which ring of the polygon they are, not by
        /// their winding. <c>normalize</c> settles it by inverting any loop that covers more than half the
        /// sphere, which is right for every polygon that is not itself most of the Earth.
        /// </remarks>
        static S2Loop? ToLoop(LineString ring)
        {
            var coordinates = ring.getCoordinates();
            var count = coordinates.Length;

            // JTS repeats the first coordinate of a ring at the end and S2 does not
            if (count > 1 && coordinates[0].equals2D(coordinates[count - 1]))
                count--;

            if (count < 3)
                return null;

            var vertices = ToPoints(coordinates, count);
            if (vertices is null)
                return null;

            var loop = new S2Loop(ToList(vertices));
            if (loop.isValid() == false)
                return null;

            loop.normalize();
            return loop;
        }

    }

}
