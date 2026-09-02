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
    /// <para>What the operations <em>mean</em> is what Calcite means, because Calcite's spatial functions are
    /// the specification with the plane swapped for the sphere: <c>ST_Within</c> is
    /// <c>geom1.within(geom2)</c> and <c>ST_Intersects</c> is <c>geom1.intersects(geom2)</c>, which are JTS
    /// words with JTS definitions. <c>within</c> is not containment — it is the DE-9IM relation, so every
    /// point of the one must lie in the other <em>and</em> their interiors must meet, which is why a point on
    /// a polygon's boundary is not within the polygon and a point at the end of a line is not within the
    /// line. <c>GeographyDifferentialTests</c> runs every operation against Calcite's over shapes small
    /// enough that the sphere and the plane must agree, which is what holds this to that specification.</para>
    ///
    /// <para>A JTS coordinate is <c>(x, y)</c> and WGS84 order is longitude then latitude, so <c>x</c> is the
    /// longitude and <c>y</c> is the latitude — the order <c>POINT(lng lat)</c> in WKT and GeoJSON.</para>
    ///
    /// <para>The pairwise operations are quadratic in the vertex counts: every edge of one against every edge
    /// of the other. S2 has indexed forms of these queries and this does not use them. That is deliberate for
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
        /// The angle below which two things are taken to touch, in radians.
        /// </summary>
        /// <remarks>
        /// A coordinate arrives as a pair of doubles and leaves as a unit vector, so a vertex written to lie
        /// on an edge does not land on it exactly, and a predicate that asked for an exact zero would answer
        /// no to <c>POINT(2 2)</c> on <c>LINESTRING(0 0, 4 4)</c>. This is about four tenths of a micrometre
        /// on the Earth: far below any coordinate a store records, and some nine orders of magnitude above
        /// the rounding of the conversion.
        /// </remarks>
        const double Tolerance = 1e-13;

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
            var vertices = ToPath(line);
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
        readonly List<S2Point[]> lines = [];
        readonly List<S2Point[]> rings = [];
        readonly java.util.ArrayList loops = new();
        S2Polygon? polygon;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        S2Geographies()
        {

        }

        /// <summary>
        /// Whether the geography names nothing at all.
        /// </summary>
        public bool IsEmpty => points.Count == 0 && lines.Count == 0 && rings.Count == 0;

        /// <summary>
        /// The dimension of the geography: two if it has area, one if it has length, zero otherwise.
        /// </summary>
        /// <remarks>
        /// <c>Geometry.getDimension</c>, which for a collection is the largest of its parts. It decides which
        /// relations are possible at all — nothing of a higher dimension lies within something of a lower one.
        /// </remarks>
        public int Dimension => rings.Count > 0 ? 2 : lines.Count > 0 ? 1 : 0;

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

                foreach (var path in Paths)
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
        public IEnumerable<(S2Point, S2Point)> Edges => EdgesOf(Paths);

        IEnumerable<S2Point[]> Paths
        {
            get
            {
                foreach (var line in lines)
                    yield return line;

                foreach (var ring in rings)
                    yield return ring;
            }
        }

        static IEnumerable<(S2Point, S2Point)> EdgesOf(IEnumerable<S2Point[]> paths)
        {
            foreach (var path in paths)
                for (var i = 1; i < path.Length; i++)
                    yield return (path[i - 1], path[i]);
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
                        AddLine(line);

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

        void AddLine(LineString line)
        {
            var vertices = ToPath(line);
            if (vertices is not null)
                lines.Add(vertices);
        }

        void AddRing(LineString ring)
        {
            var vertices = ToPath(ring);
            if (vertices is not null)
                rings.Add(vertices);

            var loop = ToLoop(ring);
            if (loop is not null)
                loops.add(loop);
        }

        static S2Point[]? ToPath(LineString path)
        {
            var coordinates = path.getCoordinates();
            return ToPoints(coordinates, coordinates.Length);
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
        /// Returns the distance between two geographies, in metres.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        /// <remarks>
        /// An empty geography is zero away from everything, which is JTS answering its own question:
        /// <c>DistanceOp</c> returns zero the moment either side is empty, and <c>ST_Distance</c> is
        /// <c>geom1.distance(geom2)</c>. PostGIS answers null there instead. Calcite is the specification
        /// this mirrors, so a caller who writes the two functions side by side gets the same shape of answer
        /// from both and the only difference between them is the one this package exists for.
        /// </remarks>
        public static double Distance(S2Geographies a, S2Geographies b)
        {
            if (a.IsEmpty || b.IsEmpty)
                return 0;

            return Angle(a, b) * EarthRadiusMeters;
        }

        /// <summary>
        /// Returns whether two geographies are within the given distance in metres of one another.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <param name="distance"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>ST_DWithin</c> is <c>geom1.distance(geom2) &lt;= distance</c> and nothing cheaper, so this is
        /// the same. An early exit would want the indexed query this does not yet use.
        /// </remarks>
        public static bool DWithin(S2Geographies a, S2Geographies b, double distance)
        {
            return Distance(a, b) <= distance;
        }

        /// <summary>
        /// Returns whether two geographies have any point in common.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        /// <remarks>
        /// An empty geography intersects nothing, where it is zero away from everything. The two are not in
        /// tension and both are JTS: an empty set shares no point with anything, and a distance to one is
        /// answered before the geometry is looked at.
        /// </remarks>
        public static bool Intersects(S2Geographies a, S2Geographies b)
        {
            return a.IsEmpty == false && b.IsEmpty == false && Angle(a, b) == 0;
        }

        /// <summary>
        /// Returns whether <paramref name="a"/> lies within <paramref name="b"/>.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        /// <remarks>
        /// The DE-9IM relation JTS means by the word, which is two conditions rather than one: every point of
        /// <paramref name="a"/> lies in <paramref name="b"/>, and their interiors meet. The second is what
        /// makes a point on a polygon's boundary not within it, and a line lying along a polygon's edge not
        /// within it either.
        ///
        /// <para>An edge of <paramref name="a"/> is checked by cutting it wherever the boundary of
        /// <paramref name="b"/> meets it and testing the middle of each piece — exact rather than a sample;
        /// see <see cref="Pieces"/>.</para>
        ///
        /// <para>Where a store will disagree, if it disagrees at all, is on the boundary itself, and that is
        /// exactly what the agreement measurement in the design issue has to settle before any of this may
        /// recheck a pushed-down predicate.</para>
        /// </remarks>
        public static bool Within(S2Geographies a, S2Geographies b)
        {
            if (a.IsEmpty || b.IsEmpty)
                return false;

            // nothing of a higher dimension lies inside something of a lower one
            if (a.Dimension > b.Dimension)
                return false;

            foreach (var vertex in a.Vertices)
                if (b.Contains(vertex) == false)
                    return false;

            foreach (var (p, q) in a.Edges)
                foreach (var middle in b.Pieces(p, q))
                    if (b.Contains(middle) == false)
                        return false;

            // the boundary of a says nothing about a hole of b lying wholly inside a: no edge of a goes near
            // one, so the areal parts have to be compared as areas rather than as boundaries
            if (a.polygon is not null && (b.polygon is null || Encloses(b.polygon, a.polygon) == false))
                return false;

            return MeetsInterior(a, b);
        }

        /// <summary>
        /// Returns whether the interior of <paramref name="a"/>, already known to lie in
        /// <paramref name="b"/>, meets the interior of <paramref name="b"/>.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        static bool MeetsInterior(S2Geographies a, S2Geographies b)
        {
            switch (b.Dimension)
            {
                case 2:
                    // an areal part of a lies in b and has interior of its own, so the two interiors meet
                    if (a.Dimension == 2)
                        return true;

                    if (a.Dimension == 1)
                    {
                        foreach (var (p, q) in a.Edges)
                            foreach (var middle in b.Pieces(p, q))
                                if (b.ContainsInterior(middle))
                                    return true;

                        return false;
                    }

                    foreach (var point in a.points)
                        if (b.ContainsInterior(point))
                            return true;

                    return false;

                case 1:
                    // a lies on b and has length of its own, and the boundary of b is finitely many points
                    if (a.Dimension == 1)
                        return true;

                    foreach (var point in a.points)
                        if (b.OnBoundary(point) == false)
                            return true;

                    return false;

                default:
                    // b is a set of points and a is contained in it; a point is its own interior
                    return true;
            }
        }

        /// <summary>
        /// Returns whether the given point lies in this geography, boundary included.
        /// </summary>
        /// <param name="point"></param>
        /// <returns></returns>
        public bool Contains(S2Point point)
        {
            if (polygon is not null && polygon.contains(point))
                return true;

            if (OnEdge(point))
                return true;

            foreach (var isolated in points)
                if (Near(isolated, point))
                    return true;

            return false;
        }

        /// <summary>
        /// Returns whether the given point lies in the interior of the areal part of this geography.
        /// </summary>
        /// <param name="point"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>S2Polygon.contains</c> is half-open on the boundary — it answers one way along one edge of a
        /// shared vertex and the other way along the next — so the boundary is excluded here rather than
        /// trusted to it.
        /// </remarks>
        public bool ContainsInterior(S2Point point)
        {
            return polygon is not null && polygon.contains(point) && OnEdge(rings, point) == false;
        }

        /// <summary>
        /// Returns whether the given point is on an edge of this geography.
        /// </summary>
        /// <param name="point"></param>
        /// <returns></returns>
        public bool OnEdge(S2Point point)
        {
            return OnEdge(Paths, point);
        }

        static bool OnEdge(IEnumerable<S2Point[]> paths, S2Point point)
        {
            foreach (var (p, q) in EdgesOf(paths))
                if (S2EdgeUtil.getDistance(point, p, q).radians() <= Tolerance)
                    return true;

            return false;
        }

        /// <summary>
        /// Returns whether the given point is on the boundary of the linear part of this geography.
        /// </summary>
        /// <param name="point"></param>
        /// <returns></returns>
        /// <remarks>
        /// The mod-2 rule JTS uses: an endpoint that an odd number of line ends meet is a boundary point, so
        /// two lines joined end to end have none where they join and a closed line has none at all. A ring has
        /// no boundary either, which is why only the lines are counted.
        /// </remarks>
        public bool OnBoundary(S2Point point)
        {
            var ends = 0;

            foreach (var line in lines)
            {
                if (Near(line[0], point))
                    ends++;

                if (Near(line[^1], point))
                    ends++;
            }

            return ends % 2 == 1;
        }

        /// <summary>
        /// Returns the middle of each piece the given edge is cut into by this geography.
        /// </summary>
        /// <param name="p"></param>
        /// <param name="q"></param>
        /// <returns></returns>
        /// <remarks>
        /// An edge passes from inside this geography to outside it only where it meets an edge of it, and it
        /// meets one only at a proper crossing or at a vertex of this geography lying on the edge — a stretch
        /// that merely runs along an edge starts and ends at that edge's own vertices. So the middle of a
        /// piece stands for the whole piece, and testing the middles is exact rather than a sample.
        /// </remarks>
        public IEnumerable<S2Point> Pieces(S2Point p, S2Point q)
        {
            var cuts = new List<double> { 0, 1 };

            foreach (var (r, s) in Edges)
            {
                if (S2EdgeUtil.robustCrossing(p, q, r, s) > 0)
                    Cut(cuts, p, q, S2EdgeUtil.getIntersection(p, q, r, s));

                Cut(cuts, p, q, r);
                Cut(cuts, p, q, s);
            }

            foreach (var point in points)
                Cut(cuts, p, q, point);

            cuts.Sort();

            for (var i = 1; i < cuts.Count; i++)
                yield return S2EdgeUtil.interpolate((cuts[i - 1] + cuts[i]) / 2, p, q);
        }

        static void Cut(List<double> cuts, S2Point p, S2Point q, S2Point vertex)
        {
            if (S2EdgeUtil.getDistance(vertex, p, q).radians() > Tolerance)
                return;

            var fraction = S2EdgeUtil.getDistanceFraction(vertex, p, q);
            if (fraction > 0 && fraction < 1)
                cuts.Add(fraction);
        }

        /// <summary>
        /// Returns the angle between two geographies in radians. Neither may be empty.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        static double Angle(S2Geographies a, S2Geographies b)
        {
            var min = MinAngle(a, b);
            if (min <= Tolerance)
                return 0;

            // the nearest edges of two shapes are far apart when one is wholly inside the other
            if (Encloses(a, b) || Encloses(b, a))
                return 0;

            return min;
        }

        /// <summary>
        /// Returns the least angle between any part of one geography and any part of the other, without
        /// regard to whether one encloses the other.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        static double MinAngle(S2Geographies a, S2Geographies b)
        {
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
        /// Returns whether the areal part of <paramref name="a"/> holds any of <paramref name="b"/>.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        static bool Encloses(S2Geographies a, S2Geographies b)
        {
            if (a.polygon is null)
                return false;

            foreach (var vertex in b.Vertices)
                if (a.polygon.contains(vertex))
                    return true;

            return b.polygon is not null && a.polygon.intersects(b.polygon);
        }

        /// <summary>
        /// Returns whether one polygon covers another, boundary sharing allowed.
        /// </summary>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>S2Polygon.contains</c> alone will not do. It is strict about a shared boundary, so two squares
        /// meeting the corners of a hole from opposite sides are not contained by the polygon they sit in,
        /// though every point of them is. Taking the difference answers the question that was asked — is
        /// there any part of the inner polygon outside the outer one — and <c>contains</c> is kept in front
        /// of it because it settles the ordinary nested case without building a polygon.
        /// </remarks>
        static bool Encloses(S2Polygon outer, S2Polygon inner)
        {
            if (outer.contains(inner))
                return true;

            var difference = new S2Polygon();
            difference.initToDifference(inner, outer);
            return difference.isEmpty();
        }

        static bool Near(S2Point a, S2Point b)
        {
            return a.equals(b) || new S1Angle(a, b).radians() <= Tolerance;
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
        /// The orientation of a loop is what says which side of it is the interior, and JTS carries no
        /// orientation S2 can trust — a shell and a hole are told apart by which ring of the polygon they are,
        /// not by their winding. <c>normalize</c> settles it by inverting any loop that covers more than half
        /// the sphere, which is right for every polygon that is not itself most of the Earth.
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
