using System;
using System.Collections.Generic;

using com.google.common.geometry;

using org.locationtech.jts.geom;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// A JTS geometry read as WGS84 and held as S2 points, polylines and a polygon, with the geodesic predicates and
    /// measurements the <c>CLR_ST_GEOG_*</c> functions are built on.
    /// </summary>
    /// <remarks>
    /// S2 models the Earth as a sphere and joins vertices with great-circle arcs. It decides topology here: whether
    /// shapes meet, which contains which, and which pair of points is closest. Lengths, distances and areas are then
    /// measured on the WGS84 ellipsoid by <see cref="Wgs84"/>.
    ///
    /// <para>The relations mean what Calcite's do, with the plane replaced by the sphere. Calcite's
    /// <c>ST_Within</c> is JTS <c>within</c>, the DE-9IM relation: every point of the first lies in the second and
    /// their interiors meet. So a point on a polygon's boundary is not within it, and a point at the end of a line is
    /// not within the line.</para>
    ///
    /// <para>A JTS coordinate's <c>x</c> is the longitude and <c>y</c> the latitude, the order WKT and GeoJSON
    /// use.</para>
    ///
    /// <para>The pairwise operations compare every edge of one shape with every edge of the other and are quadratic
    /// in the vertex counts; S2's indexed queries are not used.</para>
    /// </remarks>
    sealed class S2Geographies
    {

        /// <summary>
        /// The angle, in radians, within which two things are taken to touch.
        /// </summary>
        /// <remarks>
        /// Converting a coordinate to a unit vector rounds it, so a vertex written to lie on an edge does not land on it
        /// exactly, and an exact test would say <c>POINT(2 2)</c> is not on <c>LINESTRING(0 0, 4 4)</c>. 1e-13 radians
        /// is under a micrometre on the Earth, far above the rounding and far below any precision a store records.
        /// </remarks>
        const double Tolerance = 1e-13;

        /// <summary>
        /// The shortest length of edge, in radians, that counts as boundary shared by two geographies.
        /// </summary>
        /// <remarks>
        /// Where two lines meet at a vertex both name, the great-circle edges either side of it can cross the other
        /// line fractionally before and after that vertex, leaving a piece around a picoradian long that lies on both.
        /// This threshold, about six millimetres, keeps such a piece from being read as a shared stretch.
        /// </remarks>
        const double MinimumStretch = 1e-9;

        /// <summary>
        /// Reads a JTS geometry as WGS84 and builds its S2 shapes.
        /// </summary>
        /// <param name="geometry">The geometry.</param>
        /// <returns>The shapes.</returns>
        /// <exception cref="NotSupportedException">The geometry is of a kind that cannot be read.</exception>
        /// <remarks>
        /// A coordinate that is not a valid latitude and longitude drops the line or ring it belongs to, and a ring that
        /// S2 does not accept as a loop contributes edges but no area.
        /// </remarks>
        public static S2Geographies Of(Geometry geometry)
        {
            ArgumentNullException.ThrowIfNull(geometry);

            var self = new S2Geographies();
            self.Add(geometry);
            self.Close();
            return self;
        }

        /// <summary>
        /// Returns whether a geometry is a valid geography.
        /// </summary>
        /// <remarks>
        /// This is S2's validity rather than JTS's planar validity: every coordinate a valid latitude and longitude,
        /// every line a valid polyline, every polygon a valid set of loops, and the parts of a multi-polygon neither
        /// overlapping nor sharing an edge. A ring whose edges do not cross as straight lines in degrees may still cross
        /// as great-circle arcs.
        /// </remarks>
        /// <param name="geometry">The geometry to check.</param>
        /// <returns><c>true</c> if the geometry is valid on the sphere; an empty geometry is valid.</returns>
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
                case MultiPolygon multi:
                    return multi.isEmpty() || IsValidPolygonSet(multi);
                case GeometryCollection collection:
                    for (var i = 0; i < collection.getNumGeometries(); i++)
                        if (IsValid(collection.getGeometryN(i)) == false)
                            return false;

                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Returns whether a geometry touches itself only where JTS allows.
        /// </summary>
        /// <remarks>
        /// JTS's rule on geodesic edges: a point is simple; a multi-point is simple when no point repeats; a line when
        /// no two edges meet except where they join, its two ends being allowed to coincide; a polygon always, its
        /// self-intersections being a matter of validity; a collection when every part is.
        /// </remarks>
        /// <param name="geometry">The geometry to check.</param>
        /// <returns><c>true</c> if the geometry is simple.</returns>
        public static bool IsSimple(Geometry geometry)
        {
            ArgumentNullException.ThrowIfNull(geometry);

            switch (geometry)
            {
                case Point _:
                    return true;
                case MultiPoint multi:
                    return NoRepeatedPoint(multi);
                case LineString line:
                    return line.isEmpty() || IsSimpleLine(line);
                // an area's self-intersections are a question of validity rather than simplicity, which is
                // where JTS puts them too
                case Polygon _:
                case MultiPolygon _:
                    return true;
                case GeometryCollection collection:
                    for (var i = 0; i < collection.getNumGeometries(); i++)
                        if (IsSimple(collection.getGeometryN(i)) == false)
                            return false;

                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Returns whether a geometry is a non-empty line that is closed and simple.
        /// </summary>
        /// <param name="geometry">The geometry to check.</param>
        /// <returns><c>true</c> if the geometry is a non-empty line string that is closed and simple.</returns>
        public static bool IsRing(Geometry geometry)
        {
            ArgumentNullException.ThrowIfNull(geometry);

            return geometry is LineString line
                && line.isEmpty() == false
                && line.isClosed()
                && IsSimpleLine(line);
        }

        static bool NoRepeatedPoint(MultiPoint multi)
        {
            var seen = new java.util.HashSet();

            for (var i = 0; i < multi.getNumGeometries(); i++)
                if (seen.add(multi.getGeometryN(i).getCoordinate().toString()) == false)
                    return false;

            return true;
        }

        static bool IsSimpleLine(LineString line)
        {
            var vertices = ToPath(line);
            if (vertices is null || vertices.Length < 2)
                return false;

            var closed = vertices[0].Equals(vertices[^1]);
            var count = vertices.Length - 1;

            // a vertex reached twice is a self-touch, save for the one that closes a ring
            var seen = new java.util.HashSet();
            for (var i = 0; i < (closed ? count : vertices.Length); i++)
                if (seen.add(vertices[i].toString()) == false)
                    return false;

            for (var i = 0; i < count; i++)
            {
                for (var j = i + 1; j < count; j++)
                {
                    // edges joined at a vertex meet there by construction, and the first and last edge of a
                    // ring are joined the same way
                    if (j == i + 1 || (closed && i == 0 && j == count - 1))
                        continue;

                    if (S2EdgeUtil.edgeOrVertexCrossing(vertices[i], vertices[i + 1], vertices[j], vertices[j + 1]))
                        return false;
                }
            }

            return true;
        }

        static bool IsValidCoordinate(Coordinate coordinate)
        {
            return S2LatLng.fromDegrees(coordinate.getY(), coordinate.getX()).isValid();
        }

        static bool IsValidLine(LineString line)
        {
            var vertices = ToPath(line);

            // a line of one distinct place is no line, which S2 does not mind and JTS does. The length is
            // checked after the conversion because that is where a repeated coordinate is dropped, so
            // LINESTRING(0 0, 0 0) arrives here as one vertex rather than two.
            if (vertices is null || vertices.Length < 2)
                return false;

            // in S2 2.0.0 isValid is an instance method, so the polyline is constructed first; the constructor
            // stores the vertices without checking them
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

        /// <summary>
        /// Returns whether the parts of a multi-polygon are each valid, do not overlap, and share no edge.
        /// </summary>
        /// <remarks>
        /// JTS requires both of the last two. Giving S2 every ring of every part at once catches parts whose
        /// boundaries cross; <see cref="Separate"/> catches a part inside another, which <c>S2Polygon.init</c> would
        /// read as a hole, and parts sharing an edge. A geometry collection is not held to either rule, as in JTS, which
        /// validates its parts separately.
        /// </remarks>
        /// <param name="multi">The multi-polygon to check.</param>
        /// <returns><c>true</c> if every part is valid and no two overlap or share an edge; a multi-polygon whose parts are all
        /// empty is valid.</returns>
        static bool IsValidPolygonSet(MultiPolygon multi)
        {
            var loops = new java.util.ArrayList();
            var parts = new List<S2Geographies>();

            for (var i = 0; i < multi.getNumGeometries(); i++)
            {
                if (multi.getGeometryN(i) is not Polygon part)
                    return false;

                if (part.isEmpty())
                    continue;

                if (IsValidPolygon(part) == false)
                    return false;

                var shell = ToLoop(part.getExteriorRing());
                if (shell is null)
                    return false;

                loops.add(shell);

                for (var j = 0; j < part.getNumInteriorRing(); j++)
                {
                    var hole = ToLoop(part.getInteriorRingN(j));
                    if (hole is null)
                        return false;

                    loops.add(hole);
                }

                parts.Add(Of(part));
            }

            if (loops.isEmpty())
                return true;

            if (S2Polygon.isValid(loops) == false)
                return false;

            for (var i = 0; i < parts.Count; i++)
                for (var j = i + 1; j < parts.Count; j++)
                    if (Separate(parts[i], parts[j]) == false)
                        return false;

            return true;
        }

        /// <summary>
        /// Returns whether two parts of a multi-polygon have no interior in common and touch at most at points.
        /// </summary>
        /// <remarks>
        /// The interiors meet if any piece of either boundary lies in the interior of the other, which also covers one
        /// part lying wholly inside the other. Touching along a line is <see cref="SharesAnEdge"/>.
        /// </remarks>
        /// <param name="a">One part.</param>
        /// <param name="b">The other part.</param>
        /// <returns><c>true</c> if the interiors do not meet and the parts share no edge.</returns>
        static bool Separate(S2Geographies a, S2Geographies b)
        {
            foreach (var (p, q) in a.RingEdges)
                foreach (var piece in b.Pieces(p, q))
                    if (b.ContainsInterior(piece.Middle))
                        return false;

            foreach (var (p, q) in b.RingEdges)
                foreach (var piece in a.Pieces(p, q))
                    if (a.ContainsInterior(piece.Middle))
                        return false;

            return SharesAnEdge(a, b) == false;
        }

        /// <summary>
        /// Returns whether two geographies have a stretch of boundary in common, rather than meeting only at points.
        /// </summary>
        /// <remarks>
        /// Every edge of <paramref name="a"/> is cut wherever <paramref name="b"/> meets it; a piece longer than
        /// <see cref="MinimumStretch"/> whose middle lies on <paramref name="b"/> lies on it entirely.
        /// </remarks>
        /// <param name="a">The geography whose edges are cut.</param>
        /// <param name="b">The geography tested for running along them.</param>
        /// <returns><c>true</c> if a stretch of an edge of <paramref name="a"/> lies on <paramref name="b"/>.</returns>
        static bool SharesAnEdge(S2Geographies a, S2Geographies b)
        {
            foreach (var (p, q) in a.Edges)
                foreach (var piece in b.Pieces(p, q))
                    if (piece.Length > MinimumStretch && b.OnEdge(piece.Middle))
                        return true;

            return false;
        }

        readonly List<S2Point> points = [];
        readonly List<S2Point[]> lines = [];
        readonly List<S2Point[]> rings = [];
        readonly java.util.ArrayList loops = new();
        S2Polygon? polygon;

        /// <summary>
        /// Gets the polygon formed by the geography's rings, or <see langword="null"/> where it has none.
        /// </summary>
        public S2Polygon? Polygon => polygon;

        /// <summary>
        /// Creates an empty geography, which <see cref="Of"/> fills and closes.
        /// </summary>
        S2Geographies()
        {

        }

        /// <summary>
        /// Gets whether the geography has no points, lines or rings.
        /// </summary>
        public bool IsEmpty => points.Count == 0 && lines.Count == 0 && rings.Count == 0;

        /// <summary>
        /// Gets the dimension of the geography: 2 if it has rings, 1 if it has lines, 0 otherwise.
        /// </summary>
        /// <remarks>
        /// As JTS <c>getDimension</c>, a collection takes the largest dimension of its parts.
        /// </remarks>
        public int Dimension => rings.Count > 0 ? 2 : lines.Count > 0 ? 1 : 0;

        /// <summary>
        /// Gets every vertex of every point, line and ring.
        /// </summary>
        /// <remarks>
        /// A ring's first vertex appears twice, because JTS repeats it at the end. Every caller takes a minimum, a
        /// maximum or a containment test over the sequence, so the repeat has no effect.
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
        /// Gets every edge of every line and ring as a pair of endpoints.
        /// </summary>
        /// <remarks>
        /// A ring's first coordinate is repeated at its end, so walking consecutive pairs includes the closing edge.
        /// </remarks>
        public IEnumerable<(S2Point, S2Point)> Edges => EdgesOf(Paths);

        /// <summary>
        /// Gets every edge of every ring, which is the boundary of the polygon.
        /// </summary>
        public IEnumerable<(S2Point, S2Point)> RingEdges => EdgesOf(rings);

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
        /// Builds the polygon once every ring has been read.
        /// </summary>
        /// <remarks>
        /// <c>S2Polygon.init</c> works out which loops are holes of which and reorders them, so the loops go in as they
        /// arrived. It requires each loop to be normalized, which <see cref="ToLoop"/> has done.
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
        /// Returns the distance between two geographies in metres.
        /// </summary>
        /// <remarks>
        /// Zero where either is empty, as JTS <c>distance</c> (and so Calcite's <c>ST_Distance</c>) returns; PostGIS
        /// returns null there instead. Zero also where the two touch or one encloses the other. Otherwise the closest
        /// pair of points is chosen on the sphere and measured on the ellipsoid.
        /// </remarks>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>The distance in metres.</returns>
        public static double Distance(S2Geographies a, S2Geographies b)
        {
            if (a.IsEmpty || b.IsEmpty)
                return 0;

            // S2 decides whether they touch at all, and whether one encloses the other; only when they are
            // genuinely apart is there a distance to measure, and it is measured on the ellipsoid
            if (Angle(a, b) == 0)
                return 0;

            var pair = ClosestPair(a, b);

            return pair is null ? 0 : Wgs84.Distance(pair.Value.A, pair.Value.B);
        }

        /// <summary>
        /// Returns the pair of points, one on each geography, that are closest on the sphere.
        /// </summary>
        /// <returns>The pair, or <see langword="null"/> where either geography has no vertices.</returns>
        /// <remarks>
        /// Choosing the pair on the sphere and measuring it on the ellipsoid is not exact, but its error is second
        /// order in how far the chosen pair is from the ellipsoidal one.
        /// </remarks>
        /// <param name="a">The geography the first point of the pair lies on.</param>
        /// <param name="b">The geography the second point of the pair lies on.</param>
        public static (S2Point A, S2Point B)? ClosestPair(S2Geographies a, S2Geographies b)
        {
            var min = double.NaN;
            (S2Point A, S2Point B)? best = null;

            void Consider(S2Point p, S2Point q)
            {
                var angle = new S1Angle(p, q).radians();

                if (double.IsNaN(min) || angle < min)
                {
                    min = angle;
                    best = (p, q);
                }
            }

            // no edge-to-edge pass, and none is needed. Two edges that cross are zero apart and never reach
            // here, Angle having answered already; for two that do not, the least distance is attained at a
            // vertex of one and its projection on the other, which the passes below take. S2 does have a
            // four-argument getClosestPoint, and it is not an edge pair -- the fourth argument is the
            // precomputed normal a x b.
            foreach (var vertex in a.Vertices)
                foreach (var (b0, b1) in b.Edges)
                    Consider(vertex, S2EdgeUtil.getClosestPoint(vertex, b0, b1));

            foreach (var vertex in b.Vertices)
                foreach (var (a0, a1) in a.Edges)
                    Consider(S2EdgeUtil.getClosestPoint(vertex, a0, a1), vertex);

            foreach (var va in a.Vertices)
                foreach (var vb in b.Vertices)
                    Consider(va, vb);

            return best;
        }

        /// <summary>
        /// Returns whether two geographies are within a distance in metres of one another.
        /// </summary>
        /// <remarks>
        /// <see cref="Distance"/> compared with the bound, as Calcite's <c>ST_DWithin</c> is <c>distance &lt;= d</c>.
        /// </remarks>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <param name="distance">The bound in metres.</param>
        /// <returns><c>true</c> if the distance between them is at most <paramref name="distance"/>.</returns>
        public static bool DWithin(S2Geographies a, S2Geographies b, double distance)
        {
            return Distance(a, b) <= distance;
        }

        /// <summary>
        /// Returns whether two geographies have any point in common.
        /// </summary>
        /// <remarks>
        /// An empty geography intersects nothing, though <see cref="Distance"/> puts it at zero from everything; JTS
        /// answers both ways.
        /// </remarks>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns><c>true</c> if they share a point; <c>false</c> if either is empty.</returns>
        public static bool Intersects(S2Geographies a, S2Geographies b)
        {
            return a.IsEmpty == false && b.IsEmpty == false && Angle(a, b) == 0;
        }

        /// <summary>
        /// Returns whether <paramref name="a"/> lies within <paramref name="b"/>.
        /// </summary>
        /// <remarks>
        /// The DE-9IM relation JTS calls <c>within</c>: every point of <paramref name="a"/> lies in
        /// <paramref name="b"/>, and their interiors meet. The second condition is why a point on a polygon's boundary,
        /// or a line along its edge, is not within it.
        /// </remarks>
        /// <param name="a">The geography tested for lying within the other.</param>
        /// <param name="b">The containing geography.</param>
        /// <returns><c>true</c> if <paramref name="a"/> is within <paramref name="b"/>.</returns>
        public static bool Within(S2Geographies a, S2Geographies b)
        {
            return Covers(b, a) && MeetsInterior(a, b);
        }

        /// <summary>
        /// Returns whether every point of <paramref name="inner"/> lies in <paramref name="outer"/>, boundary included.
        /// </summary>
        /// <remarks>
        /// Each edge of <paramref name="inner"/> is cut wherever <paramref name="outer"/> meets it and the middle of
        /// every piece is tested, which is exact; see <see cref="Pieces"/>. Where <paramref name="inner"/> has area the
        /// rings of <paramref name="outer"/> are tested the same way against its interior, because a hole of
        /// <paramref name="outer"/> can lie wholly inside <paramref name="inner"/> without any edge of
        /// <paramref name="inner"/> going near it. Testing only the hole's vertices is not enough: a hole can have every
        /// corner on the boundary and still lie inside. Finally the areas are compared, to catch a polygon that is
        /// exactly a hole of the other.
        /// </remarks>
        /// <param name="outer">The covering geography.</param>
        /// <param name="inner">The geography tested for being covered.</param>
        /// <returns><c>true</c> if <paramref name="outer"/> covers <paramref name="inner"/>; <c>false</c> if either is
        /// empty.</returns>
        public static bool Covers(S2Geographies outer, S2Geographies inner)
        {
            if (outer.IsEmpty || inner.IsEmpty)
                return false;

            // nothing of a higher dimension lies inside something of a lower one
            if (inner.Dimension > outer.Dimension)
                return false;

            foreach (var vertex in inner.Vertices)
                if (outer.Holds(vertex) == false)
                    return false;

            foreach (var (p, q) in inner.Edges)
                foreach (var piece in outer.Pieces(p, q))
                    if (outer.Holds(piece.Middle) == false)
                        return false;

            if (inner.polygon is not null)
            {
                foreach (var (p, q) in outer.RingEdges)
                    foreach (var piece in inner.Pieces(p, q))
                        if (inner.ContainsInterior(piece.Middle))
                            return false;

                // and the areas, because a polygon that is exactly a hole of the other has every one of its
                // points on a boundary of it and none of its area inside it
                if (outer.polygon is null || Enclosed(outer.polygon, inner.polygon) == false)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Returns whether <paramref name="outer"/> holds all of <paramref name="inner"/>'s area, allowing for the
        /// sliver S2's snapping leaves.
        /// </summary>
        /// <param name="outer">The enclosing polygon.</param>
        /// <param name="inner">The polygon tested for lying inside it.</param>
        /// <returns><c>true</c> unless more than <see cref="SliverShare"/> of <paramref name="inner"/>'s area lies outside
        /// <paramref name="outer"/>.</returns>
        static bool Enclosed(S2Polygon outer, S2Polygon inner)
        {
            var intersection = new S2Polygon();
            intersection.initToIntersection(inner, outer);
            return intersection.getArea() >= inner.getArea() * (1 - SliverShare);
        }

        /// <summary>
        /// Returns whether <paramref name="a"/> contains <paramref name="b"/>: <see cref="Within"/> reversed.
        /// </summary>
        public static bool Contains(S2Geographies a, S2Geographies b)
        {
            return Within(b, a);
        }

        /// <summary>
        /// Returns whether every point of <paramref name="a"/> lies in <paramref name="b"/>: <see cref="Covers"/>
        /// reversed.
        /// </summary>
        public static bool CoveredBy(S2Geographies a, S2Geographies b)
        {
            return Covers(b, a);
        }

        /// <summary>
        /// Returns whether two geographies are the same set of points, each covering the other.
        /// </summary>
        /// <remarks>
        /// Topological equality: a line and the same line reversed are equal.
        /// </remarks>
        public static bool Equals(S2Geographies a, S2Geographies b)
        {
            return Covers(a, b) && Covers(b, a);
        }

        /// <summary>
        /// Returns whether two geographies have no point in common: the negation of <see cref="Intersects"/>.
        /// </summary>
        public static bool Disjoint(S2Geographies a, S2Geographies b)
        {
            return Intersects(a, b) == false;
        }



















        /// <summary>
        /// Returns whether the interior of <paramref name="a"/>, already known to lie in <paramref name="b"/>, meets the
        /// interior of <paramref name="b"/>.
        /// </summary>
        /// <param name="a">A geography already known to lie in <paramref name="b"/>.</param>
        /// <param name="b">The containing geography.</param>
        /// <returns><c>true</c> if the interiors meet.</returns>
        static bool MeetsInterior(S2Geographies a, S2Geographies b)
        {
            switch (b.Dimension)
            {
                case 2:
                    // every point of a is in b by here, but a can be exactly a hole of b, its boundary on b's
                    // and its interior outside b; walking boundaries cannot tell that from a being inside, so the
                    // areas are compared
                    if (a.Dimension == 2)
                        return a.polygon is not null && b.polygon is not null && Overlaps(a.polygon, b.polygon);

                    // every part of a, not only the parts of its own dimension: a collection of a point and a
                    // line is one-dimensional, and it is within a polygon whose boundary its line runs along
                    // as long as the point is inside, because the point is part of the interior of a too
                    foreach (var point in a.points)
                        if (b.ContainsInterior(point))
                            return true;

                    foreach (var (p, q) in a.Edges)
                        foreach (var piece in b.Pieces(p, q))
                            if (b.ContainsInterior(piece.Middle))
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

        // There is no CROSSES, TOUCHES, OVERLAPS or CONTAINSPROPERLY. Each depends on whether the interiors of
        // two geographies meet, and where a line comes back and touches itself that point is both an end of the
        // line and the middle of one of its edges, so boundary by one rule and interior by the other. Neither
        // choice agrees with JTS everywhere: treating it as an end makes some crossings read as touches, and
        // treating it as interior makes some touches read as crossings. JTS decides it with a node graph built
        // over both geometries, which S2's Java library (having no S2BooleanOperation) does not provide.

        /// <summary>
        /// Returns whether a point lies in this geography, boundary included.
        /// </summary>
        /// <param name="point">The point, as a unit vector.</param>
        /// <returns><c>true</c> if the point is inside the polygon, on an edge, or at one of the isolated points.</returns>
        public bool Holds(S2Point point)
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
        /// Returns whether a point lies in the interior of this geography's polygon.
        /// </summary>
        /// <remarks>
        /// <c>S2Polygon.contains</c> is half-open on the boundary, answering differently on either side of a shared
        /// vertex, so points on a ring are excluded explicitly.
        /// </remarks>
        /// <param name="point">The point, as a unit vector.</param>
        /// <returns><c>true</c> if the point is inside the polygon and on none of its rings.</returns>
        public bool ContainsInterior(S2Point point)
        {
            return polygon is not null && polygon.contains(point) && OnEdge(rings, point) == false;
        }

        /// <summary>
        /// Returns whether a point lies on an edge of this geography, within <see cref="Tolerance"/>.
        /// </summary>
        /// <param name="point">The point, as a unit vector.</param>
        /// <returns><c>true</c> if the point is within <see cref="Tolerance"/> of an edge of a line or ring.</returns>
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
        /// Returns whether a point is on the boundary of this geography's lines.
        /// </summary>
        /// <remarks>
        /// JTS's mod-2 rule: an endpoint met by an odd number of line ends is on the boundary, so two lines joined end to
        /// end have no boundary at the join and a closed line has none. Rings have no boundary and are not counted.
        /// </remarks>
        /// <param name="point">The point, as a unit vector.</param>
        /// <returns><c>true</c> if an odd number of line ends lie at the point.</returns>
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
        /// Cuts the edge from <paramref name="p"/> to <paramref name="q"/> wherever this geography meets it, and returns
        /// the middle and angular length of each piece.
        /// </summary>
        /// <remarks>
        /// An edge can pass from inside this geography to outside only where it crosses an edge of it or meets one of its
        /// vertices; a stretch that runs along an edge starts and ends at that edge's vertices. So each piece is wholly
        /// inside or wholly outside, and testing its middle is exact.
        /// </remarks>
        /// <param name="p">The start of the edge.</param>
        /// <param name="q">The end of the edge.</param>
        /// <returns>Each piece's middle point and its length in radians, in order from <paramref name="p"/>, pieces of
        /// negligible length omitted.</returns>
        public IEnumerable<(S2Point Middle, double Length)> Pieces(S2Point p, S2Point q)
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

            // Two cuts that fall in the same place — a crossing that is also a vertex of this geography, say
            // — would otherwise leave a piece of no length between them, and the middle of a piece of no
            // length is the cut itself, which lies on this geography by construction. A caller asking whether
            // a stretch of edge runs along this one would then be told yes by a stretch that is a point.
            const double Together = 1e-9;

            var whole = new S1Angle(p, q).radians();

            for (var i = 1; i < cuts.Count; i++)
                if (cuts[i] - cuts[i - 1] > Together)
                    yield return (S2EdgeUtil.interpolate((cuts[i - 1] + cuts[i]) / 2, p, q), (cuts[i] - cuts[i - 1]) * whole);
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
        /// Returns the area of the geography's polygon in square metres, measured on the WGS84 ellipsoid.
        /// </summary>
        /// <remarks>
        /// Zero for a geography with no polygon, as JTS gives a line no area.
        /// </remarks>
        /// <param name="g">The geography.</param>
        /// <returns>The area in square metres.</returns>
        public static double Area(S2Geographies g)
        {
            return g.polygon is null ? 0 : Wgs84.Area(g.polygon);
        }

        /// <summary>
        /// Returns the length of every edge of the geography in metres, measured on the WGS84 ellipsoid.
        /// </summary>
        /// <remarks>
        /// Polygon rings are included, as JTS <c>getLength</c> includes them.
        /// </remarks>
        /// <param name="g">The geography.</param>
        /// <returns>The length in metres.</returns>
        public static double Length(S2Geographies g)
        {
            return Wgs84.Length(g.Edges);
        }

        /// <summary>
        /// Returns the length of the geography's rings in metres, measured on the WGS84 ellipsoid.
        /// </summary>
        /// <param name="g">The geography.</param>
        /// <returns>The perimeter in metres; zero where the geography has no rings.</returns>
        public static double Perimeter(S2Geographies g)
        {
            return Wgs84.Length(g.RingEdges);
        }

        /// <summary>
        /// Returns the greatest distance in metres between a vertex of one geography and a vertex of the other.
        /// </summary>
        /// <remarks>
        /// Only vertices are compared, as Calcite's <c>ST_MaxDistance</c> compares only coordinates.
        /// </remarks>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>The greatest distance in metres; zero where either has no vertices.</returns>
        public static double MaxDistance(S2Geographies a, S2Geographies b)
        {
            var max = 0.0;

            foreach (var va in a.Vertices)
                foreach (var vb in b.Vertices)
                    max = System.Math.Max(max, Wgs84.Distance(va, vb));

            return max;
        }

        /// <summary>
        /// Returns whether the latitude-longitude rectangles bounding two geographies meet.
        /// </summary>
        /// <remarks>
        /// The rectangles are S2's (see <see cref="Bound"/>), so they are correct across the antimeridian and include the
        /// poleward bulge of great-circle edges.
        /// </remarks>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns><c>true</c> if the rectangles, each widened by <see cref="Tolerance"/>, meet; <c>false</c> if either
        /// geography is empty.</returns>
        public static bool EnvelopesIntersect(S2Geographies a, S2Geographies b)
        {
            if (a.IsEmpty || b.IsEmpty)
                return false;

            // expanded, because the two bounds are built from unit vectors and compared as latitudes and
            // longitudes, and boxes that meet exactly at a corner can miss each other by the last bit of that
            // round trip. The margin is the same angle everything else here treats as touching.
            var margin = S2LatLng.fromRadians(Tolerance, Tolerance);

            return a.Bound().expanded(margin).intersects(b.Bound().expanded(margin));
        }

        /// <summary>
        /// Returns the smallest latitude-longitude rectangle containing the geography.
        /// </summary>
        /// <remarks>
        /// An <c>S2LatLngRect</c>'s longitude interval can wrap, so a shape straddling the antimeridian gets a narrow
        /// rectangle rather than one spanning most of the globe. It is built from the edges, so it includes the part of
        /// a great-circle edge that bulges beyond its endpoints' latitudes.
        /// </remarks>
        /// <returns>The bounding rectangle; empty for an empty geography.</returns>
        public S2LatLngRect Bound()
        {
            var bound = S2LatLngRect.empty();

            foreach (var point in points)
                bound = bound.addPoint(point);

            foreach (var line in lines)
                bound = bound.union(new S2Polyline(ToList(line)).getRectBound());

            if (polygon is not null)
                bound = bound.union(polygon.getRectBound());

            return bound;
        }

        /// <summary>
        /// Returns the angle in radians between two non-empty geographies: zero where they touch or one encloses the
        /// other.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>The angle in radians; zero where they are within <see cref="Tolerance"/> or one encloses the
        /// other.</returns>
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
        /// Returns the centroid of the geography as a point on the sphere, or <see langword="null"/> where it has none.
        /// </summary>
        /// <remarks>
        /// JTS's dimensional rule: a geography with a polygon is answered by the polygon's centroid, one with lines by
        /// the length-weighted mean of its edge midpoints, and otherwise by the mean of its vertices. Each is a mean of
        /// directions from the Earth's centre, which stays correct across the antimeridian where a mean of longitudes
        /// does not. Where the directions cancel, as for two antipodal points, there is no centroid.
        /// </remarks>
        /// <param name="g">The geography.</param>
        /// <returns>The centroid as a unit vector, or <see langword="null"/>.</returns>
        public static S2Point? Centroid(S2Geographies g)
        {
            if (g.polygon is not null && g.polygon.numLoops() > 0)
                return Normalized(g.polygon.getCentroid(), g.polygon.getArea());

            var sum = new S2Point(0, 0, 0);
            var weight = 0.0;

            foreach (var (p, q) in g.Edges)
            {
                // weighted by the length of the edge it stands for, so a long edge counts for more
                var length = new S1Angle(p, q).radians();

                sum = S2Point.add(sum, S2Point.mul(S2Point.add(p, q).normalize(), length));
                weight += length;
            }

            if (weight > 0)
                return Normalized(sum, weight);

            foreach (var vertex in g.Vertices)
            {
                sum = S2Point.add(sum, vertex);
                weight += 1;
            }

            return weight > 0 ? Normalized(sum, weight) : null;
        }

        /// <summary>
        /// Returns the direction of a weighted sum of directions, or <see langword="null"/> where the sum is negligible.
        /// </summary>
        /// <param name="sum">The weighted sum.</param>
        /// <param name="weight">The total weight that went into it.</param>
        /// <remarks>
        /// Negligible is judged against the weight rather than against zero. Two antipodal points leave a sum of about
        /// 1e-16 in floating point rather than exactly zero, while a very small polygon has a small sum and a
        /// well-defined centre; the ratio tells the two apart.
        /// </remarks>
        /// <returns>The unit vector in the direction of <paramref name="sum"/>, or <see langword="null"/>.</returns>
        static S2Point? Normalized(S2Point sum, double weight)
        {
            return sum.norm() <= weight * 1e-12 ? null : sum.normalize();
        }

        /// <summary>
        /// Returns the least angle between any part of one geography and any part of the other, ignoring whether one
        /// encloses the other.
        /// </summary>
        /// <param name="a">The first geography.</param>
        /// <param name="b">The second geography.</param>
        /// <returns>The least angle in radians, or <see cref="double.NaN"/> where either has no vertices.</returns>
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
        /// Returns whether the polygon of <paramref name="a"/> holds any vertex of <paramref name="b"/> or meets the
        /// polygon of <paramref name="b"/>.
        /// </summary>
        /// <param name="a">The geography whose polygon is tested.</param>
        /// <param name="b">The geography tested for lying in it.</param>
        /// <returns><c>true</c> if it does; <c>false</c> where <paramref name="a"/> has no polygon.</returns>
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
        /// The share of a polygon's area below which an intersection is taken to be an artefact of S2's snapping rather
        /// than an overlap.
        /// </summary>
        /// <remarks>
        /// Two polygons that only share an edge can leave an intersection whose area is roughly the length of the shared
        /// boundary times S2's snap radius, around a billionth of the polygons' area for the shapes this is used on. A
        /// genuine overlap is a much larger share. The cost is that an overlap thinner than a millionth of the polygon's
        /// area is reported as touching.
        /// </remarks>
        const double SliverShare = 1e-6;

        /// <summary>
        /// Returns whether two polygons share area rather than only boundary.
        /// </summary>
        /// <remarks>
        /// S2 snaps while it intersects, so polygons that only share an edge can leave a sliver; the intersection's area
        /// is compared against <see cref="SliverShare"/> of <paramref name="a"/>'s rather than tested for emptiness.
        /// </remarks>
        /// <param name="a">The polygon whose area sets the threshold.</param>
        /// <param name="b">The other polygon.</param>
        /// <returns><c>true</c> if the intersection's area exceeds <see cref="SliverShare"/> of <paramref name="a"/>'s.</returns>
        static bool Overlaps(S2Polygon a, S2Polygon b)
        {
            var intersection = new S2Polygon();
            intersection.initToIntersection(a, b);
            return intersection.getArea() > a.getArea() * SliverShare;
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
        /// Converts the first <paramref name="count"/> coordinates to S2 points, or returns <c>null</c> if any is not a
        /// valid latitude and longitude.
        /// </summary>
        /// <param name="coordinates">The coordinates, longitude in x and latitude in y.</param>
        /// <param name="count">How many of them to convert, from the first.</param>
        /// <returns>The points with consecutive repeats removed, or <c>null</c>.</returns>
        static S2Point[]? ToPoints(Coordinate[] coordinates, int count)
        {
            var points = new List<S2Point>(count);

            for (var i = 0; i < count; i++)
            {
                var latLng = S2LatLng.fromDegrees(coordinates[i].getY(), coordinates[i].getX());
                if (latLng.isValid() == false)
                    return null;

                var point = latLng.toPoint();

                // a repeated coordinate is a zero-length edge, which makes S2Loop and S2Polyline invalid; JTS
                // treats the repeat as harmless, so it is dropped here rather than costing a valid polygon its
                // area
                if (points.Count > 0 && points[^1].equals(point))
                    continue;

                points.Add(point);
            }

            return [.. points];
        }

        /// <summary>
        /// Returns the points as the <c>java.util.List</c> S2's constructors take.
        /// </summary>
        /// <param name="points">The points.</param>
        /// <returns>A new list holding the points in order.</returns>
        static java.util.List ToList(S2Point[] points)
        {
            var list = new java.util.ArrayList(points.Length);

            foreach (var point in points)
                list.add(point);

            return list;
        }

        /// <summary>
        /// Converts a JTS ring to a normalized S2 loop, or returns <c>null</c> if it is not a valid loop.
        /// </summary>
        /// <remarks>
        /// A loop's orientation says which side is inside, and JTS rings carry no orientation S2 can rely on.
        /// <c>normalize</c> inverts any loop that covers more than half the sphere, which is right for every polygon
        /// smaller than a hemisphere.
        /// </remarks>
        /// <param name="ring">The ring, its first coordinate repeated at the end or not.</param>
        /// <returns>The loop, or <c>null</c>.</returns>
        static S2Loop? ToLoop(LineString ring)
        {
            var coordinates = ring.getCoordinates();
            var count = coordinates.Length;

            // JTS repeats the first coordinate of a ring at the end and S2 does not
            if (count > 1 && coordinates[0].equals2D(coordinates[count - 1]))
                count--;

            var vertices = ToPoints(coordinates, count);

            // after the conversion, because a repeated coordinate is dropped there and a ring written with
            // one has fewer vertices than it has coordinates
            if (vertices is null || vertices.Length < 3)
                return null;

            var loop = new S2Loop(ToList(vertices));
            if (loop.isValid() == false)
                return null;

            loop.normalize();
            return loop;
        }

    }

}
