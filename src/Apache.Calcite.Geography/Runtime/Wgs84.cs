using System.Collections.Generic;

using com.google.common.geometry;

using net.sf.geographiclib;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// Geodesic measurements on the WGS84 ellipsoid, using GeographicLib's implementation of Karney's algorithms.
    /// </summary>
    /// <remarks>
    /// S2 treats the Earth as a sphere, which is close enough to decide topology but not to measure: a spherical
    /// distance differs from the ellipsoidal one by up to about half a percent. At the equator one degree of
    /// longitude is 111319.49 m and one degree of latitude is 110574.39 m, which a sphere cannot reproduce. So S2
    /// chooses which points to measure between and this class measures them. Choosing the closest pair on the
    /// sphere introduces an error second order in the distance between that pair and the true closest pair.
    ///
    /// <para>Predicates stay entirely on S2. The sphere moves an edge far less than it changes a length, and
    /// containment depends on which side of an edge a point lies, not on how long the edge is.</para>
    /// </remarks>
    static class Wgs84
    {

        /// <summary>
        /// Returns the geodesic distance between two points in metres.
        /// </summary>
        /// <param name="a">The first point, as a unit vector.</param>
        /// <param name="b">The second point, as a unit vector.</param>
        /// <returns>The distance in metres on the WGS84 ellipsoid.</returns>
        public static double Distance(S2Point a, S2Point b)
        {
            var p = new S2LatLng(a);
            var q = new S2LatLng(b);

            return Geodesic.WGS84.Inverse(p.latDegrees(), p.lngDegrees(), q.latDegrees(), q.lngDegrees()).s12;
        }

        /// <summary>
        /// The mean radius of the WGS84 ellipsoid, <c>(2a + b) / 3</c>, in metres.
        /// </summary>
        /// <remarks>
        /// Used only to turn a distance into an angle for growing a bounding rectangle, where the result is already an
        /// over-approximation and a fraction of a percent does not matter. No measurement uses it.
        /// </remarks>
        public const double MeanRadiusMeters = 6371008.7714;

        /// <summary>
        /// Returns the angle a distance in metres subtends at the centre of a sphere of <see cref="MeanRadiusMeters"/>.
        /// </summary>
        /// <param name="metres">The distance in metres.</param>
        /// <returns>The angle.</returns>
        public static S1Angle AngleFor(double metres)
        {
            return S1Angle.radians(metres / MeanRadiusMeters);
        }

        /// <summary>
        /// Returns the geodesic distance between two coordinates in metres.
        /// </summary>
        /// <remarks>
        /// A JTS coordinate carries longitude in x and latitude in y, the reverse of the order GeographicLib takes.
        /// </remarks>
        /// <param name="a">The first coordinate.</param>
        /// <param name="b">The second coordinate.</param>
        /// <returns>The distance in metres on the WGS84 ellipsoid.</returns>
        public static double Distance(org.locationtech.jts.geom.Coordinate a, org.locationtech.jts.geom.Coordinate b)
        {
            return Geodesic.WGS84.Inverse(a.getY(), a.getX(), b.getY(), b.getX()).s12;
        }




        /// <summary>
        /// Returns the coordinate a fraction of the way along the geodesic between two coordinates, offset sideways.
        /// </summary>
        /// <param name="from">The start of the geodesic.</param>
        /// <param name="to">The end of the geodesic.</param>
        /// <param name="fraction">How far along, from 0 to 1.</param>
        /// <param name="offset">Metres to the left of the direction of travel, negative for the right.</param>
        /// <remarks>
        /// The sideways direction is taken from the azimuth at the point reached rather than at the start, because the
        /// azimuth of a geodesic changes along it.
        /// </remarks>
        /// <returns>The coordinate reached, longitude in x and latitude in y.</returns>
        public static org.locationtech.jts.geom.Coordinate Along(
            org.locationtech.jts.geom.Coordinate from,
            org.locationtech.jts.geom.Coordinate to,
            double fraction,
            double offset)
        {
            var line = Geodesic.WGS84.Inverse(from.getY(), from.getX(), to.getY(), to.getX());
            var at = Geodesic.WGS84.Direct(from.getY(), from.getX(), line.azi1, line.s12 * fraction);

            if (offset == 0)
                return new org.locationtech.jts.geom.Coordinate(at.lon2, at.lat2);

            var aside = Geodesic.WGS84.Direct(at.lat2, at.lon2, at.azi2 - 90, offset);

            return new org.locationtech.jts.geom.Coordinate(aside.lon2, aside.lat2);
        }

        /// <summary>
        /// Returns the azimuth, in degrees clockwise from north, at which the geodesic from one coordinate to another
        /// sets out.
        /// </summary>
        /// <param name="from">The start of the geodesic.</param>
        /// <param name="to">The end of the geodesic.</param>
        /// <returns>The azimuth in degrees, from -180 to 180.</returns>
        public static double Azimuth(
            org.locationtech.jts.geom.Coordinate from,
            org.locationtech.jts.geom.Coordinate to)
        {
            return Geodesic.WGS84.Inverse(from.getY(), from.getX(), to.getY(), to.getX()).azi1;
        }

        /// <summary>
        /// Returns the coordinate reached by travelling a distance along a geodesic from a coordinate.
        /// </summary>
        /// <param name="from">The starting coordinate.</param>
        /// <param name="azimuth">The initial direction, in degrees clockwise from north.</param>
        /// <param name="metres">The distance to travel.</param>
        /// <remarks>
        /// Points traced at a fixed distance this way are exactly that far away on the ellipsoid, so a buffer built from
        /// them agrees with <c>CLR_ST_GEOG_DWITHIN</c>.
        /// </remarks>
        /// <returns>The coordinate reached, longitude in x and latitude in y.</returns>
        public static org.locationtech.jts.geom.Coordinate Offset(
            org.locationtech.jts.geom.Coordinate from,
            double azimuth,
            double metres)
        {
            var step = Geodesic.WGS84.Direct(from.getY(), from.getX(), azimuth, metres);

            return new org.locationtech.jts.geom.Coordinate(step.lon2, step.lat2);
        }

        /// <summary>
        /// Returns the points that divide the geodesic between two coordinates into equal segments no longer than a
        /// distance, excluding the two ends.
        /// </summary>
        /// <param name="a">The first end.</param>
        /// <param name="b">The second end.</param>
        /// <param name="longest">The greatest segment length in metres.</param>
        /// <returns>The points in order from <paramref name="a"/>; none if the geodesic is already short enough or
        /// <paramref name="longest"/> is not positive.</returns>
        /// <remarks>
        /// The points lie on the ellipsoidal geodesic, which differs from both the great circle and a straight line in
        /// degrees; between two points on a parallel away from the equator the geodesic bows toward the pole.
        /// </remarks>
        public static List<org.locationtech.jts.geom.Coordinate> Divide(
            org.locationtech.jts.geom.Coordinate a,
            org.locationtech.jts.geom.Coordinate b,
            double longest)
        {
            var found = new List<org.locationtech.jts.geom.Coordinate>();
            var line = Geodesic.WGS84.Inverse(a.getY(), a.getX(), b.getY(), b.getX());

            if (longest <= 0 || line.s12 <= longest)
                return found;

            var segments = (int)java.lang.Math.ceil(line.s12 / longest);

            for (var i = 1; i < segments; i++)
            {
                var step = Geodesic.WGS84.Direct(a.getY(), a.getX(), line.azi1, line.s12 * i / segments);

                found.Add(new org.locationtech.jts.geom.Coordinate(step.lon2, step.lat2));
            }

            return found;
        }

        /// <summary>
        /// Returns the total geodesic length of the given edges in metres.
        /// </summary>
        /// <param name="edges">The edges, each a pair of unit vectors.</param>
        /// <returns>The sum of the edges' lengths in metres.</returns>
        public static double Length(IEnumerable<(S2Point, S2Point)> edges)
        {
            var total = 0.0;

            foreach (var (p, q) in edges)
                total += Distance(p, q);

            return total;
        }

        /// <summary>
        /// Returns the geodesic area of an S2 polygon in square metres.
        /// </summary>
        /// <remarks>
        /// Each loop's area is measured as a magnitude, leaving orientation to S2, and added for a shell (even depth)
        /// or subtracted for a hole (odd depth).
        /// </remarks>
        /// <param name="polygon">The polygon.</param>
        /// <returns>The area in square metres.</returns>
        public static double Area(S2Polygon polygon)
        {
            var total = 0.0;

            for (var i = 0; i < polygon.numLoops(); i++)
            {
                var loop = polygon.loop(i);
                var area = new PolygonArea(Geodesic.WGS84, false);

                for (var j = 0; j < loop.numVertices(); j++)
                {
                    var vertex = new S2LatLng(loop.vertex(j));
                    area.AddPoint(vertex.latDegrees(), vertex.lngDegrees());
                }

                var magnitude = java.lang.Math.abs(area.Compute(false, true).area);

                total += loop.depth() % 2 == 0 ? magnitude : -magnitude;
            }

            return total;
        }

    }

}
