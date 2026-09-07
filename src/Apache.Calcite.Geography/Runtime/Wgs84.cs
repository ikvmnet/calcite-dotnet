using System.Collections.Generic;

using com.google.common.geometry;

using net.sf.geographiclib;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// The measurements, on the WGS84 ellipsoid.
    /// </summary>
    /// <remarks>
    /// S2 models the Earth as a sphere. That is not a defect in S2 — it is spherical by construction and says
    /// so — but it is the wrong instrument for a distance, and the gap was measured rather than assumed:
    /// against a live Cosmos DB account with <c>geospatialConfig</c> Geography, a spherical distance is out by
    /// up to 0.56%. The diagnosis is in one pair of numbers. One degree east and one degree north of the
    /// equator are the same distance on a sphere, and the service answers 111319.490736 and 110574.388493 —
    /// the first being the WGS84 semi-major axis times π/180, to every digit reported.
    ///
    /// <para>Karney's algorithm answers the ellipsoid to nanometres, and reproduces those two figures here.
    /// So the engines split by what each is for, which is what the design proposed before either was written:
    /// S2 decides topology — which points of two shapes are closest, whether one contains another — and this
    /// measures between the points S2 chose. Choosing the closest pair on a sphere and then measuring it on
    /// the ellipsoid is not exact, but the error is second order in the distance between the true closest
    /// pair and the spherical one, where using the sphere for the measurement itself is first order.</para>
    ///
    /// <para>A predicate stays on S2 entirely. Sphere against ellipsoid moves an edge by far less than it
    /// moves a distance, and containment asks which side of an edge a point falls on rather than how long the
    /// edge is.</para>
    /// </remarks>
    static class Wgs84
    {

        /// <summary>
        /// Returns the geodesic distance between two points in metres.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
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
        /// The one place a single radius is defensible: turning a distance into an angle so that a bounding
        /// rectangle can be grown by it. A bound is an over-approximation already — it is a rectangle around
        /// a shape that is not one — so a fraction of a percent in how far it grows changes nothing it
        /// promises. Nothing else here uses a radius, and a measurement never does.
        /// </remarks>
        public const double MeanRadiusMeters = 6371008.7714;

        /// <summary>
        /// Returns the angle a distance in metres subtends at the Earth's centre.
        /// </summary>
        /// <param name="metres"></param>
        /// <returns></returns>
        /// <inheritdoc cref="MeanRadiusMeters" />
        public static S1Angle AngleFor(double metres)
        {
            return S1Angle.radians(metres / MeanRadiusMeters);
        }

        /// <summary>
        /// Returns the geodesic distance between two coordinates in metres.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        /// <remarks>
        /// A JTS coordinate carries longitude in x and latitude in y, which is the order WKT writes and the
        /// reverse of the order a geodesic library takes.
        /// </remarks>
        public static double Distance(org.locationtech.jts.geom.Coordinate a, org.locationtech.jts.geom.Coordinate b)
        {
            return Geodesic.WGS84.Inverse(a.getY(), a.getX(), b.getY(), b.getX()).s12;
        }

        /// <summary>
        /// Returns the total geodesic length of the given edges in metres.
        /// </summary>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static double Length(IEnumerable<(S2Point, S2Point)> edges)
        {
            var total = 0.0;

            foreach (var (p, q) in edges)
                total += Distance(p, q);

            return total;
        }

        /// <summary>
        /// Returns the geodesic area of the given polygon in square metres.
        /// </summary>
        /// <param name="polygon"></param>
        /// <returns></returns>
        /// <remarks>
        /// Loop by loop, because a hole subtracts. S2 records nesting as a loop's depth — even is a shell and
        /// odd is a hole — and <c>PolygonArea</c> is asked for the magnitude of each, the winding being S2's
        /// business rather than the measurement's.
        /// </remarks>
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
