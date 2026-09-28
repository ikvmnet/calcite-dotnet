using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Pins what happens to a geometry's SRID when Calcite's spatial functions derive a new geometry from it.
    /// </summary>
    /// <remarks>
    /// The SRID is an <c>int</c> on the JTS <c>Geometry</c> instance, taken from the <c>GeometryFactory</c>
    /// that built it and settable with <c>setSRID</c>; it is part of neither the coordinates nor the type.
    /// Because most of Calcite's <c>ST_*</c> functions return a geometry with SRID 0, a <c>CLR_ST_GEOG_</c>
    /// operator cannot refuse an operand for not carrying 4326: a geodesic value that has passed through
    /// <c>ST_BUFFER</c> comes back with SRID 0.
    /// </remarks>
    public class SridPropagationTests
    {

        static org.locationtech.jts.geom.Geometry Stamped(string wkt)
        {
            var g = SpatialTypeUtils.fromWkt(wkt);
            g.setSRID(GeographyFunctions.Wgs84);
            return g;
        }

        /// <summary>
        /// A geometry Calcite reads from WKT has SRID 0.
        /// </summary>
        [Fact]
        public void ShouldReadWktWithNoSrid()
        {
            SpatialTypeUtils.fromWkt("POINT(1 2)").getSRID().Should().Be(0);
        }

        /// <summary>
        /// Buffer, centroid, envelope and intersection return SRID 0; reverse keeps the operand's.
        /// </summary>
        /// <remarks>
        /// JTS takes a derived geometry's SRID from the <c>GeometryFactory</c> that built it, and whether
        /// that is the operand's depends on the operation: <c>reverse</c> copies the geometry, while the
        /// overlay and construction operations build through a factory with no SRID.
        /// </remarks>
        [Fact]
        public void ShouldMostlyDropTheSridThroughAnOperation()
        {
            var g = Stamped("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
            var p = Stamped("POINT(5 5)");

            g.getSRID().Should().Be(GeographyFunctions.Wgs84, "the stamp itself takes");

            SpatialTypeFunctions.ST_Buffer(g, 0.1)!.getSRID().Should().Be(0);
            SpatialTypeFunctions.ST_Centroid(g)!.getSRID().Should().Be(0);
            SpatialTypeFunctions.ST_Envelope(g)!.getSRID().Should().Be(0);
            SpatialTypeFunctions.ST_Intersection(g, p)!.getSRID().Should().Be(0);

            SpatialTypeFunctions.ST_Reverse(g)!.getSRID().Should().Be(GeographyFunctions.Wgs84);
        }

        /// <summary>
        /// A round trip through EWKT keeps the SRID; one through WKT does not.
        /// </summary>
        [Fact]
        public void ShouldCarryTheSridThroughEwktAndNotWkt()
        {
            var g = Stamped("POINT(1 2)");

            SpatialTypeUtils.fromWkt(SpatialTypeFunctions.ST_AsText(g))!.getSRID().Should().Be(0);
            SpatialTypeFunctions.ST_GeomFromEWKT(SpatialTypeFunctions.ST_AsEWKT(g))!.getSRID()
                .Should().Be(GeographyFunctions.Wgs84);
        }

    }

}
