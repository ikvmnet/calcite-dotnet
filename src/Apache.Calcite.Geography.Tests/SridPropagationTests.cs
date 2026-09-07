using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// What JTS does with a geometry's SRID when Calcite's spatial functions build a new one from it.
    /// </summary>
    /// <remarks>
    /// The SRID is an <c>int</c> field on the JTS <c>Geometry</c> instance, taken from the
    /// <c>GeometryFactory</c> that built it and settable afterwards with <c>setSRID</c>. It is not part of
    /// the coordinates and not part of the type — a tag hanging off the object.
    ///
    /// <para>This is here because the SRID is the only marking available to a design that types its
    /// operators over <c>GEOMETRY</c> rather than over a class of its own, and the measurement says it is
    /// not a marking that can be relied upon. A geometry Calcite's own <c>ST_*</c> derived from a stamped one
    /// mostly comes back on zero. Which means an <c>ST_GEOG_</c> operator cannot refuse a geometry on the
    /// grounds that it is not stamped 4326: the value may be perfectly geodesic and merely have been through
    /// <c>ST_BUFFER</c> on the way.</para>
    ///
    /// <para>The tag survives a pipeline of this package's own operators, because every one of them stamps
    /// what it returns. It is stripped exactly at the boundary with Calcite's spatial library, which is the
    /// boundary a guard would exist to police.</para>
    /// </remarks>
    [TestClass]
    public class SridPropagationTests
    {

        static org.locationtech.jts.geom.Geometry Stamped(string wkt)
        {
            var g = SpatialTypeUtils.fromWkt(wkt);
            g.setSRID(GeographyFunctions.Wgs84);
            return g;
        }

        /// <summary>
        /// A geometry Calcite reads from WKT carries no reference system.
        /// </summary>
        [TestMethod]
        public void ShouldReadWktWithNoSrid()
        {
            SpatialTypeUtils.fromWkt("POINT(1 2)").getSRID().Should().Be(0);
        }

        /// <summary>
        /// Most of Calcite's spatial functions drop it, and one keeps it.
        /// </summary>
        /// <remarks>
        /// Not a rule with an exception so much as an absence of a rule: JTS takes a derived geometry's SRID
        /// from whichever <c>GeometryFactory</c> built it, and whether that factory is the operand's depends
        /// on how the particular operation is written. <c>reverse</c> copies the geometry, so it keeps it;
        /// the overlay and construction operations build through a factory that does not.
        /// </remarks>
        [TestMethod]
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
        /// WKT does not carry it and EWKT does, which is the whole difference between the two.
        /// </summary>
        [TestMethod]
        public void ShouldCarryTheSridThroughEwktAndNotWkt()
        {
            var g = Stamped("POINT(1 2)");

            SpatialTypeUtils.fromWkt(SpatialTypeFunctions.ST_AsText(g))!.getSRID().Should().Be(0);
            SpatialTypeFunctions.ST_GeomFromEWKT(SpatialTypeFunctions.ST_AsEWKT(g))!.getSRID()
                .Should().Be(GeographyFunctions.Wgs84);
        }

    }

}
