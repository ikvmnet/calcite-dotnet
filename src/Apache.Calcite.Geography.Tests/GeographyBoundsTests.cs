using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The bounding rectangle, which is where the antimeridian stops being an abstraction.
    /// </summary>
    /// <remarks>
    /// A planar envelope is the least and greatest of the coordinates. That is the wrong answer for anything
    /// that crosses longitude 180: a shape two degrees across gets a rectangle 358 degrees wide, and every
    /// index and every pre-filter built on it then reads most of the globe. S2's rectangle knows a longitude
    /// interval may wrap.
    ///
    /// <para>An expansion has the same shape of problem one dimension down. Growing a box by a degree moves
    /// its northern edge further than its eastern one everywhere off the equator, so the planar function has
    /// no fixed meaning on the Earth; this grows by metres.</para>
    /// </remarks>
    [TestClass]
    public class GeographyBoundsTests
    {

        static Geometry Wkt(string wkt)
        {
            return GeographyFunctions.FromWkt(wkt) ?? throw new InvalidOperationException($"'{wkt}' did not parse.");
        }

        static string Text(Geometry? g)
        {
            return g is null ? "null" : g.toText();
        }

        /// <summary>
        /// An ordinary shape gets an ordinary box, and agrees with Calcite.
        /// </summary>
        [TestMethod]
        public void ShouldBoundAnOrdinaryShapeAsCalciteDoes()
        {
            var shape = Wkt("POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))");

            var ours = GeographyFunctions.Envelope(shape);
            var theirs = SpatialTypeFunctions.ST_Envelope(shape);

            ours!.getEnvelopeInternal().getMinX().Should().BeApproximately(theirs.getEnvelopeInternal().getMinX(), 1e-9);
            ours.getEnvelopeInternal().getMaxX().Should().BeApproximately(theirs.getEnvelopeInternal().getMaxX(), 1e-9);
        }

        /// <summary>
        /// The whole reason this function exists.
        /// </summary>
        /// <remarks>
        /// Two degrees of shape across the antimeridian. Calcite answers a box spanning 358 degrees of
        /// longitude, because -179 and 179 are its least and greatest coordinates; this answers the two
        /// degrees that are actually there, as the two halves either side of the line.
        /// </remarks>
        [TestMethod]
        public void ShouldNotSpanTheGlobeAcrossTheAntimeridian()
        {
            var shape = Wkt("LINESTRING(179 0, -179 0)");

            var theirs = SpatialTypeFunctions.ST_Envelope(shape).getEnvelopeInternal();
            (theirs.getMaxX() - theirs.getMinX()).Should().BeApproximately(358, 1e-9);

            var ours = GeographyFunctions.Envelope(shape);

            // two pieces, and neither is wide
            ours!.getNumGeometries().Should().Be(2);
            Span(ours.getGeometryN(0)).Should().BeApproximately(1, 1e-6);
            Span(ours.getGeometryN(1)).Should().BeApproximately(1, 1e-6);
        }

        /// <summary>
        /// A degenerate rectangle answers what JTS answers for one.
        /// </summary>
        [TestMethod]
        public void ShouldDegenerateAsJtsDoes()
        {
            // approximately, because the rectangle is the sphere's: a coordinate reaches it as a unit
            // vector and comes back a few bits shy of the degrees it went in as
            var point = GeographyFunctions.Envelope(Wkt("POINT(1 2)"))!;
            point.getGeometryType().Should().Be("Point");
            point.getCoordinate().getX().Should().BeApproximately(1, 1e-12);
            point.getCoordinate().getY().Should().BeApproximately(2, 1e-12);
            GeographyFunctions.Envelope(Wkt("LINESTRING(0 0, 2 0)"))!.getGeometryType().Should().Be("LineString");
            GeographyFunctions.Envelope(Wkt("POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))"))!.getGeometryType().Should().Be("Polygon");
        }

        /// <summary>
        /// The extent is the same rectangle, as Calcite's two are the same call.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerTheSameForExtent()
        {
            var shape = Wkt("POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))");

            Text(GeographyFunctions.Extent(shape)).Should().Be(Text(GeographyFunctions.Envelope(shape)));
        }

        /// <summary>
        /// An expansion is in metres, and a degree of longitude is not a degree of latitude.
        /// </summary>
        /// <remarks>
        /// At 60 degrees north a degree of longitude is about half a degree of latitude on the ground, so a
        /// box grown by the same distance in every direction grows about twice as far in longitude as in
        /// latitude. A planar expansion grows both by the same number and is therefore short to the east and
        /// west by half.
        /// </remarks>
        [TestMethod]
        public void ShouldExpandInMetresRatherThanDegrees()
        {
            var expanded = GeographyFunctions.Expand(Wkt("POINT(0 60)"), java.lang.Double.valueOf(111319.49));

            expanded.Should().NotBeNull();
            var box = expanded!.getEnvelopeInternal();

            var latSpan = box.getMaxY() - box.getMinY();
            var lngSpan = box.getMaxX() - box.getMinX();

            latSpan.Should().BeApproximately(2.0, 0.02);
            lngSpan.Should().BeGreaterThan(latSpan * 1.5);
        }

        /// <summary>
        /// Nothing to expand is nothing.
        /// </summary>
        [TestMethod]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.Envelope(null).Should().BeNull();
            GeographyFunctions.Extent(null).Should().BeNull();
            GeographyFunctions.Expand(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Expand(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        /// <summary>
        /// All three run as operators.
        /// </summary>
        [TestMethod]
        public void ShouldRunEachAsAnOperator()
        {
            (GeographyExecutionTests.Run("SELECT ST_GEOG_X(ST_GEOG_ENVELOPE(ST_GEOG_GEOMFROMTEXT('POINT(1 2)')))")[0][0] is java.lang.Number x ? x.doubleValue() : double.NaN).Should().BeApproximately(1, 1e-12);

            (GeographyExecutionTests.Run("SELECT ST_GEOG_Y(ST_GEOG_EXTENT(ST_GEOG_GEOMFROMTEXT('POINT(1 2)')))")[0][0] is java.lang.Number y ? y.doubleValue() : double.NaN).Should().BeApproximately(2, 1e-12);

            (GeographyExecutionTests.Run("SELECT ST_GEOG_ISVALID(ST_GEOG_EXPAND(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 1000.0))")[0][0]
                is java.lang.Boolean valid && valid.booleanValue()).Should().BeTrue();
        }

        static double Span(Geometry g)
        {
            var box = g.getEnvelopeInternal();

            return box.getMaxX() - box.getMinX();
        }

    }

}
