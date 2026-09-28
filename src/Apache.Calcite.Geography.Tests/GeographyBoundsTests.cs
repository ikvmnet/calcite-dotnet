using System;

using Apache.Calcite.Geography.Runtime;

using FluentAssertions;

using org.apache.calcite.runtime;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests <c>CLR_ST_GEOG_ENVELOPE</c>, <c>CLR_ST_GEOG_EXTENT</c> and <c>CLR_ST_GEOG_EXPAND</c>.
    /// </summary>
    /// <remarks>
    /// A planar envelope is the least and greatest coordinates, so a shape crossing longitude 180 gets a
    /// rectangle nearly 360 degrees wide. The envelope here is S2's rectangle, whose longitude interval may
    /// wrap. The expansion is a distance in metres rather than in degrees.
    /// </remarks>
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
        /// Away from the antimeridian the envelope agrees with Calcite's.
        /// </summary>
        [Fact]
        public void ShouldBoundAnOrdinaryShapeAsCalciteDoes()
        {
            var shape = Wkt("POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))");

            var ours = GeographyFunctions.Envelope(shape);
            var theirs = SpatialTypeFunctions.ST_Envelope(shape);

            ours!.getEnvelopeInternal().getMinX().Should().BeApproximately(theirs.getEnvelopeInternal().getMinX(), 1e-9);
            ours.getEnvelopeInternal().getMaxX().Should().BeApproximately(theirs.getEnvelopeInternal().getMaxX(), 1e-9);
        }

        /// <summary>
        /// A shape two degrees wide across the antimeridian gets an envelope in two one-degree pieces, where
        /// Calcite's spans 358 degrees.
        /// </summary>
        [Fact]
        public void ShouldNotSpanTheGlobeAcrossTheAntimeridian()
        {
            var shape = Wkt("LINESTRING(179 0, -179 0)");

            var theirs = SpatialTypeFunctions.ST_Envelope(shape).getEnvelopeInternal();
            (theirs.getMaxX() - theirs.getMinX()).Should().BeApproximately(358, 1e-9);

            var ours = GeographyFunctions.Envelope(shape);

            ours!.getNumGeometries().Should().Be(2);
            Span(ours.getGeometryN(0)).Should().BeApproximately(1, 1e-6);
            Span(ours.getGeometryN(1)).Should().BeApproximately(1, 1e-6);
        }

        /// <summary>
        /// A degenerate envelope is a point or a line, as JTS's is.
        /// </summary>
        [Fact]
        public void ShouldDegenerateAsJtsDoes()
        {
            // Approximate: the rectangle is computed on the sphere, and a coordinate converted to a unit
            // vector and back is not exactly the degrees it started as.
            var point = GeographyFunctions.Envelope(Wkt("POINT(1 2)"))!;
            point.getGeometryType().Should().Be("Point");
            point.getCoordinate().getX().Should().BeApproximately(1, 1e-12);
            point.getCoordinate().getY().Should().BeApproximately(2, 1e-12);
            GeographyFunctions.Envelope(Wkt("LINESTRING(0 0, 2 0)"))!.getGeometryType().Should().Be("LineString");
            GeographyFunctions.Envelope(Wkt("POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))"))!.getGeometryType().Should().Be("Polygon");
        }

        /// <summary>
        /// The extent is the envelope, as in Calcite.
        /// </summary>
        [Fact]
        public void ShouldAnswerTheSameForExtent()
        {
            var shape = Wkt("POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))");

            Text(GeographyFunctions.Extent(shape)).Should().Be(Text(GeographyFunctions.Envelope(shape)));
        }

        /// <summary>
        /// The expansion is in metres.
        /// </summary>
        /// <remarks>
        /// At 60 degrees north a degree of longitude is about half a degree of latitude on the ground, so an
        /// expansion by the same distance in every direction spans about twice as many degrees of longitude as
        /// of latitude.
        /// </remarks>
        [Fact]
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
        /// <c>Envelope</c>, <c>Extent</c> and <c>Expand</c> return <c>null</c> when any argument is <c>null</c>.
        /// </summary>
        [Fact]
        public void ShouldAnswerNullForANullArgument()
        {
            GeographyFunctions.Envelope(null).Should().BeNull();
            GeographyFunctions.Extent(null).Should().BeNull();
            GeographyFunctions.Expand(null, java.lang.Double.valueOf(1)).Should().BeNull();
            GeographyFunctions.Expand(Wkt("POINT(0 0)"), null).Should().BeNull();
        }

        /// <summary>
        /// <c>CLR_ST_GEOG_ENVELOPE</c>, <c>CLR_ST_GEOG_EXTENT</c> and <c>CLR_ST_GEOG_EXPAND</c> each run as an operator
        /// in a query.
        /// </summary>
        [Fact]
        public void ShouldRunEachAsAnOperator()
        {
            (GeographyExecutionTests.Run("SELECT CLR_ST_GEOG_X(CLR_ST_GEOG_ENVELOPE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 2)')))")[0][0] is java.lang.Number x ? x.doubleValue() : double.NaN).Should().BeApproximately(1, 1e-12);

            (GeographyExecutionTests.Run("SELECT CLR_ST_GEOG_Y(CLR_ST_GEOG_EXTENT(CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 2)')))")[0][0] is java.lang.Number y ? y.doubleValue() : double.NaN).Should().BeApproximately(2, 1e-12);

            (GeographyExecutionTests.Run("SELECT CLR_ST_GEOG_ISVALID(CLR_ST_GEOG_EXPAND(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 1000.0))")[0][0]
                is java.lang.Boolean valid && valid.booleanValue()).Should().BeTrue();
        }

        static double Span(Geometry g)
        {
            var box = g.getEnvelopeInternal();

            return box.getMaxX() - box.getMinX();
        }

    }

}
