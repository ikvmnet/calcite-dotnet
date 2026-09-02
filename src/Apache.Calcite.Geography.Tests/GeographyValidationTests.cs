using System;

using Apache.Calcite.Geography.Rel.Type;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// What the validator makes of a call over a geography column.
    /// </summary>
    /// <remarks>
    /// The central property is the first test here. Calcite's spatial library is a set of reflective bindings
    /// over <c>org.locationtech.jts.geom.Geometry</c>, and routine resolution refuses to pass a geography to
    /// one — the harmless accessors included. Without that, a geodesic value would answer in degrees, in a
    /// different ordering, with no error anywhere.
    /// </remarks>
    [TestClass]
    public class GeographyValidationTests
    {

        /// <summary>
        /// Validates the given query, requires it to be refused, and returns every message in the chain.
        /// </summary>
        /// <remarks>
        /// The chain, because the validator wraps: a signature error from an operand checker arrives inside a
        /// <c>ValidationException</c> and names the operator only further down. IKVM makes a Java throwable a
        /// <see cref="Exception"/>, so its cause is the inner exception.
        /// </remarks>
        static string Refuse(string sql)
        {
            var thrown = ((Action)(() => GeographyFixture.Validate(sql))).Should().Throw<Exception>().Which;
            var text = "";

            for (Exception? current = thrown; current is not null; current = current.InnerException)
                text += current.Message + "\n";

            return text;
        }

        /// <summary>
        /// Validates the given query and returns the type of the one column it selects.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static RelDataType Column(string sql)
        {
            var row = GeographyFixture.Validate(sql);
            row.getFieldList().size().Should().Be(1);
            return ((RelDataTypeField)row.getFieldList().get(0)).getType();
        }

        [TestMethod]
        public void ShouldRejectCalcitesStDistanceOverAGeographyColumn()
        {
            Refuse("SELECT ST_DISTANCE(GEOG, GEOG) FROM GEO").Should().Contain("ST_DISTANCE");
        }

        /// <summary>
        /// The same accessors go the same way, so there is no group of Calcite's spatial functions that comes
        /// free.
        /// </summary>
        [TestMethod]
        public void ShouldRejectCalcitesStSridOverAGeographyColumn()
        {
            Refuse("SELECT ST_SRID(GEOG) FROM GEO").Should().Contain("ST_SRID");
        }

        /// <summary>
        /// The control. The rejection above is the type and not the fixture.
        /// </summary>
        [TestMethod]
        public void ShouldAcceptCalcitesStDistanceOverAGeometryColumn()
        {
            Column("SELECT ST_DISTANCE(GEOM, GEOM) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        [TestMethod]
        public void ShouldAcceptStGeogDistanceOverAGeographyColumn()
        {
            Column("SELECT ST_GEOG_DISTANCE(GEOG, GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        /// <summary>
        /// The refusal runs both ways: a geodesic operator will not take a plane's coordinates either.
        /// </summary>
        [TestMethod]
        public void ShouldRejectStGeogDistanceOverAGeometryColumn()
        {
            Refuse("SELECT ST_GEOG_DISTANCE(GEOM, GEOM) FROM GEO").Should().Contain("ST_GEOG_DISTANCE");
        }

        /// <summary>
        /// The operand checker is what decides the error a caller sees, and a checker that took anything
        /// would let this validate.
        /// </summary>
        [TestMethod]
        public void ShouldRejectStGeogDistanceOverCharacterArguments()
        {
            var message = Refuse("SELECT ST_GEOG_DISTANCE('a', 'b') FROM GEO");

            message.Should().Contain("ST_GEOG_DISTANCE");
            message.Should().Contain("GEOGRAPHY");
        }

        [TestMethod]
        public void ShouldRejectStGeogDWithinWithoutADistance()
        {
            Refuse("SELECT ST_GEOG_DWITHIN(GEOG, GEOG) FROM GEO").Should().Contain("ST_GEOG_DWITHIN");
        }

        [TestMethod]
        public void ShouldTypeTheWktConstructorAsGeography()
        {
            GeographyTypes.IsGeography(Column("SELECT ST_GEOG_GEOMFROMTEXT('POINT(0 0)') FROM GEO")).Should().BeTrue();
        }

        [TestMethod]
        public void ShouldTypeTheGeoJsonConstructorAsGeography()
        {
            GeographyTypes.IsGeography(Column("SELECT ST_GEOG_GEOMFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[0,0]}') FROM GEO")).Should().BeTrue();
        }

        /// <summary>
        /// Calcite declares two arities for each WKT constructor, and so do we; the routine lookup picks
        /// between them by argument count.
        /// </summary>
        [TestMethod]
        public void ShouldTypeTheWktConstructorWithAnSridAsGeography()
        {
            GeographyTypes.IsGeography(Column("SELECT ST_GEOG_GEOMFROMTEXT('POINT(0 0)', 4326) FROM GEO")).Should().BeTrue();
            GeographyTypes.IsGeography(Column("SELECT ST_GEOG_GEOMFROMWKT('POINT(0 0)', 4326) FROM GEO")).Should().BeTrue();
        }

        [TestMethod]
        public void ShouldRejectAWktConstructorWithATooLongArgumentList()
        {
            Refuse("SELECT ST_GEOG_GEOMFROMTEXT('POINT(0 0)', 4326, 1) FROM GEO").Should().Contain("ST_GEOG_GEOMFROMTEXT");
        }

        /// <summary>
        /// A constructed geography goes into a geodesic operator without a column to hold it.
        /// </summary>
        [TestMethod]
        public void ShouldAcceptAConstructedGeography()
        {
            GeographyFixture.Validate("SELECT ST_GEOG_DISTANCE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), GEOG) FROM GEO");
        }

        /// <summary>
        /// The crossing is deliberate and explicit: having said so, Calcite's planar functions will take the
        /// value.
        /// </summary>
        [TestMethod]
        public void ShouldCarryAGeographyIntoCalcitesStDistanceThroughAsGeom()
        {
            Column("SELECT ST_DISTANCE(ST_GEOG_ASGEOM(GEOG), GEOM) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        [TestMethod]
        public void ShouldTypeTheOtherCrossingAsGeography()
        {
            GeographyTypes.IsGeography(Column("SELECT ST_GEOM_ASGEOG(GEOM) FROM GEO")).Should().BeTrue();
        }

        /// <summary>
        /// Each crossing takes the reading it converts from, so neither is a way to launder the other.
        /// </summary>
        [TestMethod]
        public void ShouldRejectACrossingOverTheWrongReading()
        {
            Refuse("SELECT ST_GEOM_ASGEOG(GEOG) FROM GEO").Should().Contain("ST_GEOM_ASGEOG");
            Refuse("SELECT ST_GEOG_ASGEOM(GEOM) FROM GEO").Should().Contain("ST_GEOG_ASGEOM");
        }

        [TestMethod]
        public void ShouldTypeThePredicatesAsBoolean()
        {
            foreach (var sql in new[]
            {
                "SELECT ST_GEOG_INTERSECTS(GEOG, GEOG) FROM GEO",
                "SELECT ST_GEOG_WITHIN(GEOG, GEOG) FROM GEO",
                "SELECT ST_GEOG_DWITHIN(GEOG, GEOG, 100.0) FROM GEO",
                "SELECT ST_GEOG_ISVALID(GEOG) FROM GEO",
            })
                Column(sql).getSqlTypeName().Should().BeSameAs(SqlTypeName.BOOLEAN, sql);
        }

        /// <summary>
        /// A geodesic predicate in a WHERE clause, which is where one is actually written.
        /// </summary>
        [TestMethod]
        public void ShouldAcceptAPredicateInAWhereClause()
        {
            Column("SELECT ID FROM GEO WHERE ST_GEOG_DWITHIN(GEOG, ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 1000.0)")
                .getSqlTypeName().Should().BeSameAs(SqlTypeName.INTEGER);
        }

    }

}
