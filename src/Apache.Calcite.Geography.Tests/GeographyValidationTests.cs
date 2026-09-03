using System;
using System.Linq;

using Apache.Calcite.Geography.Rel.Type;
using Apache.Calcite.Geography.Sql;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
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

        /// <summary>
        /// Every operator the table declares is also an operator the table hands out.
        /// </summary>
        /// <remarks>
        /// The declarations are one list and the registrations are another, and nothing but this connects
        /// them. A field added without its line in the second list is an operator that exists, compiles,
        /// resolves nowhere and fails only as <c>No match found for function signature</c> in whichever query
        /// reaches for it first.
        /// </remarks>
        [TestMethod]
        public void ShouldRegisterEveryDeclaredOperator()
        {
            var declared = typeof(GeographyOperatorTable)
                .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
                .Where(field => typeof(SqlFunction).IsAssignableFrom(field.FieldType))
                .Select(field => (Name: field.Name, Operator: (SqlFunction)field.GetValue(null)!))
                .ToList();

            declared.Should().HaveCountGreaterThan(50);

            var registered = GeographyOperatorTable.Instance().getOperatorList();
            var missing = declared.Where(d => registered.contains(d.Operator) == false).Select(d => d.Name).ToList();

            missing.Should().BeEmpty(string.Join(", ", missing));
            registered.size().Should().Be(declared.Count);
        }

        /// <summary>
        /// The accessors insist on a geography exactly as the operations do.
        /// </summary>
        /// <remarks>
        /// They read coordinates without interpreting the space between them, so it would be tempting to let
        /// them take either reading. They must not: <c>ST_GEOG_ASTEXT</c> over a geometry would be a way to
        /// spell <c>ST_ASTEXT</c>, and every such way is a place the two readings can be confused.
        /// </remarks>
        [TestMethod]
        public void ShouldRejectAnAccessorOverAGeometryColumn()
        {
            foreach (var sql in new[]
            {
                "SELECT ST_GEOG_X(GEOM) FROM GEO",
                "SELECT ST_GEOG_ASTEXT(GEOM) FROM GEO",
                "SELECT ST_GEOG_NUMPOINTS(GEOM) FROM GEO",
                "SELECT ST_GEOG_POINTN(GEOM, 1) FROM GEO",
            })
                Refuse(sql).Should().Contain("ST_GEOG_", sql);
        }

        [TestMethod]
        public void ShouldTypeTheAccessorsOverAGeographyColumn()
        {
            Column("SELECT ST_GEOG_X(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
            Column("SELECT ST_GEOG_NUMPOINTS(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.INTEGER);
            Column("SELECT ST_GEOG_ISEMPTY(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.BOOLEAN);
            Column("SELECT ST_GEOG_ASTEXT(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.VARCHAR);
            Column("SELECT ST_GEOG_ASWKB(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.VARBINARY);
            GeographyTypes.IsGeography(Column("SELECT ST_GEOG_BOUNDARY(GEOG) FROM GEO")).Should().BeTrue();
            GeographyTypes.IsGeography(Column("SELECT ST_GEOG_GEOMFROMWKB(ST_GEOG_ASWKB(GEOG)) FROM GEO")).Should().BeTrue();
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
