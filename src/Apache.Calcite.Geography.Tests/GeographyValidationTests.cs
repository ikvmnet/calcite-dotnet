using System;
using System.Linq;

using Apache.Calcite.Geography.Rel.Type;
using Apache.Calcite.Geography.Sql;

using FluentAssertions;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests how the validator resolves and types calls to the operators, over the fixture's two geometry
    /// columns.
    /// </summary>
    /// <remarks>
    /// Both columns have the same type, so Calcite's planar functions and this package's geodesic ones each
    /// accept either column; only the operator's name says how a value is read.
    /// </remarks>
    public class GeographyValidationTests
    {

        /// <summary>
        /// Validates a query, requires it to throw, and returns the messages of the exception and all its
        /// causes.
        /// </summary>
        /// <remarks>
        /// A signature error from an operand checker arrives wrapped in a <c>ValidationException</c>, and only
        /// an inner exception names the operator. IKVM exposes a Java throwable's cause as
        /// <see cref="Exception.InnerException"/>.
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
        /// Validates a query that selects one column and returns that column's type.
        /// </summary>
        /// <param name="sql">A query that selects exactly one column.</param>
        /// <returns>The type of that column.</returns>
        static RelDataType Column(string sql)
        {
            var row = GeographyFixture.Validate(sql);
            row.getFieldList().size().Should().Be(1);
            return ((RelDataTypeField)row.getFieldList().get(0)).getType();
        }

        /// <summary>
        /// Every operator field <see cref="GeographyOperatorTable"/> declares is in its operator list.
        /// </summary>
        /// <remarks>
        /// The fields and the registrations are separate lists. An operator declared but not registered
        /// compiles and then fails only as <c>No match found for function signature</c> when a query uses it.
        /// </remarks>
        [Fact]
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
        /// An accessor accepts the <c>GEOM</c> column as well as <c>GEOG</c>, both having the same type.
        /// </summary>
        [Fact]
        public void ShouldAcceptAnAccessorOverEitherColumn()
        {
            foreach (var sql in new[]
            {
                "SELECT CLR_ST_GEOG_X(GEOM) FROM GEO",
                "SELECT CLR_ST_GEOG_ASTEXT(GEOM) FROM GEO",
                "SELECT CLR_ST_GEOG_NUMPOINTS(GEOM) FROM GEO",
                "SELECT CLR_ST_GEOG_POINTN(GEOM, 1) FROM GEO",
            })
                GeographyFixture.Validate(sql);
        }

        [Fact]
        public void ShouldTypeTheAccessorsOverAGeographyColumn()
        {
            Column("SELECT CLR_ST_GEOG_X(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
            Column("SELECT CLR_ST_GEOG_NUMPOINTS(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.INTEGER);
            Column("SELECT CLR_ST_GEOG_ISEMPTY(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.BOOLEAN);
            Column("SELECT CLR_ST_GEOG_ASTEXT(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.VARCHAR);
            Column("SELECT CLR_ST_GEOG_ASWKB(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.VARBINARY);
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOG_BOUNDARY(GEOG) FROM GEO")).Should().BeTrue();
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOG_GEOMFROMWKB(CLR_ST_GEOG_ASWKB(GEOG)) FROM GEO")).Should().BeTrue();
        }

        [Fact]
        public void ShouldAcceptCalcitesStDistanceOverAGeographyColumn()
        {
            Column("SELECT ST_DISTANCE(GEOG, GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        /// <summary>
        /// Calcite's spatial accessors accept the <c>GEOG</c> column too.
        /// </summary>
        [Fact]
        public void ShouldAcceptCalcitesStSridOverAGeographyColumn()
        {
            Column("SELECT ST_SRID(GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.INTEGER);
        }

        [Fact]
        public void ShouldAcceptCalcitesStDistanceOverAGeometryColumn()
        {
            Column("SELECT ST_DISTANCE(GEOM, GEOM) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        [Fact]
        public void ShouldAcceptStGeogDistanceOverAGeographyColumn()
        {
            Column("SELECT CLR_ST_GEOG_DISTANCE(GEOG, GEOG) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        /// <summary>
        /// A geodesic operator accepts the <c>GEOM</c> column and reads its coordinates as degrees of longitude
        /// and latitude; the validator cannot tell planar coordinates apart.
        /// </summary>
        [Fact]
        public void ShouldAcceptStGeogDistanceOverAGeometryColumn()
        {
            Column("SELECT CLR_ST_GEOG_DISTANCE(GEOM, GEOM) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        /// <summary>
        /// Character arguments are refused by the operand checker, with an error naming the operator and
        /// <c>GEOMETRY</c>.
        /// </summary>
        [Fact]
        public void ShouldRejectStGeogDistanceOverCharacterArguments()
        {
            var message = Refuse("SELECT CLR_ST_GEOG_DISTANCE('a', 'b') FROM GEO");

            message.Should().Contain("CLR_ST_GEOG_DISTANCE");
            message.Should().Contain("GEOMETRY");
        }

        [Fact]
        public void ShouldRejectStGeogDWithinWithoutADistance()
        {
            Refuse("SELECT CLR_ST_GEOG_DWITHIN(GEOG, GEOG) FROM GEO").Should().Contain("CLR_ST_GEOG_DWITHIN");
        }

        [Fact]
        public void ShouldTypeTheWktConstructorAsGeography()
        {
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)') FROM GEO")).Should().BeTrue();
        }

        [Fact]
        public void ShouldTypeTheGeoJsonConstructorAsGeography()
        {
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOG_GEOMFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[0,0]}') FROM GEO")).Should().BeTrue();
        }

        /// <summary>
        /// Each WKT constructor has a one- and a two-argument form, as Calcite's do, and the lookup chooses by
        /// argument count.
        /// </summary>
        [Fact]
        public void ShouldTypeTheWktConstructorWithAnSridAsGeography()
        {
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)', 4326) FROM GEO")).Should().BeTrue();
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOG_GEOMFROMWKT('POINT(0 0)', 4326) FROM GEO")).Should().BeTrue();
        }

        [Fact]
        public void ShouldRejectAWktConstructorWithATooLongArgumentList()
        {
            Refuse("SELECT CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)', 4326, 1) FROM GEO").Should().Contain("CLR_ST_GEOG_GEOMFROMTEXT");
        }

        /// <summary>
        /// A constructed value can be passed straight to a geodesic operator.
        /// </summary>
        [Fact]
        public void ShouldAcceptAConstructedGeography()
        {
            GeographyFixture.Validate("SELECT CLR_ST_GEOG_DISTANCE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), GEOG) FROM GEO");
        }

        /// <summary>
        /// A value passed through <c>CLR_ST_GEOG_ASGEOM</c> is accepted by Calcite's planar functions.
        /// </summary>
        [Fact]
        public void ShouldCarryAGeographyIntoCalcitesStDistanceThroughAsGeom()
        {
            Column("SELECT ST_DISTANCE(CLR_ST_GEOG_ASGEOM(GEOG), GEOM) FROM GEO").getSqlTypeName().Should().BeSameAs(SqlTypeName.DOUBLE);
        }

        [Fact]
        public void ShouldTypeTheOtherCrossingAsGeography()
        {
            GeographyTypes.IsGeometry(Column("SELECT CLR_ST_GEOM_ASGEOG(GEOM) FROM GEO")).Should().BeTrue();
        }

        /// <summary>
        /// <c>CLR_ST_GEOM_ASGEOG</c> and <c>CLR_ST_GEOG_ASGEOM</c> each accept either column.
        /// </summary>
        /// <remarks>
        /// Both return their argument unchanged; they mark in the SQL which reading the author intends.
        /// </remarks>
        [Fact]
        public void ShouldAcceptEitherCrossingOverEitherColumn()
        {
            GeographyFixture.Validate("SELECT CLR_ST_GEOM_ASGEOG(GEOG) FROM GEO");
            GeographyFixture.Validate("SELECT CLR_ST_GEOG_ASGEOM(GEOM) FROM GEO");
        }

        [Fact]
        public void ShouldTypeThePredicatesAsBoolean()
        {
            foreach (var sql in new[]
            {
                "SELECT CLR_ST_GEOG_INTERSECTS(GEOG, GEOG) FROM GEO",
                "SELECT CLR_ST_GEOG_WITHIN(GEOG, GEOG) FROM GEO",
                "SELECT CLR_ST_GEOG_DWITHIN(GEOG, GEOG, 100.0) FROM GEO",
                "SELECT CLR_ST_GEOG_ISVALID(GEOG) FROM GEO",
            })
                Column(sql).getSqlTypeName().Should().BeSameAs(SqlTypeName.BOOLEAN, sql);
        }

        /// <summary>
        /// A geodesic predicate validates in a <c>WHERE</c> clause.
        /// </summary>
        [Fact]
        public void ShouldAcceptAPredicateInAWhereClause()
        {
            Column("SELECT ID FROM GEO WHERE CLR_ST_GEOG_DWITHIN(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 1000.0)")
                .getSqlTypeName().Should().BeSameAs(SqlTypeName.INTEGER);
        }

    }

}
