using Apache.Calcite.Geography.Rel.Type;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Tests that the type <see cref="GeographyTypes.Of"/> returns is Calcite's <c>GEOMETRY</c>, carried by the
    /// JTS <c>Geometry</c> class.
    /// </summary>
    /// <remarks>
    /// There is no separate geography type: a schema function's parameter types are checked with
    /// <c>SqlTypeAssignmentRule</c>, which has no entry for a type name such as <c>OTHER</c> and asserts rather
    /// than rejects, and a schema is how an adapter brings its functions with it.
    /// </remarks>
    public class GeographyTypeTests
    {

        [Fact]
        public void ShouldBeCalcitesGeometryType()
        {
            var type = GeographyTypes.Of(GeographyFixture.TypeFactory());

            type.getSqlTypeName().Should().BeSameAs(SqlTypeName.GEOMETRY);
        }

        /// <summary>
        /// The type's Java class is JTS <c>Geometry</c>, whether asked of the type or of the type factory.
        /// </summary>
        [Fact]
        public void ShouldReportJtsGeometryAsItsJavaClass()
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var type = GeographyTypes.Of(typeFactory);

            ((RelDataTypeFactoryImpl.JavaType)type).getJavaClass().Should().BeSameAs((java.lang.Class)typeof(Geometry));
            typeFactory.getJavaClass(type).Should().BeSameAs((java.lang.Class)typeof(Geometry));
        }

        /// <summary>
        /// <see cref="GeographyTypes.IsGeometry"/> accepts both this type and Calcite's plain <c>GEOMETRY</c>.
        /// </summary>
        /// <remarks>
        /// Only the name of the operator applied to a value says it is to be read geodesically.
        /// </remarks>
        [Fact]
        public void ShouldBeTheSameTypeAsCalcitesGeometry()
        {
            var typeFactory = GeographyFixture.TypeFactory();

            GeographyTypes.IsGeometry(GeographyTypes.Of(typeFactory)).Should().BeTrue();
            GeographyTypes.IsGeometry(typeFactory.createSqlType(SqlTypeName.GEOMETRY)).Should().BeTrue();
            GeographyTypes.IsGeometry(typeFactory.createSqlType(SqlTypeName.INTEGER)).Should().BeFalse();
        }

        /// <summary>
        /// The type is interned, so two calls return the same instance.
        /// </summary>
        [Fact]
        public void ShouldInternTheType()
        {
            var typeFactory = GeographyFixture.TypeFactory();

            GeographyTypes.Of(typeFactory).Should().BeSameAs(GeographyTypes.Of(typeFactory));
        }

        /// <summary>
        /// A column declared <c>NOT NULL</c> keeps the geometry type.
        /// </summary>
        /// <remarks>
        /// <c>RelDataTypeFactoryImpl.copySimpleType</c> changes a <c>JavaType</c>'s nullability by constructing
        /// a plain <c>JavaType</c>, which would lose any subclass.
        /// </remarks>
        [Fact]
        public void ShouldSurviveAColumnDeclaredNotNull()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);

            var row = typeFactory.builder().add("GEOG", geography).nullable(false).build();
            var column = ((RelDataTypeField)row.getFieldList().get(0)).getType();

            GeographyTypes.IsGeometry(column).Should().BeTrue();
            column.isNullable().Should().BeFalse();
        }

        /// <summary>
        /// The least restrictive type of this type and Calcite's plain <c>GEOMETRY</c>, as a <c>UNION</c>
        /// computes, is a geometry.
        /// </summary>
        [Fact]
        public void ShouldBringAnyTwoGeometriesTogether()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            // the Java type and the SQL type are distinct instances
            var javaType = GeographyTypes.Of(typeFactory);
            var sqlType = typeFactory.createSqlType(SqlTypeName.GEOMETRY);

            var both = typeFactory.leastRestrictive(java.util.Arrays.asList([javaType, sqlType]));

            GeographyTypes.IsGeometry(both).Should().BeTrue();
        }

    }

}
