using Apache.Calcite.Geography.Rel.Type;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// What a geography column's type is, which is Calcite's <c>GEOMETRY</c> and nothing of this package's
    /// own.
    /// </summary>
    /// <remarks>
    /// There was a <c>GEOGRAPHY</c> type here — a <c>JavaType</c> subclass answering
    /// <see cref="SqlTypeName.OTHER"/> — and it worked for a host that chained an operator table by hand. It
    /// could not work for anything else: a schema function's parameter type is compared under
    /// <c>SqlTypeAssignmentRule</c>, which has no entry for <c>OTHER</c> and asserts rather than rejects, and
    /// a schema is the only way an adapter can bring its functions with it. These tests pin what was given up,
    /// so that the giving up stays deliberate rather than becoming folklore.
    /// </remarks>
    [TestClass]
    public class GeographyTypeTests
    {

        [TestMethod]
        public void ShouldBeCalcitesGeometryType()
        {
            var type = GeographyTypes.Of(GeographyFixture.TypeFactory());

            type.getSqlTypeName().Should().BeSameAs(SqlTypeName.GEOMETRY);
        }

        /// <summary>
        /// The runtime carrier is an ordinary JTS geometry, asked either way round.
        /// </summary>
        [TestMethod]
        public void ShouldReportJtsGeometryAsItsJavaClass()
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var type = GeographyTypes.Of(typeFactory);

            ((RelDataTypeFactoryImpl.JavaType)type).getJavaClass().Should().BeSameAs((java.lang.Class)typeof(Geometry));
            typeFactory.getJavaClass(type).Should().BeSameAs((java.lang.Class)typeof(Geometry));
        }

        /// <summary>
        /// A geography and a geometry are one type, and nothing tells them apart.
        /// </summary>
        /// <remarks>
        /// The cost of the design, stated as an assertion so that it cannot be mislaid. What says a value is
        /// to be read geodesically is the name of the operator applied to it, and nothing else.
        /// </remarks>
        [TestMethod]
        public void ShouldBeTheSameTypeAsCalcitesGeometry()
        {
            var typeFactory = GeographyFixture.TypeFactory();

            GeographyTypes.Of(typeFactory).Should().BeSameAs(GeographyTypes.GeometryOf(typeFactory));
            GeographyTypes.IsGeometry(GeographyTypes.Of(typeFactory)).Should().BeTrue();
            GeographyTypes.IsGeometry(typeFactory.createSqlType(SqlTypeName.GEOMETRY)).Should().BeTrue();
            GeographyTypes.IsGeometry(typeFactory.createSqlType(SqlTypeName.INTEGER)).Should().BeFalse();
        }

        /// <summary>
        /// The type is interned, so two asks give one instance.
        /// </summary>
        [TestMethod]
        public void ShouldInternTheType()
        {
            var typeFactory = GeographyFixture.TypeFactory();

            GeographyTypes.Of(typeFactory).Should().BeSameAs(GeographyTypes.Of(typeFactory));
        }

        /// <summary>
        /// A column an adapter declares <c>NOT NULL</c> is still a geography column.
        /// </summary>
        /// <remarks>
        /// The shape an adapter reaches for, and the one a type of this package's own could not survive:
        /// <c>RelDataTypeFactoryImpl.copySimpleType</c> answers a change of a <c>JavaType</c>'s nullability by
        /// constructing a plain one, so a subclass was dropped the first time a column was declared
        /// <c>NOT NULL</c>. Being Calcite's own type, there is nothing left to drop.
        /// </remarks>
        [TestMethod]
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
        /// A geography and a geometry can be brought together.
        /// </summary>
        /// <remarks>
        /// <c>leastRestrictive</c> is what a <c>UNION</c> asks. It used to throw over one of each, reaching
        /// the assignment rules for <c>OTHER</c>; there being one type, there is nothing left to refuse.
        /// </remarks>
        [TestMethod]
        public void ShouldBringAnyTwoGeometriesTogether()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);
            var geometry = GeographyTypes.GeometryOf(typeFactory);

            var both = typeFactory.leastRestrictive(java.util.Arrays.asList([geography, geometry]));

            GeographyTypes.IsGeometry(both).Should().BeTrue();
        }

    }

}
