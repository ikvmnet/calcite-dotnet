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
    /// What the <c>GEOGRAPHY</c> type reports about itself.
    /// </summary>
    [TestClass]
    public class GeographyTypeTests
    {

        [TestMethod]
        public void ShouldReportGeographyAsItsTypeString()
        {
            var type = GeographyTypes.Of(GeographyFixture.TypeFactory());

            type.getFullTypeString().Should().Be("GEOGRAPHY");
        }

        [TestMethod]
        public void ShouldReportOtherAsItsSqlTypeName()
        {
            var type = GeographyTypes.Of(GeographyFixture.TypeFactory());

            type.getSqlTypeName().Should().BeSameAs(SqlTypeName.OTHER);
        }

        /// <summary>
        /// The runtime carrier is an ordinary JTS geometry, asked either way round.
        /// </summary>
        /// <remarks>
        /// This is what makes the type free: there is no new class, so nothing new for code generation to
        /// name and nothing to convert at a boundary.
        /// </remarks>
        [TestMethod]
        public void ShouldReportJtsGeometryAsItsJavaClass()
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var type = GeographyTypes.Of(typeFactory);

            ((RelDataTypeFactoryImpl.JavaType)type).getJavaClass().Should().BeSameAs((java.lang.Class)typeof(Geometry));
            typeFactory.getJavaClass(type).Should().BeSameAs((java.lang.Class)typeof(Geometry));
        }

        /// <summary>
        /// The digest is what keeps the two apart, and it is the only thing that does.
        /// </summary>
        [TestMethod]
        public void ShouldBeDistinctFromCalcitesGeometryType()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);
            var geometry = GeographyTypes.GeometryOf(typeFactory);

            geometry.getFullTypeString().Should().Be("JavaType(class org.locationtech.jts.geom.Geometry)");
            geography.getFullTypeString().Should().NotBe(geometry.getFullTypeString());
            geography.Equals(geometry).Should().BeFalse();
            geometry.getSqlTypeName().Should().BeSameAs(SqlTypeName.GEOMETRY);
        }

        /// <summary>
        /// One instance, however many times it is asked for and by however many type factories.
        /// </summary>
        /// <remarks>
        /// <c>RelDataTypeFactoryImpl</c> interns types on a static cache keyed by digest, and
        /// <c>GeographyTypes.Of</c> goes through <c>copyType</c> to reach it, since <c>canonize</c> is
        /// protected.
        /// </remarks>
        [TestMethod]
        public void ShouldBeCanonizedToOneInstance()
        {
            var typeFactory = GeographyFixture.TypeFactory();

            GeographyTypes.Of(typeFactory).Should().BeSameAs(GeographyTypes.Of(typeFactory));
            GeographyTypes.Of(GeographyFixture.TypeFactory()).Should().BeSameAs(GeographyTypes.Of(typeFactory));
        }

        [TestMethod]
        public void ShouldBeNullable()
        {
            GeographyTypes.Of(GeographyFixture.TypeFactory()).isNullable().Should().BeTrue();
        }

        /// <summary>
        /// Asking for the type <c>NOT NULL</c> loses the marking.
        /// </summary>
        /// <remarks>
        /// <c>RelDataTypeFactoryImpl.copySimpleType</c> answers a change of nullability on any
        /// <c>JavaType</c> with a plain <c>new JavaType(clazz, nullable)</c>, which is not this subclass. The
        /// type is nullable so that the path everything actually takes —
        /// <c>createTypeWithNullability(geography, true)</c> — finds nothing to change and returns it
        /// unchanged, and so that no return type strategy here may run through
        /// <c>SqlTypeTransforms.TO_NULLABLE</c>. Recorded rather than fixed: the copy is Calcite's and there
        /// is no hook in it.
        /// </remarks>
        [TestMethod]
        public void ShouldLoseTheMarkingWhenMadeNotNullable()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);

            typeFactory.createTypeWithNullability(geography, true).Should().BeSameAs(geography);

            var notNull = typeFactory.createTypeWithNullability(geography, false);
            GeographyTypes.IsGeography(notNull).Should().BeFalse();
            notNull.getSqlTypeName().Should().BeSameAs(SqlTypeName.GEOMETRY);
        }

        [TestMethod]
        public void ShouldTellTheTwoApart()
        {
            var typeFactory = GeographyFixture.TypeFactory();

            GeographyTypes.IsGeography(GeographyTypes.Of(typeFactory)).Should().BeTrue();
            GeographyTypes.IsGeometry(GeographyTypes.Of(typeFactory)).Should().BeFalse();
            GeographyTypes.IsGeography(GeographyTypes.GeometryOf(typeFactory)).Should().BeFalse();
            GeographyTypes.IsGeometry(GeographyTypes.GeometryOf(typeFactory)).Should().BeTrue();
        }

    }

}
