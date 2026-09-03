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

        /// <summary>
        /// A column declared <c>NOT NULL</c> is not a geography column, and nothing says so.
        /// </summary>
        /// <remarks>
        /// The shape an adapter reaches for: build a row type, name a column, say it cannot be null. The last
        /// of those goes through <c>createTypeWithNullability</c>, and that is the call
        /// <c>RelDataTypeFactoryImpl.copySimpleType</c> answers with a plain <c>new JavaType(clazz,
        /// nullable)</c> — so the column that comes out is an ordinary geometry, and Calcite's planar
        /// <c>ST_*</c> will take it. A geodesic column becomes a planar one with no error anywhere, which is
        /// the exact failure this package exists to prevent.
        ///
        /// <para>It cannot be fixed from here. <c>copySimpleType</c> is private;
        /// <c>createTypeWithNullability</c> is public and could be overridden, but a type factory of our own
        /// cannot be put in front of Calcite — <c>PlannerImpl</c> and <c>CalciteConnectionImpl</c> each build
        /// a <c>JavaTypeFactoryImpl</c> outright, and only a protected constructor takes one. Nor does
        /// dropping <c>JavaType</c> help: a type that is not one survives this untouched, and then
        /// <c>getJavaClass</c> answers null and no plan compiles.</para>
        ///
        /// <para>So the rule for an adapter is that a geography column is declared nullable, and this is here
        /// to keep that rule honest rather than to approve of it. Nullability at the level of the row is
        /// fine — only the field-level call degrades.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldNotSurviveAColumnDeclaredNotNull()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);

            var row = typeFactory.builder().add("GEOG", geography).nullable(false).build();
            var column = ((RelDataTypeField)row.getFieldList().get(0)).getType();

            GeographyTypes.IsGeography(column).Should().BeFalse();
            column.getSqlTypeName().Should().BeSameAs(SqlTypeName.GEOMETRY);

            // the row being not-nullable is a different call and does not degrade the field
            var nullableRow = typeFactory.builder().add("GEOG", geography).build();
            var kept = ((RelDataTypeField)typeFactory.createTypeWithNullability(nullableRow, false)
                .getFieldList().get(0)).getType();

            GeographyTypes.IsGeography(kept).Should().BeTrue();
        }

        /// <summary>
        /// Two geography columns can be brought together; a geography and a geometry cannot.
        /// </summary>
        /// <remarks>
        /// <c>leastRestrictive</c> is what a <c>UNION</c> asks. Over two geographies it answers the geography,
        /// which is what a set operation over two such columns needs. Over one of each it reaches the
        /// assignment rules and throws the same <c>No assign rules for OTHER defined</c> the schema-function
        /// route dies on — an error rather than a wrong answer, but an assertion rather than a validation
        /// error, and worth knowing before it turns up in a query plan.
        /// </remarks>
        [TestMethod]
        public void ShouldBringTwoGeographiesTogetherAndRefuseAMixture()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);
            var geometry = GeographyTypes.GeometryOf(typeFactory);

            var both = typeFactory.leastRestrictive(java.util.Arrays.asList([geography, geography]));
            GeographyTypes.IsGeography(both).Should().BeTrue();

            var mixed = () => typeFactory.leastRestrictive(java.util.Arrays.asList([geography, geometry]));
            mixed.Should().Throw<java.lang.AssertionError>().WithMessage("*OTHER*");
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
