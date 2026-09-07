using Apache.Calcite.Geography.Rel.Type;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    // inside the namespace deliberately: from Apache.Calcite.Geography.Tests the simple name
    // Geography reaches the namespace Apache.Calcite.Geography before any compilation-unit
    // alias, and a using-alias of the namespace body is resolved ahead of both.
    using Geography = Apache.Calcite.Geography.Runtime.Geography;

    /// <summary>
    /// What the <c>GEOGRAPHY</c> type reports about itself.
    /// </summary>
    [TestClass]
    public class GeographyTypeTests
    {

        /// <summary>
        /// The digest names the carrier class, which is what keeps the type distinct.
        /// </summary>
        /// <remarks>
        /// It reads <c>JavaType(class cli.…)</c> rather than <c>GEOGRAPHY</c> because the type is an ordinary
        /// <c>JavaType</c> rather than a subclass answering a name of its own. That is the trade the design
        /// makes: a subclass could write <c>GEOGRAPHY</c> here and would be discarded by
        /// <c>copySimpleType</c> the first time an adapter declared a column <c>NOT NULL</c>.
        /// </remarks>
        [TestMethod]
        public void ShouldReportItsCarrierClassAsItsTypeString()
        {
            var type = GeographyTypes.Of(GeographyFixture.TypeFactory());

            type.getFullTypeString().Should().Be("JavaType(class cli.Apache.Calcite.Geography.Runtime.Geography)");
        }

        [TestMethod]
        public void ShouldReportOtherAsItsSqlTypeName()
        {
            var type = GeographyTypes.Of(GeographyFixture.TypeFactory());

            type.getSqlTypeName().Should().BeSameAs(SqlTypeName.OTHER);
        }

        /// <summary>
        /// The runtime carrier is <see cref="Geography"/>, asked either way round.
        /// </summary>
        /// <remarks>
        /// A real class rather than <c>Object</c>, which is what a type that is not a <c>JavaType</c> would
        /// have answered — so the conventions type a geography column properly and generated code names the
        /// class. Janino resolves the <c>cli.</c>-prefixed name IKVM gives it, which it could not before
        /// 8.16.0.
        /// </remarks>
        [TestMethod]
        public void ShouldReportTheCarrierAsItsJavaClass()
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var type = GeographyTypes.Of(typeFactory);

            ((RelDataTypeFactoryImpl.JavaType)type).getJavaClass().Should().BeSameAs((java.lang.Class)typeof(Geography));
            typeFactory.getJavaClass(type).Should().BeSameAs((java.lang.Class)typeof(Geography));
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
        /// Asking for the type <c>NOT NULL</c> keeps the marking.
        /// </summary>
        /// <remarks>
        /// <c>RelDataTypeFactoryImpl.copySimpleType</c> answers a change of nullability on a <c>JavaType</c>
        /// with <c>new JavaType(Primitive.box(clazz), nullable)</c>, and <c>Primitive.box</c> returns a class
        /// that is neither a primitive nor a box unchanged. So the copy is a <c>JavaType</c> over the same
        /// class — the type again, at the other nullability — and it holds on a stock
        /// <c>JavaTypeFactoryImpl</c> with no factory of ours in front of Calcite.
        ///
        /// <para>This is the whole reason the marking is a class rather than a subclass of <c>JavaType</c>
        /// answering a <c>SqlTypeName</c> of its own. That method's other branch, the one for everything
        /// that is not a <c>JavaType</c>, returns the type untouched and so cannot change nullability at
        /// all; and a subclass is discarded outright. Only a distinct class is both copied and kept.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldKeepTheMarkingWhenMadeNotNullable()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);

            typeFactory.createTypeWithNullability(geography, true).Should().BeSameAs(geography);

            var notNull = typeFactory.createTypeWithNullability(geography, false);
            GeographyTypes.IsGeography(notNull).Should().BeTrue();
            notNull.isNullable().Should().BeFalse();
            notNull.getSqlTypeName().Should().BeSameAs(SqlTypeName.OTHER);

            // and back again, to the interned instance
            typeFactory.createTypeWithNullability(notNull, true).Should().BeSameAs(geography);
        }

        /// <summary>
        /// A column an adapter declares <c>NOT NULL</c> is still a geography column.
        /// </summary>
        /// <remarks>
        /// The shape an adapter reaches for: build a row type, name a column, say it cannot be null. The last
        /// of those goes through <c>createTypeWithNullability</c>, which is where a <c>JavaType</c> subclass
        /// would have been dropped and an ordinary geometry handed back — a geodesic column silently becoming
        /// a planar one that Calcite's own <c>ST_*</c> would take. Distinguishing by class survives it.
        /// </remarks>
        [TestMethod]
        public void ShouldSurviveAColumnDeclaredNotNull()
        {
            var typeFactory = GeographyFixture.TypeFactory();
            var geography = GeographyTypes.Of(typeFactory);

            var row = typeFactory.builder().add("GEOG", geography).nullable(false).build();
            var column = ((RelDataTypeField)row.getFieldList().get(0)).getType();

            GeographyTypes.IsGeography(column).Should().BeTrue();
            column.isNullable().Should().BeFalse();

            // and the row being not-nullable, which is a different call, keeps it too
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
