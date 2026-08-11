using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Clr;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// What IKVM shows Java of a .NET type, and what Calcite's type factory makes of it.
    /// </summary>
    /// <remarks>
    /// Every one of these is a measurement the reflective schema is built on rather than a behaviour of ours,
    /// and each was measured before a line of it was written. They are here because they are IKVM's answers
    /// and not Calcite's: an upgrade that changes any of them changes what <see cref="ClrRecordType"/> is for,
    /// and the failure should be this rather than a query returning nothing.
    /// </remarks>
    [TestClass]
    public class ClrRecordTypeTests
    {

        static ClrRecordTypeTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
        }

        public class PropertyRow
        {

            public int Empid { get; set; }

            public string Name { get; set; } = "";

        }

        public class FieldRow
        {

            public int Empid;

            public string Name = "";

        }

        /// <summary>
        /// A .NET property is not a field to Java, and a class of them has no fields at all.
        /// </summary>
        /// <remarks>
        /// The fact the whole design turns on. A property is a <c>get_</c>/<c>set_</c> method pair and its
        /// backing field is private under a mangled name, so there is nothing for <c>Types.nthField</c> to
        /// index into and <c>JavaRowFormat.CUSTOM.field</c>'s second branch cannot be used.
        /// </remarks>
        [TestMethod]
        public void APropertyIsNotAFieldToJava()
        {
            ((java.lang.Class)typeof(PropertyRow)).getFields().Should().BeEmpty();
            ((java.lang.Class)typeof(FieldRow)).getFields().Should().HaveCount(2);
        }

        /// <summary>
        /// So Calcite's type factory makes a row of no columns of one, and says nothing.
        /// </summary>
        /// <remarks>
        /// <c>createStructType(Class)</c> walks <c>getFields()</c>. Over a class of properties that is a row
        /// of nothing, and a plan over it would run and return no columns rather than fail. That silence is
        /// what <c>ClrTypeMapper.RowType</c> replaces.
        /// </remarks>
        [TestMethod]
        public void CalcitesTypeFactoryMakesAnEmptyRowOfAClassOfProperties()
        {
            var factory = new JavaTypeFactoryImpl();

            factory.createType((java.lang.Class)typeof(PropertyRow)).getFieldCount().Should().Be(0);
            factory.createType((java.lang.Class)typeof(FieldRow)).getFieldCount().Should().Be(2);
        }

        /// <summary>
        /// A CLR sequence is not a Java one, so nothing of Calcite's would take it for a table.
        /// </summary>
        /// <remarks>
        /// <c>ReflectiveSchema.getElementType</c> accepts an array or an <c>Iterable</c> and answers null
        /// otherwise, so a member holding a <c>List&lt;Employee&gt;</c> is not a candidate table there at all.
        /// </remarks>
        [TestMethod]
        public void AClrSequenceIsNotAJavaIterable()
        {
            ((java.lang.Class)typeof(java.lang.Iterable))
                .isAssignableFrom((java.lang.Class)typeof(IEnumerable<PropertyRow>))
                .Should().BeFalse();
        }

        /// <summary>
        /// The CLR types Calcite has no representation for are refused by name rather than mapped to nothing.
        /// </summary>
        /// <remarks>
        /// <c>decimal</c>, <c>DateTime</c> and <c>int?</c> reach Java as <c>cli.System.Decimal</c> and the
        /// like, which no rule of Calcite's knows, so <c>createType</c> would send each to
        /// <c>createStructType</c> and answer a column of no columns. Each has a representation Calcite would
        /// accept and reaching it is a conversion per value, which a row that is the object itself has nowhere
        /// to put.
        /// </remarks>
        [TestMethod]
        public void ATypeCalciteCannotRepresentIsRefused()
        {
            ClrTypeMapper.IsNativelyRepresented(typeof(int)).Should().BeTrue();
            ClrTypeMapper.IsNativelyRepresented(typeof(long)).Should().BeTrue();
            ClrTypeMapper.IsNativelyRepresented(typeof(double)).Should().BeTrue();
            ClrTypeMapper.IsNativelyRepresented(typeof(bool)).Should().BeTrue();
            ClrTypeMapper.IsNativelyRepresented(typeof(string)).Should().BeTrue();

            ClrTypeMapper.IsNativelyRepresented(typeof(decimal)).Should().BeFalse();
            ClrTypeMapper.IsNativelyRepresented(typeof(DateTime)).Should().BeFalse();
            ClrTypeMapper.IsNativelyRepresented(typeof(Guid)).Should().BeFalse();
            ClrTypeMapper.IsNativelyRepresented(typeof(int?)).Should().BeFalse();

            var refused = () => ClrTypeMapper.RowType(new JavaTypeFactoryImpl(), typeof(Unrepresentable));
            refused.Should().Throw<NotSupportedException>().WithMessage("*Paid*System.Decimal*");
        }

        public record Unrepresentable(int Empid, decimal Paid);

        /// <summary>
        /// A record's columns are its constructor's parameters, in that order.
        /// </summary>
        /// <remarks>
        /// .NET promises no order from <c>GetProperties</c>, and <c>SELECT *</c> is positional. Where a
        /// constructor takes exactly the members it is the authority, because that constructor is what
        /// <c>JavaRowFormat.CUSTOM.record</c> calls to build a row back.
        /// </remarks>
        [TestMethod]
        public void ARecordsColumnsAreItsConstructorsParameters()
        {
            var members = ClrReflect.Members(typeof(Ordered));

            members.Should().HaveCount(3);
            members[0].Member.Name.Should().Be("Zebra");
            members[1].Member.Name.Should().Be("Apple");
            members[2].Member.Name.Should().Be("Mango");
        }

        public record Ordered(int Zebra, string Apple, int Mango);

    }

}
