using System;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    [TestClass]
    public class JavaUuidsTests
    {

        [TestMethod]
        [DataRow("00000000-0000-0000-0000-000000000000")]
        [DataRow("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [DataRow("cccccccc-0000-0000-0000-000000000001")]
        [DataRow("12345678-1234-4321-7777-987654321000")]
        [DataRow("00000000-0000-0000-ffff-ffffffffffff")] // least-significant half only
        [DataRow("ffffffff-ffff-ffff-0000-000000000000")] // most-significant half only
        public void GuidShouldRoundTripThroughUuid(string literal)
        {
            var value = Guid.Parse(literal);

            var uuid = JavaUuids.ToUuid(value);
            var back = JavaUuids.ToGuid(uuid);

            back.Should().Be(value);
        }

        [TestMethod]
        [DataRow("00000000-0000-0000-0000-000000000000")]
        [DataRow("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [DataRow("cccccccc-0000-0000-0000-000000000001")]
        [DataRow("12345678-1234-4321-7777-987654321000")]
        public void BothFormsShouldWriteTheSameCanonicalText(string literal)
        {
            var value = Guid.Parse(literal);

            // the byte order is the one the canonical 8-4-4-4-12 text writes, so a transfer that
            // got either half or their order wrong would show up here
            value.ToString("D").Should().Be(literal);
            JavaUuids.ToUuid(value).toString().Should().Be(literal);
        }

        [TestMethod]
        [DataRow("00000000-0000-0000-0000-000000000000")]
        [DataRow("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [DataRow("cccccccc-0000-0000-0000-000000000001")]
        [DataRow("12345678-1234-4321-7777-987654321000")]
        public void UuidShouldRoundTripThroughGuid(string literal)
        {
            var value = java.util.UUID.fromString(literal);

            var guid = JavaUuids.ToGuid(value);
            var back = JavaUuids.ToUuid(guid);

            back.Should().Be(value);
        }

        /// <summary>
        /// And through the wrapper Calcite actually holds a UUID as at run time.
        /// </summary>
        /// <remarks>
        /// <c>UuidValue</c> is what <c>JavaTypeFactoryImpl.getJavaClass</c> answers for a UUID, so it is
        /// what a generated plan casts a bound value to. It carries the same two halves as the bare
        /// <c>UUID</c>, and this says the transfer does not disagree with the one that goes through it.
        /// </remarks>
        [TestMethod]
        [DataRow("00000000-0000-0000-0000-000000000000")]
        [DataRow("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [DataRow("cccccccc-0000-0000-0000-000000000001")]
        [DataRow("12345678-1234-4321-7777-987654321000")]
        [DataRow("00000000-0000-0000-ffff-ffffffffffff")] // least-significant half only
        [DataRow("ffffffff-ffff-ffff-0000-000000000000")] // most-significant half only
        public void GuidShouldRoundTripThroughUuidValue(string literal)
        {
            var value = Guid.Parse(literal);

            var uuid = JavaUuids.ToUuidValue(value);
            var back = JavaUuids.ToGuid(uuid);

            back.Should().Be(value);
            uuid.uuid().Should().Be(JavaUuids.ToUuid(value));
            uuid.uuid().toString().Should().Be(literal);
        }

        /// <summary>
        /// The class a UUID column's value is cast to is the wrapper, not the bare type.
        /// </summary>
        /// <remarks>
        /// This is the fact the three producers rest on, and it is read from the type factory rather than
        /// transcribed: a value handed over as a <c>java.util.UUID</c> fails the cast a plan generates.
        /// </remarks>
        [TestMethod]
        public void TheJavaClassOfAUuidShouldBeTheWrapper()
        {
            var typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
            var uuidType = typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.UUID);

            typeFactory.getJavaClass(uuidType).Should().Be((object)(java.lang.Class)typeof(org.apache.calcite.util.UuidValue));
        }

    }

}
