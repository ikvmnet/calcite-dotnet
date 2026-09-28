using System;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    public class JavaUuidsTests
    {

        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        [InlineData("00000000-0000-0000-ffff-ffffffffffff")] // least-significant half only
        [InlineData("ffffffff-ffff-ffff-0000-000000000000")] // most-significant half only
        public void GuidShouldRoundTripThroughUuid(string literal)
        {
            var value = Guid.Parse(literal);

            var uuid = JavaUuids.ToUuid(value);
            var back = JavaUuids.ToGuid(uuid);

            back.Should().Be(value);
        }

        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        public void BothFormsShouldWriteTheSameCanonicalText(string literal)
        {
            var value = Guid.Parse(literal);

            // both forms write their bytes in the order of the canonical 8-4-4-4-12 text, so a conversion that
            // swapped or reordered the halves shows up here
            value.ToString("D").Should().Be(literal);
            JavaUuids.ToUuid(value).toString().Should().Be(literal);
        }

        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        public void UuidShouldRoundTripThroughGuid(string literal)
        {
            var value = java.util.UUID.fromString(literal);

            var guid = JavaUuids.ToGuid(value);
            var back = JavaUuids.ToUuid(guid);

            back.Should().Be(value);
        }

        /// <summary>
        /// A <see cref="Guid"/> round-trips through <c>UuidValue</c>, the class Calcite holds a UUID as at run
        /// time, and the wrapped <c>UUID</c> is the one <c>JavaUuids.ToUuid</c> gives.
        /// </summary>
        /// <param name="literal">The GUID, in its standard text form.</param>
        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        [InlineData("00000000-0000-0000-ffff-ffffffffffff")] // least-significant half only
        [InlineData("ffffffff-ffff-ffff-0000-000000000000")] // most-significant half only
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
        /// <c>JavaTypeFactoryImpl.getJavaClass</c> maps a UUID column to <c>UuidValue</c>, not
        /// <c>java.util.UUID</c>, so a value supplied as a bare <c>UUID</c> fails the cast a generated plan makes.
        /// </summary>
        [Fact]
        public void TheJavaClassOfAUuidShouldBeTheWrapper()
        {
            var typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
            var uuidType = typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.UUID);

            typeFactory.getJavaClass(uuidType).Should().Be((object)(java.lang.Class)typeof(org.apache.calcite.util.UuidValue));
        }

    }

}
