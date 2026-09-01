using System;

using Xunit;

namespace Apache.Calcite.Data.Internal.Tests
{

    public class UuidConverterTests
    {

        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        [InlineData("00000000-0000-0000-ffff-ffffffffffff")] // least-significant half only
        [InlineData("ffffffff-ffff-ffff-0000-000000000000")] // most-significant half only
        public void Guid_should_roundtrip_through_UUID(string literal)
        {
            var value = Guid.Parse(literal);

            var uuid = UuidConverter.ToUuid(value);
            var back = UuidConverter.ToGuid(uuid);

            Assert.Equal(value, back);
        }

        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        public void Both_forms_should_write_the_same_canonical_text(string literal)
        {
            var value = Guid.Parse(literal);

            // the byte order is the one the canonical 8-4-4-4-12 text writes, so a transfer that
            // got either half or their order wrong would show up here
            Assert.Equal(literal, value.ToString("D"));
            Assert.Equal(literal, UuidConverter.ToUuid(value).toString());
        }

        [Theory]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
        [InlineData("cccccccc-0000-0000-0000-000000000001")]
        [InlineData("12345678-1234-4321-7777-987654321000")]
        public void UUID_should_roundtrip_through_Guid(string literal)
        {
            var value = java.util.UUID.fromString(literal);

            var guid = UuidConverter.ToGuid(value);
            var back = UuidConverter.ToUuid(guid);

            Assert.Equal(value, back);
        }

    }

}
