using System;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Tests.Interop
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

    }

}
