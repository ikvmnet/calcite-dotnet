using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Data.Tests
{

    [TestClass]
    public class ConnectionStringBuilderTests
    {

        [TestMethod]
        public void CanParseModel()
        {
            var b = new CalciteConnectionStringBuilder("model=file://foo.model");
            b.Model.Should().Be("file://foo.model");

            var z = new CalciteSchema();
        }

    }

}
