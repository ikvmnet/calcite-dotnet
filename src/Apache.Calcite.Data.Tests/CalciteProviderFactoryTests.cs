
using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteProviderFactoryTests
    {

        [Fact]
        public void Should_expose_singleton_instance()
        {
            Assert.NotNull(CalciteProviderFactory.Instance);
            Assert.Same(CalciteProviderFactory.Instance, CalciteProviderFactory.Instance);
        }

        [Fact]
        public void CreateConnection_should_return_CalciteConnection()
        {
            Assert.IsType<CalciteConnection>(CalciteProviderFactory.Instance.CreateConnection());
        }

        [Fact]
        public void CreateCommand_should_return_CalciteCommand()
        {
            Assert.IsType<CalciteCommand>(CalciteProviderFactory.Instance.CreateCommand());
        }

        [Fact]
        public void CreateParameter_should_return_CalciteParameter()
        {
            Assert.IsType<CalciteParameter>(CalciteProviderFactory.Instance.CreateParameter());
        }

        [Fact]
        public void CreateConnectionStringBuilder_should_return_CalciteConnectionStringBuilder()
        {
            Assert.IsType<CalciteConnectionStringBuilder>(CalciteProviderFactory.Instance.CreateConnectionStringBuilder());
        }

    }

}
