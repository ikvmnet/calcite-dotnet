using System.Data;
using System.Threading.Tasks;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteDataSourceTests
    {

        [Fact]
        public void CreateConnection_returns_closed_connection_with_configured_connection_string()
        {
            using var dataSource = new CalciteDataSource(TestModels.InlineEmptyModelConnectionString);

            using var connection = dataSource.CreateConnection();

            Assert.Equal(ConnectionState.Closed, connection.State);
            var expected = new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString).ConnectionString;
            Assert.Equal(expected, connection.ConnectionString);
        }

        [Fact]
        public void OpenConnection_returns_open_connection()
        {
            using var dataSource = new CalciteDataSource(TestModels.InlineEmptyModelConnectionString);

            using var connection = dataSource.OpenConnection();

            Assert.Equal(ConnectionState.Open, connection.State);
        }

        [Fact]
        public async Task OpenConnectionAsync_returns_open_connection()
        {
            await using var dataSource = new CalciteDataSource(TestModels.InlineEmptyModelConnectionString);

            await using var connection = await dataSource.OpenConnectionAsync();

            Assert.Equal(ConnectionState.Open, connection.State);
        }

        [Fact]
        public void CreateCommand_returns_command_bound_to_data_source_connection()
        {
            using var dataSource = new CalciteDataSource(TestModels.InlineEmptyModelConnectionString);

            using var command = dataSource.CreateCommand("SELECT 1");

            Assert.Equal("SELECT 1", command.CommandText);
            var result = command.ExecuteScalar();
            Assert.Equal(1, System.Convert.ToInt32(result));
        }

        [Fact]
        public void Builder_constructor_uses_builder_connection_string()
        {
            var builder = new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString);

            using var dataSource = new CalciteDataSource(builder);

            Assert.Equal(builder.ConnectionString, dataSource.ConnectionString);
        }

    }

}
