using System.Threading.Tasks;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Tests that verify DDL execution via the ADO.NET surface when <c>serverDdl</c> is enabled.
    /// </summary>
    public class CalciteDdlTests
    {

        static CalciteDdlTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl).Assembly);
        }

        static readonly string ServerDdlConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
        }.ConnectionString;

        [Fact]
        public void CreateSchema_IfNotExists_should_succeed_when_serverDdl_is_enabled()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"myschema\"";
            var affected = cmd.ExecuteNonQuery();
            Assert.True(affected >= 0);
        }

        [Fact]
        public void CreateSchema_IfNotExists_should_be_idempotent()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"myschema\"";
            cmd.ExecuteNonQuery();

            // Running the same statement a second time should not throw.
            var ex = Record.Exception(() => cmd.ExecuteNonQuery());
            Assert.Null(ex);
        }

        [Fact]
        public void ExecuteReader_after_ddl_should_return_empty_result_set()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"readertest\"";
            using var r = cmd.ExecuteReader();

            Assert.Equal(0, r.FieldCount);
            Assert.False(r.Read());
        }

        [Fact]
        public void ExecuteReaderAsync_after_ddl_should_return_empty_result_set()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"readertest2\"";
            using var r = cmd.ExecuteReaderAsync().GetAwaiter().GetResult();

            Assert.Equal(0, r.FieldCount);
            Assert.False(r.Read());
        }

        [Fact]
        public void ExecuteNonQuery_insert_should_return_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"dmltest\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"dmltest\" VALUES (2)";
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        [Fact]
        public async Task ExecuteNonQueryAsync_insert_should_return_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"dmltest_async\" (\"id\" INTEGER NOT NULL)";
            await cmd.ExecuteNonQueryAsync();

            cmd.CommandText = "INSERT INTO \"dmltest_async\" VALUES (2)";
            var affected = await cmd.ExecuteNonQueryAsync();
            Assert.Equal(1, affected);
        }

    }

}
