using System;

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
            cmd.CommandText = "CREATE SCHEMA \"myschema\"";
            var affected = cmd.ExecuteNonQuery();
            Assert.True(affected >= 0);
        }

        [Fact]
        public void CreateSchema_IfNotExists_should_be_idempotent()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA \"myschema\"";
            cmd.ExecuteNonQuery();

            // Running the same statement a second time should not throw.
            var ex = Record.Exception(() => cmd.ExecuteNonQuery());
            Assert.Null(ex);
        }

    }

}
