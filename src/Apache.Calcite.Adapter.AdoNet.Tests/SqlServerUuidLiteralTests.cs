using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests a <c>UUID</c> literal pushed down to SQL Server.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A comparison against a <c>uniqueidentifier</c> column coerces its string literal to <c>UUID</c>, and
    /// <c>SqlUuidLiteral.unparse</c> writes the typed literal <c>UUID '…'</c> without consulting the dialect.
    /// SQL Server has neither that literal syntax nor a <c>UUID</c> type.
    /// </para>
    /// <para>
    /// The SQL Server metadata's <c>IAdoSqlSyntax.Rewrite</c> rewrites each such literal into a cast of its
    /// text, whose target type the dialect's <c>getCastSpec</c> writes as <c>UNIQUEIDENTIFIER</c>.
    /// </para>
    /// </remarks>
    public class SqlServerUuidLiteralTests
    {

        static SqlServerUuidLiteralTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        /// <summary>
        /// The GUID stored in <c>dbo.TYPES</c> row 1, the one a comparison should find.
        /// </summary>
        const string KnownGuid = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public SqlServerUuidLiteralTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// What a query returned, and the statements sent to the server to answer it.
        /// </summary>
        /// <param name="Rows">The first column of each row, as a string.</param>
        /// <param name="Statements">The statements the adapter generated.</param>
        readonly record struct Answer(List<string> Rows, IReadOnlyList<string> Statements);

        /// <summary>
        /// Runs a query against the fixture's database, recording the generated SQL.
        /// </summary>
        /// <param name="sql">The statement to run through Calcite against the fixture's SQL Server
        /// database.</param>
        /// <returns>The first column of every row, and the SQL statements the adapter sent to the
        /// server.</returns>
        static Answer Run(string sql)
        {
            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            var generated = new GeneratedSql();
            var handle = Hook.QUERY_PLAN.addThread(generated);

            try
            {
                using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);
                var root = ((CalciteConnection)connection).getRootSchema();
                root.add("ADO", AdoSchema.Create(root, "ADO", SqlServerFixture.Shared.DataSource, null, "dbo"));

                using var statement = connection.createStatement();
                var results = statement.executeQuery(sql);

                var rows = new List<string>();
                while (results.next())
                    rows.Add(results.getObject(1)?.ToString() ?? "NULL");

                return new Answer(rows, generated.Statements);
            }
            finally
            {
                handle.close();
            }
        }

        /// <summary>
        /// The equality finds its row, so the literal reached the server in a form it parses.
        /// </summary>
        [Fact]
        public void AGuidEqualityMatchesItsRow()
        {
            Assert.Equal(
                new[] { "1" },
                Run($"SELECT ID FROM ADO.TYPES WHERE C_GUID = '{KnownGuid}'").Rows);
        }

        /// <summary>
        /// A GUID that matches nothing gives an empty result rather than a failure.
        /// </summary>
        [Fact]
        public void AGuidEqualityMatchingNothingIsEmpty()
        {
            Assert.Equal(
                System.Array.Empty<string>(),
                Run("SELECT ID FROM ADO.TYPES WHERE C_GUID = '00000000-0000-0000-0000-000000000000'").Rows);
        }

        /// <summary>
        /// The GUID is pushed down as a cast to <c>uniqueidentifier</c>, not as a <c>UUID '…'</c> typed literal.
        /// </summary>
        [Fact]
        public void TheGuidLiteralIsCastToUniqueidentifierOnTheServer()
        {
            var answer = Run($"SELECT ID FROM ADO.TYPES WHERE C_GUID = '{KnownGuid}'");
            var generated = string.Join("\n", answer.Statements);

            answer.Statements.Count.Should().NotBe(0, "nothing was pushed down at all");
            Assert.True(
                answer.Statements.Any(s => s.ToUpperInvariant().Contains("UNIQUEIDENTIFIER")),
                $"the GUID was not cast to uniqueidentifier: {generated}");
            Assert.False(
                answer.Statements.Any(s => s.Contains("UUID '")),
                $"a bare UUID typed literal reached the server: {generated}");
        }

    }

}
