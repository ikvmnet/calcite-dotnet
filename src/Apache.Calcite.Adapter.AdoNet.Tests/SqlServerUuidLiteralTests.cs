using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers a <c>UUID</c> literal reaching a real SQL Server, where the rendering of the literal is the
    /// thing in question.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A comparison against a <c>uniqueidentifier</c> column coerces its string literal to <c>UUID</c>, and a
    /// <c>UUID</c> literal unparses as the standard typed literal <c>UUID '…'</c> — <c>SqlUuidLiteral.unparse</c>
    /// writes it with no reference to the dialect at all. SQL Server has no such literal syntax and no
    /// <c>UUID</c> type name, so the statement never parses: it answers "Incorrect syntax" on the string.
    /// </para>
    /// <para>
    /// <see cref="AdoImplementor.AsStatement"/> rewrites the literal into <c>CAST('…' AS UNIQUEIDENTIFIER)</c>
    /// once the plan is a statement, which is the same seam <c>getCastSpec</c> is — the type named as the product
    /// names it, reached from the one place the literal's own unparse would not ask the dialect for.
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
        /// What a query answered, and what was sent to the server to answer it.
        /// </summary>
        /// <param name="Rows"></param>
        /// <param name="Statements"></param>
        readonly record struct Answer(List<string> Rows, IReadOnlyList<string> Statements);

        /// <summary>
        /// Runs a query against the fixture's database, capturing what was pushed down.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
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
        /// The equality finds its row — the literal reached the server as something it could parse.
        /// </summary>
        [Fact]
        public void AGuidEqualityMatchesItsRow()
        {
            Assert.Equal(
                new[] { "1" },
                Run($"SELECT ID FROM ADO.TYPES WHERE C_GUID = '{KnownGuid}'").Rows);
        }

        /// <summary>
        /// And a GUID that matches nothing is an empty answer rather than a failure — the point being that the
        /// statement ran at all, which before the rewrite it did not.
        /// </summary>
        [Fact]
        public void AGuidEqualityMatchingNothingIsEmpty()
        {
            Assert.Equal(
                System.Array.Empty<string>(),
                Run("SELECT ID FROM ADO.TYPES WHERE C_GUID = '00000000-0000-0000-0000-000000000000'").Rows);
        }

        /// <summary>
        /// The claim that the server answered: the GUID went down as a cast to the type SQL Server names, not
        /// as the <c>UUID '…'</c> typed literal it has no syntax for.
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
