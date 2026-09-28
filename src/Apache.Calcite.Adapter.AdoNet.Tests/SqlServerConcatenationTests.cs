using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests string concatenation against SQL Server through SqlClient, ODBC and OLE DB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// T-SQL has no <c>||</c>; the SQL Server dialect writes <c>+</c>. <see cref="AdoSqlDialectsTests"/>
    /// checks the rendering without a database. These check that the server accepts the statement and that
    /// the result keeps the meaning of <c>||</c>, including null propagation, which <c>+</c> has and T-SQL's
    /// <c>CONCAT</c> does not.
    /// </para>
    /// <para>
    /// All three drivers select the same dialect from the product name, so each runs the same statements.
    /// </para>
    /// </remarks>
    public class SqlServerConcatenationTests
    {

        const string SqlClient = "sqlclient";
        const string Odbc = "odbc";
        const string OleDb = "oledb";

        static SqlServerConcatenationTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public SqlServerConcatenationTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// Returns the data source for a provider, skipping the test where it is not installed.
        /// </summary>
        /// <param name="provider">The provider name: one of the <c>SqlClient</c>, <c>Odbc</c> or <c>OleDb</c>
        /// constants.</param>
        /// <returns>The fixture's data source for that provider, connected to the shared test database.</returns>
        static DbDataSource DataSourceFor(string provider)
        {
            switch (provider)
            {
                case SqlClient:
                    return SqlServerFixture.Shared.DataSource;
                case Odbc:
                    if (SqlServerFixture.OdbcDriver is null)
                        Assert.Skip("No SQL Server ODBC driver is installed on this machine.");

                    return SqlServerFixture.Shared.OdbcDataSource;
                case OleDb:
                    if (SqlServerFixture.OleDbProvider is null)
                        Assert.Skip("No SQL Server OLE DB provider is registered for this process architecture.");

                    return SqlServerFixture.Shared.OleDbDataSource;
                default:
                    throw new ArgumentException($"unknown provider {provider}", nameof(provider));
            }
        }

        /// <summary>
        /// What a query returned, and the statements sent to the server to answer it.
        /// </summary>
        /// <param name="Rows">Each row's values joined by a pipe.</param>
        /// <param name="Statements">The statements the adapter generated.</param>
        readonly record struct Answer(List<string> Rows, IReadOnlyList<string> Statements);

        /// <summary>
        /// Runs a query against the fixture's database through a provider, recording the generated SQL.
        /// </summary>
        /// <param name="provider">The provider to reach SQL Server through, as <see cref="DataSourceFor"/>
        /// takes it.</param>
        /// <param name="sql">The statement to run through Calcite.</param>
        /// <returns>The rows, each pipe-joined, and the SQL the adapter sent to the server.</returns>
        static Answer Run(string provider, string sql)
        {
            var dataSource = DataSourceFor(provider);

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            var generated = new GeneratedSql();
            var handle = Hook.QUERY_PLAN.addThread(generated);

            try
            {
                using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);
                var root = ((CalciteConnection)connection).getRootSchema();
                root.add("ADO", AdoSchema.Create(root, "ADO", dataSource, null, "dbo"));

                using var statement = connection.createStatement();
                var results = statement.executeQuery(sql);

                var rows = new List<string>();
                var columns = results.getMetaData().getColumnCount();

                while (results.next())
                {
                    var values = new string[columns];
                    for (int i = 0; i < columns; i++)
                        values[i] = results.getObject(i + 1)?.ToString() ?? "NULL";

                    rows.Add(string.Join("|", values));
                }

                return new Answer(rows, generated.Statements);
            }
            finally
            {
                handle.close();
            }
        }

        /// <summary>
        /// Runs a query and returns its rows.
        /// </summary>
        /// <param name="provider">The provider to reach SQL Server through.</param>
        /// <param name="sql">The statement to run through Calcite.</param>
        /// <returns>One string per row, its values joined by a pipe with <c>NULL</c> for a null.</returns>
        static List<string> Rows(string provider, string sql)
        {
            return Run(provider, sql).Rows;
        }

        #region The shapes the operator reaches the server in

        /// <summary>
        /// Concatenation in a select list.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void AProjectionConcatenates(string provider)
        {
            Assert.Equivalent(
                new[] { "aabb" },
                Rows(provider, "SELECT A || B FROM ADO.CAT WHERE ID = 1"), strict: true);
        }

        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void APredicateConcatenates(string provider)
        {
            Assert.Equivalent(
                new[] { "1" },
                Rows(provider, "SELECT ID FROM ADO.CAT WHERE A || B = 'aabb'"), strict: true);
        }

        /// <summary>
        /// Concatenation in a sort key, descending so that an ignored sort would give a different order. The
        /// null row is excluded so that null ordering does not affect the result.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ASortKeyConcatenates(string provider)
        {
            Assert.Equal(
                new[] { "3", "1" },
                Rows(provider, "SELECT ID FROM ADO.CAT WHERE B IS NOT NULL ORDER BY A || B DESC"));
        }

        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void AnAggregateArgumentConcatenates(string provider)
        {
            Assert.Equivalent(
                new[] { "ddee" },
                Rows(provider, "SELECT MAX(A || B) FROM ADO.CAT"), strict: true);
        }

        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void AGroupKeyConcatenates(string provider)
        {
            Assert.Equivalent(
                new[] { "aabb|1", "ddee|1", "NULL|1" },
                Rows(provider, "SELECT A || B, COUNT(*) FROM ADO.CAT GROUP BY A || B"), strict: true);
        }

        /// <summary>
        /// Two literals, which are not folded away before the statement is generated.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void TwoLiteralsConcatenate(string provider)
        {
            Assert.Equivalent(
                new[] { "xy" },
                Rows(provider, "SELECT 'x' || 'y' FROM ADO.CAT WHERE ID = 1"), strict: true);
        }

        #endregion

        #region What the operator means

        /// <summary>
        /// <c>||</c> yields null when either operand is null, as <c>+</c> does under the default
        /// <c>CONCAT_NULL_YIELDS_NULL</c>; T-SQL's <c>CONCAT</c> would read the null as the empty string and
        /// return <c>cc</c>.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ANullOperandMakesTheWholeExpressionNull(string provider)
        {
            Assert.Equivalent(
                new[] { "NULL" },
                Rows(provider, "SELECT A || B FROM ADO.CAT WHERE ID = 2"), strict: true);
        }

        /// <summary>
        /// The same null propagation in a predicate: under <c>CONCAT</c> the row would match.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ANullOperandMatchesNothing(string provider)
        {
            Assert.Empty(Rows(provider, "SELECT ID FROM ADO.CAT WHERE A || B = 'cc'"));
        }

        #endregion

        #region Precedence

        /// <summary>
        /// <c>||</c> has precedence 60 and <c>+</c> has 40, so substituting one for the other can misplace
        /// parentheses in a nested expression. These are nestings a validated plan produces, run on the server
        /// to check the grouping.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ConcatenationNestsInConcatenation(string provider)
        {
            Assert.Equivalent(
                new[] { "aabbaa" },
                Rows(provider, "SELECT A || B || A FROM ADO.CAT WHERE ID = 1"), strict: true);
        }

        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ConcatenationNestsToTheRight(string provider)
        {
            Assert.Equivalent(
                new[] { "aabbaa" },
                Rows(provider, "SELECT A || (B || A) FROM ADO.CAT WHERE ID = 1"), strict: true);
        }

        /// <summary>
        /// Arithmetic reaches a string only through a cast, which writes its own parentheses.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ConcatenationAgainstArithmetic(string provider)
        {
            Assert.Equivalent(
                new[] { "aa2" },
                Rows(provider, "SELECT A || CAST(ID + 1 AS VARCHAR(4)) FROM ADO.CAT WHERE ID = 1"), strict: true);
        }

        /// <summary>
        /// A comparison binds looser than either spelling, and a conjunction looser still.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ConcatenationInsideAConjunction(string provider)
        {
            Assert.Equivalent(
                new[] { "3" },
                Rows(provider, "SELECT ID FROM ADO.CAT WHERE A || B > 'aabb' AND ID > 1"), strict: true);
        }

        #endregion

        #region What went to the server

        /// <summary>
        /// The concatenation is evaluated on the server: the generated statement carries <c>+</c> and no
        /// <c>||</c>.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void TheOperatorIsPushedDownAsPlus(string provider)
        {
            var answer = Run(provider, "SELECT A || B FROM ADO.CAT WHERE ID = 1");
            var generated = string.Join("\n", answer.Statements);

            answer.Statements.Count.Should().NotBe(0, "nothing was pushed down at all");
            Assert.False(generated.Contains("||"), $"the operator the server refuses was pushed down: {generated}");
            Assert.True(answer.Statements.Any(s => s.Contains('+')), $"nothing concatenated on the server: {generated}");
        }

        #endregion

    }

}
