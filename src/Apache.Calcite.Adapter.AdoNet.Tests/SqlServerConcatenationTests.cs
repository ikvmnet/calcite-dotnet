using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers string concatenation against a real SQL Server through each of the three drivers that reach
    /// it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// T-SQL has no <c>||</c>. <c>SqlStdOperatorTable.CONCAT</c> unparses as one and
    /// <c>MssqlSqlDialect.unparseCall</c> does not intercept it, so every statement that concatenated
    /// reached the server carrying an operator it will not parse and answered "Incorrect syntax near '|'" —
    /// in a select list, in a predicate, in a sort key and in an aggregate argument alike, and over two
    /// literals, which are not folded away. <see cref="Metadata.AdoSqlDialects"/> writes <c>+</c> instead.
    /// </para>
    /// <para>
    /// The rendering is pinned without a database in <see cref="AdoSqlDialectsTests"/>; what these add is
    /// that the server accepts the statement and answers what the operator means. Those are separate
    /// claims, and the second is the one that decides between <c>+</c> and <c>CONCAT</c>: both concatenate
    /// and only <c>+</c> propagates null, so a rendering that passed a syntax check would still have been
    /// wrong.
    /// </para>
    /// <para>
    /// All three drivers reach one dialect, chosen from the product name, so a correction that only
    /// SqlClient carried would be a correction in the wrong place. Running the same statements over each is
    /// what says it is in the right one.
    /// </para>
    /// </remarks>
    [TestClass]
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

        [TestInitialize]
        public void Setup()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Inconclusive("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// Returns the data source for a provider, skipping the test where it is not installed.
        /// </summary>
        /// <param name="provider"></param>
        /// <returns></returns>
        static DbDataSource DataSourceFor(string provider)
        {
            switch (provider)
            {
                case SqlClient:
                    return SqlServerFixture.Shared.DataSource;
                case Odbc:
                    if (SqlServerFixture.OdbcDriver is null)
                        Assert.Inconclusive("No SQL Server ODBC driver is installed on this machine.");

                    return SqlServerFixture.Shared.OdbcDataSource;
                case OleDb:
                    if (SqlServerFixture.OleDbProvider is null)
                        Assert.Inconclusive("No SQL Server OLE DB provider is registered for this process architecture.");

                    return SqlServerFixture.Shared.OleDbDataSource;
                default:
                    throw new ArgumentException($"unknown provider {provider}", nameof(provider));
            }
        }

        /// <summary>
        /// Collects the statements the adapter generates, which <c>AdoToEnumerableConverter</c> announces on
        /// <c>Hook.QUERY_PLAN</c> as it builds each one.
        /// </summary>
        /// <remarks>
        /// IKVM does not project a Java default method onto a CLR class that implements the interface, so
        /// <c>andThen</c> has to be written even though a hook handler is never composed with another.
        /// </remarks>
        sealed class GeneratedSql : java.util.function.Consumer
        {

            /// <summary>
            /// Two handlers as one, which is what <c>Consumer.andThen</c> answers in Java.
            /// </summary>
            /// <param name="first"></param>
            /// <param name="then"></param>
            sealed class Composed(java.util.function.Consumer first, java.util.function.Consumer then) : java.util.function.Consumer
            {

                /// <inheritdoc />
                public void accept(object value)
                {
                    first.accept(value);
                    then.accept(value);
                }

                /// <inheritdoc />
                public java.util.function.Consumer andThen(java.util.function.Consumer after)
                {
                    return new Composed(this, after);
                }

            }

            readonly List<string> _statements = [];

            /// <summary>
            /// Gets the statements generated so far.
            /// </summary>
            public IReadOnlyList<string> Statements => _statements;

            /// <inheritdoc />
            public void accept(object value)
            {
                _statements.Add(value?.ToString() ?? "");
            }

            /// <inheritdoc />
            public java.util.function.Consumer andThen(java.util.function.Consumer after)
            {
                return new Composed(this, after);
            }

        }

        /// <summary>
        /// What a query answered, and what was sent to the server to answer it.
        /// </summary>
        /// <param name="Rows"></param>
        /// <param name="Statements"></param>
        readonly record struct Answer(List<string> Rows, IReadOnlyList<string> Statements);

        /// <summary>
        /// Runs a query against the fixture's database through a provider.
        /// </summary>
        /// <param name="provider"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
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
        /// <param name="provider"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        static List<string> Rows(string provider, string sql)
        {
            return Run(provider, sql).Rows;
        }

        #region The shapes the operator reaches the server in

        /// <summary>
        /// A select list, which is the shape in the report.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void AProjectionConcatenates(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "aabb" },
                Rows(provider, "SELECT A || B FROM ADO.CAT WHERE ID = 1"));
        }

        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void APredicateConcatenates(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "1" },
                Rows(provider, "SELECT ID FROM ADO.CAT WHERE A || B = 'aabb'"));
        }

        /// <summary>
        /// A sort key, ordered so that a sort which did nothing would answer differently. The null row is
        /// left out: where a null sorts is a separate question from what the operator is written as, and
        /// Calcite already answers it.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ASortKeyConcatenates(string provider)
        {
            CollectionAssert.AreEqual(
                new[] { "3", "1" },
                Rows(provider, "SELECT ID FROM ADO.CAT WHERE B IS NOT NULL ORDER BY A || B DESC"));
        }

        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void AnAggregateArgumentConcatenates(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "ddee" },
                Rows(provider, "SELECT MAX(A || B) FROM ADO.CAT"));
        }

        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void AGroupKeyConcatenates(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "aabb|1", "ddee|1", "NULL|1" },
                Rows(provider, "SELECT A || B, COUNT(*) FROM ADO.CAT GROUP BY A || B"));
        }

        /// <summary>
        /// Two literals, which are not folded away — so there is no shape of the expression that avoids the
        /// operator.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void TwoLiteralsConcatenate(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "xy" },
                Rows(provider, "SELECT 'x' || 'y' FROM ADO.CAT WHERE ID = 1"));
        }

        #endregion

        #region What the operator means

        /// <summary>
        /// The reason the rendering is <c>+</c> and not <c>CONCAT</c>. <c>||</c> yields null when either
        /// operand is null and so does <c>+</c>, under the default <c>CONCAT_NULL_YIELDS_NULL</c>; T-SQL's
        /// <c>CONCAT</c> reads a null operand as the empty string and would have answered <c>cc</c>.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ANullOperandMakesTheWholeExpressionNull(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "NULL" },
                Rows(provider, "SELECT A || B FROM ADO.CAT WHERE ID = 2"));
        }

        /// <summary>
        /// And the same claim where it decides whether a row is returned at all, which is the failure a
        /// syntax check would not have caught: under <c>CONCAT</c> the row matches.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ANullOperandMatchesNothing(string provider)
        {
            Assert.AreEqual(0, Rows(provider, "SELECT ID FROM ADO.CAT WHERE A || B = 'cc'").Count);
        }

        #endregion

        #region Precedence

        /// <summary>
        /// <c>SqlSyntax.BINARY.unparse</c> is handed an operator whose precedence is not the one the call
        /// carries — <c>||</c> is 60 and <c>+</c> is 40 — so a nested expression is where a substitution of
        /// this shape goes wrong. These are the nestings a validated plan produces, and the server is the
        /// authority on whether the parentheses came out right.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ConcatenationNestsInConcatenation(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "aabbaa" },
                Rows(provider, "SELECT A || B || A FROM ADO.CAT WHERE ID = 1"));
        }

        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ConcatenationNestsToTheRight(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "aabbaa" },
                Rows(provider, "SELECT A || (B || A) FROM ADO.CAT WHERE ID = 1"));
        }

        /// <summary>
        /// Arithmetic reaches a string only through a cast, which writes its own parentheses — and the cast
        /// is the one thing on either side of the operator whose rendering this project already corrects.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ConcatenationAgainstArithmetic(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "aa2" },
                Rows(provider, "SELECT A || CAST(ID + 1 AS VARCHAR(4)) FROM ADO.CAT WHERE ID = 1"));
        }

        /// <summary>
        /// A comparison binds looser than either spelling, and a conjunction looser still.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ConcatenationInsideAConjunction(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "3" },
                Rows(provider, "SELECT ID FROM ADO.CAT WHERE A || B > 'aabb' AND ID > 1"));
        }

        #endregion

        #region What went to the server

        /// <summary>
        /// The rows are the claim that matters, and this is the claim that they were answered by the server
        /// rather than by Calcite: the statement the adapter generated carries the operator T-SQL has, and
        /// no longer the one it does not.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void TheOperatorIsPushedDownAsPlus(string provider)
        {
            var answer = Run(provider, "SELECT A || B FROM ADO.CAT WHERE ID = 1");
            var generated = string.Join("\n", answer.Statements);

            Assert.AreNotEqual(0, answer.Statements.Count, "nothing was pushed down at all");
            Assert.IsFalse(generated.Contains("||"), $"the operator the server refuses was pushed down: {generated}");
            Assert.IsTrue(answer.Statements.Any(s => s.Contains('+')), $"nothing concatenated on the server: {generated}");
        }

        #endregion

    }

}
