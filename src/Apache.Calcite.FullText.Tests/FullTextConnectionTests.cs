using System;

using Apache.Calcite.FullText.Schema;
using Apache.Calcite.FullText.Sql;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.schema;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// The operators reached through the stock Calcite JDBC driver, with nothing chained and nothing
    /// subclassed.
    /// </summary>
    /// <remarks>
    /// This is what the schema declarations are for. Everything else in this suite builds a validator by hand
    /// and hands it an operator table, which only a host embedding Calcite can do. Here the connection is
    /// <c>jdbc:calcite:</c>, the schema is registered the way an adapter would register one, and the SQL goes
    /// through <c>Statement.executeQuery</c> — the path a consumer who has never heard of this package takes.
    /// </remarks>
    [TestClass]
    public class FullTextConnectionTests
    {

        static java.sql.Connection Connect(bool declare = true, string url = "jdbc:calcite:")
        {
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            var connection = java.sql.DriverManager.getConnection(url);
            var calcite = (CalciteConnection)connection.unwrap((java.lang.Class)typeof(CalciteConnection));
            var root = calcite.getRootSchema();

            root.add("DOCS", new FullTextFixture.DocumentTable());

            if (declare)
                FullTextSchema.AddTo(root);

            return connection;
        }

        static void Run(string sql, bool declare = true, string url = "jdbc:calcite:")
        {
            using var connection = Connect(declare, url);
            using var statement = connection.createStatement();
            using var results = statement.executeQuery(sql);

            while (results.next())
            {
            }
        }

        /// <summary>
        /// The name resolves through a plain connection.
        /// </summary>
        /// <remarks>
        /// It gets as far as code generation and is refused there, which is the correct outcome — nothing here
        /// evaluates full text. What this measures is that the failure is the refusal and not
        /// <c>No match found for function signature</c>, which is what a connection said before the
        /// declarations existed.
        /// </remarks>
        [TestMethod]
        public void ShouldResolveTheNameThroughAPlainConnection()
        {
            var act = () => Run("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')");

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().NotContain("No match found for function signature",
                    "the name has to resolve; refusing to evaluate it is a later and different thing");
        }

        /// <summary>
        /// And without the declarations it does not, which is what says the test above measures something.
        /// </summary>
        [TestMethod]
        public void ShouldNotResolveThroughAPlainConnectionWithoutThem()
        {
            var act = () => Run("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", declare: false);

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().Contain("No match found for function signature");
        }

        /// <summary>
        /// The refusal says what cannot be done and why, rather than naming an interface.
        /// </summary>
        /// <remarks>
        /// Declining <c>ImplementableFunction</c> outright leaves Calcite to report it as <c>User defined
        /// function CLR_FT_CONTAINS must implement ImplementableFunction</c>, which names an interface rather than
        /// a reason and reads as a defect in the adapter. Implementing it and throwing puts the same refusal
        /// at the same moment — Calcite asks for a body while generating code — with a sentence saying why.
        /// </remarks>
        [TestMethod]
        public void ShouldRefuseToEvaluateInWordsThatSayWhy()
        {
            foreach (var sql in new[]
            {
                "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')",
                "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame')",
                "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ANY(BODY, 'steel', 'frame')",
                "SELECT ID, CLR_FT_SCORE(BODY, 'steel') AS S FROM DOCS",
                "SELECT ID FROM DOCS ORDER BY CLR_FT_RRF(CLR_FT_SCORE(BODY, 'steel'), CLR_FT_SCORE(DOC, 'frame')) DESC",
            })
            {
                var act = () => Run(sql);
                var described = FullTextFixture.Describe(act.Should().Throw<Exception>("'{0}' cannot be answered here", sql).Which);

                described.Should().NotContain("must implement ImplementableFunction",
                    "naming an interface reads as a defect in the adapter: '{0}'", sql);
                described.Should().Match(d => d.Contains("analyzer") || d.Contains("relevance score"),
                    "the refusal has to say why: '{0}'", sql);
            }
        }

        /// <summary>
        /// Every declaration refuses, and each refusal names its function.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseFromEveryDeclaration()
        {
            var entries = FullTextSchema.Functions().entries().iterator();
            var seen = 0;

            while (entries.hasNext())
            {
                var entry = (java.util.Map.Entry)entries.next();
                var name = (string)entry.getKey();
                var function = entry.getValue();

                function.Should().BeAssignableTo<ScalarFunction>();

                var implementable = function.Should()
                    .BeAssignableTo<ImplementableFunction>(
                        "'{0}' has to be the one that refuses, so that the refusal can say why", name)
                    .Which;

                var act = () => implementable.getImplementor();

                act.Should().Throw<java.lang.UnsupportedOperationException>(
                        "'{0}' is evaluated by the store, not here", name)
                    .WithMessage("*" + name + "*");

                seen++;
            }

            // five fixed-arity operators declare once each; the four variadic ones run from two operands to
            // the limit
            seen.Should().Be(5 + (4 * (FullTextSchema.VariadicOperandLimit - 1)));
        }

        /// <summary>
        /// A connection setting <c>fun</c> chains its libraries ahead of the catalog reader, and the names
        /// still reach the schema's declarations.
        /// </summary>
        /// <remarks>
        /// The half that would actually bite a host: <c>fun=all</c> is an ordinary connection string, and a
        /// shadowing library function would resolve first. <c>REVERSE</c> comes first and not incidentally —
        /// Calcite's standard table does not carry it, so it resolves only where the libraries really were
        /// chained. Without it this would pass just as well against a library table that had failed to load.
        /// </remarks>
        [TestMethod]
        public void ShouldNotBeShadowedByTheFunLibraries()
        {
            Run("SELECT REVERSE(BODY) AS R FROM DOCS", url: "jdbc:calcite:fun=all");

            var withoutLibraries = () => Run("SELECT REVERSE(BODY) AS R FROM DOCS");
            withoutLibraries.Should().Throw<Exception>("REVERSE is a library function, so chaining them above is doing something");

            var act = () => Run("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", url: "jdbc:calcite:fun=all");
            var described = FullTextFixture.Describe(act.Should().Throw<Exception>().Which);

            described.Should().NotContain("No match found for function signature",
                "the schema's declaration still has to be reached with every library chained ahead of it");
            described.Should().Contain("analyzer",
                "and it has to be our declaration that answered, not something else of that name");
        }

        /// <summary>
        /// Registering on the root makes the names visible unqualified everywhere on the connection.
        /// </summary>
        /// <remarks>
        /// <c>CalciteCatalogReader</c> searches the connection's default schema and the root, and nowhere
        /// else — never a subschema. So an adapter whose tables live a level down declares at both levels, and
        /// this is the root half of that.
        /// </remarks>
        [TestMethod]
        public void ShouldReachTheNamesFromASubschema()
        {
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:");
            var calcite = (CalciteConnection)connection.unwrap((java.lang.Class)typeof(CalciteConnection));
            var root = calcite.getRootSchema();

            var inner = root.add("INNER", new org.apache.calcite.schema.impl.AbstractSchema());
            inner.add("DOCS", new FullTextFixture.DocumentTable());
            FullTextSchema.AddTo(root);

            using var statement = connection.createStatement();

            var act = () =>
            {
                using var results = statement.executeQuery("SELECT ID FROM INNER.DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')");
                while (results.next())
                {
                }
            };

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().NotContain("No match found for function signature",
                    "a name declared on the root resolves for a query against a subschema's table");
        }

        /// <summary>
        /// The table itself reads, so a failure above is the full text call and not the fixture.
        /// </summary>
        [TestMethod]
        public void ShouldReadTheTableWithNoFullTextCall()
        {
            using var connection = Connect();
            using var statement = connection.createStatement();
            using var results = statement.executeQuery("SELECT ID, BODY FROM DOCS");

            results.next().Should().BeTrue();
            results.getInt(1).Should().Be(1);
            results.getString(2).Should().Be("a steel frame");
            results.next().Should().BeFalse();
        }

    }

}
