using System;

using Apache.Calcite.FullText.Schema;
using Apache.Calcite.FullText.Sql;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.schema;

using Xunit;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// The operators reached through a plain <c>jdbc:calcite:</c> connection, with the declarations from
    /// <see cref="FullTextSchema"/> on the root schema and no operator table chained.
    /// </summary>
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
        /// The statement still fails, at code generation, because nothing evaluates full text in process. The
        /// test checks that the failure is not the validator's <c>No match found for function signature</c>.
        /// </remarks>
        [Fact]
        public void ShouldResolveTheNameThroughAPlainConnection()
        {
            var act = () => Run("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')");

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().NotContain("No match found for function signature",
                    "the name has to resolve; refusing to evaluate it is a later and different thing");
        }

        /// <summary>
        /// Without the declarations the name does not resolve; the control for the test above.
        /// </summary>
        [Fact]
        public void ShouldNotResolveThroughAPlainConnectionWithoutThem()
        {
            var act = () => Run("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", declare: false);

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().Contain("No match found for function signature");
        }

        /// <summary>
        /// The refusal explains why the call cannot be evaluated, rather than Calcite's report that the
        /// function does not implement <c>ImplementableFunction</c>.
        /// </summary>
        [Fact]
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
        /// Every declaration's implementor refuses, naming its function.
        /// </summary>
        [Fact]
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

            // five fixed-arity operators declare once each; the four variadic ones once per arity from two to
            // the limit
            seen.Should().Be(5 + (4 * (FullTextSchema.VariadicOperandLimit - 1)));
        }

        /// <summary>
        /// A connection setting <c>fun</c> chains its libraries ahead of the catalog reader, and the names
        /// still reach the schema's declarations.
        /// </summary>
        /// <remarks>
        /// <c>REVERSE</c> is a library function absent from the standard table, so resolving it confirms the
        /// libraries really were chained.
        /// </remarks>
        [Fact]
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
        /// Names declared on the root resolve unqualified in a query against a subschema's table.
        /// </summary>
        /// <remarks>
        /// <c>CalciteCatalogReader</c> looks up an unqualified function in the connection's default schema and
        /// the root only.
        /// </remarks>
        [Fact]
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
        /// The table reads without a full text call, so a failure in the tests above comes from the call and
        /// not the fixture.
        /// </summary>
        [Fact]
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
