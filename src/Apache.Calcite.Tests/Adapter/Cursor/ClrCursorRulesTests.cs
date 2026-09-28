using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Runs queries, read through the awaiting open, through <c>Programs.standard</c> with this convention's
    /// rules added to the planner: the configuration the prepare pipeline uses and a caller driving a
    /// <c>Frameworks</c> planner should use.
    /// </summary>
    /// <remarks>
    /// <c>Programs.standard</c>'s planner pass installs no rules and plans with whatever the planner carries,
    /// which is Calcite's own rules plus this convention's (added here by <see cref="AddRulesProgram"/>, and by
    /// <c>ClrPrepareImpl.CreatePlanner</c> for a prepared statement). AVG, DISTINCT aggregates and OVER windows
    /// each need a logical rewrite from Calcite's rules that belongs to no convention, so they are the shapes
    /// covered here.
    ///
    /// <para><see cref="ClrCursorConventionTests"/> runs the same queries opened synchronously, so the two
    /// classes together cover both bodies of each node for one planned root.</para>
    /// </remarks>
    public class ClrCursorRulesTests
    {

        /// <summary>
        /// Puts Calcite's JDBC assembly on the boot class path.
        /// </summary>
        static ClrCursorRulesTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        /// <param name="parameters">The map the implementor stashed values into, which <c>get</c> answers from.</param>
        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// Plans a statement through the shipped program and returns its rows, read through the awaiting open.
        /// </summary>
        /// <param name="sql">The statement, over the asynchronous <c>SALES</c> table.</param>
        /// <returns>The rows, a one-column result wrapped in a one-element array.</returns>
        /// <remarks>
        /// The sequence is one <c>Program</c>, so there is one <c>transform</c>. The requested traits are the
        /// logical root's own rather than an empty set: an empty set asks for no collation, and
        /// <c>SortRemoveRule</c> then removes the ORDER BY.
        /// </remarks>
        static async Task<List<object[]>> Run(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.sequence(
                        new AddRulesProgram(ClrCursorRules.Rules()),
                        Programs.standard(Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider),
                        Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider)))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            var traits = logical.getTraitSet().replace(ClrCursorConvention.Instance).simplify();
            var physical = (ClrCursorRel)planner.transform(0, traits, logical);

            var parameters = new java.util.HashMap();
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot(physical, ClrCursorPrefer.Array);

            var rows = new List<object[]>();
            await using var cursor = await factory.OpenAsync(new TestDataContext(rootSchema, parameters), System.Threading.CancellationToken.None);
            while (await cursor.ReadAsync(System.Threading.CancellationToken.None))
                rows.Add(cursor.Current as object[] ?? [cursor.Current!]);

            return rows;
        }

        [Fact]
        public async Task ShouldScanThroughTheShippedProgram()
        {
            var rows = await Run("SELECT ID, REGION FROM SALES ORDER BY ID");

            rows.Should().HaveCount(6);
            rows.Select(r => r[0]).Should().Equal(System.Linq.Enumerable.Range(1, 6).Select(i => (object)java.lang.Integer.valueOf(i)));
        }

        /// <inheritdoc cref="ClrCursorConventionTests.ShouldAverageThroughTheShippedProgram" />
        [Fact]
        public async Task ShouldAverageThroughTheShippedProgram()
        {
            var rows = await Run("SELECT AVG(AMOUNT) FROM SALES");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Integer.valueOf(17));
        }

        [Fact]
        public async Task ShouldCountDistinctThroughTheShippedProgram()
        {
            var rows = await Run("SELECT COUNT(DISTINCT REGION) FROM SALES");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Long.valueOf(2L));
        }

        [Fact]
        public async Task ShouldWindowThroughTheShippedProgram()
        {
            var rows = await Run("SELECT ID, SUM(AMOUNT) OVER (PARTITION BY REGION ORDER BY ID) FROM SALES ORDER BY ID");

            rows.Should().HaveCount(6);
            rows.Select(r => r[1]).Should().Equal(
                java.lang.Integer.valueOf(10),
                java.lang.Integer.valueOf(30),
                java.lang.Integer.valueOf(50),
                java.lang.Integer.valueOf(30),
                java.lang.Integer.valueOf(30),
                java.lang.Integer.valueOf(35));
        }

        /// <summary>
        /// An ORDER BY survives the shipped program.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>SortRemoveRule</c>, one of Calcite's rules the planner pass keeps, removes a sort when the traits
        /// requested for the root carry no collation. A plan requested with an empty trait set then returns the
        /// right rows in the wrong order, which only a test of the order detects.
        /// </remarks>
        [Fact]
        public async Task ShouldSortThroughTheShippedProgram()
        {
            var rows = await Run("SELECT ID FROM SALES ORDER BY REGION, ID DESC");

            rows.Select(r => r[0]).Should().Equal(
                java.lang.Integer.valueOf(3),
                java.lang.Integer.valueOf(2),
                java.lang.Integer.valueOf(1),
                java.lang.Integer.valueOf(6),
                java.lang.Integer.valueOf(5),
                java.lang.Integer.valueOf(4));
        }

    }

}
