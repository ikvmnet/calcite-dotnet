using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a query through <c>Programs.standard()</c> with this convention's rules, which is what the
    /// prepare pipeline runs and what a caller driving the planner reaches for.
    /// </summary>
    /// <remarks>
    /// Nothing ran through it before. Every other test of this convention builds its own program — the
    /// differential harness registers its rules by hand, and the cancellation tests do the same — so the
    /// configuration a caller actually gets was the only one nothing measured, and three whole shapes could
    /// not plan through it while their own suites were green: AVG, every DISTINCT aggregate, and every OVER
    /// window. Each needs a logical rewrite that belongs to no convention, and <c>Programs.ofRules</c> had
    /// cleared Calcite's rules off the planner along with it. Running <c>Programs.standard</c> itself is
    /// what keeps them: its planner pass installs nothing and plans with what the planner carries, which is
    /// Calcite's own rules and — by way of <see cref="AddRulesProgram"/> here, and
    /// <c>ClrPrepareImpl.CreatePlanner</c> for a prepared statement — this convention's.
    ///
    /// <para><c>ClrEnumerableQueryTests</c> is the synchronous counterpart and holds the same four queries.
    /// The two programs are the same shape and were changed together, which is the reason to run both rather
    /// than trust one.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerableProgramsTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static ClrAsyncEnumerableProgramsTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
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
        /// Plans a statement through the shipped program and returns its rows.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks>
        /// One <c>transform</c>, because the sequence is one <c>Program</c> as <c>Programs.standard</c> is,
        /// and the traits are the logical root's own rather than an empty set — an empty one asks for no
        /// collation and <c>SortRemoveRule</c> takes the ORDER BY away.
        /// </remarks>
        static async Task<List<object[]>> Run(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrAsyncEnumerableRules.CalcRules())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.sequence(
                        new AddRulesProgram(ClrAsyncEnumerableRules.Rules()),
                        Programs.standard(),
                        Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            var traits = logical.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance).simplify();
            var physical = (ClrAsyncEnumerableRel)planner.transform(0, traits, logical);

            var parameters = new java.util.HashMap();
            var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, physical, ClrEnumerablePrefer.Array);

            var rows = new List<object[]>();
            await foreach (var current in bindable.Bind(new TestDataContext(rootSchema, parameters)))
                rows.Add(current as object[] ?? [current]);

            return rows;
        }

        [TestMethod]
        public async Task ShouldScanThroughTheShippedProgram()
        {
            var rows = await Run("SELECT ID, REGION FROM SALES ORDER BY ID");

            rows.Should().HaveCount(6);
            rows.Select(r => r[0]).Should().Equal(Enumerable.Range(1, 6).Select(i => (object)java.lang.Integer.valueOf(i)));
        }

        /// <inheritdoc cref="ClrEnumerableQueryTests" />
        [TestMethod]
        public async Task ShouldAverageThroughTheShippedProgram()
        {
            var rows = await Run("SELECT AVG(AMOUNT) FROM SALES");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Integer.valueOf(17));
        }

        [TestMethod]
        public async Task ShouldCountDistinctThroughTheShippedProgram()
        {
            var rows = await Run("SELECT COUNT(DISTINCT REGION) FROM SALES");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Long.valueOf(2L));
        }

        [TestMethod]
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
        /// <remarks>
        /// Its own test because it is what caught the trait set being wrong. <c>SortRemoveRule</c> arrives
        /// with Calcite's abstract rules, which the planner pass now keeps, and it takes a sort away when the
        /// traits the root is asked for carry no collation — so a plan driven from
        /// <c>getEmptyTraitSet()</c> returns the right rows in the wrong order and nothing else notices.
        /// </remarks>
        [TestMethod]
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
