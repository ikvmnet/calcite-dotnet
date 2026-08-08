using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs the same query through the asynchronous convention and the synchronous one, and requires the
    /// same rows.
    /// </summary>
    /// <remarks>
    /// The comparison is against <see cref="ClrEnumerableConvention"/> rather than against Calcite, and that
    /// is not a weaker oracle: the synchronous convention is checked against Calcite query by query in
    /// <see cref="ClrEnumerableDifferentialTests"/>, so agreeing with it is agreeing with Calcite. It is also
    /// the only comparison available — the asynchronous convention reads a table Calcite has no SPI for, so
    /// its schema is its own and a three-way comparison would be comparing two different sets of rows.
    ///
    /// <para>Both sides read <see cref="AsyncTestRows"/>, one copy, for exactly that reason.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerableDifferentialTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static ClrAsyncEnumerableDifferentialTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.util.Smalls).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        static org.apache.calcite.schema.SchemaPlus Schema(bool async)
        {
            var rootSchema = Frameworks.createRootSchema(true);

            if (async)
            {
                rootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
                rootSchema.add("SORTED", new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
            }
            else
            {
                rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
                rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
            }

            return rootSchema;
        }

        /// <summary>
        /// Plans a statement in one convention and returns its rows, rendered.
        /// </summary>
        static async Task<List<string>> Run(string sql, bool async, bool planOnly = false)
        {
            var rootSchema = Schema(async);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in async ? ClrAsyncEnumerableRules.CalcRules() : ClrEnumerableRules.CalcRules())
                calcRules.add(rule);

            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, false, false, null, null),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = async ? (Convention)ClrAsyncEnumerableConvention.Instance : ClrEnumerableConvention.Instance;
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            if (async)
            {
                var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, null, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);
                await foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }
            else
            {
                var bindable = ClrEnumerableInterpretable.ToBindable(parameters, null, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
                foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }

            return rows;
        }

        /// <summary>
        /// The context a plan of either convention is bound with.
        /// </summary>
        /// <remarks>
        /// Its own rather than the synchronous harness's, which is nested and private there. It answers
        /// <c>get</c> from the map the plan stashed into, because a plan reads its compile-time values back
        /// through the context and answering null to one is how a query fails in a way no assertion here
        /// would explain.
        /// </remarks>
        sealed class TestDataContext(org.apache.calcite.schema.SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public org.apache.calcite.schema.SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions.
        /// </summary>
        static async Task Same(string sql)
        {
            var async = await Run(sql, true);
            var sync = await Run(sql, false);

            async.Should().Equal(sync, "'{0}' should give what ClrEnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires the same rows, and that the asynchronous convention really planned the node aimed at.
        /// </summary>
        /// <remarks>
        /// The plan assertion is what stops a test from comparing something against itself. It matters more
        /// here than in the synchronous harness, not less: there is no converter to fall through to, so a
        /// rule that fails to fire does not quietly produce a plan of the other convention — it produces no
        /// plan at all — but a node reached by a route nobody intended still looks like a pass.
        /// </remarks>
        static async Task SameThrough(string node, string sql)
        {
            (await Run(sql, true, planOnly: true))[0]
                .Should().Contain(node, "'{0}' should be planned through {1}", sql, node);

            await Same(sql);
        }

        [TestMethod]
        public Task ShouldAgreeOnAScan() => Same("SELECT * FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAOneColumnScan() => Same("SELECT ID FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAFilter() => SameThrough("ClrAsyncEnumerableCalc", "SELECT * FROM SALES WHERE AMOUNT > 10");

        [TestMethod]
        public Task ShouldAgreeOnAProjection() => Same("SELECT ID, REGION FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAnExpression() => Same("SELECT ID + 1, UPPER(REGION) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnANullableColumn() => Same("SELECT AMOUNT FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnASort() => SameThrough("ClrAsyncEnumerableSort", "SELECT * FROM SALES ORDER BY AMOUNT");

        [TestMethod]
        public Task ShouldAgreeOnASortWithLimit() => Same("SELECT * FROM SALES ORDER BY ID OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public Task ShouldAgreeOnValues() => SameThrough("ClrAsyncEnumerableValues", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        [TestMethod]
        public Task ShouldAgreeOnAnAggregate() => SameThrough("ClrAsyncEnumerableAggregate", "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION");

        [TestMethod]
        public Task ShouldAgreeOnACountOverEverything() => Same("SELECT COUNT(*) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAGrandTotal() => Same("SELECT SUM(AMOUNT), MIN(ID), MAX(ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnDistinct() => Same("SELECT DISTINCT REGION FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAUnion() => Same("SELECT ID FROM SALES UNION SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAUnionAll() => Same("SELECT ID FROM SALES UNION ALL SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAnIntersect() => Same("SELECT ID FROM SALES INTERSECT SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAMinus() => Same("SELECT ID FROM SALES EXCEPT SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAJoin() => Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID = t.K");

        [TestMethod]
        public Task ShouldAgreeOnALeftJoin() => Same("SELECT s.ID, t.V FROM SALES s LEFT JOIN SORTED t ON s.ID = t.K");

        [TestMethod]
        public Task ShouldAgreeOnANestedLoopJoin() => Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID > t.K");

        [TestMethod]
        public Task ShouldAgreeOnASemiJoin() => Same("SELECT ID FROM SALES WHERE ID IN (SELECT K FROM SORTED)");

        [TestMethod]
        public Task ShouldAgreeOnAWindow() => Same("SELECT ID, SUM(AMOUNT) OVER (PARTITION BY REGION ORDER BY ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnARowNumber() => Same("SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnACorrelatedSubQuery() =>
            Same("SELECT ID, (SELECT COUNT(*) FROM SORTED t WHERE t.K = s.ID) FROM SALES s");

    }

}
