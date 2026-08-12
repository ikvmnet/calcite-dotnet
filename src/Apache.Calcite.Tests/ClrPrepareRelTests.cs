using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Prepare;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql.fun;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Running a plan that was built rather than parsed.
    /// </summary>
    /// <remarks>
    /// <c>CalcitePrepareImpl.prepare2_</c> has three entry points and this project long had one, because
    /// <c>CalciteSession</c> only ever arrives with SQL text. The <c>rel</c> branch is what Calcite answers
    /// <c>RelRunner</c> with, and it is where a .NET LINQ provider's translation would land: the
    /// <c>queryable</c> branch cannot be ported, since <c>LixToRelTranslator</c> is package-private and
    /// translates linq4j expression trees rather than <c>System.Linq.Expressions</c> ones.
    /// </remarks>
    [TestClass]
    public class ClrPrepareRelTests
    {

        /// <summary>
        /// Puts a convention's rules on the planner the plan was built with, which is the planner
        /// <c>Prepare.optimize</c> reads off the root and therefore the one that chooses.
        /// </summary>
        static RelNode Stocked(RelNode rel, bool async = false)
        {
            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
                rel.getCluster().getPlanner().addRule(rule);

            return rel;
        }

        /// <summary>
        /// Builds a plan with <see cref="RelBuilder"/>, runs it, and renders its rows.
        /// </summary>
        /// <param name="build"></param>
        /// <returns></returns>
        static List<string> Run(Func<RelBuilder, RelNode> build)
        {
            return ClrPrepareFixture.WithContext("", (context, rootSchema) =>
            {
                var config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema.plus())
                    .build();

                var rel = Stocked(build(RelBuilder.create(config)));
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(rel), typeof(object[]), -1);

                var rows = new List<string>();
                foreach (var row in signature.Bind(context.getDataContext()))
                    rows.Add(row is object[] a ? string.Join("|", System.Linq.Enumerable.Select(a, c => c?.ToString() ?? "null")) : row?.ToString() ?? "null");

                return rows;
            });
        }

        [TestMethod]
        public void Should_run_a_scan()
        {
            var rows = Run(b => b.scan("NUMS").build());

            CollectionAssert.AreEquivalent(new[] { "3", "1", "2" }, rows);
        }

        [TestMethod]
        public void Should_run_a_filter_and_project()
        {
            var rows = Run(b => b
                .scan("SALES")
                // a Java box, not a CLR int: RelBuilder.literal takes an Object and Calcite cannot make a
                // constant from a cli.System.Int32, which is the same adapter invariant the convention keeps
                .filter(b.call(SqlStdOperatorTable.GREATER_THAN, b.field("ID"), b.literal(java.lang.Integer.valueOf(4))))
                .project(b.field("REGION"))
                .build());

            CollectionAssert.AreEquivalent(new[] { "WEST", "NORTH" }, rows);
        }

        [TestMethod]
        public void Should_run_an_aggregate()
        {
            var rows = Run(b => b
                .scan("SALES")
                .aggregate(b.groupKey(), b.count(false, "C"))
                .build());

            CollectionAssert.AreEqual(new[] { "6" }, rows);
        }

        /// <summary>
        /// A sort's collation is the one thing <c>prepare_</c> reads off the node rather than defaulting,
        /// so it is worth a plan of its own.
        /// </summary>
        [TestMethod]
        public void Should_keep_a_sort_collation()
        {
            var rows = Run(b => b
                .scan("NUMS")
                .sort(b.field("N"))
                .build());

            CollectionAssert.AreEqual(new[] { "1", "2", "3" }, rows);
        }

        /// <summary>
        /// A plan carries no statement kind and no dynamic parameters, and there is nothing to validate, so
        /// it reports none of the three. That is Calcite's shape, not a simplification.
        /// </summary>
        [TestMethod]
        public void Should_describe_a_plan_with_no_parameters_and_no_origins()
        {
            ClrPrepareFixture.WithContext("", (context, rootSchema) =>
            {
                var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema.plus()).build();
                var rel = Stocked(RelBuilder.create(config).scan("SALES").build());

                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(rel), typeof(object[]), -1);

                Assert.AreEqual(0, signature.Parameters.size());
                Assert.AreEqual(nameof(org.apache.calcite.avatica.Meta.StatementType.SELECT), signature.StatementType.name());
                Assert.AreEqual(4, signature.Columns.size());
                Assert.IsNotNull(signature.Bind(context.getDataContext()));

                return 0;
            });
        }

        /// <summary>
        /// A built plan reaches the asynchronous convention too.
        /// </summary>
        /// <remarks>
        /// <c>PrepareRel</c> named <c>ClrEnumerableRules</c> and constructed <c>ClrEnumerablePreparingStmt</c>
        /// directly, so it planned synchronously whatever a caller asked for — it calls neither
        /// <c>CreatePlanner</c> nor <c>GetPreparingStmt</c>, which is where the convention is chosen, and the
        /// commit that wired the asynchronous convention through the pipeline therefore went straight past
        /// it. A connection plans asynchronously by default, so <c>BindAsync</c> would have refused the
        /// signature this returned.
        /// </remarks>
        [TestMethod]
        public async System.Threading.Tasks.Task Should_run_a_built_plan_asynchronously()
        {
            var rows = await ClrPrepareFixture.WithContext("", (context, rootSchema) =>
            {
                var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema.plus()).build();
                var rel = Stocked(RelBuilder.create(config).scan("NUMS").build(), async: true);

                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(rel), typeof(object[]), -1, true);

                return Collect(signature.BindAsync(context.getDataContext()));
            });

            CollectionAssert.AreEquivalent(new[] { "3", "1", "2" }, rows);

            static async System.Threading.Tasks.Task<List<string>> Collect(IAsyncEnumerable<object> source)
            {
                var rows = new List<string>();
                await foreach (var row in source)
                    rows.Add(row is object[] a ? string.Join("|", System.Linq.Enumerable.Select(a, c => c?.ToString() ?? "null")) : row?.ToString() ?? "null");

                return rows;
            }
        }

        /// <summary>
        /// The limit is applied by the signature, so it holds however the plan was arrived at.
        /// </summary>
        [TestMethod]
        public void Should_apply_max_row_count()
        {
            var rows = ClrPrepareFixture.WithContext("", (context, rootSchema) =>
            {
                var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema.plus()).build();
                var rel = Stocked(RelBuilder.create(config).scan("SALES").build());

                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(rel), typeof(object[]), 2);

                var list = new List<object>();
                foreach (var row in signature.Bind(context.getDataContext()))
                    list.Add(row);

                return list;
            });

            Assert.AreEqual(2, rows.Count);
        }

    }

}
