using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql.fun;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Prepare.Tests
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
    public class ClrPrepareImplRelTests
    {

        /// <summary>
        /// Puts the cursor convention's rules on the planner the plan was built with, which is the planner
        /// <c>Prepare.optimize</c> reads off the root and therefore the one that chooses.
        /// </summary>
        static RelNode Stocked(RelNode rel)
        {
            foreach (var rule in Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.Rules())
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

        [Fact]
        public void Should_run_a_scan()
        {
            var rows = Run(b => b.scan("NUMS").build());

            rows.Should().BeEquivalentTo(new[] { "3", "1", "2" });
        }

        [Fact]
        public void Should_run_a_filter_and_project()
        {
            var rows = Run(b => b
                .scan("SALES")
                // a Java box, not a CLR int: RelBuilder.literal takes an Object and Calcite cannot make a
                // constant from a cli.System.Int32, which is the same adapter invariant the convention keeps
                .filter(b.call(SqlStdOperatorTable.GREATER_THAN, b.field("ID"), b.literal(java.lang.Integer.valueOf(4))))
                .project(b.field("REGION"))
                .build());

            rows.Should().BeEquivalentTo(new[] { "WEST", "NORTH" });
        }

        [Fact]
        public void Should_run_an_aggregate()
        {
            var rows = Run(b => b
                .scan("SALES")
                .aggregate(b.groupKey(), b.count(false, "C"))
                .build());

            Assert.Equal(new[] { "6" }, rows);
        }

        /// <summary>
        /// A sort's collation is the one thing <c>prepare_</c> reads off the node rather than defaulting,
        /// so it is worth a plan of its own.
        /// </summary>
        [Fact]
        public void Should_keep_a_sort_collation()
        {
            var rows = Run(b => b
                .scan("NUMS")
                .sort(b.field("N"))
                .build());

            Assert.Equal(new[] { "1", "2", "3" }, rows);
        }

        /// <summary>
        /// A plan carries no statement kind and no dynamic parameters, and there is nothing to validate, so
        /// it reports none of the three. That is Calcite's shape, not a simplification.
        /// </summary>
        [Fact]
        public void Should_describe_a_plan_with_no_parameters_and_no_origins()
        {
            ClrPrepareFixture.WithContext("", (context, rootSchema) =>
            {
                var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema.plus()).build();
                var rel = Stocked(RelBuilder.create(config).scan("SALES").build());

                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(rel), typeof(object[]), -1);

                Assert.Equal(0, signature.Parameters.size());
                Assert.Equal(nameof(org.apache.calcite.avatica.Meta.StatementType.SELECT), signature.StatementType.name());
                Assert.Equal(4, signature.Columns.size());
                Assert.NotNull(signature.Bind(context.getDataContext()));

                return 0;
            });
        }

        /// <summary>
        /// A built plan can be read with await.
        /// </summary>
        /// <remarks>
        /// The same signature the synchronous tests above bind: a statement is planned once and answers
        /// either <c>Bind</c> or <c>BindAsync</c>, so a plan built through <see cref="RelBuilder"/> rather
        /// than parsed is asynchronous on request like any other. This test predates that and used to have to
        /// ask for a second convention up front, which <c>PrepareRel</c> could not do.
        /// </remarks>
        [Fact]
        public async System.Threading.Tasks.Task Should_run_a_built_plan_asynchronously()
        {
            var rows = await ClrPrepareFixture.WithContext("", (context, rootSchema) =>
            {
                var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema.plus()).build();
                var rel = Stocked(RelBuilder.create(config).scan("NUMS").build());

                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(rel), typeof(object[]), -1);

                return Collect(signature.BindAsync(context.getDataContext()));
            });

            rows.Should().BeEquivalentTo(new[] { "3", "1", "2" });

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
        [Fact]
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

            Assert.Equal(2, rows.Count);
        }

    }

}
