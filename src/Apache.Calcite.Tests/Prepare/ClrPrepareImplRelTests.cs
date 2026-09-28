using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Cursor;
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
    /// Preparing and running a plan built with <see cref="RelBuilder"/> rather than parsed from SQL.
    /// </summary>
    /// <remarks>
    /// This is the <c>rel</c> branch of <c>CalcitePrepareImpl.prepare2_</c>, which Calcite uses for
    /// <c>RelRunner</c>. The <c>queryable</c> branch is not ported: <c>LixToRelTranslator</c> is package
    /// private and translates linq4j expression trees, not <c>System.Linq.Expressions</c> ones.
    /// </remarks>
    public class ClrPrepareImplRelTests
    {

        /// <summary>
        /// Adds the cursor convention's rules to the planner the plan was built with, which is the planner
        /// <c>Prepare.optimize</c> takes from the root.
        /// </summary>
        /// <param name="rel">A plan built against a planner that does not yet carry those rules.</param>
        /// <returns><paramref name="rel"/>, unchanged.</returns>
        static RelNode Stocked(RelNode rel)
        {
            foreach (var rule in Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.Rules())
                rel.getCluster().getPlanner().addRule(rule);

            return rel;
        }

        /// <summary>
        /// Builds a plan with <see cref="RelBuilder"/>, runs it, and renders each row as text.
        /// </summary>
        /// <param name="build">Builds the plan against a builder over the fixture's schema.</param>
        /// <returns>The rows, a multi-column row written as its values joined with <c>|</c>, and a null as <c>null</c>.</returns>
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
                // literal from a boxed System.Int32
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
        /// A sort's collation is read from the root node rather than defaulted, as <c>prepare_</c> does.
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
        /// A built plan has no dynamic parameters and reports a <c>SELECT</c> statement type, as Calcite's
        /// <c>rel</c> branch does.
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
        /// A built plan can be read through <c>BindAsync</c> as well as <c>Bind</c>; one signature serves both.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
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
        /// The signature applies <c>maxRowCount</c> to a built plan as it does to a parsed one.
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
