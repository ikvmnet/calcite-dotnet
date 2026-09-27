using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Rel.Metadata;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Plans the same statement in this convention and in Calcite's, and requires the same plan with the
    /// node names mapped across.
    /// </summary>
    /// <remarks>
    /// The differential tests compare rows, and a plan that costs its nodes differently from Calcite's still
    /// returns the right ones. <c>ClrCursorMergeJoin</c> did: it cost its output rows alone where
    /// <c>EnumerableMergeJoin</c> costs its inputs as well, so a join of a million rows to a thousand sorted
    /// both inputs and merged them where Calcite built a hash table over the thousand, and ran ten times
    /// slower. A cost is part of the port, and the plan is where it shows.
    ///
    /// <para>The tables say how many rows they hold and nothing else, and are never read.</para>
    /// </remarks>
    public class ClrCursorCostTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static ClrCursorCostTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of two integer columns that states its row count, and that its rows arrive sorted by the
        /// first where <paramref name="sorted"/> says so.
        /// </summary>
        sealed class CountedTable(double rowCount, bool sorted = false) : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("A", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("B", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public override Statistic getStatistic()
            {
                return sorted
                    ? Statistics.of(rowCount, new java.util.ArrayList(), com.google.common.collect.ImmutableList.of(RelCollations.of(0)))
                    : Statistics.of(rowCount, null);
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                return org.apache.calcite.linq4j.Linq4j.emptyEnumerable();
            }

        }

        /// <summary>
        /// Plans <paramref name="sql"/> rooted in <paramref name="convention"/>.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="convention"></param>
        /// <param name="batch">Whether to offer the convention's batch nested loop join rule, which neither
        /// convention registers by default.</param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite's side runs <c>Programs.standard</c> over the rules the planner already carries, which is
        /// <c>Prepare.getProgram</c>; this convention's adds its rules first and its calc pass after, and gives
        /// both <c>ClrCursorRelMetadata.Provider</c>, which is <c>ClrPrepare.GetProgram</c>. Nothing sets the
        /// cluster's query supplier: <c>standard</c>'s sub-query pass sets the thread's provider, and that is
        /// what the planner pass costs with.
        /// </remarks>
        static RelNode PlanRel(string sql, Convention convention, bool batch)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("BIG", new CountedTable(1_000_000));
            rootSchema.add("SMALL", new CountedTable(1_000));
            rootSchema.add("TINY", new CountedTable(50));
            rootSchema.add("SBIG", new CountedTable(1_000_000, true));
            rootSchema.add("SSMALL", new CountedTable(1_000, true));

            var rules = new List<RelOptRule>();
            Program program;
            if (convention == ClrCursorConvention.Instance)
            {
                var calcRules = new java.util.ArrayList();
                foreach (var rule in ClrCursorRules.CalcRules())
                    calcRules.add(rule);

                rules.AddRange(ClrCursorRules.Rules());
                if (batch)
                    rules.Add(ClrCursorRules.ClrCursorBatchNestedLoopJoinRule);

                program = Programs.sequence(
                    new AddRulesProgram(rules),
                    Programs.standard(ClrCursorRelMetadata.Provider),
                    Programs.hep(calcRules, true, ClrCursorRelMetadata.Provider));
            }
            else
            {
                if (batch)
                    rules.Add(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);

                program = Programs.sequence(
                    new AddRulesProgram(rules),
                    Programs.standard());
            }

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(program)
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var traits = logical.getTraitSet().replace(convention).simplify();
            return planner.transform(0, traits, logical);
        }

        /// <summary>
        /// Plans <paramref name="sql"/> rooted in <paramref name="convention"/> and returns the plan as text.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="convention"></param>
        /// <param name="batch"></param>
        /// <returns></returns>
        static string Plan(string sql, Convention convention, bool batch = false)
        {
            var rel = PlanRel(sql, convention, batch);
            var text = new System.Text.StringBuilder(RelOptUtil.toString(rel));

            // a fresh query, because the cluster caches one and a plan that fired no rule after the sub-query
            // pass set the thread's provider is still holding the one it made before
            rel.getCluster().invalidateMetadataQuery();
            Metadata(rel, rel.getCluster().getMetadataQuery(), 0, text);
            return text.ToString();
        }

        /// <summary>
        /// Gives <paramref name="cluster"/> the query supplier <c>ClrPrepareImpl</c> gives its own.
        /// </summary>
        /// <param name="cluster"></param>
        static void Install(RelOptCluster cluster)
        {
            cluster.setMetadataQuerySupplier(ClrRelMetadataProvider.QuerySupplier(ClrCursorRelMetadata.Provider));
            cluster.invalidateMetadataQuery();
        }

        /// <summary>
        /// Writes what the metadata query answers for every node of <paramref name="rel"/>: the row count and
        /// its bounds, the collations and the cumulative cost, which is what a plan is chosen from.
        /// </summary>
        /// <param name="rel"></param>
        /// <param name="mq"></param>
        /// <param name="depth"></param>
        /// <param name="text"></param>
        static void Metadata(RelNode rel, org.apache.calcite.rel.metadata.RelMetadataQuery mq, int depth, System.Text.StringBuilder text)
        {
            text.Append(new string(' ', depth * 2))
                .Append(rel.getRelTypeName())
                .Append(": rows=").Append(mq.getRowCount(rel))
                .Append(", max=").Append(mq.getMaxRowCount(rel))
                .Append(", min=").Append(mq.getMinRowCount(rel))
                .Append(", collations=").Append(mq.collations(rel))
                .Append(", cost=").Append(mq.getCumulativeCost(rel))
                .Append('\n');

            foreach (RelNode input in (IEnumerable<object>)rel.getInputs().toArray())
                Metadata(input, mq, depth + 1, text);
        }

        [Theory]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG JOIN SMALL ON BIG.B = SMALL.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM SMALL JOIN BIG ON BIG.B = SMALL.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG LEFT JOIN SMALL ON BIG.B = SMALL.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG RIGHT JOIN SMALL ON BIG.B = SMALL.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG FULL JOIN SMALL ON BIG.B = SMALL.A")]
        [InlineData("SELECT BIG.A FROM BIG WHERE BIG.B IN (SELECT SMALL.A FROM SMALL)")]
        [InlineData("SELECT SMALL.A FROM SMALL WHERE SMALL.B NOT IN (SELECT BIG.A FROM BIG)")]
        [InlineData("SELECT L.A, SMALL.B FROM (SELECT * FROM BIG LIMIT 10) L JOIN SMALL ON L.B = SMALL.A")]
        [InlineData("SELECT L.A, SMALL.B FROM (SELECT * FROM BIG LIMIT 10 OFFSET 5) L JOIN SMALL ON L.B = SMALL.A")]
        [InlineData("SELECT BIG.A, S.B FROM BIG JOIN (SELECT * FROM SMALL ORDER BY A) S ON BIG.B = S.A")]
        [InlineData("SELECT SBIG.B, SSMALL.B FROM SBIG JOIN SSMALL ON SBIG.A = SSMALL.A")]
        [InlineData("SELECT SBIG.B, SSMALL.B FROM SBIG LEFT JOIN SSMALL ON SBIG.A = SSMALL.A ORDER BY SBIG.A")]
        [InlineData("SELECT A FROM SBIG UNION ALL SELECT A FROM SSMALL ORDER BY A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG JOIN SMALL ON BIG.B < SMALL.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG JOIN SMALL ON BIG.B = SMALL.A ORDER BY BIG.B")]
        [InlineData("SELECT * FROM SBIG LIMIT 10")]
        [InlineData("SELECT * FROM BIG LIMIT 10 OFFSET 5")]
        [InlineData("SELECT * FROM BIG OFFSET 5")]
        public void ShouldPlanTheSameJoinAsCalcite(string sql)
        {
            var expected = Plan(sql, EnumerableConvention.INSTANCE).Replace("Enumerable", "ClrCursor");
            var actual = Plan(sql, ClrCursorConvention.Instance);

            actual.Should().Be(expected);
        }

        /// <summary>
        /// The batch nested loop join, which neither convention registers by default, offered to both.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Both choose it in every one of these, so this holds the plan and cannot tell the rescan charge
        /// apart: see <see cref="ShouldCostABatchNestedLoopJoinAsCalciteDoes"/>.
        /// </remarks>
        [Theory]
        [InlineData("SELECT TINY.A, BIG.B FROM TINY JOIN BIG ON TINY.B = BIG.A")]
        [InlineData("SELECT TINY.A, BIG.B FROM TINY LEFT JOIN BIG ON TINY.B = BIG.A")]
        [InlineData("SELECT SMALL.A, TINY.B FROM SMALL JOIN TINY ON SMALL.B = TINY.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG JOIN SMALL ON BIG.B = SMALL.A")]
        public void ShouldPlanTheSameBatchNestedLoopJoinAsCalcite(string sql)
        {
            var expected = Plan(sql, EnumerableConvention.INSTANCE, true).Replace("Enumerable", "ClrCursor");
            var actual = Plan(sql, ClrCursorConvention.Instance, true);

            actual.Should().Be(expected);
        }

        /// <summary>
        /// Builds this convention's batch nested loop join over the inputs of the one Calcite planned, and
        /// requires the two to cost the same.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// A batch nested loop join rescans its right input once per batch after the first, and Calcite
        /// charges <c>max(1, batches - 1)</c> rescans, so at least one however few batches the left fills.
        /// This one charged <c>max(1, batches) - 1</c>, which is none below two batches: fifty rows is half a
        /// batch of a hundred. Only the node is compared, because the plans in
        /// <see cref="ShouldPlanTheSameBatchNestedLoopJoinAsCalcite"/> choose it either way.
        /// </remarks>
        [Theory]
        [InlineData("SELECT TINY.A, BIG.B FROM TINY JOIN BIG ON TINY.B = BIG.A")]
        [InlineData("SELECT TINY.A, BIG.B FROM TINY LEFT JOIN BIG ON TINY.B = BIG.A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG JOIN SMALL ON BIG.B = SMALL.A")]
        public void ShouldCostABatchNestedLoopJoinAsCalciteDoes(string sql)
        {
            var calcite = Find<EnumerableBatchNestedLoopJoin>(PlanRel(sql, EnumerableConvention.INSTANCE, true));
            var ours = ClrCursorBatchNestedLoopJoin.Create(
                calcite.getLeft(),
                calcite.getRight(),
                calcite.getCondition(),
                calcite.getVariablesSet(),
                org.apache.calcite.util.ImmutableBitSet.of(),
                calcite.getJoinType());

            var planner = calcite.getCluster().getPlanner();
            var mq = calcite.getCluster().getMetadataQuery();

            ours.computeSelfCost(planner, mq).ToString().Should().Be(calcite.computeSelfCost(planner, mq).ToString());
        }

        /// <summary>
        /// Builds both conventions' interpreters over one input and requires the same cumulative cost, which
        /// Calcite takes to be the interpreter's own cost with its input's left out.
        /// </summary>
        /// <remarks>
        /// Built rather than planned, because <c>ClrCursorInterpreterRule</c> is a field a caller adds and
        /// the planner never reaches the node otherwise. Calcite answers it from a handler keyed on
        /// <c>EnumerableInterpreter</c>, and without one of ours the interpreter is charged its input too.
        /// </remarks>
        [Fact]
        public void ShouldCostAnInterpreterAsCalciteDoes()
        {
            var input = PlanRel("SELECT * FROM BIG WHERE A > 5", EnumerableConvention.INSTANCE, false);
            Install(input.getCluster());

            var calcite = EnumerableInterpreter.create(input, 0.5);
            var ours = ClrCursorInterpreter.Create(input, 0.5);
            var mq = input.getCluster().getMetadataQuery();

            mq.getCumulativeCost(ours).ToString().Should().Be(mq.getCumulativeCost(calcite).ToString());
            mq.getCumulativeCost(ours).ToString().Should().Be(mq.getNonCumulativeCost(ours).ToString());
        }

        /// <summary>
        /// Asks every node of a plan every metadata question through both dispatchers over
        /// <see cref="ClrCursorRelMetadata.Provider"/>, and requires the same answers.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// The provider reaches a plan two ways. <c>ClrPrepareImpl</c> dispatches through
        /// <see cref="ClrRelMetadataProvider"/>; a caller driving <c>Frameworks</c> gets Janino's, because each
        /// hep pass sets the thread's provider. The plan tests above only go the second way, so this is what
        /// says the prepare path answers the same — handlers written in .NET included, which Janino reaches
        /// by their <c>cli.</c> names.
        /// </remarks>
        [Theory]
        [InlineData("SELECT SBIG.B, SSMALL.B FROM SBIG LEFT JOIN SSMALL ON SBIG.A = SSMALL.A ORDER BY SBIG.A")]
        [InlineData("SELECT A FROM SBIG UNION ALL SELECT A FROM SSMALL ORDER BY A")]
        [InlineData("SELECT BIG.A, SMALL.B FROM BIG JOIN SMALL ON BIG.B < SMALL.A")]
        [InlineData("SELECT L.A, SMALL.B FROM (SELECT * FROM BIG LIMIT 10 OFFSET 5) L JOIN SMALL ON L.B = SMALL.A")]
        [InlineData("SELECT * FROM SBIG LIMIT 10")]
        public void ShouldAnswerTheSameThroughEitherDispatcher(string sql)
        {
            var plan = PlanRel(sql, ClrCursorConvention.Instance, false);
            var janino = org.apache.calcite.rel.metadata.JaninoRelMetadataProvider.of(ClrCursorRelMetadata.Provider);
            var clr = ClrRelMetadataProvider.Of(ClrCursorRelMetadata.Provider);

            var differences = new List<string>();
            foreach (var rel in Rel.Metadata.Tests.ClrRelMetadataProviderTests.Nodes(plan))
            {
                // a fresh query per question on each side, so neither answers from what the other cached
                foreach (var (name, ask) in Rel.Metadata.Tests.ClrRelMetadataProviderTests.Questions(rel))
                {
                    var j = Rel.Metadata.Tests.ClrRelMetadataProviderTests.Answer(ask, new org.apache.calcite.rel.metadata.RelMetadataQuery(janino));
                    var c = Rel.Metadata.Tests.ClrRelMetadataProviderTests.Answer(ask, new org.apache.calcite.rel.metadata.RelMetadataQuery(clr));

                    if (j != c)
                        differences.Add($"{rel.getRelTypeName()}.{name}: janino={j} clr={c}");
                }
            }

            differences.Should().BeEmpty();
        }

        /// <summary>
        /// The handlers are reached through <see cref="ClrRelMetadataProvider"/> at all: a limit's bounds are
        /// the handler's answer over the chain and the catch-all's over Calcite's provider alone.
        /// </summary>
        /// <remarks>
        /// Two dispatchers agreeing would hold as well if neither reached a handler of ours. This is the
        /// check that one does.
        /// </remarks>
        [Fact]
        public void ShouldReachTheHandlersThroughThePreparePathsDispatcher()
        {
            var plan = PlanRel("SELECT * FROM SBIG LIMIT 10 OFFSET 5", ClrCursorConvention.Instance, false);
            var limit = Find<ClrCursorLimit>(plan);

            var ours = new org.apache.calcite.rel.metadata.RelMetadataQuery(ClrRelMetadataProvider.Of(ClrCursorRelMetadata.Provider));
            var calcite = new org.apache.calcite.rel.metadata.RelMetadataQuery(ClrRelMetadataProvider.Of(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE));

            ours.getMaxRowCount(limit).Should().Be(java.lang.Double.valueOf(10d));
            ours.getMinRowCount(limit).Should().Be(java.lang.Double.valueOf(0d));
            ours.collations(limit).Should().NotBeNull();
            calcite.getMaxRowCount(limit).Should().BeNull();
            calcite.collations(limit).Should().BeNull();
        }

        /// <summary>
        /// Returns the one node of type <typeparamref name="T"/> in <paramref name="rel"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rel"></param>
        /// <returns></returns>
        static T Find<T>(RelNode rel) where T : RelNode
        {
            var found = new List<T>();
            var stack = new Stack<RelNode>([rel]);
            while (stack.Count > 0)
            {
                var node = stack.Pop();
                if (node is T t)
                    found.Add(t);

                foreach (RelNode input in (IEnumerable<object>)node.getInputs().toArray())
                    stack.Push(input);
            }

            return found.Should().ContainSingle().Subject;
        }

    }

}
