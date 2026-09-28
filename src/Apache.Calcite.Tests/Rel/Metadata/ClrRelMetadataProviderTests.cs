using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Apache.Calcite.Extensions.Prepare.Tests;
using Apache.Calcite.Extensions.Rel.Metadata;

using FluentAssertions;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

using Xunit;

namespace Apache.Calcite.Extensions.Rel.Metadata.Tests
{

    /// <summary>
    /// Compares <see cref="ClrRelMetadataProvider"/> with <c>JaninoRelMetadataProvider</c>: both are asked the
    /// same question about the same rel and must give the same answer.
    /// </summary>
    /// <remarks>
    /// Metadata decides which plan the planner picks, and a worse plan returns the same rows, so a row-level
    /// differential test cannot see a defect here. The comparison is made method by method and rel by rel.
    /// </remarks>
    public class ClrRelMetadataProviderTests
    {

        /// <summary>
        /// A cluster with a Volcano planner, which <c>getLowerBoundCost</c> requires.
        /// </summary>
        /// <returns>A new cluster over a Volcano planner with the convention trait registered.</returns>
        static RelOptCluster Cluster()
        {
            var typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
            var planner = new org.apache.calcite.plan.volcano.VolcanoPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            return RelOptCluster.create(planner, new RexBuilder(typeFactory));
        }

        /// <summary>
        /// A plan holding the node kinds the handlers dispatch on: values, filter, project, union, join and
        /// sort.
        /// </summary>
        /// <param name="cluster">The cluster to build the plan in.</param>
        /// <returns>The plan's root.</returns>
        static RelNode Plan(RelOptCluster cluster)
        {
            var typeFactory = cluster.getTypeFactory();
            var rexBuilder = cluster.getRexBuilder();
            var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
            var rowType = typeFactory.builder().add("A", intType).add("B", intType).build();

            RexLiteral Literal(int i) => (RexLiteral)rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(i), intType);

            com.google.common.collect.ImmutableList Row(int a, int b) =>
                com.google.common.collect.ImmutableList.copyOf(java.util.Arrays.asList([Literal(a), Literal(b)]));

            var tuples = com.google.common.collect.ImmutableList.copyOf(java.util.Arrays.asList([Row(1, 2), Row(3, 4), Row(5, 6)]));

            var left = LogicalValues.create(cluster, rowType, tuples);
            var right = LogicalValues.create(cluster, rowType, tuples);

            var filter = LogicalFilter.create(left,
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(left, 0), Literal(1)));

            var project = LogicalProject.create(filter, java.util.Collections.emptyList(),
                java.util.Arrays.asList([rexBuilder.makeInputRef(filter, 1), rexBuilder.makeInputRef(filter, 0)]),
                java.util.Arrays.asList(["B", "A"]));

            var union = LogicalUnion.create(java.util.Arrays.asList([(RelNode)project, right]), true);

            var join = LogicalJoin.create(union, right, java.util.Collections.emptyList(),
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(intType, 0),
                    rexBuilder.makeInputRef(intType, 2)),
                java.util.Collections.emptySet(),
                JoinRelType.INNER);

            return LogicalSort.create(join,
                RelCollations.of(0),
                null,
                Literal(2));
        }

        /// <summary>
        /// Every rel in the plan, inputs before the rel that reads them.
        /// </summary>
        /// <param name="rel">The plan's root.</param>
        /// <returns>Every node of the plan, in post-order.</returns>
        internal static IEnumerable<RelNode> Nodes(RelNode rel)
        {
            var inputs = rel.getInputs();
            for (int i = 0; i < inputs.size(); i++)
                foreach (var child in Nodes((RelNode)inputs.get(i)))
                    yield return child;

            yield return rel;
        }

        /// <summary>
        /// The metadata questions to ask of <paramref name="rel"/>, each named for the comparison report.
        /// </summary>
        /// <param name="rel">The node the questions are about; column-level questions use its first column or all of them.</param>
        /// <returns>Each question's name and a function that asks it of a metadata query.</returns>
        internal static (string Name, Func<RelMetadataQuery, object?> Ask)[] Questions(RelNode rel)
        {
            var bits = ImmutableBitSet.of(0);
            var all = ImmutableBitSet.range(0, rel.getRowType().getFieldCount());
            var rexBuilder = rel.getCluster().getRexBuilder();

            return
            [
                ("getRowCount", q => q.getRowCount(rel)),
                ("getMaxRowCount", q => q.getMaxRowCount(rel)),
                ("getMinRowCount", q => q.getMinRowCount(rel)),
                ("getPercentageOriginalRows", q => q.getPercentageOriginalRows(rel)),
                ("getCumulativeCost", q => q.getCumulativeCost(rel)),
                ("getNonCumulativeCost", q => q.getNonCumulativeCost(rel)),
                ("getColumnOrigins(0)", q => q.getColumnOrigins(rel, 0)),
                ("getColumnOrigins(1)", q => q.getColumnOrigins(rel, 1)),
                ("getSelectivity(null)", q => q.getSelectivity(rel, null)),
                ("getUniqueKeys", q => q.getUniqueKeys(rel)),
                ("areColumnsUnique(true)", q => q.areColumnsUnique(rel, bits, true)),
                ("areColumnsUnique(false)", q => q.areColumnsUnique(rel, bits, false)),
                ("areColumnsUnique(all)", q => q.areColumnsUnique(rel, all)),
                ("collations", q => q.collations(rel)),
                ("distribution", q => q.distribution(rel)),
                ("getPopulationSize", q => q.getPopulationSize(rel, bits)),
                ("getAverageRowSize", q => q.getAverageRowSize(rel)),
                ("getAverageColumnSizes", q => q.getAverageColumnSizes(rel)),
                ("getDistinctRowCount", q => q.getDistinctRowCount(rel, bits, null)),
                ("getPulledUpPredicates", q => q.getPulledUpPredicates(rel).pulledUpPredicates),
                ("getAllPredicates", q => q.getAllPredicates(rel)),
                ("isVisibleInExplain(ALL)", q => q.isVisibleInExplain(rel, SqlExplainLevel.ALL_ATTRIBUTES)),
                ("isVisibleInExplain(NO)", q => q.isVisibleInExplain(rel, SqlExplainLevel.NO_ATTRIBUTES)),
                ("getNodeTypes", q => q.getNodeTypes(rel)),
                ("memory", q => q.memory(rel)),
                ("cumulativeMemoryWithinPhase", q => q.cumulativeMemoryWithinPhase(rel)),
                ("isPhaseTransition", q => q.isPhaseTransition(rel)),
                ("splitCount", q => q.splitCount(rel)),
                ("getTableReferences", q => q.getTableReferences(rel)),
                ("getExpressionLineage", q => q.getExpressionLineage(rel, rexBuilder.makeInputRef(rel, 0))),
                ("getLowerBoundCost", q => q.getLowerBoundCost(rel, (org.apache.calcite.plan.volcano.VolcanoPlanner)rel.getCluster().getPlanner())),
            ];
        }

        /// <summary>
        /// Asks a question and renders the answer, or the innermost exception it threw, as text.
        /// </summary>
        /// <param name="ask">The question.</param>
        /// <param name="mq">The metadata query to ask it of.</param>
        /// <returns>The answer's <c>ToString()</c>, <c>null</c>, or <c>threw</c> followed by the innermost exception's
        /// type name and message.</returns>
        internal static string Answer(Func<RelMetadataQuery, object?> ask, RelMetadataQuery mq)
        {
            try
            {
                return ask(mq)?.ToString() ?? "null";
            }
            catch (Exception e)
            {
                while (e.InnerException is Exception inner)
                    e = inner;

                return "threw " + e.GetType().Name + ": " + e.Message;
            }
        }

        /// <summary>
        /// The two providers answer the same, for every method and every node of a plan.
        /// </summary>
        [Fact]
        public void Should_answer_what_janino_answers()
        {
            var cluster = Cluster();
            var plan = Plan(cluster);

            var differences = new List<string>();

            foreach (var rel in Nodes(plan))
            {
                // a new query per question on each side, so no answer comes from a cache an earlier question
                // filled
                foreach (var (name, ask) in Questions(rel))
                {
                    var janino = Answer(ask, new RelMetadataQuery(JaninoRelMetadataProvider.DEFAULT));
                    var clr = Answer(ask, new RelMetadataQuery(ClrRelMetadataProvider.Default));

                    if (janino != clr)
                        differences.Add($"{rel.getRelTypeName()}.{name}: janino={janino} clr={clr}");
                }
            }

            differences.Count.Should().Be(0, string.Join(Environment.NewLine, differences));
        }

        /// <summary>
        /// The two providers also agree when every question about every node goes through one query, as a
        /// planner asks them, so answers come from the query's cache.
        /// </summary>
        [Fact]
        public void Should_answer_what_janino_answers_through_one_query()
        {
            var cluster = Cluster();
            var plan = Plan(cluster);

            var janinoQuery = new RelMetadataQuery(JaninoRelMetadataProvider.DEFAULT);
            var clrQuery = new RelMetadataQuery(ClrRelMetadataProvider.Default);

            var differences = new List<string>();

            foreach (var rel in Nodes(plan))
                foreach (var (name, ask) in Questions(rel))
                {
                    var janino = Answer(ask, janinoQuery);
                    var clr = Answer(ask, clrQuery);

                    if (janino != clr)
                        differences.Add($"{rel.getRelTypeName()}.{name}: janino={janino} clr={clr}");
                }

            differences.Count.Should().Be(0, string.Join(Environment.NewLine, differences));
        }

        /// <summary>
        /// A row-count handler written in .NET that answers 7 for any <c>Values</c>.
        /// </summary>
        public class ClrValuesRowCount : MetadataHandler
        {

            public java.lang.Double getRowCount(Values rel, RelMetadataQuery mq) => java.lang.Double.valueOf(7d);

            public MetadataDef getDef() => BuiltInMetadata.RowCount.DEF;

        }

        /// <summary>
        /// A metadata handler written in .NET answers through this provider and through Janino's.
        /// </summary>
        /// <remarks>
        /// Janino's generated source names the handler class by its IKVM name, which begins <c>cli.</c>, and
        /// resolves it through the class loader <c>IKVM.Maven.Sdk</c> assigns to <c>calcite-core</c>, which
        /// searches every loaded assembly. That requires IKVM 8.16.0 or later.
        /// </remarks>
        [Fact]
        public void Should_take_a_handler_written_in_dotnet()
        {
            var handlerClass = (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler);
            var source = ReflectiveRelMetadataProvider.reflectiveSource(new ClrValuesRowCount(), handlerClass);
            var chained = ChainedRelMetadataProvider.of(java.util.Arrays.asList([source, DefaultRelMetadataProvider.INSTANCE]));

            var cluster = Cluster();
            var typeFactory = cluster.getTypeFactory();
            var rowType = typeFactory.builder().add("A", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
            var values = LogicalValues.createEmpty(cluster, rowType);

            var mq = new RelMetadataQuery(ClrRelMetadataProvider.Of(chained));
            mq.getRowCount(values).doubleValue().Should().BeApproximately(7d, 0.0001);

            Assert.NotNull(JaninoRelMetadataProvider.of(chained).revise(handlerClass));

            var janino = new RelMetadataQuery(JaninoRelMetadataProvider.of(chained));
            janino.getRowCount(values).doubleValue().Should().BeApproximately(7d, 0.0001);
        }

        /// <summary>
        /// Preparing a statement compiles no metadata handler.
        /// </summary>
        /// <remarks>
        /// A handler is generated once per process, so timing cannot show this; the test reads the private
        /// static cache <c>JaninoRelMetadataProvider</c> keeps its generated handlers in. IKVM renames the
        /// backing field of a Java <c>static final</c>, so the field is looked up under both names.
        /// </remarks>
        [Fact]
        public void Should_prepare_without_compiling_a_handler()
        {
            var field = typeof(JaninoRelMetadataProvider).GetField("HANDLERS", BindingFlags.NonPublic | BindingFlags.Static)
                ?? typeof(JaninoRelMetadataProvider).GetField("__<>HANDLERS", BindingFlags.NonPublic | BindingFlags.Static);

            field.Should().NotBeNull("JaninoRelMetadataProvider.HANDLERS could not be read, so nothing was measured.");

            var cache = (com.google.common.cache.LoadingCache)field.GetValue(null)!;

            const string sql = "SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION ORDER BY REGION";
            JaninoRelMetadataProvider.clearStaticCache();

            ClrPrepareFixture.WithContext(sql, (context, _) =>
                new Apache.Calcite.Extensions.Prepare.ClrPrepareImpl().PrepareSql(context, Apache.Calcite.Extensions.Prepare.IClrPrepare.Query.Of(sql), typeof(object[]), -1));

            cache.size().Should().Be(0L, "Janino generated a metadata handler while preparing.");
        }

        /// <summary>
        /// A row-count handler for table scans that counts how often it is asked.
        /// </summary>
        public class CountingRowCount : MetadataHandler
        {

            public static int Asked;

            public java.lang.Double getRowCount(TableScan rel, RelMetadataQuery mq)
            {
                Asked++;
                return java.lang.Double.valueOf(rel.estimateRowCount(mq));
            }

            public MetadataDef getDef() => BuiltInMetadata.RowCount.DEF;

        }

        /// <summary>
        /// A prepare that installs a metadata provider the way Calcite describes: subclass the prepare,
        /// override the cluster factory, and set the query supplier there.
        /// </summary>
        /// <param name="provider">The provider the cluster's metadata query supplier dispatches to.</param>
        sealed class PrepareWithProvider(RelMetadataProvider provider) : Apache.Calcite.Extensions.Prepare.ClrPrepareImpl
        {

            protected override RelOptCluster CreateCluster(RelOptPlanner planner, RexBuilder rexBuilder)
            {
                var cluster = base.CreateCluster(planner, rexBuilder);
                cluster.setMetadataQuerySupplier(ClrRelMetadataProvider.QuerySupplier(provider));
                cluster.invalidateMetadataQuery();
                return cluster;
            }

        }

        /// <summary>
        /// A provider installed through <c>CreateCluster</c> answers a prepared statement's metadata.
        /// </summary>
        /// <remarks>
        /// Mirrors Calcite's recipe: <c>CalcitePrepareImpl.createCluster</c> is protected on a class that is
        /// not final, and <c>RelMetadataQueryBase</c>'s comment ends by setting the supplier on the cluster
        /// and planning with that cluster.
        /// </remarks>
        [Fact]
        public void Should_take_a_provider_from_a_prepare_of_ones_own()
        {
            var source = ReflectiveRelMetadataProvider.reflectiveSource(
                new CountingRowCount(), (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler));
            var chained = ChainedRelMetadataProvider.of(java.util.Arrays.asList([source, DefaultRelMetadataProvider.INSTANCE]));

            const string sql = "SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION";

            CountingRowCount.Asked = 0;
            ClrPrepareFixture.WithContext(sql, (context, _) =>
                new PrepareWithProvider(chained).PrepareSql(context, Apache.Calcite.Extensions.Prepare.IClrPrepare.Query.Of(sql), typeof(object[]), -1));

            Assert.True(CountingRowCount.Asked > 0, "the provider the prepare installed was never asked.");
        }

        /// <summary>
        /// Every <c>Handler</c> interface nested in <c>BuiltInMetadata</c>.
        /// </summary>
        /// <returns>The <c>Handler</c> interfaces, in the order reflection lists their enclosing types.</returns>
        static Type[] HandlerInterfaces()
        {
            return typeof(BuiltInMetadata).GetNestedTypes()
                .Select(t => t.GetNestedType("Handler"))
                .Where(t => t is not null)
                .ToArray()!;
        }

        /// <summary>
        /// Asking for a handler answers the handler, not a stand-in.
        /// </summary>
        /// <remarks>
        /// Janino's provider answers <c>handler</c> with a <c>java.lang.reflect.Proxy</c> that throws, to defer
        /// a Java compile until a statement needs one. This provider compiles nothing, so it returns the
        /// handler directly and a query does not build a proxy per handler interface.
        /// </remarks>
        [Fact]
        public void Should_answer_with_the_handler_itself()
        {
            var handlerClass = (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler);

            var handler = ClrRelMetadataProvider.Default.handler(handlerClass);

            Assert.StartsWith("GeneratedMetadata_", handler.GetType().Name);
            Assert.Same(handler, ClrRelMetadataProvider.Default.revise(handlerClass));
        }

        /// <summary>
        /// Returns a placeholder argument of the type <paramref name="parameter"/> takes.
        /// </summary>
        /// <param name="parameter">The parameter to supply.</param>
        /// <param name="rel">The node, passed for a parameter of a rel type.</param>
        /// <param name="mq">The metadata query, passed for a <c>RelMetadataQuery</c> parameter.</param>
        /// <returns>A value the parameter accepts: a zero, false, a one-column bit set or an enum's first constant,
        /// and null for any other type.</returns>
        static object? Argument(ParameterInfo parameter, RelNode rel, RelMetadataQuery mq)
        {
            var type = parameter.ParameterType;

            if (typeof(RelNode).IsAssignableFrom(type))
                return rel;
            if (type == typeof(RelMetadataQuery))
                return mq;
            if (type == typeof(int))
                return 0;
            if (type == typeof(bool))
                return false;
            if (type == typeof(ImmutableBitSet))
                return ImmutableBitSet.of(0);
            if (typeof(java.lang.Enum).IsAssignableFrom(type))
                return ((Array)type.GetMethod("values", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, null)!).GetValue(0);

            return null;
        }

        /// <summary>
        /// Every method of every handler interface is emitted correctly and answers what Janino's answers.
        /// </summary>
        /// <remarks>
        /// Emitted IL is verified only when a method is first called, and malformed IL surfaces then as
        /// <c>InvalidProgramException</c>. Some handler interfaces, such as <c>Measure</c>,
        /// <c>FunctionalDependency</c> and <c>InputFieldsUsed</c>, are not reached by any query, so this calls
        /// every method directly.
        /// </remarks>
        [Fact]
        public void Should_emit_every_handler_method_callably()
        {
            var cluster = Cluster();
            var rel = Plan(cluster);

            var differences = new List<string>();

            foreach (var handlerInterface in HandlerInterfaces())
            {
                var handlerClass = (java.lang.Class)handlerInterface;
                var clr = ClrRelMetadataProvider.Default.revise(handlerClass);
                var janino = JaninoRelMetadataProvider.DEFAULT.revise(handlerClass);

                foreach (var method in handlerInterface.GetMethods().Where(m => m.IsAbstract && !m.IsStatic))
                {
                    var clrQuery = new RelMetadataQuery(ClrRelMetadataProvider.Default);
                    var janinoQuery = new RelMetadataQuery(JaninoRelMetadataProvider.DEFAULT);

                    var ours = Answer(_ => method.Invoke(clr, method.GetParameters().Select(p => Argument(p, rel, clrQuery)).ToArray()), clrQuery);
                    var theirs = Answer(_ => method.Invoke(janino, method.GetParameters().Select(p => Argument(p, rel, janinoQuery)).ToArray()), janinoQuery);

                    if (ours != theirs)
                        differences.Add($"{handlerInterface.DeclaringType!.Name}.{method.Name}: janino={theirs} clr={ours}");
                }
            }

            differences.Count.Should().Be(0, string.Join(Environment.NewLine, differences));
        }

        /// <summary>
        /// A handler that asks the question it is answering.
        /// </summary>
        public class CyclicRowCount : MetadataHandler
        {

            public java.lang.Double getRowCount(Values rel, RelMetadataQuery mq) => mq.getRowCount(rel);

            public MetadataDef getDef() => BuiltInMetadata.RowCount.DEF;

        }

        /// <summary>
        /// A question that depends on itself is reported as a cycle rather than running out of stack, and
        /// the rel's row is cleared on the way out.
        /// </summary>
        /// <remarks>
        /// The mark a call leaves under its own key before dispatching detects the repeated call, and the catch
        /// around the dispatch clears the row. A query that completes reaches neither branch.
        /// </remarks>
        [Fact]
        public void Should_report_a_cycle_and_clear_the_row()
        {
            var handlerClass = (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler);
            var source = ReflectiveRelMetadataProvider.reflectiveSource(new CyclicRowCount(), handlerClass);

            var cluster = Cluster();
            var typeFactory = cluster.getTypeFactory();
            var rowType = typeFactory.builder().add("A", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
            var values = LogicalValues.createEmpty(cluster, rowType);

            var mq = new RelMetadataQuery(ClrRelMetadataProvider.Of(source));

            Assert.ThrowsAny<CyclicMetadataException>(() => mq.getRowCount(values));
            mq.map.row(values).size().Should().Be(0, "the rel's row was left behind.");
        }

        /// <summary>
        /// A question no handler answers for the rel is refused, as Calcite's generated dispatch refuses it
        /// when no handler matches.
        /// </summary>
        [Fact]
        public void Should_refuse_a_rel_no_handler_declares()
        {
            var source = ReflectiveRelMetadataProvider.reflectiveSource(
                new ClrValuesRowCount(), (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler));

            var cluster = Cluster();
            var typeFactory = cluster.getTypeFactory();
            var rowType = typeFactory.builder().add("A", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
            var values = LogicalValues.createEmpty(cluster, rowType);

            var mq = new RelMetadataQuery(ClrRelMetadataProvider.Of(source));

            mq.getRowCount(values).doubleValue().Should().BeApproximately(7d, 0.0001);

            var refused = Assert.ThrowsAny<java.lang.IllegalArgumentException>(() => mq.getMaxRowCount(values));
            Assert.Contains("No handler for method", refused.Message);
            Assert.Contains("catch-all", refused.Message);
        }

    }

}
