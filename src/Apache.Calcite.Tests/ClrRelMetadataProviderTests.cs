using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Apache.Calcite.Extensions.Rel.Metadata;

using Microsoft.VisualStudio.TestTools.UnitTesting;

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

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// <see cref="ClrRelMetadataProvider"/> against <c>JaninoRelMetadataProvider</c>, which is the only
    /// oracle for it: the two are asked the same question about the same rel and have to answer the same
    /// thing.
    /// </summary>
    /// <remarks>
    /// A row-level differential test cannot see this. Metadata decides which plan the planner picks, and a
    /// worse plan returns the same rows; the whole of what a defect here looks like is a slower query, until
    /// it is a cost model that never converges. So the comparison is direct, method by method and rel by rel.
    /// </remarks>
    [TestClass]
    public class ClrRelMetadataProviderTests
    {

        /// <summary>
        /// A cluster whose planner is Volcano's, because a cost is asked of one.
        /// </summary>
        /// <returns></returns>
        static RelOptCluster Cluster()
        {
            var typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
            var planner = new org.apache.calcite.plan.volcano.VolcanoPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            return RelOptCluster.create(planner, new RexBuilder(typeFactory));
        }

        /// <summary>
        /// A plan holding the node kinds the dispatch chain separates: values, filter, project, join, union
        /// and sort.
        /// </summary>
        /// <param name="cluster"></param>
        /// <returns></returns>
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
        /// Every rel in the plan, deepest first.
        /// </summary>
        /// <param name="rel"></param>
        /// <returns></returns>
        static IEnumerable<RelNode> Nodes(RelNode rel)
        {
            var inputs = rel.getInputs();
            for (int i = 0; i < inputs.size(); i++)
                foreach (var child in Nodes((RelNode)inputs.get(i)))
                    yield return child;

            yield return rel;
        }

        /// <summary>
        /// The questions, one per handler method that a plan of this shape can be asked.
        /// </summary>
        /// <param name="rel"></param>
        /// <returns></returns>
        static (string Name, Func<RelMetadataQuery, object?> Ask)[] Questions(RelNode rel)
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
        /// Asks, and answers with what came back or with what was thrown.
        /// </summary>
        /// <param name="ask"></param>
        /// <param name="mq"></param>
        /// <returns></returns>
        static string Answer(Func<RelMetadataQuery, object?> ask, RelMetadataQuery mq)
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
        [TestMethod]
        public void Should_answer_what_janino_answers()
        {
            var cluster = Cluster();
            var plan = Plan(cluster);

            var differences = new List<string>();

            foreach (var rel in Nodes(plan))
            {
                // a fresh query per rel on each side, so neither can answer from what the other's walk left
                // in a shared cache
                foreach (var (name, ask) in Questions(rel))
                {
                    var janino = Answer(ask, new RelMetadataQuery(JaninoRelMetadataProvider.DEFAULT));
                    var clr = Answer(ask, new RelMetadataQuery(ClrRelMetadataProvider.Default));

                    if (janino != clr)
                        differences.Add($"{rel.getRelTypeName()}.{name}: janino={janino} clr={clr}");
                }
            }

            Assert.AreEqual(0, differences.Count, string.Join(Environment.NewLine, differences));
        }

        /// <summary>
        /// One query's answers are the same when every node is asked through one query, which is what a
        /// planner does and what the cache is for.
        /// </summary>
        [TestMethod]
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

            Assert.AreEqual(0, differences.Count, string.Join(Environment.NewLine, differences));
        }

        /// <summary>
        /// A handler written in .NET. Calcite's own provider cannot take one.
        /// </summary>
        public class ClrValuesRowCount : MetadataHandler
        {

            public java.lang.Double getRowCount(Values rel, RelMetadataQuery mq) => java.lang.Double.valueOf(7d);

            public MetadataDef getDef() => BuiltInMetadata.RowCount.DEF;

        }

        /// <summary>
        /// A metadata handler written in .NET answers here, and does not compile under Janino.
        /// </summary>
        /// <remarks>
        /// The generated source names the handler class and the rel class the way Java names them, and IKVM's
        /// name for a CLR class begins <c>cli.</c>. Janino answers "Cannot determine simple type name cli",
        /// which is what stops a .NET user-defined function under <c>EnumerableConvention</c> as well. It is
        /// the reason this provider exists; the compile it saves is the other one.
        /// </remarks>
        [TestMethod]
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
            Assert.AreEqual(7d, mq.getRowCount(values).doubleValue(), 0.0001);

            var refused = Assert.ThrowsException<java.lang.RuntimeException>(
                () => JaninoRelMetadataProvider.of(chained).revise(handlerClass));
            StringAssert.Contains(refused.ToString(), "cli");
        }

        /// <summary>
        /// Preparing a statement compiles no metadata handler.
        /// </summary>
        /// <remarks>
        /// The cache <c>JaninoRelMetadataProvider</c> holds its generated handlers in is the only thing that
        /// can say so — a handler is generated once per process, so a second statement would pay nothing and
        /// a timing would not tell. It is a private static, and IKVM renames the backing field of a Java
        /// <c>static final</c>, so it is looked up under both names.
        ///
        /// <para>Measured before this provider: a statement of this shape generated nine of the twenty-seven
        /// handlers, and that was two to three seconds of a cold first prepare.</para>
        /// </remarks>
        [TestMethod]
        public void Should_prepare_without_compiling_a_handler()
        {
            var field = typeof(JaninoRelMetadataProvider).GetField("HANDLERS", BindingFlags.NonPublic | BindingFlags.Static)
                ?? typeof(JaninoRelMetadataProvider).GetField("__<>HANDLERS", BindingFlags.NonPublic | BindingFlags.Static);

            Assert.IsNotNull(field, "JaninoRelMetadataProvider.HANDLERS could not be read, so nothing was measured.");

            var cache = (com.google.common.cache.LoadingCache)field.GetValue(null)!;

            const string sql = "SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION ORDER BY REGION";
            JaninoRelMetadataProvider.clearStaticCache();

            ClrPrepareFixture.WithContext(sql, (context, _) =>
                new Apache.Calcite.Extensions.Prepare.ClrPrepareImpl().PrepareSql(context, Apache.Calcite.Extensions.Prepare.IClrPrepare.Query.Of(sql), typeof(object[]), -1));

            Assert.AreEqual(0L, cache.size(), "Janino generated a metadata handler while preparing.");
        }

        /// <summary>
        /// A handler counting what it is asked, so that a prepare can be seen to have used it.
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
        /// A prepare of one's own, which is how Calcite says to install a metadata provider: subclass the
        /// prepare, override the cluster factory, and set the query supplier there.
        /// </summary>
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
        /// A provider installed the way Calcite documents answers a real statement's metadata.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.createCluster</c> is protected on a class that is not final, and
        /// <c>RelMetadataQueryBase</c>'s own comment ends its recipe by setting the supplier on the cluster
        /// and planning with that cluster. This is that recipe, and it is the whole of why the prepare is
        /// open.
        /// </remarks>
        [TestMethod]
        public void Should_take_a_provider_from_a_prepare_of_ones_own()
        {
            var source = ReflectiveRelMetadataProvider.reflectiveSource(
                new CountingRowCount(), (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler));
            var chained = ChainedRelMetadataProvider.of(java.util.Arrays.asList([source, DefaultRelMetadataProvider.INSTANCE]));

            const string sql = "SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION";

            CountingRowCount.Asked = 0;
            ClrPrepareFixture.WithContext(sql, (context, _) =>
                new PrepareWithProvider(chained).PrepareSql(context, Apache.Calcite.Extensions.Prepare.IClrPrepare.Query.Of(sql), typeof(object[]), -1));

            Assert.IsTrue(CountingRowCount.Asked > 0, "the provider the prepare installed was never asked.");
        }

        /// <summary>
        /// Every handler interface, which is how many a query asks for.
        /// </summary>
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
        /// Janino's provider answers <c>handler</c> with a <c>java.lang.reflect.Proxy</c> that throws, so
        /// that a Java compile is deferred until a statement proves it needs one. Nothing here is worth
        /// deferring, and the proxy was not free: a query builds one per handler interface, and the planner
        /// builds a query per rule transformation.
        /// </remarks>
        [TestMethod]
        public void Should_answer_with_the_handler_itself()
        {
            var handlerClass = (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler);

            var handler = ClrRelMetadataProvider.Default.handler(handlerClass);

            StringAssert.StartsWith(handler.GetType().Name, "GeneratedMetadata_");
            Assert.AreSame(handler, ClrRelMetadataProvider.Default.revise(handlerClass));
        }

        /// <summary>
        /// Returns an argument for <paramref name="parameter"/>, so that a method can be called at all.
        /// </summary>
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
        /// A body that is emitted but never called is never verified: the runtime resolves it on the first
        /// call, and malformed IL surfaces as <c>InvalidProgramException</c> then and not before. Three of
        /// the interfaces — Measure, FunctionalDependency and InputFieldsUsed — are reached by no query in
        /// this suite, so this calls all of them by hand rather than waiting for one that does.
        /// </remarks>
        [TestMethod]
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

            Assert.AreEqual(0, differences.Count, string.Join(Environment.NewLine, differences));
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
        /// The mark the call leaves under its own key before dispatching is what sees the second call, and
        /// the catch around the dispatch is what clears the row. Neither branch is reached by a query that
        /// answers, so nothing else here covers them.
        /// </remarks>
        [TestMethod]
        public void Should_report_a_cycle_and_clear_the_row()
        {
            var handlerClass = (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler);
            var source = ReflectiveRelMetadataProvider.reflectiveSource(new CyclicRowCount(), handlerClass);

            var cluster = Cluster();
            var typeFactory = cluster.getTypeFactory();
            var rowType = typeFactory.builder().add("A", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
            var values = LogicalValues.createEmpty(cluster, rowType);

            var mq = new RelMetadataQuery(ClrRelMetadataProvider.Of(source));

            Assert.ThrowsException<CyclicMetadataException>(() => mq.getRowCount(values));
            Assert.AreEqual(0, mq.map.row(values).size(), "the rel's row was left behind.");
        }

        /// <summary>
        /// A handler interface no handler answers refuses the rel, as Calcite's generated chain does when it
        /// runs off the end.
        /// </summary>
        [TestMethod]
        public void Should_refuse_a_rel_no_handler_declares()
        {
            var source = ReflectiveRelMetadataProvider.reflectiveSource(
                new ClrValuesRowCount(), (java.lang.Class)typeof(BuiltInMetadata.RowCount.Handler));

            var cluster = Cluster();
            var typeFactory = cluster.getTypeFactory();
            var rowType = typeFactory.builder().add("A", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
            var values = LogicalValues.createEmpty(cluster, rowType);

            var mq = new RelMetadataQuery(ClrRelMetadataProvider.Of(source));

            Assert.AreEqual(7d, mq.getRowCount(values).doubleValue(), 0.0001);

            var refused = Assert.ThrowsException<java.lang.IllegalArgumentException>(() => mq.getMaxRowCount(values));
            StringAssert.Contains(refused.Message, "No handler for method");
            StringAssert.Contains(refused.Message, "catch-all");
        }

    }

}
