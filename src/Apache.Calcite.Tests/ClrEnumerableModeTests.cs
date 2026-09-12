using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Reads the tree the implementor builds, and requires that each mode built what that mode is made of.
    /// </summary>
    /// <remarks>
    /// The differential suites compare <em>rows</em>, which is a strong oracle for correctness and no oracle
    /// at all for which operators ran: a plan built entirely from the synchronous operators and read across
    /// once at the root would answer every one of them correctly, while awaiting nothing and blocking a
    /// thread per row. Nothing in this repository could see that, and it is exactly the defect the one
    /// convention makes possible, because the mode is no longer visible in the plan.
    ///
    /// <para>So these tests read the compiled tree instead. For one planned root implemented both ways they
    /// check the type the lambda returns, the class every operator call lands on, and that each asynchronous
    /// call carries the trailing token that <c>[EnumeratorCancellation]</c> reads. A crossing is allowed only
    /// where the schema forces one, and where it is forced it is required to be exactly one call.</para>
    /// </remarks>
    [TestClass]
    public class ClrEnumerableModeTests
    {

        static ClrEnumerableModeTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A schema of tables Calcite itself can read, so that nothing in a plan over it is forced to cross.
        /// </summary>
        static SchemaPlus Schema()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
            rootSchema.add("WIDE", new SyncRowsTable(AsyncTestRows.Wide, AsyncTestRows.WideRowType, false));

            return rootSchema;
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
        /// Plans a statement into the convention, once.
        /// </summary>
        static RelNode Plan(string sql, SchemaPlus rootSchema)
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in org.apache.calcite.plan.RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Implements a planned root the way asked for and returns the lambda.
        /// </summary>
        static LambdaExpression Implement(RelNode physical, bool async, java.util.Map parameters)
        {
            var implementor = new ClrEnumerableRelImplementor(physical.getCluster().getRexBuilder(), parameters, async);

            return implementor.ImplementRoot((ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
        }

        /// <summary>
        /// The sequence calls of the tree, by which side they belong to.
        /// </summary>
        /// <remarks>
        /// Three sources, because a plan reaches a sequence three ways: the operator sets, the interop that
        /// reads a linq4j <c>Enumerable</c> across, and the two bridges between the kinds. A bare scan over
        /// a table of Calcite's calls no operator at all — it reads the table's own sequence and hands it up
        /// — so counting only the operator sets would call that plan empty.
        /// </remarks>
        static (List<MethodCallExpression> Synchronous, List<MethodCallExpression> Asynchronous, List<MethodCallExpression> Bridges) Operators(LambdaExpression tree)
        {
            var calls = new List<MethodCallExpression>();
            new Collector(calls).Visit(tree);

            var synchronous = calls
                .Where(c => c.Method.DeclaringType?.Name == "ClrEnumerableDefaults"
                    || (c.Method.DeclaringType?.Name == "JavaSequences" && c.Method.Name == "FromJava"))
                .ToList();

            var asynchronous = calls
                .Where(c => c.Method.DeclaringType?.Name == "ClrAsyncEnumerableDefaults"
                    || (c.Method.DeclaringType?.Name == "JavaSequences" && c.Method.Name == "FromJavaAsync"))
                .ToList();

            var bridges = calls.Where(c => c.Method.DeclaringType?.Name == "ClrSequences").ToList();

            return (synchronous, asynchronous, bridges);
        }

        /// <summary>
        /// Collects every call in a tree.
        /// </summary>
        sealed class Collector(List<MethodCallExpression> calls) : ExpressionVisitor
        {

            /// <inheritdoc />
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                calls.Add(node);

                return base.VisitMethodCall(node);
            }

        }

        /// <summary>
        /// The queries read, one per node this convention is likely to build for a schema of ordinary tables.
        /// </summary>
        static readonly string[] Queries =
        [
            "SELECT * FROM SALES",
            "SELECT ID FROM SALES",
            "SELECT * FROM SALES WHERE AMOUNT > 10",
            "SELECT * FROM SALES ORDER BY AMOUNT",
            "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION",
            "SELECT SUM(AMOUNT) FROM SALES",
            "SELECT * FROM SALES ORDER BY AMOUNT LIMIT 2 OFFSET 1",
            "SELECT ID FROM SALES UNION SELECT ID FROM SALES",
            "SELECT ID FROM SALES UNION ALL SELECT ID FROM SALES",
            "SELECT ID FROM SALES INTERSECT SELECT ID FROM SALES",
            "SELECT ID FROM SALES EXCEPT SELECT ID FROM SALES",
            "SELECT a.ID, b.LABEL FROM SALES a JOIN SALES b ON a.ID = b.ID",
            "SELECT ID, SUM(AMOUNT) OVER (ORDER BY ID) FROM SALES",
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)",
            "SELECT DISTINCT REGION FROM SALES",
        ];

        /// <summary>
        /// Implemented synchronously, every operator call is a synchronous one, and the lambda says so.
        /// </summary>
        [TestMethod]
        public void ShouldBuildTheSynchronousOperatorsSynchronously()
        {
            foreach (var sql in Queries)
            {
                var rootSchema = Schema();
                var parameters = new java.util.HashMap();
                var tree = Implement(Plan(sql, rootSchema), false, parameters);

                tree.ReturnType.Should().Be(typeof(IEnumerable<object>), "'{0}' was implemented synchronously", sql);

                var (synchronous, asynchronous, bridges) = Operators(tree);

                synchronous.Should().NotBeEmpty("'{0}' has to run through some operator", sql);
                asynchronous.Should().BeEmpty("'{0}' names an awaiting operator in a plan nobody will await", sql);
                bridges.Should().BeEmpty("'{0}' reads only tables it can read directly, so nothing should cross", sql);
            }
        }

        /// <summary>
        /// Implemented asynchronously, every operator call is an awaiting one, and each carries its token.
        /// </summary>
        /// <remarks>
        /// The token is the second half of the claim. Every operator of the awaiting set ends in a
        /// <see cref="CancellationToken"/> and the tree has to pass one, because an expression tree does not
        /// apply a default argument; the value passed is <c>default</c>, which is what
        /// <c>[EnumeratorCancellation]</c> replaces with the token the caller gives
        /// <c>GetAsyncEnumerator</c>. An operator called without it would not compile, and one called with
        /// something else would silently ignore the caller's cancellation.
        /// </remarks>
        [TestMethod]
        public void ShouldBuildTheAwaitingOperatorsAsynchronously()
        {
            foreach (var sql in Queries)
            {
                var rootSchema = Schema();
                var parameters = new java.util.HashMap();
                var tree = Implement(Plan(sql, rootSchema), true, parameters);

                tree.ReturnType.Should().Be(typeof(IAsyncEnumerable<object>), "'{0}' was implemented asynchronously", sql);

                var (synchronous, asynchronous, bridges) = Operators(tree);

                asynchronous.Should().NotBeEmpty("'{0}' has to run through some operator", sql);
                bridges.Should().BeEmpty("'{0}' reads only tables it can read directly, so nothing should cross", sql);

                // the synchronous set is not required to be empty: a row level helper of it may be called
                // per row inside a selector, which is not a sequence operator. What must not appear is one
                // that takes or returns a sequence.
                foreach (var call in synchronous)
                    IsSequenceOperator(call).Should().BeFalse(
                        "'{0}' calls the synchronous {1} on a sequence in a plan that awaits", sql, call.Method.Name);

                foreach (var call in asynchronous)
                {
                    call.Method.GetParameters()[^1].ParameterType.Should().Be(typeof(CancellationToken),
                        "every awaiting operator ends in a token, and {0} does not", call.Method.Name);

                    call.Arguments[^1].Should().BeOfType<DefaultExpression>(
                        "{0} must be passed the default token, which is what [EnumeratorCancellation] reads", call.Method.Name);
                }
            }
        }

        /// <summary>
        /// Whether a call takes or returns a sequence, as against being a per-row helper.
        /// </summary>
        static bool IsSequenceOperator(MethodCallExpression call)
        {
            static bool Sequence(Type type) =>
                type.IsGenericType
                && (type.GetGenericTypeDefinition() == typeof(IEnumerable<>) || type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>));

            return Sequence(call.Method.ReturnType) || call.Method.GetParameters().Any(p => Sequence(p.ParameterType));
        }

        /// <summary>
        /// One planned root gives two trees, and neither is the other.
        /// </summary>
        /// <remarks>
        /// The property the prepare pipeline rests on: a statement is planned once and read either way, so
        /// the two implementations must come off the same nodes without either disturbing them.
        /// </remarks>
        [TestMethod]
        public async Task ShouldImplementOnePlannedRootBothWays()
        {
            var rootSchema = Schema();
            var physical = Plan("SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION ORDER BY REGION", rootSchema);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var synchronous = (Func<DataContext, IEnumerable<object>>)Implement(physical, false, parameters).Compile();
            var asynchronous = (Func<DataContext, IAsyncEnumerable<object>>)Implement(physical, true, parameters).Compile();

            var read = new List<string>();
            foreach (var row in synchronous(context))
                read.Add(Render(row));

            var awaited = new List<string>();
            await foreach (var row in asynchronous(context))
                awaited.Add(Render(row));

            awaited.Should().Equal(read);
        }

        /// <summary>
        /// A table that can only be read asynchronously crosses once, at the leaf, and only in a synchronous
        /// plan.
        /// </summary>
        /// <remarks>
        /// The crossing is what the mode costs when it disagrees with the leaf, and where it happens is the
        /// claim: at the scan, not at the root, so everything above it is the plan's own kind. One call, not
        /// one per node.
        /// </remarks>
        [TestMethod]
        public void ShouldCrossOnceAtAnAsynchronousLeaf()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var physical = Plan("SELECT ID, LABEL FROM SALES WHERE ID > 3 ORDER BY ID", rootSchema);
            var parameters = new java.util.HashMap();

            var (synchronous, asynchronous, bridges) = Operators(Implement(physical, false, parameters));

            bridges.Should().ContainSingle("the leaf awaits and the plan does not, so its rows are read across once");
            bridges[0].Method.Name.Should().Be("ToEnumerable");
            synchronous.Should().NotBeEmpty("everything above the leaf is the synchronous plan's own");
            asynchronous.Should().BeEmpty("the leaf is the table's own sequence, not an operator of ours");

            var awaiting = Operators(Implement(physical, true, new java.util.HashMap()));

            awaiting.Bridges.Should().BeEmpty("the leaf already awaits, so an asynchronous plan crosses nothing");
        }

        /// <summary>
        /// A table Calcite reads crosses once in an asynchronous plan, and that crossing never suspends.
        /// </summary>
        [TestMethod]
        public void ShouldCrossOnceAtASynchronousOnlyLeaf()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SORTED", new SyncOnlyRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType));

            var physical = Plan("SELECT K, V FROM SORTED WHERE K >= 2", rootSchema);

            var (_, _, bridges) = Operators(Implement(physical, true, new java.util.HashMap()));

            bridges.Should().ContainSingle("the leaf is pulled and the plan awaits, so its rows are read across once");
            bridges[0].Method.Name.Should().Be("ToAsyncEnumerable");
        }

        /// <summary>
        /// A table of this convention's own synchronous SPI, which has no asynchronous half to fall back to.
        /// </summary>
        sealed class SyncOnlyRowsTable(object?[][] rows, Func<org.apache.calcite.rel.type.RelDataTypeFactory, org.apache.calcite.rel.type.RelDataType> rowType) : org.apache.calcite.schema.impl.AbstractTable, Apache.Calcite.Extensions.Schema.IClrScannableTable
        {

            /// <inheritdoc />
            public override org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) => rowType(typeFactory);

            /// <inheritdoc />
            public IEnumerable<object?[]> Scan(DataContext root) => rows;

        }

        static string Render(object? row)
        {
            return row is object?[] array
                ? string.Join("|", array.Select(v => v?.ToString() ?? "NULL"))
                : row?.ToString() ?? "NULL";
        }

    }

}
