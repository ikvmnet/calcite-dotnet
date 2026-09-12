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
            var implementor = new ClrEnumerableRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            return async
                ? implementor.ImplementRootAsync((ClrEnumerableRel)physical, ClrEnumerablePrefer.Array)
                : implementor.ImplementRoot((ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
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
        /// Each fork calls the table SPI member of its own kind, and the plan carries no crossing at a leaf.
        /// </summary>
        /// <remarks>
        /// <b>This used to assert the opposite, and the change is the point.</b> While the awaiting half of
        /// the table SPI was a second interface, the scan knew which half a table had and wrote the read
        /// across into the plan, so a bridging call appeared in the compiled tree and could be counted
        /// there. There is one interface now, carrying both halves, so the scan asks for the half that
        /// matches its fork and the table answers: either with a real implementation, or with the interface
        /// default, which does the read across inside the table.
        ///
        /// <para>What that costs is this test. The crossing is no longer visible in the plan, so nothing
        /// here can count it, and the claim that a plan crosses <em>once</em> and <em>at the leaf</em> is
        /// not a claim the tree can answer any more. What is still checkable is that each fork calls its own
        /// member and that everything above the leaf belongs to that fork, which is what this now holds.
        /// <c>ShouldReadTheSameRowsThroughEitherHalfOfTheTableSpi</c> holds the part that moved.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldCallTheTableSpiMemberOfItsOwnFork()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var physical = Plan("SELECT ID, LABEL FROM SALES WHERE ID > 3 ORDER BY ID", rootSchema);

            var pulled = Operators(Implement(physical, false, new java.util.HashMap()));

            Scans(Implement(physical, false, new java.util.HashMap())).Should().Equal(["Scan"],
                "the pulled fork asks the table for its pulled half, and this table answers it by blocking");
            pulled.Synchronous.Should().NotBeEmpty("everything above the leaf is the pulled plan's own");
            pulled.Asynchronous.Should().BeEmpty("no awaiting operator belongs in a pulled plan");
            pulled.Bridges.Should().BeEmpty("the read across is the table's now, not the plan's");

            var awaiting = Operators(Implement(physical, true, new java.util.HashMap()));

            Scans(Implement(physical, true, new java.util.HashMap())).Should().Equal(["ScanAsync"],
                "the awaiting fork asks for the awaiting half, which this table really implements");
            awaiting.Asynchronous.Should().NotBeEmpty("everything above the leaf is the awaiting plan's own");
            awaiting.Synchronous.Should().BeEmpty("no pulled operator belongs in an awaiting plan");
            awaiting.Bridges.Should().BeEmpty("nothing crosses: the table awaits and so does the plan");
        }

        /// <summary>
        /// A table with only a pulled half is still asked for the awaiting one, and the plan is otherwise
        /// unchanged.
        /// </summary>
        /// <remarks>
        /// The mirror, over a table that writes <c>Scan</c> and takes the default <c>ScanAsync</c>. The plan
        /// looks the same as for a table that implements both: one call to the awaiting member, and the cost
        /// of the table having no awaiting rows to give is inside the table.
        /// </remarks>
        [TestMethod]
        public void ShouldCallTheAwaitingMemberOfAPulledOnlyTable()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SORTED", new SyncOnlyRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType));

            var physical = Plan("SELECT K, V FROM SORTED WHERE K >= 2", rootSchema);

            Scans(Implement(physical, true, new java.util.HashMap())).Should().Equal(["ScanAsync"],
                "the awaiting fork asks for the awaiting half whether or not the table wrote one");
            Scans(Implement(physical, false, new java.util.HashMap())).Should().Equal(["Scan"],
                "and the pulled fork asks for the pulled one");

            Operators(Implement(physical, true, new java.util.HashMap())).Bridges.Should().BeEmpty(
                "the default ScanAsync reads across inside the table, so the plan holds no bridge");
        }

        /// <summary>
        /// Both halves of the table SPI answer the same rows, whichever half the table actually wrote.
        /// </summary>
        /// <remarks>
        /// What the two crossing tests used to hold, moved to where the crossing now lives. A table with
        /// only a pulled half is read through its defaulted awaiting one, and a table with only an awaiting
        /// half through the pulled one it wrote over it; both have to answer the rows the other does.
        /// </remarks>
        [TestMethod]
        public async Task ShouldReadTheSameRowsThroughEitherHalfOfTheTableSpi()
        {
            var context = new TestDataContext(Frameworks.createRootSchema(true), new java.util.HashMap());

            var pulledOnly = new SyncOnlyRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            var awaitingOnly = new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true);

            foreach (var table in new Apache.Calcite.Extensions.Schema.IClrScannableTable[] { pulledOnly, awaitingOnly })
            {
                var pulled = table.Scan(context).Select(Render).ToList();

                var awaited = new List<string>();
                await foreach (var row in table.ScanAsync(context))
                    awaited.Add(Render(row));

                awaited.Should().Equal(pulled, "both halves of {0} read the same table", table.GetType().Name);
                pulled.Should().NotBeEmpty("the table has rows, or the comparison holds nothing");
            }
        }

        /// <summary>
        /// The table SPI members a plan calls, in the order the tree holds them.
        /// </summary>
        static List<string> Scans(LambdaExpression tree)
        {
            var calls = new List<MethodCallExpression>();
            new Collector(calls).Visit(tree);

            return calls
                .Where(c => c.Method.DeclaringType == typeof(Apache.Calcite.Extensions.Schema.IClrScannableTable))
                .Select(c => c.Method.Name)
                .ToList();
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
