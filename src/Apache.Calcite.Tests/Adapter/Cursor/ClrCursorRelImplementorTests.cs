using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;
using Apache.Calcite.Extensions.Schema;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests the two open expressions <c>ClrCursorRelImplementor</c> builds for a plan: what each is built
    /// from, and that opening is where the plan runs.
    /// </summary>
    /// <remarks>
    /// The differential suite compares rows, which do not show which opens ran or when. These tests inspect the
    /// trees: outside a deferred opener the synchronous open calls only synchronous opens, the awaiting open
    /// calls only awaiting opens and passes each the enclosing token, and a deferred opener of either kind
    /// holds opens of that kind. They also count what a table is asked for at <c>Open</c>, at
    /// <c>OpenAsync</c>, at each advance, and at disposal.
    /// </remarks>
    public class ClrCursorRelImplementorTests
    {

        static ClrCursorRelImplementorTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        /// <param name="parameters">The map the implementor stashed values into, which <c>get</c> answers from.</param>
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
        /// A table of this project's SPI that counts what the plan asks of it.
        /// </summary>
        /// <param name="rows">The table's rows.</param>
        /// <param name="rowType">Builds the table's row type from the type factory it is given.</param>
        /// <remarks>
        /// Both <c>Scan</c> and <c>ScanAsync</c> are implemented, so that neither reaches the other through the
        /// interface default. Acquisition is counted in <c>GetEnumerator</c> and <c>GetAsyncEnumerator</c>
        /// rather than in an iterator body, which would defer the count to the first advance; disposal is
        /// counted on a wrapping enumerator, because an iterator disposed before it moved runs no
        /// <c>finally</c> block.
        /// </remarks>
        sealed class CountingTable(object?[][] rows, Func<RelDataTypeFactory, RelDataType> rowType) : AbstractTable, IClrScannableTable
        {

            readonly object?[][] source = rows;

            public int Scans { get; private set; }

            public int AsyncScans { get; private set; }

            public int Acquired { get; private set; }

            public int RowsProduced { get; private set; }

            public int Disposed { get; private set; }

            public bool SawCancellableToken { get; private set; }

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => rowType(typeFactory);

            /// <inheritdoc />
            public IEnumerable<object?[]> Scan(DataContext root)
            {
                Scans++;
                return new Rows(this);
            }

            /// <inheritdoc />
            public IAsyncEnumerable<object?[]> ScanAsync(DataContext root)
            {
                AsyncScans++;
                return new Rows(this);
            }

            /// <summary>
            /// The table's rows, counting each acquisition when the enumerator is obtained.
            /// </summary>
            /// <param name="table">The table whose rows these are and whose counters are incremented.</param>
            sealed class Rows(CountingTable table) : IEnumerable<object?[]>, IAsyncEnumerable<object?[]>
            {

                public IEnumerator<object?[]> GetEnumerator()
                {
                    table.Acquired++;
                    return new Disposing(table, Pulled());
                }

                System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

                public IAsyncEnumerator<object?[]> GetAsyncEnumerator(CancellationToken cancellationToken = default)
                {
                    table.Acquired++;
                    table.SawCancellableToken = cancellationToken.CanBeCanceled;
                    return new DisposingAsync(table, Awaited());
                }

                IEnumerator<object?[]> Pulled()
                {
                    foreach (var row in table.source)
                    {
                        table.RowsProduced++;
                        yield return row;
                    }
                }

                async IAsyncEnumerator<object?[]> Awaited()
                {
                    foreach (var row in table.source)
                    {
                        await Task.Yield();
                        table.RowsProduced++;
                        yield return row;
                    }
                }

                /// <summary>
                /// Wraps an enumerator and counts its disposal, because an iterator disposed before it moved
                /// runs no <c>finally</c> block.
                /// </summary>
                /// <param name="table">The table whose disposal counter is incremented.</param>
                /// <param name="rows">The enumerator to wrap.</param>
                sealed class Disposing(CountingTable table, IEnumerator<object?[]> rows) : IEnumerator<object?[]>
                {

                    public object?[] Current => rows.Current;

                    object? System.Collections.IEnumerator.Current => Current;

                    public bool MoveNext() => rows.MoveNext();

                    public void Reset() => rows.Reset();

                    public void Dispose()
                    {
                        table.Disposed++;
                        rows.Dispose();
                    }

                }

                /// <summary>
                /// <see cref="Disposing"/> for the awaiting half.
                /// </summary>
                /// <param name="table">The table whose disposal counter is incremented.</param>
                /// <param name="rows">The enumerator to wrap.</param>
                sealed class DisposingAsync(CountingTable table, IAsyncEnumerator<object?[]> rows) : IAsyncEnumerator<object?[]>
                {

                    public object?[] Current => rows.Current;

                    public ValueTask<bool> MoveNextAsync() => rows.MoveNextAsync();

                    public ValueTask DisposeAsync()
                    {
                        table.Disposed++;
                        return rows.DisposeAsync();
                    }

                }

            }

        }

        /// <summary>
        /// Plans a statement into the convention.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="rootSchema">The schema to plan against.</param>
        /// <returns>The physical root, requested in this convention.</returns>
        static RelNode Plan(string sql, SchemaPlus rootSchema)
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
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
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrCursorConvention.Instance).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Implements a planned root.
        /// </summary>
        /// <param name="physical">A root planned in this convention.</param>
        /// <param name="parameters">The map the implementor stashes values into.</param>
        /// <returns>The factory holding both opens, preferring array rows.</returns>
        static ClrCursorFactory Implement(RelNode physical, java.util.Map parameters)
        {
            var implementor = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            return implementor.ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);
        }

        /// <summary>
        /// Whether a method is an open: one of the operators, the interop that opens a cursor over a linq4j
        /// sequence, or a bridge between the two kinds of open.
        /// </summary>
        /// <param name="method">The method a call in the plan's tree invokes.</param>
        /// <returns>True if the method is declared on <c>ClrCursorDefaults</c>, <c>JavaCursors</c> or <c>ClrCursors</c>.</returns>
        static bool IsOpen(System.Reflection.MethodInfo method)
        {
            var declaring = method.DeclaringType?.Name;

            return declaring is "ClrCursorDefaults" or "JavaCursors" or "ClrCursors";
        }

        /// <summary>
        /// Whether an open awaits: it returns a <see cref="ValueTask{TResult}"/>.
        /// </summary>
        /// <param name="method">An open.</param>
        /// <returns>True if the method returns a <see cref="ValueTask{TResult}"/>.</returns>
        static bool Awaits(System.Reflection.MethodInfo method)
        {
            return method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(ValueTask<>);
        }

        /// <summary>
        /// Records in <c>wrong</c> every open in a body that does not match the body's kind, descending into
        /// each deferred opener with the kind its return type gives and into no other lambda.
        /// </summary>
        /// <param name="sql">The statement the plan came from, named in each record.</param>
        /// <param name="wrong">The list each mismatch is added to.</param>
        /// <remarks>
        /// Selectors and predicates are lambdas over a row and call only translated Rex; the only lambdas that
        /// hold opens are deferred openers, recognised by returning a cursor or a <c>ValueTask</c>. An awaiting
        /// open must be passed the token parameter of the nearest enclosing awaiting lambda, which is the root's
        /// or a deferred opener's.
        /// </remarks>
        sealed class OpenChecker(string sql, List<string> wrong) : ExpressionVisitor
        {

            readonly Stack<(bool Awaiting, ParameterExpression? Token)> scopes = new();

            public void Check(LambdaExpression lambda, bool awaiting)
            {
                scopes.Push((awaiting, awaiting ? lambda.Parameters.Single(p => p.Type == typeof(CancellationToken)) : null));
                Visit(lambda.Body);
                scopes.Pop();
            }

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                var returns = node.ReturnType;

                if (returns.IsGenericType && returns.GetGenericTypeDefinition() == typeof(IClrCursor<>))
                {
                    Check(node, false);
                    return node;
                }

                if (returns.IsGenericType && returns.GetGenericTypeDefinition() == typeof(ValueTask<>))
                {
                    Check(node, true);
                    return node;
                }

                // a row lambda: nothing in it opens anything
                return node;
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (IsOpen(node.Method))
                {
                    var (awaiting, token) = scopes.Peek();

                    if (node.Method.DeclaringType?.Name == "ClrCursors")
                        wrong.Add($"'{sql}' bridges with {node.Method.Name}, and nothing in this schema forces a crossing");
                    else if (Awaits(node.Method) != awaiting)
                        wrong.Add($"'{sql}' calls {node.Method.Name} from a body that {(awaiting ? "awaits" : "does not await")}");
                    else if (awaiting && node.Method.Name.EndsWith("Async", StringComparison.Ordinal) == false)
                        wrong.Add($"'{sql}' calls {node.Method.Name}, an awaiting open without the suffix");
                    else if (awaiting == false && node.Method.Name.EndsWith("Async", StringComparison.Ordinal))
                        wrong.Add($"'{sql}' calls {node.Method.Name}, a synchronous open with the suffix");
                    else if (awaiting && ReferenceEquals(node.Arguments[^1], token) == false)
                        wrong.Add($"'{sql}' calls {node.Method.Name} with something other than the enclosing token");
                }

                return base.VisitMethodCall(node);
            }

        }

        /// <summary>
        /// The queries whose opens are inspected, together covering the convention's nodes.
        /// </summary>
        static readonly string[] Queries =
        [
            "SELECT * FROM SALES",
            "SELECT ID FROM SALES",
            "SELECT * FROM SALES WHERE AMOUNT > 10",
            "SELECT * FROM SALES ORDER BY AMOUNT",
            "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION",
            "SELECT SUM(AMOUNT) FROM SALES",
            "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY ROLLUP(REGION)",
            "SELECT DISTINCT REGION FROM SALES",
            "SELECT * FROM SALES ORDER BY AMOUNT LIMIT 2 OFFSET 1",
            "SELECT ID FROM SALES UNION SELECT ID FROM SALES",
            "SELECT ID FROM SALES UNION ALL SELECT ID FROM SALES",
            "SELECT ID FROM SALES UNION ALL SELECT K FROM SORTED UNION ALL SELECT ID FROM SALES",
            // the equality is a merge join over two sorts; IS NOT DISTINCT FROM rules out the merge join and
            // gives a hash join; the inequality alone is a nested loop; and IN is a semi hash join, whose right
            // side is a deferred opener of each kind
            "SELECT a.ID, b.LABEL FROM SALES a JOIN SALES b ON a.ID = b.ID",
            "SELECT a.ID, b.LABEL FROM SALES a LEFT JOIN SALES b ON a.ID = b.ID AND a.AMOUNT < b.AMOUNT",
            "SELECT a.ID, b.LABEL FROM SALES a JOIN SALES b ON a.AMOUNT IS NOT DISTINCT FROM b.AMOUNT",
            "SELECT a.ID, b.LABEL FROM SALES a LEFT JOIN SALES b ON a.AMOUNT IS NOT DISTINCT FROM b.AMOUNT AND a.ID < b.ID",
            "SELECT a.ID, b.LABEL FROM SALES a JOIN SALES b ON a.ID < b.ID",
            "SELECT ID FROM SALES WHERE ID IN (SELECT K FROM SORTED)",
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)",
            "SELECT a.ID, b.V FROM SALES a ASOF JOIN SORTED b MATCH_CONDITION b.K <= a.ID ON a.LABEL = b.V",
            // a correlate: the inner is a deferred opener of each kind over the outer row
            "SELECT t.x, u.y FROM (VALUES (1, ARRAY[10, 20]), (2, ARRAY[30])) AS t(x, xs), UNNEST(t.xs) AS u(y)",
            // the repeat union and the spool, with the iterative part deferred as an opener of each kind
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t",
        ];

        /// <summary>
        /// A schema holding a table of Calcite's SPI and one of this project's, so that plans have leaves of
        /// both kinds.
        /// </summary>
        /// <returns>A new root schema holding <c>SALES</c> of Calcite's SPI and <c>SORTED</c> of this project's.</returns>
        static SchemaPlus Schema()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SORTED", new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType));

            return rootSchema;
        }

        /// <summary>
        /// Outside a deferred opener, the synchronous open calls only synchronous opens.
        /// </summary>
        [Fact]
        public void ShouldBuildTheSynchronousOpenFromSynchronousOpens()
        {
            var wrong = new List<string>();

            foreach (var sql in Queries)
            {
                var factory = Implement(Plan(sql, Schema()), new java.util.HashMap());

                factory.OpenExpression.ReturnType.Should().Be(typeof(IClrCursor));
                new OpenChecker(sql, wrong).Check(factory.OpenExpression, false);
            }

            wrong.Should().BeEmpty();
        }

        /// <summary>
        /// Outside a deferred opener, the awaiting open calls only awaiting opens, each passed the enclosing
        /// token.
        /// </summary>
        [Fact]
        public void ShouldBuildTheAwaitingOpenFromAwaitingOpens()
        {
            var wrong = new List<string>();

            foreach (var sql in Queries)
            {
                var factory = Implement(Plan(sql, Schema()), new java.util.HashMap());

                factory.OpenAsyncExpression.ReturnType.Should().Be(typeof(ValueTask<IClrCursor>));

                // the root ends in ClrCursors.Untyped, which changes the type parameter; it is declared with the
                // bridges but is allowed here, so the check starts below it
                var body = (MethodCallExpression)factory.OpenAsyncExpression.Body;
                body.Method.Name.Should().Be(nameof(ClrCursors.Untyped));

                var lambda = Expression.Lambda(body.Arguments[0], factory.OpenAsyncExpression.Parameters);
                new OpenChecker(sql, wrong).Check(lambda, true);
            }

            wrong.Should().BeEmpty();
        }

        /// <summary>
        /// <c>Open</c> runs the plan: the scan is acquired and the sort drains it there, before any advance.
        /// </summary>
        [Fact]
        public void ShouldRunThePlanAtOpen()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT K, V FROM SORTED ORDER BY V DESC", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            using var cursor = factory.Open(context);

            table.Scans.Should().Be(1, "the synchronous open asks for the synchronous half");
            table.AsyncScans.Should().Be(0);
            table.Acquired.Should().Be(1, "the scan's enumerator is acquired at the open");
            table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length, "a sort drains its input at the open");
            table.Disposed.Should().Be(1, "and closes it once drained");

            cursor.Read().Should().BeTrue();
        }

        /// <summary>
        /// <c>OpenAsync</c> awaits the plan's acquisition, including the sort's drain, before it returns the
        /// cursor.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldRunThePlanAtOpenAsync()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT K, V FROM SORTED ORDER BY V DESC", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            await using var cursor = await factory.OpenAsync(context, CancellationToken.None);

            table.AsyncScans.Should().Be(1, "the awaiting open asks for the awaiting half");
            table.Scans.Should().Be(0);
            table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length, "the drain is awaited inside the open, which an IAsyncEnumerable could not do");
            table.Disposed.Should().Be(1);

            (await cursor.ReadAsync(CancellationToken.None)).Should().BeTrue();
        }

        /// <summary>
        /// A grouped aggregate folds its whole input at the open, whichever open it is, and closes it there.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// Calcite's <c>groupBy_</c> drains its input into a map when it is called and returns a
        /// <c>LookupResultEnumerable</c> over the finished map; here the call is the open.
        /// </remarks>
        [Fact]
        public async Task ShouldFoldAGroupedAggregateAtOpen()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT K, COUNT(*) FROM SORTED GROUP BY K", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            using (var cursor = factory.Open(context))
            {
                table.Scans.Should().Be(1);
                table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length, "the fold runs at the open");
                table.Disposed.Should().Be(1, "and the input is closed once folded");

                var groups = 0;
                while (cursor.Read())
                    groups++;

                groups.Should().Be(3);
            }

            await using (var cursor = await factory.OpenAsync(context, CancellationToken.None))
            {
                table.AsyncScans.Should().Be(1);
                table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length * 2, "the awaiting open awaits the fold");
                table.Disposed.Should().Be(2);

                var groups = 0;
                while (await cursor.ReadAsync(CancellationToken.None))
                    groups++;

                groups.Should().Be(3);
            }
        }

        /// <summary>
        /// A global aggregate folds its input at the open and returns one row, whichever open is used.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldFoldAGlobalAggregateAtOpen()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT COUNT(*) FROM SORTED", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            using (var cursor = factory.Open(context))
            {
                table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length, "Aggregate folds where it is called, which is the open");
                table.Disposed.Should().Be(1);

                cursor.Read().Should().BeTrue();
                cursor.Current.Should().Be(java.lang.Long.valueOf(AsyncTestRows.Sorted.Length));
                cursor.Read().Should().BeFalse();
            }

            await using (var cursor = await factory.OpenAsync(context, CancellationToken.None))
            {
                table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length * 2, "SingletonAggregateAsync folds inside the open, once");
                table.Disposed.Should().Be(2);

                (await cursor.ReadAsync(CancellationToken.None)).Should().BeTrue();
                cursor.Current.Should().Be(java.lang.Long.valueOf(AsyncTestRows.Sorted.Length));
                (await cursor.ReadAsync(CancellationToken.None)).Should().BeFalse();
            }
        }

        /// <summary>
        /// A scan under a calc is acquired at the open and read one row at a time as the cursor advances, by
        /// either advance.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldReadOneRowPerAdvanceOfEitherKind()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT K, V FROM SORTED WHERE K >= 2", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            await using var cursor = factory.Open(context);

            table.Acquired.Should().Be(1);
            table.RowsProduced.Should().Be(0, "a calc reads nothing at the open");

            cursor.Read().Should().BeTrue();
            table.RowsProduced.Should().Be(2, "the first row failed the condition and the second passed");
            cursor.Current.Should().BeOfType<object[]>().Which[0].Should().Be(java.lang.Integer.valueOf(2));

            (await cursor.ReadAsync(CancellationToken.None)).Should().BeTrue("the other advance continues from the same position");
            table.RowsProduced.Should().Be(3);
            cursor.Current.Should().BeOfType<object[]>().Which[1].Should().Be("C");

            cursor.Read().Should().BeTrue();
            (await cursor.ReadAsync(CancellationToken.None)).Should().BeFalse();
            table.RowsProduced.Should().Be(AsyncTestRows.Sorted.Length);
        }

        /// <summary>
        /// A concat acquires nothing at its open, and acquires each source when an advance reaches it, using
        /// the open of that advance's kind and that advance's token.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldDeferEachConcatSourceToTheAdvanceThatReachesIt()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var first = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            var second = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("A", first);
            rootSchema.add("B", second);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT K FROM A UNION ALL SELECT K FROM B", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            await using var cursor = factory.Open(context);

            first.Scans.Should().Be(0, "Linq4j.CompositeEnumerable's enumerator() acquires nothing");
            second.Scans.Should().Be(0);

            cursor.Read().Should().BeTrue();
            first.Scans.Should().Be(1, "the first source is acquired by the advance that reached it, synchronously");
            first.AsyncScans.Should().Be(0);
            second.Scans.Should().Be(0);

            for (var i = 1; i < AsyncTestRows.Sorted.Length; i++)
                cursor.Read().Should().BeTrue();

            second.Scans.Should().Be(0, "the second is not touched until the first is exhausted");

            using var cancellation = new CancellationTokenSource();
            (await cursor.ReadAsync(cancellation.Token)).Should().BeTrue();
            first.Disposed.Should().Be(1, "the exhausted source is closed before the next is opened");
            second.AsyncScans.Should().Be(1, "the second source is acquired by the advance that reached it, which awaited");
            second.Scans.Should().Be(0);
            second.SawCancellableToken.Should().BeTrue("with that advance's token, not the open's");
        }

        /// <summary>
        /// Disposing a cursor that was never advanced still closes what the open acquired.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldDisposeAnUnreadCursorDownToTheLeaf()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT K FROM SORTED WHERE K > 1 OFFSET 1 ROWS", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            var cursor = factory.Open(context);
            table.Acquired.Should().Be(1);
            cursor.Dispose();
            table.Disposed.Should().Be(1, "linq4j's close() reaches the source whether or not a row was read");

            var awaited = await factory.OpenAsync(context, CancellationToken.None);
            table.Acquired.Should().Be(2);
            await awaited.DisposeAsync();
            table.Disposed.Should().Be(2);
        }

        /// <summary>
        /// One planned root gives one factory, and the two opens read the same rows.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldOpenOnePlannedRootBothWays()
        {
            var rootSchema = Schema();
            var parameters = new java.util.HashMap();
            var factory = Implement(Plan("SELECT ID FROM SALES UNION ALL SELECT K FROM SORTED ORDER BY 1", rootSchema), parameters);
            var context = new TestDataContext(rootSchema, parameters);

            var read = new List<string>();
            using (var cursor = factory.Open(context))
                while (cursor.Read())
                    read.Add(cursor.Current?.ToString() ?? "<null>");

            var awaited = new List<string>();
            await using (var cursor = await factory.OpenAsync(context, CancellationToken.None))
                while (await cursor.ReadAsync(CancellationToken.None))
                    awaited.Add(cursor.Current?.ToString() ?? "<null>");

            awaited.Should().Equal(read);
            read.Should().Equal(["1", "1", "2", "2", "2", "3", "4", "4", "5", "6"]);
        }

        /// <summary>
        /// A result crossed to the other kind opens the same rows: <c>Awaited</c> wraps the synchronous open in
        /// a completed <c>ValueTask</c>, and <c>Pulled</c> blocks on the awaiting open.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// Every node implements both bodies and so crosses nothing, so the crossings are applied here directly
        /// to a plan's results and compiled into the lambdas the root would build.
        /// </remarks>
        [Fact]
        public async Task ShouldCrossAResultToTheOtherKind()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var physical = (ClrCursorRel)Plan("SELECT K, V FROM SORTED ORDER BY V DESC", rootSchema);
            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var implementor = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            // the synchronous result read as an awaiting one: a completed open over the same cursor
            var awaited = implementor.Awaited(implementor.VisitChild(null, 0, physical, ClrCursorPrefer.Array));
            awaited.Expression.Type.Should().Be(typeof(ValueTask<IClrCursor<object[]>>));
            ((MethodCallExpression)awaited.Expression).Method.Name.Should().Be(nameof(ClrCursors.Completed));

            var openAwaited = Expression.Lambda<Func<DataContext, CancellationToken, ValueTask<IClrCursor<object[]>>>>(
                awaited.Expression, implementor.Root, implementor.CancellationToken).Compile();

            var rows = new List<string>();
            await using (var cursor = await openAwaited(context, CancellationToken.None))
                while (await cursor.ReadAsync(CancellationToken.None))
                    rows.Add((string)cursor.Current[1]!);

            rows.Should().Equal(["D", "C", "B", "A"]);
            table.Scans.Should().Be(1, "the open underneath was the synchronous one");

            // the awaiting result read as a synchronous one: the open is blocked on
            var pulled = implementor.Pulled(implementor.VisitChildAsync(null, 0, physical, ClrCursorPrefer.Array));
            pulled.Expression.Type.Should().Be(typeof(IClrCursor<object[]>));
            ((MethodCallExpression)pulled.Expression).Method.Name.Should().Be(nameof(ClrCursors.Block));

            var openPulled = Expression.Lambda<Func<DataContext, IClrCursor<object[]>>>(pulled.Expression, implementor.Root).Compile();

            rows.Clear();
            using (var cursor = openPulled(context))
                while (cursor.Read())
                    rows.Add((string)cursor.Current[1]!);

            rows.Should().Equal(["D", "C", "B", "A"]);
            table.AsyncScans.Should().Be(1, "the open underneath was the awaiting one, and the drain was waited for");
        }

        /// <summary>
        /// The factory's element type is the physical row type.
        /// </summary>
        [Fact]
        public void ShouldNameTheRowType()
        {
            var rootSchema = Schema();

            Implement(Plan("SELECT * FROM SALES", rootSchema), new java.util.HashMap()).ElementType.Should().Be(typeof(object[]));
            // int rather than java.lang.Integer: the type factory's answer for a one-column NOT NULL result is
            // the primitive, as Typed.getElementType answers int.class, while the cursor itself carries the
            // boxed value
            Implement(Plan("SELECT ID FROM SALES", rootSchema), new java.util.HashMap()).ElementType.Should().Be(typeof(int));
        }

    }

}
