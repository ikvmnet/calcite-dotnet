using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
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

namespace Apache.Calcite.Extensions.Adapter.DataCursor.Tests
{

    /// <summary>
    /// Reads the two trees the implementor builds, and requires that each is made of what it is made of;
    /// then opens them, and requires that opening is where the plan runs.
    /// </summary>
    /// <remarks>
    /// The differential suite compares rows, which says nothing about which opens ran or when. So these
    /// tests read the compiled trees: the synchronous open names only synchronous opens outside a deferred
    /// opener, the awaiting open names only awaiting opens and passes each the root's token, and a deferred
    /// opener of either kind holds opens of that kind. And they watch a table: what a scan is asked for at
    /// <c>Open</c>, at <c>OpenAsync</c>, at each advance, and at disposal.
    /// </remarks>
    public class ClrDataCursorRelImplementorTests
    {

        static ClrDataCursorRelImplementorTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
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
        /// A table of this project's SPI that counts what the plan asks of it.
        /// </summary>
        /// <remarks>
        /// Both halves are written, so that a scan of either kind is the table's own and not the interface
        /// default read across. The sequence counts its acquisition where acquisition happens — at
        /// <c>GetEnumerator</c> and <c>GetAsyncEnumerator</c>, not in an iterator body, which would defer
        /// the count to the first advance and make opening look like reading — and counts its disposal on
        /// the enumerator itself, because an iterator disposed before it ever moved runs no finally block.
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
            /// The table's rows, counting acquisition at the moment it happens.
            /// </summary>
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
                /// Counts the disposal itself, because an iterator disposed before it ever moved runs no
                /// finally block and would count nothing.
                /// </summary>
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
        static RelNode Plan(string sql, SchemaPlus rootSchema)
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrDataCursorRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrDataCursorRules.CalcRules())
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
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrDataCursorConvention.Instance).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Implements a planned root.
        /// </summary>
        static ClrDataCursorFactory Implement(RelNode physical, java.util.Map parameters)
        {
            var implementor = new ClrDataCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            return implementor.ImplementRoot((ClrDataCursorRel)physical, ClrEnumerablePrefer.Array);
        }

        /// <summary>
        /// Whether a method is an open: one of the operators, the interop that opens over a linq4j sequence,
        /// or a bridge between the two kinds.
        /// </summary>
        static bool IsOpen(System.Reflection.MethodInfo method)
        {
            var declaring = method.DeclaringType?.Name;

            return declaring is "ClrDataCursorDefaults" or "JavaCursors" or "ClrDataCursors";
        }

        /// <summary>
        /// Whether an open awaits: it returns a <see cref="ValueTask{TResult}"/>.
        /// </summary>
        static bool Awaits(System.Reflection.MethodInfo method)
        {
            return method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(ValueTask<>);
        }

        /// <summary>
        /// Checks every open in a body against the kind the body is, descending into a deferred opener with
        /// the kind its delegate type says and into nothing else.
        /// </summary>
        /// <remarks>
        /// A selector or a predicate is a lambda too, but over a row, and what it calls is Rex; the only
        /// lambdas that hold opens are the deferred openers, told apart by what they return. The token an
        /// awaiting open is passed has to be the parameter of the nearest enclosing awaiting lambda, which is
        /// the root's or a deferred opener's.
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

                if (returns.IsGenericType && returns.GetGenericTypeDefinition() == typeof(ClrDataCursor<>))
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

                    if (node.Method.DeclaringType?.Name == "ClrDataCursors")
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
        /// The queries read, one per node the convention has.
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
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)",
            // the repeat union and the spool, with the transient scan Calcite's under the converter in and
            // the iterative part deferred as an opener of each kind
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t",
        ];

        /// <summary>
        /// A schema of tables Calcite itself can read, and of this project's SPI, so both leaves appear.
        /// </summary>
        static SchemaPlus Schema()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SORTED", new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType));

            return rootSchema;
        }

        /// <summary>
        /// The synchronous open names synchronous opens and nothing else, outside a deferred opener.
        /// </summary>
        [Fact]
        public void ShouldBuildTheSynchronousOpenFromSynchronousOpens()
        {
            var wrong = new List<string>();

            foreach (var sql in Queries)
            {
                var factory = Implement(Plan(sql, Schema()), new java.util.HashMap());

                factory.OpenExpression.ReturnType.Should().Be(typeof(ClrDataCursor));
                new OpenChecker(sql, wrong).Check(factory.OpenExpression, false);
            }

            wrong.Should().BeEmpty();
        }

        /// <summary>
        /// The awaiting open names awaiting opens and nothing else, each passed the enclosing token.
        /// </summary>
        [Fact]
        public void ShouldBuildTheAwaitingOpenFromAwaitingOpens()
        {
            var wrong = new List<string>();

            foreach (var sql in Queries)
            {
                var factory = Implement(Plan(sql, Schema()), new java.util.HashMap());

                factory.OpenAsyncExpression.ReturnType.Should().Be(typeof(ValueTask<ClrDataCursor>));

                // the root ends in the one continuation that changes the type parameter, which is a bridge
                // by declaring type and the one allowed at the root
                var body = (MethodCallExpression)factory.OpenAsyncExpression.Body;
                body.Method.Name.Should().Be(nameof(ClrDataCursors.Untyped));

                var lambda = Expression.Lambda(body.Arguments[0], factory.OpenAsyncExpression.Parameters);
                new OpenChecker(sql, wrong).Check(lambda, true);
            }

            wrong.Should().BeEmpty();
        }

        /// <summary>
        /// Opening runs the plan, and reading reads rows: a scan is acquired at <c>Open</c> and a sort is
        /// drained there, before any advance.
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
        /// The awaiting open awaits the plan's acquisition, drain included, before it hands back the cursor.
        /// </summary>
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
        /// <remarks>
        /// <c>groupBy_</c> drains the input into the map where it is called and returns a
        /// <c>LookupResultEnumerable</c> over a finished map; the call is the open.
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
        /// A global aggregate folds at the open and hands back one row, whichever open it is.
        /// </summary>
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
        /// A scan under a calc is acquired at the open and read per advance, whichever advance it is.
        /// </summary>
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
        /// A concat acquires nothing at its open and each source at its turn, by the open of the advance that
        /// reached it.
        /// </summary>
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
        /// A result crossed to the other kind opens the same cursor: <c>Awaited</c> completes at once, and
        /// <c>Pulled</c> blocks for the open.
        /// </summary>
        /// <remarks>
        /// No node written so far crosses — each writes both bodies — so the two crossings are held here
        /// directly, on a scan's results, compiled into the lambdas the root would have wrapped them in.
        /// </remarks>
        [Fact]
        public async Task ShouldCrossAResultToTheOtherKind()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var table = new CountingTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType);
            rootSchema.add("SORTED", table);

            var physical = (ClrDataCursorRel)Plan("SELECT K, V FROM SORTED ORDER BY V DESC", rootSchema);
            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var implementor = new ClrDataCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            // the synchronous result read as an awaiting one: a completed open over the same cursor
            var awaited = implementor.Awaited(implementor.VisitChild(null, 0, physical, ClrEnumerablePrefer.Array));
            awaited.Expression.Type.Should().Be(typeof(ValueTask<ClrDataCursor<object[]>>));
            ((MethodCallExpression)awaited.Expression).Method.Name.Should().Be(nameof(ClrDataCursors.Completed));

            var openAwaited = Expression.Lambda<Func<DataContext, CancellationToken, ValueTask<ClrDataCursor<object[]>>>>(
                awaited.Expression, implementor.Root, implementor.CancellationToken).Compile();

            var rows = new List<string>();
            await using (var cursor = await openAwaited(context, CancellationToken.None))
                while (await cursor.ReadAsync(CancellationToken.None))
                    rows.Add((string)cursor.Current[1]!);

            rows.Should().Equal(["D", "C", "B", "A"]);
            table.Scans.Should().Be(1, "the open underneath was the synchronous one");

            // the awaiting result read as a synchronous one: the open is blocked for
            var pulled = implementor.Pulled(implementor.VisitChildAsync(null, 0, physical, ClrEnumerablePrefer.Array));
            pulled.Expression.Type.Should().Be(typeof(ClrDataCursor<object[]>));
            ((MethodCallExpression)pulled.Expression).Method.Name.Should().Be(nameof(ClrDataCursors.Block));

            var openPulled = Expression.Lambda<Func<DataContext, ClrDataCursor<object[]>>>(pulled.Expression, implementor.Root).Compile();

            rows.Clear();
            using (var cursor = openPulled(context))
                while (cursor.Read())
                    rows.Add((string)cursor.Current[1]!);

            rows.Should().Equal(["D", "C", "B", "A"]);
            table.AsyncScans.Should().Be(1, "the open underneath was the awaiting one, and the drain was waited for");
        }

        /// <summary>
        /// The element type is the physical row type, the same whichever way the plan is opened.
        /// </summary>
        [Fact]
        public void ShouldNameTheRowType()
        {
            var rootSchema = Schema();

            Implement(Plan("SELECT * FROM SALES", rootSchema), new java.util.HashMap()).ElementType.Should().Be(typeof(object[]));
            // int rather than java.lang.Integer: the type factory's answer for a one-column NOT NULL result
            // is the primitive, exactly as Typed.getElementType answers int.class, and the cursor still
            // carries the box, there being no cursor of a primitive any more than an Enumerable<int>
            Implement(Plan("SELECT ID FROM SALES", rootSchema), new java.util.HashMap()).ElementType.Should().Be(typeof(int));
        }

    }

}
