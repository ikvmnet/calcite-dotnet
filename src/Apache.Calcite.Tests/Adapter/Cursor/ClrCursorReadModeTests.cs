using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Runs the same query through this convention and through Calcite's, and requires the same rows however
    /// the cursor is opened and however it is advanced.
    /// </summary>
    /// <remarks>
    /// Calcite's rows are the expected answer, as in <c>ClrCursorConventionDifferentialTests</c>. Each plan is
    /// read four ways: opened synchronously and read with <see cref="ClrCursor.Read"/>; opened with await and
    /// read with <see cref="ClrCursor.ReadAsync"/>; opened with await and read synchronously; and opened
    /// synchronously with the two advances alternating row by row. A cursor has one position, which either
    /// advance moves.
    /// </remarks>
    public class ClrCursorReadModeTests
    {

        static ClrCursorReadModeTests()
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
        /// The tables every query here runs over.
        /// </summary>
        /// <returns>A new root schema holding <c>SALES</c>, <c>SORTED</c> and <c>ASALES</c>.</returns>
        /// <remarks>
        /// <c>SALES</c> and <c>SORTED</c> implement Calcite's table SPI, which both conventions read.
        /// <c>ASALES</c> holds the same rows as <c>SALES</c> behind this project's table SPI, with a scan that
        /// suspends on every row. Calcite cannot read it, so a query over it is compared with the same query over
        /// <c>SALES</c>.
        /// </remarks>
        static SchemaPlus Schema()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
            rootSchema.add("ASALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            return rootSchema;
        }

        /// <summary>
        /// Plans a statement with its root in the given convention, with this convention's rules registered
        /// beside Calcite's.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="rootSchema">The schema to plan against.</param>
        /// <param name="root">The convention the plan's root is requested in.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The physical root the planner chose.</returns>
        static RelNode Plan(string sql, SchemaPlus rootSchema, Convention root, RelOptRule[]? remove = null)
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
                    new DefaultRulesProgram(rules, remove: remove),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(root).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Runs a statement through Calcite's convention and returns its rows rendered as text.
        /// </summary>
        /// <param name="sql">The statement, which must not name <c>ASALES</c>.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> Calcite(string sql)
        {
            var rootSchema = Schema();
            var physical = Plan(sql, rootSchema, EnumerableConvention.INSTANCE);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();
            foreach (var row in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// Plans a statement into the cursor convention and implements it.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <returns>The implemented plan, and the context to open it with.</returns>
        static (ClrCursorFactory Factory, DataContext Context) Cursor(string sql)
        {
            var rootSchema = Schema();
            var physical = Plan(sql, rootSchema, ClrCursorConvention.Instance);

            physical.Should().BeAssignableTo<ClrCursorRel>("'{0}' was asked to end in the cursor convention", sql);

            var parameters = new java.util.HashMap();
            var implementor = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            return (implementor.ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array), new TestDataContext(rootSchema, parameters));
        }

        /// <summary>
        /// Renders a row as text, so that rows from the two conventions compare by value.
        /// </summary>
        /// <param name="row">A row, an <c>object[]</c> for a multi-column result or the value itself for one column.</param>
        /// <returns>The fields' text joined with <c>|</c>, with a null written as <c>&lt;null&gt;</c>.</returns>
        static string Render(object? row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Opens synchronously and reads with <see cref="ClrCursor.Read"/>.
        /// </summary>
        /// <param name="factory">The implemented plan.</param>
        /// <param name="context">The context to open the plan with.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> OpenAndRead(ClrCursorFactory factory, DataContext context)
        {
            var rows = new List<string>();

            using var cursor = factory.Open(context);
            while (cursor.Read())
                rows.Add(Render(cursor.Current));

            return rows;
        }

        /// <summary>
        /// Opens with await and reads with <see cref="ClrCursor.ReadAsync"/>.
        /// </summary>
        /// <param name="factory">The implemented plan.</param>
        /// <param name="context">The context to open the plan with.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static async Task<List<string>> OpenAsyncAndReadAsync(ClrCursorFactory factory, DataContext context)
        {
            var rows = new List<string>();

            await using var cursor = await factory.OpenAsync(context, CancellationToken.None);
            while (await cursor.ReadAsync(CancellationToken.None))
                rows.Add(Render(cursor.Current));

            return rows;
        }

        /// <summary>
        /// Opens synchronously and alternates the two advances, row by row.
        /// </summary>
        /// <param name="factory">The implemented plan.</param>
        /// <param name="context">The context to open the plan with.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static async Task<List<string>> OpenAndAlternate(ClrCursorFactory factory, DataContext context)
        {
            var rows = new List<string>();

            await using var cursor = factory.Open(context);
            for (var i = 0; ; i++)
            {
                var moved = i % 2 == 0 ? cursor.Read() : await cursor.ReadAsync(CancellationToken.None);
                if (moved == false)
                    break;

                rows.Add(Render(cursor.Current));
            }

            return rows;
        }

        /// <summary>
        /// Opens with await and reads with <see cref="ClrCursor.Read"/>.
        /// </summary>
        /// <param name="factory">The implemented plan.</param>
        /// <param name="context">The context to open the plan with.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static async Task<List<string>> OpenAsyncAndRead(ClrCursorFactory factory, DataContext context)
        {
            var rows = new List<string>();

            using var cursor = await factory.OpenAsync(context, CancellationToken.None);
            while (cursor.Read())
                rows.Add(Render(cursor.Current));

            return rows;
        }

        /// <summary>
        /// The statements compared.
        /// </summary>
        public static TheoryData<string> Queries =>
        [
            "SELECT * FROM SALES",
            "SELECT ID FROM SALES",
            "SELECT * FROM SALES WHERE AMOUNT > 10",
            "SELECT ID, AMOUNT * 2 AS DOUBLED FROM SALES WHERE REGION = 'EAST'",
            "SELECT * FROM SALES ORDER BY AMOUNT DESC NULLS LAST, ID",
            "SELECT * FROM SALES ORDER BY AMOUNT LIMIT 2 OFFSET 1",
            "SELECT * FROM SALES LIMIT 3",
            "SELECT * FROM SALES OFFSET 4 ROWS",
            "SELECT * FROM SALES LIMIT 0",
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)",
            "SELECT x FROM (VALUES (1), (2), (3)) AS t(x) ORDER BY x DESC",
            "SELECT ID FROM SALES UNION ALL SELECT ID FROM SALES",
            "SELECT ID FROM SALES UNION ALL SELECT K FROM SORTED UNION ALL SELECT ID FROM SALES WHERE ID > 4",
            "SELECT REGION FROM SALES UNION SELECT REGION FROM SALES",
            "SELECT REGION FROM SALES UNION SELECT V FROM SORTED UNION SELECT LABEL FROM SALES",
            "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION",
            "SELECT REGION, SUM(AMOUNT) AS S FROM SALES GROUP BY REGION HAVING SUM(AMOUNT) > 10 ORDER BY REGION",
            "SELECT a.ID, b.LABEL FROM SALES a JOIN SALES b ON a.ID = b.ID WHERE a.AMOUNT IS NOT NULL ORDER BY a.ID",
            "SELECT COUNT(*) FROM SALES",
            "SELECT * FROM ASALES WHERE AMOUNT > 10 ORDER BY ID",
            "SELECT ID FROM ASALES",
            "SELECT ID FROM ASALES UNION ALL SELECT ID FROM SALES",
            "SELECT ID FROM SALES UNION ALL SELECT ID FROM ASALES",
            "SELECT REGION FROM ASALES UNION SELECT REGION FROM SALES",
            "SELECT * FROM ASALES ORDER BY AMOUNT LIMIT 2 OFFSET 1",
        ];

        /// <summary>
        /// Returns the statement Calcite runs as the expected answer: the same statement over <c>SALES</c> in
        /// place of <c>ASALES</c>.
        /// </summary>
        /// <param name="sql">A statement from <c>Queries</c>.</param>
        /// <returns>The statement with every <c>ASALES</c> replaced by <c>SALES</c>.</returns>
        static string Oracle(string sql) => sql.Replace("ASALES", "SALES");

        /// <summary>
        /// Opened synchronously and read synchronously, the rows are Calcite's.
        /// </summary>
        /// <param name="sql">A statement from <c>Queries</c>.</param>
        [Theory]
        [MemberData(nameof(Queries))]
        public void ShouldAgreeWhenOpenedAndReadSynchronously(string sql)
        {
            var (factory, context) = Cursor(sql);

            OpenAndRead(factory, context).Should().Equal(Calcite(Oracle(sql)), "'{0}'", sql);
        }

        /// <summary>
        /// Opened with await and read with await, the rows are Calcite's.
        /// </summary>
        /// <param name="sql">A statement from <c>Queries</c>.</param>
        /// <returns>A task that completes when the test has run.</returns>
        [Theory]
        [MemberData(nameof(Queries))]
        public async Task ShouldAgreeWhenOpenedAndReadWithAwait(string sql)
        {
            var (factory, context) = Cursor(sql);

            (await OpenAsyncAndReadAsync(factory, context)).Should().Equal(Calcite(Oracle(sql)), "'{0}'", sql);
        }

        /// <summary>
        /// One cursor advanced by either member on alternate rows reads every row once, in order.
        /// </summary>
        /// <param name="sql">A statement from <c>Queries</c>.</param>
        /// <returns>A task that completes when the test has run.</returns>
        [Theory]
        [MemberData(nameof(Queries))]
        public async Task ShouldAgreeWhenTheTwoAdvancesAlternate(string sql)
        {
            var (factory, context) = Cursor(sql);

            (await OpenAndAlternate(factory, context)).Should().Equal(Calcite(Oracle(sql)), "'{0}'", sql);
        }

        /// <summary>
        /// A cursor opened with await can still be read synchronously.
        /// </summary>
        /// <param name="sql">A statement from <c>Queries</c>.</param>
        /// <returns>A task that completes when the test has run.</returns>
        [Theory]
        [MemberData(nameof(Queries))]
        public async Task ShouldAgreeWhenOpenedWithAwaitAndReadSynchronously(string sql)
        {
            var (factory, context) = Cursor(sql);

            (await OpenAsyncAndRead(factory, context)).Should().Equal(Calcite(Oracle(sql)), "'{0}'", sql);
        }

        /// <summary>
        /// One factory opens the same plan again and again, and each open is a fresh cursor.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldOpenTheSamePlanRepeatedly()
        {
            var sql = "SELECT * FROM SALES WHERE AMOUNT > 10 ORDER BY ID";
            var (factory, context) = Cursor(sql);
            var expected = Calcite(sql);

            OpenAndRead(factory, context).Should().Equal(expected);
            (await OpenAsyncAndReadAsync(factory, context)).Should().Equal(expected);
            OpenAndRead(factory, context).Should().Equal(expected);
        }

        /// <summary>
        /// A plan of this convention under a node of Calcite's is read through the converter out, and the rows
        /// are Calcite's.
        /// </summary>
        /// <remarks>
        /// The root is requested in <c>EnumerableConvention</c> over a table of this project's SPI, which neither
        /// <c>EnumerableTableScan.canHandle</c> nor the bindable scan accepts, so the scan must be this
        /// convention's and the plan must hold a converter out of it; the test checks the plan for both.
        /// Removing Calcite's scan rule instead would not force the shape: the planner would route the scan
        /// through its interpreter, whose cost ties this convention's scan and which it registers first.
        /// </remarks>
        [Fact]
        public void ShouldCarryACursorPlanUnderACalciteNode()
        {
            var sql = "SELECT REGION, SUM(AMOUNT) FROM ASALES WHERE ID > 1 GROUP BY REGION ORDER BY REGION";
            var rootSchema = Schema();
            var physical = Plan(sql, rootSchema, EnumerableConvention.INSTANCE);

            RelOptUtil.toString(physical).Should().Contain("ClrCursorToEnumerableConverter")
                .And.Contain("ClrCursorTableScan");

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();
            foreach (var row in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(Render(row));

            rows.Should().Equal(Calcite(Oracle(sql)));
        }

    }

}
