using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;
using Apache.Calcite.Extensions.Schema;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Measures what a plan of the asynchronous convention costs when nothing in it suspends, read both
    /// ways, against the same query planned synchronously.
    /// </summary>
    /// <remarks>
    /// The question this answers is whether "prepare one plan, always the asynchronous one, and let
    /// <c>Read</c> block" is affordable. The interesting case is a leaf that never suspends -- an in-memory
    /// table, which is what most of a query's leaves are -- because there <c>MoveNextAsync</c> always
    /// completes synchronously and the whole cost is state-machine overhead rather than a parked thread.
    ///
    /// <para><see cref="AsyncRowsTable"/> is deliberately the wrong fixture for this: it awaits
    /// <c>Task.Yield</c> on every row so that the differential tests exercise resumption. A benchmark over
    /// it would measure the thread pool, not the convention.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncOverheadBenchmarks
    {

        const int RowCount = 200_000;
        const int Iterations = 5;

        static readonly string ReportPath = Path.Combine(Path.GetTempPath(), "clr-async-overhead.txt");

        static ClrAsyncOverheadBenchmarks()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.util.Smalls).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        static object?[][] MakeRows(int count)
        {
            var rows = new object?[count][];
            for (var i = 0; i < count; i++)
                rows[i] = [java.lang.Integer.valueOf(i), (i % 4) switch { 0 => "EAST", 1 => "WEST", 2 => "NORTH", _ => "SOUTH" }, java.lang.Integer.valueOf(i % 97)];

            return rows;
        }

        static RelDataType BenchRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("REGION", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("AMOUNT", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .build();
        }

        /// <summary>
        /// A table whose rows are already in memory, read synchronously.
        /// </summary>
        sealed class BenchSyncTable(object?[][] rows) : AbstractTable, ScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => BenchRowType(typeFactory);

            public override Statistic getStatistic() => Statistics.of(rows.Length, new java.util.ArrayList());

            public Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in rows)
                    list.add(row);

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The same rows behind <see cref="IClrAsyncScannableTable"/>, completing synchronously on every
        /// row.
        /// </summary>
        /// <remarks>
        /// The trailing await is what makes the compiler accept an async iterator with nothing to await,
        /// exactly as <c>ClrSequences.ToAsyncEnumerable</c> does. No row suspends, so every
        /// <c>MoveNextAsync</c> up the pipeline completes synchronously.
        /// </remarks>
        sealed class BenchAsyncTable(object?[][] rows) : AbstractTable, IClrAsyncScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => BenchRowType(typeFactory);

            public override Statistic getStatistic() => Statistics.of(rows.Length, new java.util.ArrayList());

            public IAsyncEnumerable<object?[]> ScanAsync(DataContext root) => Rows();

            async IAsyncEnumerable<object?[]> Rows([EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                foreach (var row in rows)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    yield return row;
                }

                await Task.CompletedTask;
            }

        }

        sealed class BenchDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            public SchemaPlus getRootSchema() => rootSchema;

            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            public QueryProvider getQueryProvider() => null!;

            public object get(string name) => parameters.get(name);

        }

        static SchemaPlus Schema(bool async, object?[][] rows)
        {
            var rootSchema = Frameworks.createRootSchema(true);

            if (async)
                rootSchema.add("BENCH", new BenchAsyncTable(rows));
            else
                rootSchema.add("BENCH", new BenchSyncTable(rows));

            return rootSchema;
        }

        /// <summary>
        /// Plans one query into one convention, and returns the compiled plan with the context to bind it.
        /// </summary>
        /// <remarks>
        /// The rule registration of <c>ClrAsyncEnumerableDifferentialTests.Run</c>, which is what makes the
        /// two sides comparable: each convention registers its own rules over Calcite's defaults, and the
        /// root trait is what decides which plan comes out.
        /// </remarks>
        static (object Bindable, DataContext Context, string Plan) Plan(string sql, bool async, object?[][] rows)
        {
            var rootSchema = Schema(async, rows);

            var rules = new java.util.ArrayList();
            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
                rules.add(rule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in async ? ClrAsyncEnumerableRules.CalcRules() : ClrEnumerableRules.CalcRules())
                calcRules.add(rule);

            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, false, false, null, null),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = async ? (Convention)ClrAsyncEnumerableConvention.Instance : ClrEnumerableConvention.Instance;
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new BenchDataContext(rootSchema, parameters);
            var plan = RelOptUtil.toString(physical);

            object bindable = async
                ? ClrAsyncEnumerableInterpretable.ToBindable(parameters, null, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array)
                : ClrEnumerableInterpretable.ToBindable(parameters, null, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            return (bindable, context, plan);
        }

        static long DrainSync(IClrBindable bindable, DataContext context)
        {
            var n = 0L;
            foreach (var row in bindable.Bind(context))
                n++;

            return n;
        }

        static async Task<long> DrainAsync(IClrAsyncBindable bindable, DataContext context)
        {
            var n = 0L;
            await foreach (var row in bindable.Bind(context))
                n++;

            return n;
        }

        /// <summary>
        /// Reads an asynchronous plan the way <c>CalciteAsyncEnumerableResult.Read</c> does, through
        /// <c>ClrSequences.ToEnumerable</c> -- which is what "prepare async, block in Read" costs.
        /// </summary>
        static long DrainBlocking(IClrAsyncBindable bindable, DataContext context)
        {
            var n = 0L;
            foreach (var row in ClrSequences.ToEnumerable(bindable.Bind(context)))
                n++;

            return n;
        }

        static double Best(Func<long> run, out long rows)
        {
            run();

            var best = double.MaxValue;
            rows = 0;

            for (var i = 0; i < Iterations; i++)
            {
                var sw = Stopwatch.StartNew();
                rows = run();
                sw.Stop();
                best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
            }

            return best;
        }

        [TestMethod]
        public void Measure()
        {
            var rows = MakeRows(RowCount);
            var report = new StringBuilder();

            report.AppendLine($"{RowCount:N0} rows, best of {Iterations}, .NET {Environment.Version}");
            report.AppendLine();

            string[] queries =
            [
                "SELECT ID, REGION, AMOUNT FROM BENCH",
                "SELECT ID, AMOUNT FROM BENCH WHERE AMOUNT > 5",
                "SELECT REGION, SUM(AMOUNT) FROM BENCH WHERE AMOUNT > 5 GROUP BY REGION",
                "SELECT A.ID, B.AMOUNT FROM BENCH A JOIN BENCH B ON A.ID = B.ID WHERE A.AMOUNT > 90",
            ];

            foreach (var sql in queries)
            {
                var (syncBindable, syncContext, syncPlan) = Plan(sql, false, rows);
                var (asyncBindable, asyncContext, asyncPlan) = Plan(sql, true, rows);

                var sync = Best(() => DrainSync((IClrBindable)syncBindable, syncContext), out var syncRows);
                var awaited = Best(() => DrainAsync((IClrAsyncBindable)asyncBindable, asyncContext).GetAwaiter().GetResult(), out var awaitedRows);
                var blocking = Best(() => DrainBlocking((IClrAsyncBindable)asyncBindable, asyncContext), out var blockingRows);

                report.AppendLine(sql);
                report.AppendLine($"  rows                     {syncRows:N0} / {awaitedRows:N0} / {blockingRows:N0}");
                report.AppendLine($"  sync plan, sync read     {sync,8:F1} ms   {sync * 1_000_000 / syncRows,7:F0} ns/row   1.00x");
                report.AppendLine($"  async plan, awaited      {awaited,8:F1} ms   {awaited * 1_000_000 / syncRows,7:F0} ns/row   {awaited / sync,4:F2}x");
                report.AppendLine($"  async plan, blocking     {blocking,8:F1} ms   {blocking * 1_000_000 / syncRows,7:F0} ns/row   {blocking / sync,4:F2}x");
                report.AppendLine($"  plans identical shape:   {(NormalizePlan(syncPlan) == NormalizePlan(asyncPlan) ? "yes" : "NO -- comparison is not like for like")}");
                report.AppendLine();

                Assert.AreEqual(syncRows, awaitedRows, $"row count differs for: {sql}");
                Assert.AreEqual(syncRows, blockingRows, $"row count differs for: {sql}");
            }

            File.WriteAllText(ReportPath, report.ToString());
            Console.WriteLine(report.ToString());
        }

        /// <summary>
        /// Strips the convention prefix so that two plans that differ only by which convention implemented
        /// them compare equal.
        /// </summary>
        static string NormalizePlan(string plan) => plan.Replace("ClrAsyncEnumerable", "ClrEnumerable");

    }

}
