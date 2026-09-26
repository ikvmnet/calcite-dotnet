using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
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

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Runs queries over the table SPI these conventions add, and requires the same rows Calcite's own SPI
    /// gives.
    /// </summary>
    /// <remarks>
    /// Four interfaces were added and nothing implemented any of them, so they compiled and had never run.
    /// These are the smallest queries that make each one produce a row.
    ///
    /// <para>The oracle is the same table's rows read through Calcite's <see cref="ScannableTable"/>, which
    /// <c>ClrEnumerableConventionDifferentialTests</c> already checks against Calcite itself. What is under test is the
    /// route, not the rows: a scannable table is called, a queryable one hands back an expression the scan
    /// composes, and neither goes through linq4j.</para>
    /// </remarks>
    public class ClrCursorTableScanTests
    {

        static ClrCursorTableScanTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of this convention's own synchronous SPI, whose rows never become a linq4j sequence.
        /// </summary>
        sealed class ClrRowsTable : AbstractTable, IClrScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            /// <inheritdoc />
            public IEnumerable<object?[]> Scan(DataContext root) => AsyncTestRows.Sorted;

        }

        /// <summary>
        /// A table of this convention's own synchronous SPI that hands back an expression.
        /// </summary>
        sealed class ClrQueryableRowsTable : AbstractTable, IClrQueryableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            /// <inheritdoc />
            public Type ElementType => typeof(object?[]);

            /// <inheritdoc />
            /// <remarks>
            /// The whole point of the interface: the reading is composed into the plan rather than reached
            /// through an interface call. Here it is a constant, which is the simplest expression that
            /// yields rows; a real table would inline a provider read.
            /// </remarks>
            public Expression GetExpression(SchemaPlus? schema, string tableName) =>
                Expression.Constant(AsyncTestRows.Sorted, typeof(IEnumerable<object?[]>));

        }

        /// <summary>
        /// A table of the asynchronous SPI that hands back an expression.
        /// </summary>
        sealed class AsyncQueryableRowsTable : AbstractTable, IClrQueryableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            /// <inheritdoc />
            public Type ElementType => typeof(object?[]);

            /// <inheritdoc />
            public Expression GetAsyncExpression(SchemaPlus? schema, string tableName) =>
                Expression.Call(null, RowsMethod, Expression.Default(typeof(CancellationToken)));

            /// <inheritdoc />
            /// <remarks>
            /// Written from the awaiting half, because there is no pulled reading of these rows to offer.
            /// The other order, which the interface would have supplied for free, is the one this table
            /// cannot use. The drain is the test's own, because the convention's is internal and an adapter
            /// outside this repository would have to write its own too.
            /// </remarks>
            public Expression GetExpression(SchemaPlus? schema, string tableName) =>
                Expression.Call(null, DrainMethod.MakeGenericMethod(ElementType), GetAsyncExpression(schema, tableName));

            static readonly System.Reflection.MethodInfo DrainMethod =
                typeof(BlockingDrain).GetMethod(nameof(BlockingDrain.Of))!;

            static readonly System.Reflection.MethodInfo RowsMethod =
                typeof(AsyncQueryableRowsTable).GetMethod(nameof(Rows), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!;

            /// <summary>
            /// The rows, suspending on each, so that a plan reading this really is asynchronous.
            /// </summary>
            public static async IAsyncEnumerable<object?[]> Rows([EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                foreach (var row in AsyncTestRows.Sorted)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Yield();

                    yield return row;
                }
            }

        }

        /// <summary>
        /// A table that breaks the SPI's contract, holding its values boxed the CLR way.
        /// </summary>
        /// <remarks>
        /// What an implementer writing ordinary C# would produce, and what
        /// <see cref="ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys"/> exists to pin.
        /// </remarks>
        sealed class ClrBoxedRowsTable : AbstractTable, IClrScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            /// <inheritdoc />
            public IEnumerable<object?[]> Scan(DataContext root) =>
            [
                [1, "A"],
                [2, "B"],
            ];

        }

        /// <summary>
        /// A table whose values are not the type factory's fails, and fails at once.
        /// </summary>
        /// <remarks>
        /// The contract on <see cref="IClrScannableTable"/> is Calcite's own contract on
        /// <see cref="ScannableTable"/>: the values in a row are what the type factory says they are, which
        /// is to say Java's. Nothing checks it, and nothing needs to -- what reads a field is
        /// <c>SqlFunctions.toInt</c> and a cast to <c>java.lang.Integer</c>, both of them Calcite's own and
        /// neither of them able to see a <see cref="int"/> boxed the CLR way. So the simplest query there is
        /// over such a table stops on its first row.
        ///
        /// <para>Recorded as a test because the failure is the correct behaviour and should stay correct. It
        /// would be easy to read "Cannot convert 1 to int" as a defect in the scan and to answer it by
        /// converting every row on the way in — an adapter on a boundary that does not have one, paid for by
        /// every table that was already right.</para>
        /// </remarks>
        [Fact]
        public void ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys()
        {
            var scan = () => Run("SELECT \"K\" FROM \"T\"", new ClrBoxedRowsTable(), false);
            var distinct = () => Run("SELECT DISTINCT \"K\" FROM \"T\"", new ClrBoxedRowsTable(), false);

            // SqlFunctions.toInt, which knows java.lang.Number and nothing else
            scan.Should().Throw<org.apache.calcite.runtime.CalciteException>().WithMessage("*Cannot convert 1 to int*");

            // and the group key, which casts the field to the boxed type the row type declares
            distinct.Should().Throw<InvalidCastException>().WithMessage("*System.Int32*java.lang.Integer*");
        }

        static RelNode Plan(string sql, SchemaPlus rootSchema)
        {
            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in ClrEnumerableRules.CalcRules())
                if (calcRules.contains(rule) == false)
                    calcRules.add(rule);
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

            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrCursorConvention.Instance).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        static (RelNode Plan, List<string> Rows) Run(string sql, Table table, bool async)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("T", table);

            var physical = Plan(sql, rootSchema);

            var parameters = new java.util.HashMap();
            var context = new SpiDataContext(rootSchema, parameters);
            var rows = new List<string>();

            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot((ClrCursorRel)physical, ClrEnumerablePrefer.Array);

            if (async)
            {
                var cursor = factory.OpenAsync(context, CancellationToken.None).AsTask().GetAwaiter().GetResult();
                try
                {
                    while (cursor.ReadAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult())
                        rows.Add(Render(cursor.Current));
                }
                finally
                {
                    cursor.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
            else
            {
                using var cursor = factory.Open(context);
                while (cursor.Read())
                    rows.Add(Render(cursor.Current));
            }

            return (physical, rows);
        }


        /// <summary>
        /// A table of this project's cursor SPI, recording the token of each advance it was given.
        /// </summary>
        sealed class CursorRowsTable : AbstractTable, IClrCursorTable
        {

            public List<CancellationToken> Tokens { get; } = [];

            public int Opened { get; private set; }

            public int OpenedAsync { get; private set; }

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            public ClrCursor<object?[]> Open(DataContext root)
            {
                Opened++;
                return new RowsCursor(this);
            }

            public ValueTask<ClrCursor<object?[]>> OpenAsync(DataContext root, CancellationToken cancellationToken)
            {
                OpenedAsync++;
                return new ValueTask<ClrCursor<object?[]>>(new RowsCursor(this));
            }

            sealed class RowsCursor(CursorRowsTable table) : ClrCursor<object?[]>
            {

                int index = -1;

                public override object?[] Current => AsyncTestRows.Sorted[index];

                public override bool Read() => ++index < AsyncTestRows.Sorted.Length;

                public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
                {
                    table.Tokens.Add(cancellationToken);
                    return new ValueTask<bool>(Read());
                }

                public override void Dispose()
                {

                }

            }

        }

        sealed class SpiDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
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

        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        static readonly string[] Expected = ["1|A", "2|B", "2|C", "4|D"];

        const string Sql = "SELECT K, V FROM T ORDER BY K, V";

        /// <summary>
        /// The rows Calcite's own SPI gives, which every other case is measured against.
        /// </summary>
        [Fact]
        public void ShouldReadACalciteScannableTable()
        {
            var (_, rows) = Run(Sql, new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), false);

            rows.Should().Equal(Expected);
        }

        [Fact]
        public void ShouldReadAClrScannableTable()
        {
            var (plan, rows) = Run(Sql, new ClrRowsTable(), false);

            RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
            rows.Should().Equal(Expected);
        }

        /// <summary>
        /// A cursor table is opened by the open of the plan's kind and its cursor is the plan's leaf.
        /// </summary>
        [Fact]
        public void ShouldReadAClrCursorTable()
        {
            var table = new CursorRowsTable();
            var (plan, rows) = Run(Sql, table, false);

            RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
            rows.Should().Equal(Expected);
            table.Opened.Should().Be(1);
            table.OpenedAsync.Should().Be(0);

            var awaited = new CursorRowsTable();
            Run(Sql, awaited, true).Rows.Should().Equal(Expected);
            awaited.OpenedAsync.Should().Be(1);
            awaited.Opened.Should().Be(0);
        }

        /// <summary>
        /// Each advance's token reaches the cursor table, which is what the SPI exists for: a sequence
        /// takes its token once, at its enumerator, and a cursor takes one per advance.
        /// </summary>
        [Fact]
        public async Task ShouldHandEachAdvancesTokenToAClrCursorTable()
        {
            var table = new CursorRowsTable();
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("T", table);

            // no sort: a sort drains its input at the open, and then no advance of the plan reaches the table
            var physical = Plan("SELECT K, V FROM T WHERE K > 0", rootSchema);
            var parameters = new java.util.HashMap();
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot((ClrCursorRel)physical, ClrEnumerablePrefer.Array);

            using var first = new CancellationTokenSource();
            using var second = new CancellationTokenSource();

            using var cursor = factory.Open(new SpiDataContext(rootSchema, parameters));
            (await cursor.ReadAsync(first.Token)).Should().BeTrue();
            (await cursor.ReadAsync(second.Token)).Should().BeTrue();
            cursor.Read().Should().BeTrue();

            table.Tokens.Should().Equal([first.Token, second.Token], "the two awaited advances carried their own tokens and the synchronous one none");
        }

        [Fact]
        public void ShouldReadAClrQueryableTable()
        {
            var (plan, rows) = Run(Sql, new ClrQueryableRowsTable(), false);

            RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
            rows.Should().Equal(Expected);
        }

        [Fact]
        public void ShouldReadAnAsyncScannableTable()
        {
            var (plan, rows) = Run(Sql, new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), true);

            RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
            rows.Should().Equal(Expected);
        }

        [Fact]
        public void ShouldReadAnAsyncQueryableTable()
        {
            var (plan, rows) = Run(Sql, new AsyncQueryableRowsTable(), true);

            RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
            rows.Should().Equal(Expected);
        }

        /// <summary>
        /// A table of Calcite's is read by this convention's own scan, without a converter.
        /// </summary>
        /// <remarks>
        /// The point of reaching Calcite's tables the way Calcite reaches them. Before this an awaited plan
        /// could not read a <see cref="ScannableTable"/> at all, so a query over one was a Calcite subtree
        /// under <c>EnumerableToClrEnumerableConverter</c> — correct, but a converter and a planning step
        /// for something that is one node.
        ///
        /// <para>The converter is still there, and still needed, for what this convention has no node for at
        /// all — a table function, a MATCH_RECOGNIZE, a recursive query's transient scan.</para>
        /// </remarks>
        [Fact]
        public void ShouldReadACalciteTableWithoutAConverter()
        {
            var (plan, rows) = Run(Sql, new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), true);
            var text = RelOptUtil.toString(plan);

            text.Should().Contain("ClrCursorTableScan");
            text.Should().NotContain("EnumerableToClrCursorConverter");
            rows.Should().Equal(Expected);
        }

        /// <summary>
        /// Each of the four deduces the element type its Calcite counterpart would.
        /// </summary>
        /// <remarks>
        /// The thing that decides the row format, and the reason both scans have a <c>DeduceElementType</c>
        /// of the same name and shape as <c>EnumerableTableScan.deduceElementType</c>: a scannable table
        /// yields arrays, a queryable one names its own type, and everything else is Calcite's answer.
        /// </remarks>
        [Fact]
        public void ShouldDeduceTheElementTypeCalciteWould()
        {
            var arrays = (java.lang.Class)typeof(object[]);

            ClrCursorTableScan.DeduceElementType(new ClrRowsTable()).Should().Be(arrays);
            ClrCursorTableScan.DeduceElementType(new ClrQueryableRowsTable()).Should().Be(arrays);
            ClrCursorTableScan.DeduceElementType(new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)).Should().Be(arrays);
            ClrCursorTableScan.DeduceElementType(new AsyncQueryableRowsTable()).Should().Be(arrays);

            // and a table of Calcite's own SPI is still Calcite's answer, unchanged
            ClrCursorTableScan.DeduceElementType(new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false))
                .Should().Be(org.apache.calcite.adapter.enumerable.EnumerableTableScan.deduceElementType(new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));
        }

    }

}
