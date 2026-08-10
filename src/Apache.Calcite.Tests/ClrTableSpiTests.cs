using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Schema;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
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
    /// <c>ClrEnumerableDifferentialTests</c> already checks against Calcite itself. What is under test is the
    /// route, not the rows: a scannable table is called, a queryable one hands back an expression the scan
    /// composes, and neither goes through linq4j.</para>
    /// </remarks>
    [TestClass]
    public class ClrTableSpiTests
    {

        static ClrTableSpiTests()
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
        sealed class AsyncQueryableRowsTable : AbstractTable, IClrAsyncQueryableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            /// <inheritdoc />
            public Type ElementType => typeof(object?[]);

            /// <inheritdoc />
            public Expression GetAsyncExpression(SchemaPlus? schema, string tableName) =>
                Expression.Call(null, RowsMethod, Expression.Default(typeof(CancellationToken)));

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
        /// <see cref="ShouldFailOverATableWhoseValuesAreNotTheTypeFactory's"/> exists to pin.
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
        [TestMethod]
        public void ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys()
        {
            var scan = () => Run("SELECT \"K\" FROM \"T\"", new ClrBoxedRowsTable(), false);
            var distinct = () => Run("SELECT DISTINCT \"K\" FROM \"T\"", new ClrBoxedRowsTable(), false);

            // SqlFunctions.toInt, which knows java.lang.Number and nothing else
            scan.Should().Throw<org.apache.calcite.runtime.CalciteException>().WithMessage("*Cannot convert 1 to int*");

            // and the group key, which casts the field to the boxed type the row type declares
            distinct.Should().Throw<InvalidCastException>().WithMessage("*System.Int32*java.lang.Integer*");
        }

        static (RelNode Plan, List<string> Rows) Run(string sql, Table table, bool async)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("T", table);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in async ? ClrAsyncEnumerableRules.CalcRules() : ClrEnumerableRules.CalcRules())
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

            var convention = async ? (Convention)ClrAsyncEnumerableConvention.Instance : ClrEnumerableConvention.Instance;
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new SpiDataContext(rootSchema, parameters);
            var rows = new List<string>();

            if (async)
            {
                var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);
                var reader = bindable.Bind(context).GetAsyncEnumerator();
                try
                {
                    while (reader.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                        rows.Add(Render(reader.Current));
                }
                finally
                {
                    reader.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
            else
            {
                var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
                foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }

            return (physical, rows);
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
        [TestMethod]
        public void ShouldReadACalciteScannableTable()
        {
            var (_, rows) = Run(Sql, new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), false);

            rows.Should().Equal(Expected);
        }

        [TestMethod]
        public void ShouldReadAClrScannableTable()
        {
            var (plan, rows) = Run(Sql, new ClrRowsTable(), false);

            RelOptUtil.toString(plan).Should().Contain("ClrEnumerableTableScan");
            rows.Should().Equal(Expected);
        }

        [TestMethod]
        public void ShouldReadAClrQueryableTable()
        {
            var (plan, rows) = Run(Sql, new ClrQueryableRowsTable(), false);

            RelOptUtil.toString(plan).Should().Contain("ClrEnumerableTableScan");
            rows.Should().Equal(Expected);
        }

        [TestMethod]
        public void ShouldReadAnAsyncScannableTable()
        {
            var (plan, rows) = Run(Sql, new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), true);

            RelOptUtil.toString(plan).Should().Contain("ClrAsyncEnumerableTableScan");
            rows.Should().Equal(Expected);
        }

        [TestMethod]
        public void ShouldReadAnAsyncQueryableTable()
        {
            var (plan, rows) = Run(Sql, new AsyncQueryableRowsTable(), true);

            RelOptUtil.toString(plan).Should().Contain("ClrAsyncEnumerableTableScan");
            rows.Should().Equal(Expected);
        }

        /// <summary>
        /// A table of Calcite's is read by this convention's own scan, without a converter.
        /// </summary>
        /// <remarks>
        /// The point of reaching Calcite's tables the way Calcite reaches them. Before this the asynchronous
        /// convention could not read a <see cref="ScannableTable"/> at all, so a query over one was a Calcite
        /// subtree under <c>EnumerableToClrAsyncEnumerableConverter</c> — correct, but a converter and a
        /// planning step for something that is one node.
        ///
        /// <para>The converter is still there, and still needed, for what this convention has no node for at
        /// all — a table function, a MATCH_RECOGNIZE, a recursive query's transient scan.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldReadACalciteTableWithoutAConverter()
        {
            var (plan, rows) = Run(Sql, new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), true);
            var text = RelOptUtil.toString(plan);

            text.Should().Contain("ClrAsyncEnumerableTableScan");
            text.Should().NotContain("EnumerableToClrAsyncEnumerableConverter");
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
        [TestMethod]
        public void ShouldDeduceTheElementTypeCalciteWould()
        {
            var arrays = (java.lang.Class)typeof(object[]);

            ClrEnumerableTableScan.DeduceElementType(new ClrRowsTable()).Should().Be(arrays);
            ClrEnumerableTableScan.DeduceElementType(new ClrQueryableRowsTable()).Should().Be(arrays);
            ClrAsyncEnumerableTableScan.DeduceElementType(new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)).Should().Be(arrays);
            ClrAsyncEnumerableTableScan.DeduceElementType(new AsyncQueryableRowsTable()).Should().Be(arrays);

            // and a table of Calcite's own SPI is still Calcite's answer, unchanged
            ClrEnumerableTableScan.DeduceElementType(new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false))
                .Should().Be(org.apache.calcite.adapter.enumerable.EnumerableTableScan.deduceElementType(new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));
        }

    }

}
