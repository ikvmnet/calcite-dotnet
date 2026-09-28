using System;
using System.Collections.Generic;
using System.Linq;
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
    /// Runs queries over each kind of table in this project's table SPI, and requires the rows Calcite's own
    /// SPI gives for the same data.
    /// </summary>
    /// <remarks>
    /// The expected rows are those of the same data read through Calcite's <see cref="ScannableTable"/>, which
    /// <c>ClrCursorConventionDifferentialTests</c> checks against Calcite itself. What these check is the route
    /// to the rows: a scannable table is called, and a cursor table's cursor becomes the plan's leaf.
    /// </remarks>
    public class ClrCursorTableScanTests
    {

        static ClrCursorTableScanTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of this project's scannable SPI, returning its rows as an <see cref="IEnumerable{T}"/>.
        /// </summary>
        sealed class ClrRowsTable : AbstractTable, IClrScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            /// <inheritdoc />
            public IEnumerable<object?[]> Scan(DataContext root) => AsyncTestRows.Sorted;

        }

        /// <summary>
        /// A table that breaks the SPI's contract by holding CLR-boxed values, as ordinary C# produces, where the
        /// type factory expects Java-boxed ones.
        /// </summary>
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
        /// A query over a table whose values are not of the types the type factory declares fails on the first
        /// row.
        /// </summary>
        /// <remarks>
        /// <see cref="IClrScannableTable"/> has Calcite's contract for <see cref="ScannableTable"/>: a row's
        /// values are the Java types the type factory declares. The scan does not convert them. What reads a
        /// field is <c>SqlFunctions.toInt</c> or a cast to <c>java.lang.Integer</c>, neither of which accepts an
        /// <see cref="int"/> boxed by the CLR. This failure is the intended behaviour; converting every row in
        /// the scan would cost every table that already meets the contract.
        /// </remarks>
        [Fact]
        public void ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys()
        {
            var scan = () => Run("SELECT \"K\" FROM \"T\"", new ClrBoxedRowsTable(), false);
            var distinct = () => Run("SELECT DISTINCT \"K\" FROM \"T\"", new ClrBoxedRowsTable(), false);

            // SqlFunctions.toInt, which accepts only java.lang.Number
            scan.Should().Throw<org.apache.calcite.runtime.CalciteException>().WithMessage("*Cannot convert 1 to int*");

            // the group key, which casts the field to the boxed type the row type declares
            distinct.Should().Throw<InvalidCastException>().WithMessage("*System.Int32*java.lang.Integer*");
        }

        static RelNode Plan(string sql, SchemaPlus rootSchema)
        {
            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrCursorRules.CalcRules())
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

            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

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
        /// A table of this project's cursor SPI that counts its opens of each kind and records the token of
        /// each awaiting advance.
        /// </summary>
        sealed class CursorRowsTable : AbstractTable, IClrCursorTable
        {

            public List<CancellationToken> Tokens { get; } = [];

            public int Opened { get; private set; }

            public int OpenedAsync { get; private set; }

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => AsyncTestRows.SortedRowType(typeFactory);

            public IClrCursor<object?[]> Open(DataContext root)
            {
                Opened++;
                return new RowsCursor(this);
            }

            public ValueTask<IClrCursor<object?[]>> OpenAsync(DataContext root, CancellationToken cancellationToken)
            {
                OpenedAsync++;
                return new ValueTask<IClrCursor<object?[]>>(new RowsCursor(this));
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
        /// A table of Calcite's own SPI gives the expected rows, against which every other case is compared.
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
        /// A cursor table is opened by the open of the same kind as the plan's, synchronous or awaiting.
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
        /// The token given to each awaiting advance of the plan reaches the cursor table's advance. A sequence
        /// takes one token, when it is enumerated; a cursor takes one per advance.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldHandEachAdvancesTokenToAClrCursorTable()
        {
            var table = new CursorRowsTable();
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("T", table);

            // no sort: a sort drains its input at the open, so no advance of the plan would reach the table
            var physical = Plan("SELECT K, V FROM T WHERE K > 0", rootSchema);
            var parameters = new java.util.HashMap();
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

            using var first = new CancellationTokenSource();
            using var second = new CancellationTokenSource();

            using var cursor = factory.Open(new SpiDataContext(rootSchema, parameters));
            (await cursor.ReadAsync(first.Token)).Should().BeTrue();
            (await cursor.ReadAsync(second.Token)).Should().BeTrue();
            cursor.Read().Should().BeTrue();

            table.Tokens.Should().Equal([first.Token, second.Token], "the two awaited advances carried their own tokens and the synchronous one none");
        }

        [Fact]
        public void ShouldReadAnAsyncScannableTable()
        {
            var (plan, rows) = Run(Sql, new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false), true);

            RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
            rows.Should().Equal(Expected);
        }

        /// <summary>
        /// A table of Calcite's SPI is read by this convention's own scan, not by a Calcite scan under
        /// <c>EnumerableToClrCursorConverter</c>.
        /// </summary>
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
        /// Rows of one nullable VARCHAR column, each an array as the SPI requires for any column count.
        /// </summary>
        static readonly object?[][] OneColumn = [["{}"], ["[]"], [null]];

        /// <summary>
        /// Returns the row type of <see cref="OneColumn"/>.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>A single nullable VARCHAR column <c>DOC</c>.</returns>
        static RelDataType OneColumnRowType(RelDataTypeFactory typeFactory) =>
            typeFactory.builder()
                .add("DOC", typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR), true))
                .build();

        /// <summary>
        /// A table of this project's cursor SPI with the one column of <see cref="OneColumn"/>.
        /// </summary>
        sealed class OneColumnCursorTable : AbstractTable, IClrCursorTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => OneColumnRowType(typeFactory);

            /// <inheritdoc />
            public IClrCursor<object?[]> Open(DataContext root) => new RowsCursor();

            /// <summary>
            /// A forward-only cursor over <see cref="OneColumn"/>.
            /// </summary>
            sealed class RowsCursor : ClrCursor<object?[]>
            {

                int index = -1;

                /// <inheritdoc />
                public override object?[] Current => OneColumn[index];

                /// <inheritdoc />
                public override bool Read() => ++index < OneColumn.Length;

                /// <inheritdoc />
                public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken) => new(Read());

                /// <inheritdoc />
                public override void Dispose()
                {

                }

            }

        }

        /// <summary>
        /// A one-column table of this project's scannable or cursor SPI yields an array per row,
        /// which the scan narrows to its value as Calcite's scan narrows the arrays of a
        /// <see cref="ScannableTable"/>, whether the plan is opened synchronously or awaiting.
        /// </summary>
        /// <param name="sql">The query, over the one-column table <c>T</c>.</param>
        [Theory]
        [InlineData("SELECT \"DOC\" FROM \"T\"")]
        [InlineData("SELECT \"DOC\" FROM \"T\" WHERE \"DOC\" IS NOT NULL")]
        [InlineData("SELECT \"DOC\" FROM \"T\" ORDER BY \"DOC\"")]
        public void ShouldReadAOneColumnTable(string sql)
        {
            var expected = Run(sql, new SyncRowsTable(OneColumn, OneColumnRowType, false), false).Rows;
            expected.Should().NotBeEmpty();

            foreach (var async in new[] { false, true })
            {
                Run(sql, new AsyncRowsTable(OneColumn, OneColumnRowType, false), async).Rows.Should().Equal(expected);

                var (plan, rows) = Run(sql, new OneColumnCursorTable(), async);
                RelOptUtil.toString(plan).Should().Contain("ClrCursorTableScan");
                rows.Should().Equal(expected);
            }
        }

        /// <summary>
        /// <c>ClrCursorTableScan.DeduceElementType</c> gives each of this project's table kinds the element type
        /// Calcite's <c>EnumerableTableScan.deduceElementType</c> gives its counterpart, and gives a table of
        /// Calcite's SPI Calcite's own answer.
        /// </summary>
        /// <remarks>
        /// The element type decides the row format: a scannable or cursor table yields arrays, and anything else
        /// takes Calcite's answer.
        /// </remarks>
        [Fact]
        public void ShouldDeduceTheElementTypeCalciteWould()
        {
            var arrays = (java.lang.Class)typeof(object[]);

            ClrCursorTableScan.DeduceElementType(new ClrRowsTable()).Should().Be(arrays);
            ClrCursorTableScan.DeduceElementType(new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)).Should().Be(arrays);

            // a table of Calcite's own SPI gets Calcite's answer
            ClrCursorTableScan.DeduceElementType(new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false))
                .Should().Be(org.apache.calcite.adapter.enumerable.EnumerableTableScan.deduceElementType(new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, false)));
        }

    }

}
