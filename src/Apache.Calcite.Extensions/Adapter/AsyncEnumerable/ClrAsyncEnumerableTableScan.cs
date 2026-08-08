using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Schema;

using java.util.function;
using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="TableScan"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The one node of this convention that is not its counterpart with the operator table swapped, because
    /// it is the one that reads a table rather than a sequence. Only an
    /// <see cref="IClrAsyncScannableTable"/> can be read: this convention has no converter to any other, so a
    /// query touching a <see cref="ScannableTable"/>, a <see cref="QueryableTable"/> or a
    /// <see cref="FilterableTable"/> does not plan here at all and the planner says so rather than falling
    /// back.
    ///
    /// <para>It is shorter than <see cref="ClrEnumerableTableScan"/> rather than longer. There is no
    /// <c>getExpression(Queryable.class)</c>, so no linq4j tree to translate, no <c>ToEnumerable</c> to bring
    /// whatever it yielded to an <c>Enumerable</c>, and no <c>FromJava</c> to read that back: the table is a
    /// constant of the plan and <see cref="IClrAsyncScannableTable.ScanAsync"/> is called on it. The one
    /// linq4j hop the synchronous scan makes is simply absent.</para>
    ///
    /// <para>A row is an <c>object[]</c> whatever the row type is, which is what
    /// <see cref="ScannableTable"/> yields too, so the format is <see cref="JavaRowFormat.ARRAY"/> — or
    /// <see cref="JavaRowFormat.SCALAR"/> for one column, where a one column result is the value and
    /// <c>Slice0</c> takes it out.</para>
    /// </remarks>
    public class ClrAsyncEnumerableTableScan : TableScan, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableTableScan"/>.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="relOptTable"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableTableScan Create(RelOptCluster cluster, RelOptTable relOptTable)
        {
            var table = (Table)relOptTable.unwrap(typeof(Table));
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => table != null ? table.getStatistic().getCollations() : com.google.common.collect.ImmutableList.of()));

            return new ClrAsyncEnumerableTableScan(cluster, traitSet, relOptTable);
        }

        /// <summary>
        /// Returns whether this convention can produce rows for a particular variant of the table SPI.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        /// <remarks>
        /// One kind of table and no other. Where the synchronous scan lists the four variants
        /// <c>RelOptTableImpl.getClassExpressionFunction</c> knows how to build an expression for, this asks
        /// a single question, because a table that cannot produce rows asynchronously cannot be read here at
        /// any remove — there is no converter to carry them.
        /// </remarks>
        public static bool CanHandle(Table table)
        {
            return table is IClrAsyncScannableTable;
        }

        /// <summary>
        /// Returns whether this convention can produce rows for a particular variant of the table SPI.
        /// </summary>
        /// <param name="relOptTable"></param>
        /// <returns></returns>
        public static bool CanHandle(RelOptTable relOptTable)
        {
            var table = (Table)relOptTable.unwrap(typeof(Table));
            if (table == null || CanHandle(table) == false)
                return false;

            var supportArray = ((java.lang.Boolean)org.apache.calcite.config.CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_ARRAY.value()).booleanValue();
            var supportMap = ((java.lang.Boolean)org.apache.calcite.config.CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_MAP.value()).booleanValue();
            var supportMultiset = ((java.lang.Boolean)org.apache.calcite.config.CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_MULTISET.value()).booleanValue();
            if (supportArray && supportMap && supportMultiset)
                return true;

            // struct fields are not supported
            for (int i = 0; i < relOptTable.getRowType().getFieldList().size(); i++)
            {
                var field = (RelDataTypeField)relOptTable.getRowType().getFieldList().get(i);
                var unsupportedType = field.getType().getSqlTypeName().name() switch
                {
                    nameof(SqlTypeName.ARRAY) => supportArray,
                    nameof(SqlTypeName.MAP) => supportMap,
                    nameof(SqlTypeName.MULTISET) => supportMultiset,
                    _ => false,
                };

                if (unsupportedType)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="table"></param>
        public ClrAsyncEnumerableTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), table)
        {
            if (getConvention() is not ClrAsyncEnumerableConvention)
                throw new java.lang.AssertionError();
            if (CanHandle(table) == false)
                throw new java.lang.AssertionError($"ClrAsyncEnumerableTableScan can't implement {table}, see ClrAsyncEnumerableTableScan.CanHandle");
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrAsyncEnumerableTableScan(getCluster(), traitSet, table);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Where a table had an index on the required collation keys this is where an index scan would be
        /// returned. There is none, and Calcite's own answer here is the same null.
        /// </remarks>
        public RelNode passThrough(RelTraitSet required)
        {
            return null!;
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.PROHIBITED;
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), Format());

            var scannable = (IClrAsyncScannableTable)(table.unwrap(typeof(Table))
                ?? throw new java.lang.IllegalStateException($"{table} has no Table to unwrap"));

            // the table is reached as a constant, the way EnumerableRelImplementor.stash reaches an object a
            // plan cannot hold. An expression tree can hold one, so it is a constant rather than a stash.
            var source = Expression.Call(
                Expression.Constant(scannable, typeof(IClrAsyncScannableTable)),
                ScanAsync,
                implementor.Root);

            return implementor.Result(physType, ToRows(physType, source));
        }

        static readonly System.Reflection.MethodInfo ScanAsync = typeof(IClrAsyncScannableTable).GetMethod(nameof(IClrAsyncScannableTable.ScanAsync))
            ?? throw new InvalidOperationException($"'{nameof(IClrAsyncScannableTable.ScanAsync)}' is missing.");

        /// <summary>
        /// Brings the table's rows into the physical type asked for.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// Two cases where the synchronous scan has four, because the element type is not the table's choice
        /// here: it is always <c>object[]</c>. A one column row is the value, which is <c>Slice0</c>; anything
        /// else is the array, which is already what an <see cref="JavaRowFormat.ARRAY"/> row is.
        /// </remarks>
        Expression ToRows(ClrPhysType physType, Expression source)
        {
            if (physType.Format == JavaRowFormat.SCALAR)
                return ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Slice0.MakeGenericMethod(physType.RowType), source);

            if (physType.RowType == typeof(object[]))
                return source;

            throw new java.lang.IllegalStateException(
                $"{RelOptUtil.toString(this, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}: an asynchronous table yields object[] rows, which cannot be read as {physType.RowType}.");
        }

        /// <summary>
        /// Returns how a row of this table is represented.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// The two branches <c>ClrEnumerableTableScan.Format</c> takes for an <c>Object[]</c> element type,
        /// which is the only element type there is here.
        /// </remarks>
        JavaRowFormat Format()
        {
            var fieldCount = getRowType().getFieldCount();
            if (fieldCount == 0)
                return JavaRowFormat.LIST;

            return fieldCount == 1 ? JavaRowFormat.SCALAR : JavaRowFormat.ARRAY;
        }

    }

}
