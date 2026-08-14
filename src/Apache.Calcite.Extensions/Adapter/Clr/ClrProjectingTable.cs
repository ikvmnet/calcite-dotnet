using System;
using System.Collections;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Schema;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// A table over a sequence of .NET objects, whose rows are those objects broken into columns.
    /// </summary>
    /// <remarks>
    /// The half of the pair a <see cref="ClrObjectTable"/> cannot be, taken when the element type has a
    /// member Calcite has no representation for — a <see cref="decimal"/>, a <see cref="DateTime"/>, a
    /// <c>Guid</c>, an <c>int?</c> — or has no constructor to build a row back with. Both are things a row
    /// that <em>is</em> the object cannot do: a field read of one is <c>Expression.Property</c> with nowhere
    /// to put a conversion, and <c>JavaRowFormat.CUSTOM.record</c> rebuilds one by calling a constructor.
    ///
    /// <para>So the objects are read once each into an <c>object?[]</c> by <see cref="ClrRowConverter"/>, and
    /// everything above the scan is what already runs over any <see cref="IClrScannableTable"/>. What is
    /// given up is object identity — a query over this returns rows, not your instances.</para>
    /// </remarks>
    public class ClrProjectingTable : AbstractTable, IClrScannableTable, TranslatableTable
    {

        readonly Type elementType;
        readonly IEnumerable source;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="elementType">The type one object is an instance of.</param>
        /// <param name="source">A sequence of <paramref name="elementType"/>.</param>
        public ClrProjectingTable(Type elementType, IEnumerable source)
        {
            this.elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
            this.source = source ?? throw new ArgumentNullException(nameof(source));
        }

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return ClrTypeMapper.ColumnRowType((JavaTypeFactory)typeFactory, elementType);
        }

        /// <inheritdoc />
        public IEnumerable<object?[]> Scan(DataContext root)
        {
            var read = ClrRowConverter.Of(elementType);

            foreach (var item in source)
                yield return read(item);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>ReflectiveSchema.getRowCount</c>: a collection knows how many rows it has and a bare sequence
        /// does not, and counting one would run it.
        /// </remarks>
        public override Statistic getStatistic()
        {
            return source is ICollection collection ? Statistics.of(collection.Count, null) : Statistics.UNKNOWN;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The same node <c>RelOptTableImpl.toRel</c> would have built without this. It is here so that a
        /// table macro can return one, <c>TableMacro.apply</c> being declared to answer a
        /// <see cref="TranslatableTable"/>.
        /// </remarks>
        public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable)
        {
            return LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
        }

        /// <inheritdoc />
        public override string toString() => $"ClrProjectingTable({elementType})";

    }

}
