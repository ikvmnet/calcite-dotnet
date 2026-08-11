using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Schema;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// A table over a sequence of .NET objects, whose rows are those objects.
    /// </summary>
    /// <remarks>
    /// <c>ReflectiveSchema.ReflectiveTable</c>, for a .NET class. The row type is the members broken out as
    /// columns, so SQL sees <c>EMPID, NAME</c>; the row itself stays the object, so a scan with nothing
    /// projected over it hands back your instances rather than copies of them. Both, rather than a choice
    /// between them, which is what Calcite's reflective schema gives and the reason to keep the object.
    ///
    /// <para>An <see cref="IClrQueryableTable"/> rather than a scannable one: there is a sequence in hand and
    /// no reason to call anything per row. The expression is a constant, an expression tree being able to hold
    /// an object where a plan Janino compiles cannot.</para>
    /// </remarks>
    public class ClrObjectTable : AbstractTable, IClrQueryableTable, TranslatableTable
    {

        readonly Type elementType;
        readonly object source;
        readonly Type sequenceType;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="elementType">The type one row is an instance of.</param>
        /// <param name="source">An <see cref="IEnumerable{T}"/> of <paramref name="elementType"/>.</param>
        public ClrObjectTable(Type elementType, object source)
        {
            this.elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
            this.source = source ?? throw new ArgumentNullException(nameof(source));

            sequenceType = typeof(IEnumerable<>).MakeGenericType(elementType);
            if (sequenceType.IsInstanceOfType(source) == false)
                throw new ArgumentException($"'{source.GetType()}' is not an '{sequenceType}'.", nameof(source));
        }

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return ClrTypeMapper.RowType((JavaTypeFactory)typeFactory, elementType);
        }

        /// <inheritdoc />
        public Type ElementType => elementType;

        /// <inheritdoc />
        public Expression GetExpression(SchemaPlus? schema, string tableName)
        {
            return Expression.Constant(source, sequenceType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>ReflectiveSchema.getRowCount</c>: a collection knows how many rows it has and a bare sequence
        /// does not, and counting one would run it.
        /// </remarks>
        public override Statistic getStatistic()
        {
            return source is ICollection collection
                ? Statistics.of(collection.Count, null)
                : Statistics.UNKNOWN;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The same node <c>RelOptTableImpl.toRel</c> would have built without this. It is here so that a
        /// table macro can return one, <c>TableMacro.apply</c> being declared to answer a
        /// <see cref="TranslatableTable"/>.
        /// </remarks>
        public org.apache.calcite.rel.RelNode toRel(org.apache.calcite.plan.RelOptTable.ToRelContext context, org.apache.calcite.plan.RelOptTable relOptTable)
        {
            return org.apache.calcite.rel.logical.LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
        }

        /// <inheritdoc />
        public override string toString() => $"ClrObjectTable({elementType})";

    }

}
