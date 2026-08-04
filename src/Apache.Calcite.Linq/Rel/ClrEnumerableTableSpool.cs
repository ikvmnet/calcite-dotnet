using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.schema;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="TableSpool"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// A tee: every row passes through, and the rows of the round are left in the table's collection for
    /// whatever reads it next. That is what carries one round of a recursive query to the following one.
    ///
    /// <para>Calcite reaches the collection by asking the root schema for the table by name, because the Java
    /// it generates cannot mention an object. An expression tree can hold one, so the collection is taken at
    /// planning time and captured.</para>
    /// </remarks>
    public class ClrEnumerableTableSpool : TableSpool, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableTableSpool"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="readType"></param>
        /// <param name="writeType"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static ClrEnumerableTableSpool Create(RelNode input, Spool.Type readType, Spool.Type writeType, RelOptTable table)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => mq.collations(input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => mq.distribution(input)));

            return new ClrEnumerableTableSpool(cluster, traitSet, input, readType, writeType, table);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="readType"></param>
        /// <param name="writeType"></param>
        /// <param name="table"></param>
        public ClrEnumerableTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, Spool.Type readType, Spool.Type writeType, RelOptTable table) :
            base(cluster, traitSet, input, readType, writeType, table)
        {

        }

        /// <inheritdoc />
        protected override Spool copy(RelTraitSet traitSet, RelNode input, Spool.Type readType, Spool.Type writeType)
        {
            return new ClrEnumerableTableSpool(getCluster(), traitSet, input, readType, writeType, getTable());
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (readType.name() != nameof(Spool.Type.LAZY) || writeType.name() != nameof(Spool.Type.LAZY))
                throw new java.lang.UnsupportedOperationException("only LAZY read and LAZY write are supported");

            var result = implementor.VisitChild(this, 0, (ClrEnumerableRel)getInput(), pref);
            // the rows here are the input's rows, so the format has to be the one they already have. The
            // three-argument overload re-optimises it, and for a one-column row that turns ARRAY into SCALAR
            // — a physical type saying the row *is* the value while the sequence still yields Object[]. A
            // parent then reads field 0 as the row itself. Calcite writes the three-argument call and cannot
            // see the difference, because Java erases the element type; ours is typed, and a merge join over
            // a one-column table function is where it surfaced.
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.Prefer(result.Format), false);

            var table = (ModifiableTable)getTable().unwrap(typeof(ModifiableTable))
                ?? throw new java.lang.IllegalStateException($"{getTable()} is not modifiable");

            var rowType = TypeResolver.Resolve(result.PhysType.getJavaRowType());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.LazyCollectionSpool.MakeGenericMethod(rowType),
                    Expression.Constant(table.getModifiableCollection(), typeof(java.util.Collection)),
                    result.Expression));
        }

    }

}
