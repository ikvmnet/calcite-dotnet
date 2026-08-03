using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

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
        /// Initializes a new instance.
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
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            if (readType.name() != nameof(Spool.Type.LAZY) || writeType.name() != nameof(Spool.Type.LAZY))
                throw new java.lang.UnsupportedOperationException("only LAZY read and LAZY write are supported");

            var result = implementor.VisitChild(this, 0, (ClrEnumerableRel)getInput(), pref);
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.prefer(result.Format));

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
