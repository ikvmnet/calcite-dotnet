using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="TableSpool"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableTableSpool</c>. Every input row passes through and is also added to the table's
    /// modifiable collection, which is how a recursive query carries one round's rows to the next. Only
    /// <c>LAZY</c> read and write are supported.
    ///
    /// <para>As in Calcite, the table is looked up by name in the root schema of the <c>DataContext</c> each
    /// time the plan runs, so a plan can be bound more than once.</para>
    /// </remarks>
    public class ClrCursorTableSpool : TableSpool, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorTableSpool"/>, taking its collation and distribution from its input.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="readType">How the spool is read; only <c>LAZY</c> can be implemented.</param>
        /// <param name="writeType">How the spool is written; only <c>LAZY</c> can be implemented.</param>
        /// <param name="table">The table the rows are written to.</param>
        /// <returns>The new spool.</returns>
        public static ClrCursorTableSpool Create(RelNode input, Spool.Type readType, Spool.Type writeType, RelOptTable table)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => mq.collations(input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => mq.distribution(input)));

            return new ClrCursorTableSpool(cluster, traitSet, input, readType, writeType, table);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> derives the trait set; this constructor takes it as
        /// given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="readType">How the spool is read; only <c>LAZY</c> can be implemented.</param>
        /// <param name="writeType">How the spool is written; only <c>LAZY</c> can be implemented.</param>
        /// <param name="table">The table the rows are written to.</param>
        public ClrCursorTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, Spool.Type readType, Spool.Type writeType, RelOptTable table) :
            base(cluster, traitSet, input, readType, writeType, table)
        {

        }

        /// <inheritdoc />
        protected override Spool copy(RelTraitSet traitSet, RelNode input, Spool.Type readType, Spool.Type writeType)
        {
            return new ClrCursorTableSpool(getCluster(), traitSet, input, readType, writeType, getTable());
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (readType.name() != nameof(Spool.Type.LAZY) || writeType.name() != nameof(Spool.Type.LAZY))
                throw new java.lang.UnsupportedOperationException("only LAZY read and LAZY write are supported");

            var result = implementor.VisitChild(this, 0, (ClrCursorRel)getInput(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(result.Format));

            // the table is looked up at run time rather than held, because its collection belongs to the
            // DataContext the plan is bound with
            var name = (string)getTable().getQualifiedName().get(getTable().getQualifiedName().size() - 1);
            var collection = Expression.Call(
                Expression.Convert(
                    Expression.Call(
                        Expression.Call(implementor.Root, DataContextGetRootSchema),
                        SchemaGetTable,
                        Expression.Constant(name)),
                    typeof(ModifiableTable)),
                ModifiableTableGetModifiableCollection);

            var rowType = result.PhysType.RowType;

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.LazyCollectionSpool.MakeGenericMethod(rowType),
                    collection,
                    result.Expression));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (readType.name() != nameof(Spool.Type.LAZY) || writeType.name() != nameof(Spool.Type.LAZY))
                throw new java.lang.UnsupportedOperationException("only LAZY read and LAZY write are supported");

            var result = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getInput(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(result.Format));

            // the table is looked up at run time rather than held, because its collection belongs to the
            // DataContext the plan is bound with
            var name = (string)getTable().getQualifiedName().get(getTable().getQualifiedName().size() - 1);
            var collection = Expression.Call(
                Expression.Convert(
                    Expression.Call(
                        Expression.Call(implementor.Root, DataContextGetRootSchema),
                        SchemaGetTable,
                        Expression.Constant(name)),
                    typeof(ModifiableTable)),
                ModifiableTableGetModifiableCollection);

            var rowType = result.PhysType.RowType;

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.LazyCollectionSpoolAsync.MakeGenericMethod(rowType),
                    collection,
                    result.Expression));
        }

        static readonly System.Reflection.MethodInfo DataContextGetRootSchema = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        static readonly System.Reflection.MethodInfo SchemaGetTable = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.SCHEMA_GET_TABLE.method);
        static readonly System.Reflection.MethodInfo ModifiableTableGetModifiableCollection = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);

    }

}
