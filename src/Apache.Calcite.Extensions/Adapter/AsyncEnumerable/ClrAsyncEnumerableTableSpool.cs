using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.schema;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="TableSpool"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// A tee: every row passes through, and the rows of the round are left in the table's collection for
    /// whatever reads it next. That is what carries one round of a recursive query to the following one.
    ///
    /// <para>The collection is reached by asking the root schema for the table by name, as Calcite reaches
    /// it: it belongs to the <c>DataContext</c> a run is given, and a plan compiled once can be bound more
    /// than once.</para>
    /// </remarks>
    public class ClrAsyncEnumerableTableSpool : TableSpool, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableTableSpool"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="readType"></param>
        /// <param name="writeType"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableTableSpool Create(RelNode input, Spool.Type readType, Spool.Type writeType, RelOptTable table)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => mq.collations(input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => mq.distribution(input)));

            return new ClrAsyncEnumerableTableSpool(cluster, traitSet, input, readType, writeType, table);
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
        public ClrAsyncEnumerableTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, Spool.Type readType, Spool.Type writeType, RelOptTable table) :
            base(cluster, traitSet, input, readType, writeType, table)
        {

        }

        /// <inheritdoc />
        protected override Spool copy(RelTraitSet traitSet, RelNode input, Spool.Type readType, Spool.Type writeType)
        {
            return new ClrAsyncEnumerableTableSpool(getCluster(), traitSet, input, readType, writeType, getTable());
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (readType.name() != nameof(Spool.Type.LAZY) || writeType.name() != nameof(Spool.Type.LAZY))
                throw new java.lang.UnsupportedOperationException("only LAZY read and LAZY write are supported");

            var result = implementor.VisitChild(this, 0, (ClrAsyncEnumerableRel)getInput(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(result.Format));

            // the table is looked up in the schema the plan is bound with, as Calcite looks it up, rather than
            // read here and held: the collection belongs to the DataContext a run is given, and a plan
            // compiled once can be bound more than once
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
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.LazyCollectionSpool.MakeGenericMethod(rowType),
                    collection,
                    result.Expression));
        }

        static readonly System.Reflection.MethodInfo DataContextGetRootSchema = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        static readonly System.Reflection.MethodInfo SchemaGetTable = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.SCHEMA_GET_TABLE.method);
        static readonly System.Reflection.MethodInfo ModifiableTableGetModifiableCollection = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);

    }

}
