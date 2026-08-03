using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="RepeatUnion"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// What WITH RECURSIVE becomes: the seed once, then the iterative part over and over until a round yields
    /// nothing. The iterative part reads what the spool beneath it left behind, so it is enumerated afresh each
    /// round rather than held.
    ///
    /// <para>Calcite puts the transient table into the root schema before running and takes it out after,
    /// because the Java it generates finds the table by name. Nothing of that is needed for the spool, which
    /// captures its collection; it remains needed for a scan of the same table, whose expression the schema SPI
    /// builds.</para>
    /// </remarks>
    public class ClrEnumerableRepeatUnion : RepeatUnion, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="seed"></param>
        /// <param name="iterative"></param>
        /// <param name="all"></param>
        /// <param name="iterationLimit"></param>
        /// <param name="transientTable"></param>
        public ClrEnumerableRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet, RelNode seed, RelNode iterative, bool all, int iterationLimit, RelOptTable transientTable) :
            base(cluster, traitSet, seed, iterative, all, iterationLimit, transientTable)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableRepeatUnion(getCluster(), traitSet, (RelNode)inputs.get(0), (RelNode)inputs.get(1), all, iterationLimit, getTransientTable());
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var body = new System.Collections.Generic.List<Expression>();
            Expression cleanUp = Expression.Constant(null, typeof(System.Action));

            // a scan resolves its table through the schema at run time, because that is what the schema SPI
            // builds its expression to do, so the scratch table has to be there while the query runs. This is
            // the part of Calcite's arrangement that is not merely a Janino artefact.
            var transientTable = getTransientTable();
            if (transientTable != null)
            {
                var name = (string)transientTable.getQualifiedName().get(transientTable.getQualifiedName().size() - 1);
                var rootSchema = Expression.Call(implementor.Root, DataContextGetRootSchema);
                var table = Expression.Constant(transientTable.unwrap(typeof(org.apache.calcite.schema.Table)), typeof(org.apache.calcite.schema.Table));

                body.Add(Expression.Call(rootSchema, SchemaPlusAdd, Expression.Constant(name), table));
                cleanUp = Expression.Lambda<System.Action>(
                    Expression.Call(Expression.Call(implementor.Root, DataContextGetRootSchema), SchemaPlusRemoveTable, Expression.Constant(name)));
            }

            var seedResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getSeedRel(), pref);
            var iterationResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getIterativeRel(), pref);

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.prefer(seedResult.Format));
            var rowType = TypeResolver.Resolve(seedResult.PhysType.getJavaRowType());

            body.Add(
                Expression.Call(null,
                    ClrBuiltInMethod.RepeatUnion.MakeGenericMethod(rowType),
                    seedResult.Expression,
                    iterationResult.Expression,
                    Expression.Constant(iterationLimit),
                    Expression.Constant(all),
                    ClrPhysTypes.Comparer(implementor, physType),
                    cleanUp));

            return implementor.Result(physType, body.Count == 1 ? body[0] : Expression.Block(body));
        }

        static readonly System.Reflection.MethodInfo DataContextGetRootSchema = MethodResolver.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        static readonly System.Reflection.MethodInfo SchemaPlusAdd = MethodResolver.Resolve(org.apache.calcite.util.BuiltInMethod.SCHEMA_PLUS_ADD_TABLE.method);
        static readonly System.Reflection.MethodInfo SchemaPlusRemoveTable = MethodResolver.Resolve(org.apache.calcite.util.BuiltInMethod.SCHEMA_PLUS_REMOVE_TABLE.method);

    }

}
