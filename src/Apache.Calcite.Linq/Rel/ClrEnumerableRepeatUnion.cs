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
    /// <para>The transient table goes into the root schema before the query runs and comes out after, as
    /// Calcite does it. That is not a Janino artefact: both the spool above it and the scan of the same table
    /// find it by name in the schema the plan is bound with.</para>
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
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var body = new System.Collections.Generic.List<Expression>();
            Expression cleanUp = Expression.Constant(null, typeof(System.Action));

            // the scratch table has to be in the schema while the query runs, because everything that reads it
            // resolves it there by name
            var transientTable = getTransientTable();
            if (transientTable != null)
            {
                var name = (string)transientTable.getQualifiedName().get(transientTable.getQualifiedName().size() - 1);
                var rootSchema = Expression.Call(implementor.Root, DataContextGetRootSchema);
                // a TransientTable, which is what Calcite unwraps and stashes, and refused rather than
                // written into the plan as a null the schema would reject when the query runs
                var scratch = (org.apache.calcite.schema.TransientTable)transientTable.unwrap((java.lang.Class)typeof(org.apache.calcite.schema.TransientTable))
                    ?? throw new java.lang.IllegalStateException($"{transientTable} is not a TransientTable");
                var table = Expression.Constant(scratch, typeof(org.apache.calcite.schema.TransientTable));

                body.Add(Expression.Call(rootSchema, SchemaPlusAdd, Expression.Constant(name), table));
                cleanUp = Expression.Lambda<System.Action>(
                    Expression.Call(Expression.Call(implementor.Root, DataContextGetRootSchema), SchemaPlusRemoveTable, Expression.Constant(name)));
            }

            var seedResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getSeedRel(), pref);
            var iterationResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getIterativeRel(), pref);

            // the seed's own format, and not re-optimised: a repeat union yields the rows of its two inputs
            // unchanged, so its physical type is theirs. PARITY.md 6.9.
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), seedResult.Format, false);
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
