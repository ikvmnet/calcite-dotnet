using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="RepeatUnion"/> in the <see cref="ClrDataCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// What WITH RECURSIVE becomes: the seed once, then the iterative part over and over until a round yields
    /// nothing. The iterative part reads what the spool beneath it left behind, so it is opened afresh each
    /// round rather than held.
    ///
    /// <para>The seed is acquired at the open — linq4j's enumerator takes <c>seed.enumerator()</c> in a field
    /// initializer — so it arrives as an opened cursor of the body's own kind. The iterative part is acquired
    /// inside <c>moveNext</c>, once per round, so it arrives as opens — both opens, because the advance that
    /// starts a round may be either — through <see cref="ClrDataCursorRelImplementor.Opener"/> and
    /// <see cref="ClrDataCursorRelImplementor.OpenerAsync"/>, and each body visits it through both
    /// hierarchies to build them, as <see cref="ClrDataCursorUnion"/> does for a concat.</para>
    ///
    /// <para>The transient table goes into the root schema before the query runs and comes out after, as
    /// Calcite does it. That is not a Janino artefact: both the spool above it and the scan of the same table
    /// find it by name in the schema the plan is bound with. Adding it is the first statement of the open's
    /// block, so it is there before the seed is opened.</para>
    /// </remarks>
    public class ClrDataCursorRepeatUnion : RepeatUnion, ClrDataCursorRel
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
        public ClrDataCursorRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet, RelNode seed, RelNode iterative, bool all, int iterationLimit, RelOptTable transientTable) :
            base(cluster, traitSet, seed, iterative, all, iterationLimit, transientTable)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrDataCursorRepeatUnion(getCluster(), traitSet, (RelNode)inputs.get(0), (RelNode)inputs.get(1), all, iterationLimit, getTransientTable());
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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

            var seedResult = implementor.VisitChild(this, 0, (ClrDataCursorRel)getSeedRel(), pref);
            var iterationResult = implementor.VisitChild(this, 1, (ClrDataCursorRel)getIterativeRel(), pref);

            // the other hierarchy's open of the iterative part: a round is started inside an advance, and
            // the advance may be the awaiting one
            var iterationResultAsync = implementor.VisitChildAsync(this, 1, (ClrDataCursorRel)getIterativeRel(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(seedResult.Format));
            var rowType = seedResult.PhysType.RowType;

            body.Add(
                Expression.Call(null,
                    ClrDataCursorBuiltInMethod.RepeatUnion.MakeGenericMethod(rowType),
                    seedResult.Expression,
                    implementor.Opener(iterationResult),
                    implementor.OpenerAsync(iterationResultAsync),
                    Expression.Constant(iterationLimit),
                    Expression.Constant(all),
                    physType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    cleanUp));

            return implementor.Result(physType, body.Count == 1 ? body[0] : Expression.Block(body));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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

            var seedResult = implementor.VisitChildAsync(this, 0, (ClrDataCursorRel)getSeedRel(), pref);
            var iterationResult = implementor.VisitChildAsync(this, 1, (ClrDataCursorRel)getIterativeRel(), pref);

            // the other hierarchy's open of the iterative part: a round is started inside an advance, and
            // the advance may be the synchronous one
            var iterationResultSync = implementor.VisitChild(this, 1, (ClrDataCursorRel)getIterativeRel(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(seedResult.Format));
            var rowType = seedResult.PhysType.RowType;

            body.Add(
                ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.RepeatUnionAsync.MakeGenericMethod(rowType),
                    seedResult.Expression,
                    implementor.Opener(iterationResultSync),
                    implementor.OpenerAsync(iterationResult),
                    Expression.Constant(iterationLimit),
                    Expression.Constant(all),
                    physType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    cleanUp));

            return implementor.ResultAsync(physType, body.Count == 1 ? body[0] : Expression.Block(body));
        }

        static readonly System.Reflection.MethodInfo DataContextGetRootSchema = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        static readonly System.Reflection.MethodInfo SchemaPlusAdd = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.SCHEMA_PLUS_ADD_TABLE.method);
        static readonly System.Reflection.MethodInfo SchemaPlusRemoveTable = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.SCHEMA_PLUS_REMOVE_TABLE.method);

    }

}
