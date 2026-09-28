using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="RepeatUnion"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableRepeatUnion</c>, which implements <c>WITH RECURSIVE</c>: the seed is read once,
    /// then the iterative input repeatedly until a round produces no rows or the iteration limit is reached.
    ///
    /// <para>The seed is acquired at open, as in linq4j, so it is an opened cursor of the body's own kind. The
    /// iterative input is acquired afresh inside each round's advance, which may be of either kind, so each
    /// body visits it through both hierarchies and passes both openers.</para>
    ///
    /// <para>As in Calcite, the transient table is added to the root schema at open, before the seed is
    /// opened, and removed by a clean-up action when the cursor is disposed; the spool and the scan that share
    /// it find it there by name.</para>
    /// </remarks>
    public class ClrCursorRepeatUnion : RepeatUnion, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="seed">The non-recursive input, read once.</param>
        /// <param name="iterative">The recursive input, read once per round.</param>
        /// <param name="all">Whether duplicates are kept (<c>UNION ALL</c>).</param>
        /// <param name="iterationLimit">The maximum number of rounds, or a negative value for no limit.</param>
        /// <param name="transientTable">The table holding the previous round's rows, or <see langword="null"/>.</param>
        public ClrCursorRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet, RelNode seed, RelNode iterative, bool all, int iterationLimit, RelOptTable transientTable) :
            base(cluster, traitSet, seed, iterative, all, iterationLimit, transientTable)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorRepeatUnion(getCluster(), traitSet, (RelNode)inputs.get(0), (RelNode)inputs.get(1), all, iterationLimit, getTransientTable());
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var body = new System.Collections.Generic.List<Expression>();
            Expression cleanUp = Expression.Constant(null, typeof(System.Action));

            // the transient table must be in the schema while the query runs, because the nodes that use it
            // resolve it there by name
            var transientTable = getTransientTable();
            if (transientTable != null)
            {
                var name = (string)transientTable.getQualifiedName().get(transientTable.getQualifiedName().size() - 1);
                var rootSchema = Expression.Call(implementor.Root, DataContextGetRootSchema);
                // Calcite unwraps a TransientTable too; failing here is clearer than adding a null to the
                // schema at run time
                var scratch = (org.apache.calcite.schema.TransientTable)transientTable.unwrap((java.lang.Class)typeof(org.apache.calcite.schema.TransientTable))
                    ?? throw new java.lang.IllegalStateException($"{transientTable} is not a TransientTable");
                var table = Expression.Constant(scratch, typeof(org.apache.calcite.schema.TransientTable));

                body.Add(Expression.Call(rootSchema, SchemaPlusAdd, Expression.Constant(name), table));
                cleanUp = Expression.Lambda<System.Action>(
                    Expression.Call(Expression.Call(implementor.Root, DataContextGetRootSchema), SchemaPlusRemoveTable, Expression.Constant(name)));
            }

            var seedResult = implementor.VisitChild(this, 0, (ClrCursorRel)getSeedRel(), pref);
            var iterationResult = implementor.VisitChild(this, 1, (ClrCursorRel)getIterativeRel(), pref);

            // a round starts inside an advance, which may be an awaiting one
            var iterationResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getIterativeRel(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(seedResult.Format));
            var rowType = seedResult.PhysType.RowType;

            body.Add(
                Expression.Call(null,
                    ClrCursorBuiltInMethod.RepeatUnion.MakeGenericMethod(rowType),
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
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var body = new System.Collections.Generic.List<Expression>();
            Expression cleanUp = Expression.Constant(null, typeof(System.Action));

            // the transient table must be in the schema while the query runs, because the nodes that use it
            // resolve it there by name
            var transientTable = getTransientTable();
            if (transientTable != null)
            {
                var name = (string)transientTable.getQualifiedName().get(transientTable.getQualifiedName().size() - 1);
                var rootSchema = Expression.Call(implementor.Root, DataContextGetRootSchema);
                // Calcite unwraps a TransientTable too; failing here is clearer than adding a null to the
                // schema at run time
                var scratch = (org.apache.calcite.schema.TransientTable)transientTable.unwrap((java.lang.Class)typeof(org.apache.calcite.schema.TransientTable))
                    ?? throw new java.lang.IllegalStateException($"{transientTable} is not a TransientTable");
                var table = Expression.Constant(scratch, typeof(org.apache.calcite.schema.TransientTable));

                body.Add(Expression.Call(rootSchema, SchemaPlusAdd, Expression.Constant(name), table));
                cleanUp = Expression.Lambda<System.Action>(
                    Expression.Call(Expression.Call(implementor.Root, DataContextGetRootSchema), SchemaPlusRemoveTable, Expression.Constant(name)));
            }

            var seedResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getSeedRel(), pref);
            var iterationResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getIterativeRel(), pref);

            // a round starts inside an advance, which may be a synchronous one
            var iterationResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)getIterativeRel(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(seedResult.Format));
            var rowType = seedResult.PhysType.RowType;

            body.Add(
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.RepeatUnionAsync.MakeGenericMethod(rowType),
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
