using System.Collections.Generic;
using System.Linq.Expressions;


using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of a union in the <see cref="ClrCursorConvention"/> calling convention that merges
    /// inputs already sorted on the node's collation, producing its rows in that order.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableMergeUnion</c>, which likewise extends the plain union. Every input must satisfy
    /// the node's collation.
    ///
    /// <para>The inputs are passed to the operator as openers rather than opened cursors, because linq4j's
    /// <c>MergeUnionEnumerator</c> acquires each input itself, in order, inside its constructor. Each body
    /// builds only the openers of its own kind.</para>
    /// </remarks>
    public class ClrCursorMergeUnion : ClrCursorUnion
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorMergeUnion"/> in <see cref="ClrCursorConvention"/> with the given
        /// collation.
        /// </summary>
        /// <param name="collation">The collation every input is sorted on and the union produces.</param>
        /// <param name="inputs">The inputs, a list of <see cref="RelNode"/>; the cluster is taken from the first.</param>
        /// <param name="all">Whether duplicates are kept (<c>UNION ALL</c>).</param>
        /// <returns>The new union.</returns>
        public static ClrCursorMergeUnion Create(RelCollation collation, java.util.List inputs, bool all)
        {
            var cluster = ((RelNode)inputs.get(0)).getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance).replace(collation);

            return new ClrCursorMergeUnion(cluster, traitSet, inputs, all);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> builds the trait set from a collation; this
        /// constructor takes it as given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits, which must carry a non-empty collation.</param>
        /// <param name="inputs">The inputs, a list of <see cref="RelNode"/>.</param>
        /// <param name="all">Whether duplicates are kept (<c>UNION ALL</c>).</param>
        /// <exception cref="java.lang.IllegalArgumentException">The trait set has no collation, or an input
        /// does not satisfy it.</exception>
        public ClrCursorMergeUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {
            var collations = traitSet.getCollations();
            if (collations.isEmpty() || ((RelCollation)collations.get(0)).getFieldCollations().isEmpty())
                throw new java.lang.IllegalArgumentException("ClrCursorMergeUnion with no collation");

            for (int i = 0; i < inputs.size(); i++)
            {
                // getCollations rather than getCollation, because the trait may be a RelCompositeTrait of
                // several collations; each of the node's must be satisfied by at least one of the input's
                var inputCollations = ((RelNode)inputs.get(i)).getTraitSet().getCollations();

                for (int j = 0; j < collations.size(); j++)
                {
                    var collation = (RelCollation)collations.get(j);
                    var satisfied = false;
                    for (int k = 0; k < inputCollations.size() && satisfied == false; k++)
                        satisfied = ((RelCollation)inputCollations.get(k)).satisfies(collation);

                    if (satisfied == false)
                        throw new java.lang.IllegalArgumentException(
                            $"ClrCursorMergeUnion input does not satisfy collation. ClrCursorMergeUnion collation: {collation}. Input collations: {inputCollations}. Input: {inputs.get(i)}");
                }
            }
        }

        /// <inheritdoc />
        public override org.apache.calcite.rel.core.SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrCursorMergeUnion(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public override ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            // the merge reads all inputs at once, so their openers go into one list, as in the block
            // EnumerableMergeUnion generates
            var sources = Expression.Variable(typeof(java.util.List), "mergeUnionInputs");
            var body = new List<Expression>
            {
                Expression.Assign(sources, Expression.New(ArrayListConstructor)),
            };

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrCursorRel)getInputs().get(i), pref);
                body.Add(Expression.Call(sources, CollectionAdd, Expression.Convert(implementor.Opener(result), typeof(object))));
            }

            var collation = getTraitSet().getCollation();
            if (collation == null || collation.getFieldCollations().isEmpty())
                throw new java.lang.IllegalStateException("ClrCursorMergeUnion with no collation");

            var (sortKeySelector, collationComparator) = physType.GenerateCollationKey(collation.getFieldCollations());
            var sortComparator = collationComparator == null
                ? Expression.Constant(null, typeof(java.util.Comparator))
                : collationComparator;

            body.Add(
                Expression.Call(null,
                    ClrCursorBuiltInMethod.MergeUnion.MakeGenericMethod(rowType, sortKeySelector.ReturnType),
                    sources,
                    sortKeySelector,
                    sortComparator,
                    Expression.Constant(all),
                    physType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));

            return implementor.Result(physType, Expression.Block(body[^1].Type, [sources], body));
        }

        /// <inheritdoc />
        public override ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            // the merge reads all inputs at once, so their openers go into one list, as in the block
            // EnumerableMergeUnion generates
            var sources = Expression.Variable(typeof(java.util.List), "mergeUnionInputs");
            var body = new List<Expression>
            {
                Expression.Assign(sources, Expression.New(ArrayListConstructor)),
            };

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrCursorRel)getInputs().get(i), pref);
                body.Add(Expression.Call(sources, CollectionAdd, Expression.Convert(implementor.OpenerAsync(result), typeof(object))));
            }

            var collation = getTraitSet().getCollation();
            if (collation == null || collation.getFieldCollations().isEmpty())
                throw new java.lang.IllegalStateException("ClrCursorMergeUnion with no collation");

            var (sortKeySelector, collationComparator) = physType.GenerateCollationKey(collation.getFieldCollations());
            var sortComparator = collationComparator == null
                ? Expression.Constant(null, typeof(java.util.Comparator))
                : collationComparator;

            body.Add(
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.MergeUnionAsync.MakeGenericMethod(rowType, sortKeySelector.ReturnType),
                    sources,
                    sortKeySelector,
                    sortComparator,
                    Expression.Constant(all),
                    physType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));

            return implementor.ResultAsync(physType, Expression.Block(body[^1].Type, [sources], body));
        }

        static readonly System.Reflection.ConstructorInfo ArrayListConstructor = typeof(java.util.ArrayList).GetConstructor([])
            ?? throw new System.InvalidOperationException("java.util.ArrayList has no no-arg constructor.");

        /// <summary>
        /// <c>java.util.List.add(Object)</c>, which fills the list of input openers.
        /// </summary>
        static readonly System.Reflection.MethodInfo CollectionAdd = typeof(java.util.List).GetMethod("add", [typeof(object)])
            ?? throw new System.InvalidOperationException("java.util.List has no add(Object).");

    }

}
