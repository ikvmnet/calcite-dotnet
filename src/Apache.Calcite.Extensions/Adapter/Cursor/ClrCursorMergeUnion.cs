using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of a union in the <see cref="ClrCursorConvention"/> calling convention, by merging
    /// inputs that are already sorted rather than reading them all first.
    /// </summary>
    /// <remarks>
    /// Chosen where an ORDER BY sits directly on a UNION, so the sort can be pushed to each input and the
    /// result taken in order. `EnumerableMergeUnion` extends `EnumerableUnion`, and so does this.
    ///
    /// <para>The inputs go into the list as opens rather than as cursors, because linq4j's
    /// <c>MergeUnionEnumerator</c> acquires each of them itself, in order, inside its constructor: the
    /// operator runs each open in turn and then positions every input, all inside its own open. Each
    /// body defers within its own kind, so only the opener of that kind is built.</para>
    /// </remarks>
    public class ClrCursorMergeUnion : ClrCursorUnion
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorMergeUnion"/>.
        /// </summary>
        /// <param name="collation"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        /// <returns></returns>
        public static ClrCursorMergeUnion Create(RelCollation collation, java.util.List inputs, bool all)
        {
            var cluster = ((RelNode)inputs.get(0)).getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance).replace(collation);

            return new ClrCursorMergeUnion(cluster, traitSet, inputs, all);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrCursorMergeUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {
            var collations = traitSet.getCollations();
            if (collations.isEmpty() || ((RelCollation)collations.get(0)).getFieldCollations().isEmpty())
                throw new java.lang.IllegalArgumentException("ClrCursorMergeUnion with no collation");

            for (int i = 0; i < inputs.size(); i++)
            {
                // getCollations rather than getCollation, because the slot may hold a RelCompositeTrait of
                // several collations; each required one has to be satisfied by at least one of the input's
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
        public override ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            // the inputs go into a list, because the merge walks all of them at once rather than one after
            // the other; Calcite builds the same list into the block it generates
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
        public override ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            // the inputs go into a list, because the merge walks all of them at once rather than one after
            // the other; Calcite builds the same list into the block it generates
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
        /// <c>java.util.List.add</c>, which fills the list above.
        /// </summary>
        static readonly System.Reflection.MethodInfo CollectionAdd = typeof(java.util.List).GetMethod("add", [typeof(object)])
            ?? throw new System.InvalidOperationException("java.util.List has no add(Object).");

    }

}
