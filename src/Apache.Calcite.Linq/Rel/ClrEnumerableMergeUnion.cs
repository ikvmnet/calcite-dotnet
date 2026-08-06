using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of a union in the <see cref="ClrEnumerableConvention"/> calling convention, by merging
    /// inputs that are already sorted rather than reading them all first.
    /// </summary>
    /// <remarks>
    /// Chosen where an ORDER BY sits directly on a UNION, so the sort can be pushed to each input and the
    /// result taken in order. `EnumerableMergeUnion` extends `EnumerableUnion`, and so does this.
    /// </remarks>
    public class ClrEnumerableMergeUnion : ClrEnumerableUnion
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableMergeUnion"/>.
        /// </summary>
        /// <param name="collation"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        /// <returns></returns>
        public static ClrEnumerableMergeUnion Create(RelCollation collation, java.util.List inputs, bool all)
        {
            var cluster = ((RelNode)inputs.get(0)).getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance).replace(collation);

            return new ClrEnumerableMergeUnion(cluster, traitSet, inputs, all);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrEnumerableMergeUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {
            var collations = traitSet.getCollations();
            if (collations.isEmpty() || ((RelCollation)collations.get(0)).getFieldCollations().isEmpty())
                throw new java.lang.IllegalArgumentException("ClrEnumerableMergeUnion with no collation");

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
                            $"ClrEnumerableMergeUnion input does not satisfy collation. ClrEnumerableMergeUnion collation: {collation}. Input collations: {inputCollations}. Input: {inputs.get(i)}");
                }
            }
        }

        /// <inheritdoc />
        public override org.apache.calcite.rel.core.SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrEnumerableMergeUnion(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public override ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

            // the inputs go into a list, because the merge walks all of them at once rather than one after
            // the other; Calcite builds the same list into the block it generates
            var sources = Expression.Variable(typeof(java.util.List), "mergeUnionInputs");
            var body = new List<Expression>
            {
                Expression.Assign(sources, Expression.New(typeof(java.util.ArrayList).GetConstructor([])!)),
            };

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrEnumerableRel)getInputs().get(i), pref);
                body.Add(Expression.Call(sources, CollectionAdd, Expression.Convert(result.Expression, typeof(object))));
            }

            var collation = getTraitSet().getCollation();
            if (collation == null || collation.getFieldCollations().isEmpty())
                throw new java.lang.IllegalStateException("ClrEnumerableMergeUnion with no collation");

            var pair = physType.generateCollationKey(collation.getFieldCollations());
            var sortKeySelector = implementor.Translator.TranslateSelector((org.apache.calcite.linq4j.tree.Expression)pair.getKey(), rowType);
            var sortComparator = pair.getValue() == null
                ? Expression.Constant(null, typeof(java.util.Comparator))
                : implementor.Translator.Translate((org.apache.calcite.linq4j.tree.Expression)pair.getValue());

            body.Add(
                Expression.Call(null,
                    ClrBuiltInMethod.MergeUnion.MakeGenericMethod(rowType, sortKeySelector.ReturnType),
                    sources,
                    sortKeySelector,
                    sortComparator,
                    Expression.Constant(all),
                    ClrPhysTypes.Comparer(implementor, physType)));

            return implementor.Result(physType, Expression.Block(body[^1].Type, [sources], body));
        }

        static readonly System.Reflection.MethodInfo CollectionAdd = typeof(java.util.List).GetMethod("add", [typeof(object)])!;

    }

}
