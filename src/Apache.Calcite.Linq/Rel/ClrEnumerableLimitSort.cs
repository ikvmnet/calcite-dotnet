using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Sort"/> carrying a limit, in the <see cref="ClrEnumerableConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// A sort followed by a limit reads every row and orders all of them. This orders and limits together, so
    /// only as many rows as are wanted need be kept.
    /// </remarks>
    public class ClrEnumerableLimitSort : Sort, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableLimitSort"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrEnumerableLimitSort Create(RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance).replace(collation);

            return new ClrEnumerableLimitSort(cluster, traitSet, input, collation, offset, fetch);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        public ClrEnumerableLimitSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {

        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
        {
            return new ClrEnumerableLimitSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            // the input's own format, and not re-optimised: the rows are the input's, so its physical type
            // is theirs
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), result.Format, false);

            var inputPhysType = result.PhysType;
            var pair = inputPhysType.generateCollationKey(collation.getFieldCollations());
            var sourceType = inputPhysType.RowType();

            var keySelector = implementor.Translator.TranslateSelector((J.Expression)pair.getKey(), sourceType);
            var comparator = pair.getValue() == null
                ? Expression.Constant(null, typeof(java.util.Comparator))
                : implementor.Translator.Translate((J.Expression)pair.getValue());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.OrderByWithFetchAndOffset.MakeGenericMethod(sourceType, keySelector.ReturnType),
                    result.Expression,
                    keySelector,
                    comparator,
                    offset == null ? Expression.Constant(0) : ClrEnumerableLimit.Count(implementor, offset),
                    fetch == null ? Expression.Constant(int.MaxValue) : ClrEnumerableLimit.Count(implementor, fetch)));
        }

    }

}
