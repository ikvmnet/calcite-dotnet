using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Sort"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableSort : Sort, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableSort"/>.
        /// </summary>
        /// <param name="child"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrEnumerableSort Create(RelNode child, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = child.getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance).replace(collation);

            return new ClrEnumerableSort(cluster, traitSet, child, collation, offset, fetch);
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
        public ClrEnumerableSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {
            if (offset != null || fetch != null)
                throw new java.lang.IllegalArgumentException("offset and fetch must be null");
        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
        {
            return new ClrEnumerableSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var pair = inputPhysType.generateCollationKey(collation.getFieldCollations());

            var sourceType = TypeResolver.Resolve(inputPhysType.getJavaRowType());

            // Pair carries left and right as fields and declares static methods of the same names, which C#
            // cannot tell apart; Map.Entry gives the same two values under names that are not overloaded
            var keySelector = implementor.Translator.TranslateSelector((J.Expression)pair.getKey(), sourceType);
            var comparator = pair.getValue() == null
                ? Expression.Constant(null, typeof(java.util.Comparator))
                : implementor.Translator.Translate((J.Expression)pair.getValue());

            var keyType = keySelector.ReturnType;

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.OrderBy.MakeGenericMethod(sourceType, keyType),
                    result.Expression,
                    keySelector,
                    comparator));
        }

    }

}
