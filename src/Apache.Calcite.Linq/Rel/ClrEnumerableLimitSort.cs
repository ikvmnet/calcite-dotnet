using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.util;

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
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var pair = inputPhysType.generateCollationKey(collation.getFieldCollations());
            var sourceType = TypeResolver.Resolve(inputPhysType.getJavaRowType());

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
                    offset == null ? Expression.Constant(0) : Count(implementor, offset),
                    fetch == null ? Expression.Constant(int.MaxValue) : Count(implementor, fetch)));
        }

        /// <summary>
        /// Returns the expression giving a row count, which is a literal unless the query was prepared with a
        /// parameter in its place.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="rexNode"></param>
        /// <returns></returns>
        static Expression Count(ClrEnumerableRelImplementor implementor, RexNode rexNode)
        {
            if (rexNode is RexDynamicParam param)
                return JavaCast.To(
                    Expression.Call(implementor.Root, DataContextGet, Expression.Constant("?" + param.getIndex())),
                    typeof(int));

            return Expression.Constant(RexLiteral.intValue(rexNode));
        }

        static readonly System.Reflection.MethodInfo DataContextGet = MethodResolver.Resolve(BuiltInMethod.DATA_CONTEXT_GET.method);

    }

}
