using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite;
using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Relational expression that applies a limit and/or offset, in the
    /// <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableLimit : SingleRel, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableLimit"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrEnumerableLimit Create(RelNode input, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => cluster.getMetadataQuery().collations(input)));

            return new ClrEnumerableLimit(cluster, traitSet, input, offset, fetch);
        }

        readonly RexNode? offset;
        readonly RexNode? fetch;

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        public ClrEnumerableLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input)
        {
            this.offset = offset;
            this.fetch = fetch;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List newInputs)
        {
            return new ClrEnumerableLimit(getCluster(), traitSet, (RelNode)sole(newInputs), offset, fetch);
        }

        /// <inheritdoc />
        public override RelWriter explainTerms(RelWriter pw)
        {
            return base.explainTerms(pw).itemIf("offset", offset, offset != null).itemIf("fetch", fetch, fetch != null);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), result.Format);

            var rowType = TypeResolver.Resolve(result.PhysType.getJavaRowType());
            var v = result.Expression;

            if (offset != null)
                v = Expression.Call(null, ClrBuiltInMethod.Skip.MakeGenericMethod(rowType), v, Count(implementor, offset));

            if (fetch != null)
                v = Expression.Call(null, ClrBuiltInMethod.Take.MakeGenericMethod(rowType), v, Count(implementor, fetch));

            return implementor.Result(physType, v);
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
                return implementor.Translator.Translate(
                    J.Expressions.convert_(
                        J.Expressions.call(
                            DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            J.Expressions.constant("?" + param.getIndex())),
                        java.lang.Integer.TYPE));

            return Expression.Constant(RexLiteral.intValue(rexNode));
        }

    }

}
