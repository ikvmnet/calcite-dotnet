using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Relational expression that applies a limit and/or offset, in the
    /// <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableLimit</c>.
    /// </remarks>
    public class ClrCursorLimit : SingleRel, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorLimit"/>, deriving its collation and distribution from its input.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="offset">The number of rows to skip, or null.</param>
        /// <param name="fetch">The maximum number of rows to return, or null.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorLimit Create(RelNode input, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.limit(mq, input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.limit(mq, input)));

            return new ClrCursorLimit(cluster, traitSet, input, offset, fetch);
        }

        readonly RexNode? offset;
        readonly RexNode? fetch;

        /// <summary>
        /// Gets the number of rows skipped, or null. <c>EnumerableLimit</c> exposes this as a public field.
        /// </summary>
        public RexNode? Offset => offset;

        /// <summary>
        /// Gets the maximum number of rows returned, or null. <c>EnumerableLimit</c> exposes this as a public
        /// field.
        /// </summary>
        public RexNode? Fetch => fetch;

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="offset">The number of rows to skip, or null.</param>
        /// <param name="fetch">The maximum number of rows to return, or null.</param>
        public ClrCursorLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input)
        {
            this.offset = offset;
            this.fetch = fetch;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List newInputs)
        {
            return new ClrCursorLimit(getCluster(), traitSet, (RelNode)sole(newInputs), offset, fetch);
        }

        /// <inheritdoc />
        public override RelWriter explainTerms(RelWriter pw)
        {
            return base.explainTerms(pw).itemIf("offset", offset, offset != null).itemIf("fetch", fetch, fetch != null);
        }

        /// <summary>
        /// Estimates the row count as <c>RelMdRowCount.getRowCount(EnumerableLimit, RelMetadataQuery)</c> does:
        /// the input's count less the offset, capped at the fetch.
        /// </summary>
        /// <param name="mq">The metadata query.</param>
        /// <returns>The estimated row count.</returns>
        /// <remarks>
        /// Calcite's handler is keyed on <c>EnumerableLimit</c>, so this node reaches the handler for
        /// <see cref="SingleRel"/>, which calls this method. Unlike Calcite's handler, this cannot answer null
        /// when the input's count is unknown; it unboxes the count, as <c>SingleRel.estimateRowCount</c> does.
        /// </remarks>
        public override double estimateRowCount(RelMetadataQuery mq)
        {
            var rowCount = mq.getRowCount(getInput()).doubleValue();

            var offset = RelMdUtil.literalValueApproximatedByDouble(this.offset, 0D);
            var rows = java.lang.Math.max(rowCount - offset, 0D);

            var limit = RelMdUtil.literalValueApproximatedByDouble(fetch, rows);
            return limit < rows ? limit : rows;
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var rowType = result.PhysType.RowType;
            var v = result.Expression;
            var roundingPolicy = RoundingPolicy(implementor);

            if (offset != null)
                v = Expression.Call(null, ClrCursorBuiltInMethod.SkipBigDecimal.MakeGenericMethod(rowType), v, Count(implementor, offset, "OFFSET", roundingPolicy));

            if (fetch != null)
                v = Expression.Call(null, ClrCursorBuiltInMethod.TakeBigDecimal.MakeGenericMethod(rowType), v, Count(implementor, fetch, "FETCH", roundingPolicy));

            return implementor.Result(physType, v);
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var rowType = result.PhysType.RowType;
            var v = result.Expression;
            var roundingPolicy = RoundingPolicy(implementor);

            if (offset != null)
                v = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SkipBigDecimalAsync.MakeGenericMethod(rowType), v, Count(implementor, offset, "OFFSET", roundingPolicy));

            if (fetch != null)
                v = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.TakeBigDecimalAsync.MakeGenericMethod(rowType), v, Count(implementor, fetch, "FETCH", roundingPolicy));

            return implementor.ResultAsync(physType, v);
        }

        /// <summary>
        /// Returns an expression evaluating an offset or fetch to a <c>BigDecimal</c>, validated and rounded by
        /// <c>EnumUtils.numberToBigDecimal</c>.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="rexNode">The offset or fetch: a literal, a dynamic parameter, or another expression.</param>
        /// <param name="kind"><c>OFFSET</c> or <c>FETCH</c>, for the error message.</param>
        /// <param name="roundingPolicy">The expression from <see cref="RoundingPolicy"/>.</param>
        /// <returns>The count expression.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableLimit.getExpression</c>.
        /// </remarks>
        internal static Expression Count(ClrCursorRelImplementor implementor, RexNode rexNode, string kind, Expression roundingPolicy)
        {
            Expression value;

            if (rexNode is RexDynamicParam param)
                // not converted: NumberToBigDecimal checks that whatever was bound is a number
                value = Expression.Call(implementor.Root, DataContextGet, Expression.Constant("?" + param.getIndex()));
            else if (rexNode is RexLiteral literal)
                value = Expression.Constant(RexLiteral.bigDecimalValue(literal), typeof(object));
            else
                // any other expression is translated by Calcite's translator and converted immediately. It goes
                // through translateList because every translate overload is package private; a list of one is
                // the same call
                value = ClrEnumUtils.Convert(
                    implementor.Translator.Translate(
                        (org.apache.calcite.linq4j.tree.Expression)RexToLixTranslator
                            .forAggregation(implementor.TypeFactory, new org.apache.calcite.linq4j.tree.BlockBuilder(), null, implementor.Conformance)
                            .translateList(com.google.common.collect.ImmutableList.of(rexNode))
                            .get(0)),
                    typeof(object));

            return Expression.Call(null, NumberToBigDecimal, value, Expression.Constant(kind), roundingPolicy);
        }

        /// <summary>
        /// Returns an expression giving the policy by which a FETCH or OFFSET is rounded.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <returns>The policy the caller put in the implementor's map under
        /// <see cref="ClrCursorRelImplementor.FetchOffsetRoundingPolicy"/>, or
        /// <c>FetchOffsetRoundingPolicy.NONE</c>.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableLimit.getRoundingPolicy</c>.
        /// </remarks>
        internal static Expression RoundingPolicy(ClrCursorRelImplementor implementor)
        {
            var policy = implementor.Map.get(ClrCursorRelImplementor.FetchOffsetRoundingPolicy);

            return policy == null
                ? Expression.Constant(FetchOffsetRoundingPolicy.NONE, typeof(FetchOffsetRoundingPolicy))
                : implementor.Stash(policy, (java.lang.Class)typeof(FetchOffsetRoundingPolicy));
        }

        /// <summary>
        /// <c>EnumUtils.numberToBigDecimal</c>, which checks that a FETCH or OFFSET is a non-negative number
        /// and applies the rounding policy.
        /// </summary>
        static readonly System.Reflection.MethodInfo NumberToBigDecimal = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.NUMBER_TO_BIG_DECIMAL_LIMIT.method);

        /// <summary>
        /// <c>DataContext.get</c>, through which a dynamic parameter's value is read.
        /// </summary>
        static readonly System.Reflection.MethodInfo DataContextGet = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET.method);

    }

}
