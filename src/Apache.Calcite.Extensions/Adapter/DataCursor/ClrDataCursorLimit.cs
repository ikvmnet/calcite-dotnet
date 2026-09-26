using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Relational expression that applies a limit and/or offset, in the
    /// <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    public class ClrDataCursorLimit : SingleRel, ClrDataCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorLimit"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrDataCursorLimit Create(RelNode input, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrDataCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.limit(mq, input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.limit(mq, input)));

            return new ClrDataCursorLimit(cluster, traitSet, input, offset, fetch);
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
        public ClrDataCursorLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input)
        {
            this.offset = offset;
            this.fetch = fetch;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List newInputs)
        {
            return new ClrDataCursorLimit(getCluster(), traitSet, (RelNode)sole(newInputs), offset, fetch);
        }

        /// <inheritdoc />
        public override RelWriter explainTerms(RelWriter pw)
        {
            return base.explainTerms(pw).itemIf("offset", offset, offset != null).itemIf("fetch", fetch, fetch != null);
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrDataCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var rowType = result.PhysType.RowType;
            var v = result.Expression;
            var roundingPolicy = RoundingPolicy(implementor);

            if (offset != null)
                v = Expression.Call(null, ClrDataCursorBuiltInMethod.SkipBigDecimal.MakeGenericMethod(rowType), v, Count(implementor, offset, "OFFSET", roundingPolicy));

            if (fetch != null)
                v = Expression.Call(null, ClrDataCursorBuiltInMethod.TakeBigDecimal.MakeGenericMethod(rowType), v, Count(implementor, fetch, "FETCH", roundingPolicy));

            return implementor.Result(physType, v);
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrDataCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var rowType = result.PhysType.RowType;
            var v = result.Expression;
            var roundingPolicy = RoundingPolicy(implementor);

            if (offset != null)
                v = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.SkipBigDecimalAsync.MakeGenericMethod(rowType), v, Count(implementor, offset, "OFFSET", roundingPolicy));

            if (fetch != null)
                v = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.TakeBigDecimalAsync.MakeGenericMethod(rowType), v, Count(implementor, fetch, "FETCH", roundingPolicy));

            return implementor.ResultAsync(physType, v);
        }

        /// <summary>
        /// Returns the expression giving a row count, which is a literal unless the query was prepared with a
        /// parameter in its place.
        /// </summary>
        /// <remarks>
        /// <c>ClrEnumerableLimit.Count</c>, over this convention's implementor: nothing of Calcite's
        /// generates this, so nothing here is linq4j but the one expression Calcite's translator produces,
        /// which is translated where it is produced.
        /// </remarks>
        internal static Expression Count(ClrDataCursorRelImplementor implementor, RexNode rexNode, string kind, Expression roundingPolicy)
        {
            Expression value;

            if (rexNode is RexDynamicParam param)
                // no conversion: what the parameter holds is whatever was bound, and NumberToBigDecimal is
                // what decides whether that is a number at all
                value = Expression.Call(implementor.Root, DataContextGet, Expression.Constant("?" + param.getIndex()));
            else if (rexNode is RexLiteral literal)
                value = Expression.Constant(RexLiteral.bigDecimalValue(literal), typeof(object));
            else
                // an expression rather than a literal or a parameter, which the int reading could not take.
                // Calcite's translator produces it, so it arrives as linq4j and is translated where it is
                // produced rather than composed into a larger tree first
                // through translateList, because every translate overload is package private and only the
                // list forms are reachable -- a list of one is the same call by a name that can be said
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
        /// Returns the expression giving the policy a FETCH or an OFFSET is rounded by.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableLimit.getRoundingPolicy</c>. Whatever a caller stashed under
        /// <c>_fetchOffsetRoundingPolicy</c>, and <c>FetchOffsetRoundingPolicy.NONE</c> where none did.
        /// </remarks>
        internal static Expression RoundingPolicy(ClrDataCursorRelImplementor implementor)
        {
            var policy = implementor.Map.get(ClrDataCursorRelImplementor.FetchOffsetRoundingPolicy);

            return policy == null
                ? Expression.Constant(FetchOffsetRoundingPolicy.NONE, typeof(FetchOffsetRoundingPolicy))
                : implementor.Stash(policy, (java.lang.Class)typeof(FetchOffsetRoundingPolicy));
        }

        /// <summary>
        /// <c>EnumUtils.numberToBigDecimal</c>, which checks that a FETCH or OFFSET evaluated to a
        /// non-negative number and applies the rounding policy.
        /// </summary>
        static readonly System.Reflection.MethodInfo NumberToBigDecimal = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.NUMBER_TO_BIG_DECIMAL_LIMIT.method);

        /// <summary>
        /// <c>DataContext.get</c>, which a value prepared as a parameter arrives by.
        /// </summary>
        static readonly System.Reflection.MethodInfo DataContextGet = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET.method);

    }

}
