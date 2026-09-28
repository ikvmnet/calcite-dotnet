using System.Linq.Expressions;


using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Relational operator that converts the output of an <c>EnumerableConvention</c> sub-plan to
    /// <see cref="ClrCursorConvention"/>.
    /// </summary>
    /// <remarks>
    /// The sub-plan is implemented by Calcite's <c>EnumerableRelImplementor</c>, and the linq4j block it
    /// produces is translated to an expression tree rather than compiled with Janino. A cursor is opened over
    /// the <c>Enumerable</c> the block yields, which calls the sub-plan's <c>enumerator()</c> at open.
    /// </remarks>
    public class EnumerableToClrCursorConverter : ConverterImpl, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traits">The node's traits, in <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The sub-plan, in <c>EnumerableConvention</c>.</param>
        public EnumerableToClrCursorConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new EnumerableToClrCursorConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(ClrCursorConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            // the same map, so a value Calcite stashes reaches the DataContext this plan is bound with
            var enumerable = new EnumerableRelImplementor(implementor.RexBuilder, implementor.Map);

            // the sub-plan may read the outer row of a correlate of this convention through these
            implementor.ReplayCorrelVariables(enumerable);

            var result = enumerable.visitChild(null, 0, (EnumerableRel)getInput(), pref.ToCalcite());

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, result.physType.getRowType(), result.physType.getFormat(), false);
            var rowType = physType.RowType;
            var source = implementor.Translator.TranslateBody(result.block, typeof(org.apache.calcite.linq4j.Enumerable));

            return implementor.Result(physType,
                Expression.Call(null, ClrCursorBuiltInMethod.FromJava.MakeGenericMethod(rowType), source));
        }

        /// <inheritdoc />
        /// <remarks>
        /// A linq4j <c>Enumerator</c> cannot be awaited, so the sub-plan runs synchronously within the
        /// awaiting open and each <c>ReadAsync</c>.
        /// </remarks>
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            // the same map, so a value Calcite stashes reaches the DataContext this plan is bound with
            var enumerable = new EnumerableRelImplementor(implementor.RexBuilder, implementor.Map);

            // the sub-plan may read the outer row of a correlate of this convention through these
            implementor.ReplayCorrelVariables(enumerable);

            var result = enumerable.visitChild(null, 0, (EnumerableRel)getInput(), pref.ToCalcite());

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, result.physType.getRowType(), result.physType.getFormat(), false);
            var rowType = physType.RowType;
            var source = implementor.Translator.TranslateBody(result.block, typeof(org.apache.calcite.linq4j.Enumerable));

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.FromJavaAsync.MakeGenericMethod(rowType), source));
        }

    }

}
