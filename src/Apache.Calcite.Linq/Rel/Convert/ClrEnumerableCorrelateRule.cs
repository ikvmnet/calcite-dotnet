using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCorrelate"/> to a <see cref="ClrEnumerableCorrelate"/>.
    /// </summary>
    public class ClrEnumerableCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableCorrelateRule Create()
        {
            return (ClrEnumerableCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalCorrelate), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableCorrelateRule>(c => new ClrEnumerableCorrelateRule(c)))
                .toRule(typeof(ClrEnumerableCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var correlate = (Correlate)rel;
            var traitSet = correlate.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableCorrelate(
                correlate.getCluster(),
                traitSet,
                RelOptRule.convert(correlate.getLeft(), traitSet),
                RelOptRule.convert(correlate.getRight(), traitSet),
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType());
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> with no equality to a
    /// <see cref="ClrEnumerableNestedLoopJoin"/>.
    /// </summary>
    public class ClrEnumerableNestedLoopJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableNestedLoopJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableNestedLoopJoinRule Create()
        {
            return (ClrEnumerableNestedLoopJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableNestedLoopJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableNestedLoopJoinRule>(c => new ClrEnumerableNestedLoopJoinRule(c)))
                .toRule(typeof(ClrEnumerableNestedLoopJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableNestedLoopJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var join = (Join)rel;
            var traitSet = join.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableNestedLoopJoin(
                join.getCluster(),
                traitSet,
                RelOptRule.convert(join.getLeft(), traitSet),
                RelOptRule.convert(join.getRight(), traitSet),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType());
        }

    }

}
