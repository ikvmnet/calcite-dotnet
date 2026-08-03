using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> to a <see cref="ClrEnumerableHashJoin"/>.
    /// </summary>
    public class ClrEnumerableJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableJoinRule Create()
        {
            return (ClrEnumerableJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableJoinRule>(c => new ClrEnumerableJoinRule(c)))
                .toRule(typeof(ClrEnumerableJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var join = (Join)rel;

            // a join with no equality to build a lookup on is not this rule's; it stays for the nested loop
            if (join.analyzeCondition().leftKeys.isEmpty())
                return null!;

            var traitSet = join.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableHashJoin(
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
