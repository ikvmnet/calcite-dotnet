using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalRepeatUnion"/> to a <see cref="ClrEnumerableRepeatUnion"/>.
    /// </summary>
    public class ClrEnumerableRepeatUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableRepeatUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableRepeatUnionRule Create()
        {
            return (ClrEnumerableRepeatUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalRepeatUnion), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableRepeatUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableRepeatUnionRule>(c => new ClrEnumerableRepeatUnionRule(c)))
                .toRule(typeof(ClrEnumerableRepeatUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableRepeatUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var union = (RepeatUnion)rel;
            var traitSet = union.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableRepeatUnion(
                union.getCluster(),
                traitSet,
                RelOptRule.convert(union.getSeedRel(), traitSet),
                RelOptRule.convert(union.getIterativeRel(), traitSet),
                union.all,
                union.iterationLimit,
                union.getTransientTable());
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableSpool"/> to a <see cref="ClrEnumerableTableSpool"/>.
    /// </summary>
    public class ClrEnumerableTableSpoolRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableTableSpoolRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableTableSpoolRule Create()
        {
            return (ClrEnumerableTableSpoolRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableSpool), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableTableSpoolRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableTableSpoolRule>(c => new ClrEnumerableTableSpoolRule(c)))
                .toRule(typeof(ClrEnumerableTableSpoolRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableTableSpoolRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var spool = (TableSpool)rel;
            var traitSet = spool.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableTableSpool(
                spool.getCluster(),
                traitSet,
                RelOptRule.convert(spool.getInput(), traitSet),
                Spool.Type.LAZY,
                Spool.Type.LAZY,
                spool.getTable());
        }

    }

}
