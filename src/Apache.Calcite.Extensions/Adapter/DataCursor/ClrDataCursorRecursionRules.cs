using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalRepeatUnion"/> to a <see cref="ClrDataCursorRepeatUnion"/>.
    /// </summary>
    public class ClrDataCursorRepeatUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorRepeatUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorRepeatUnionRule Create()
        {
            return (ClrDataCursorRepeatUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalRepeatUnion), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorRepeatUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorRepeatUnionRule>(c => new ClrDataCursorRepeatUnionRule(c)))
                .toRule(typeof(ClrDataCursorRepeatUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorRepeatUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (RepeatUnion)rel;
            var traitSet = union.getTraitSet().replace(ClrDataCursorConvention.Instance);
            var seedRel = union.getSeedRel();
            var iterativeRel = union.getIterativeRel();

            return new ClrDataCursorRepeatUnion(
                union.getCluster(),
                traitSet,
                convert(seedRel, seedRel.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                convert(iterativeRel, iterativeRel.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                union.all,
                union.iterationLimit,
                union.getTransientTable());
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableSpool"/> to a <see cref="ClrDataCursorTableSpool"/>.
    /// </summary>
    public class ClrDataCursorTableSpoolRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorTableSpoolRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorTableSpoolRule Create()
        {
            return (ClrDataCursorTableSpoolRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableSpool), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorTableSpoolRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorTableSpoolRule>(c => new ClrDataCursorTableSpoolRule(c)))
                .toRule(typeof(ClrDataCursorTableSpoolRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorTableSpoolRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var spool = (TableSpool)rel;

            return ClrDataCursorTableSpool.Create(
                convert(spool.getInput(), spool.getInput().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                spool.readType,
                spool.writeType,
                spool.getTable());
        }

    }

}
