using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalRepeatUnion"/> to a <see cref="ClrCursorRepeatUnion"/>.
    /// </summary>
    public class ClrCursorRepeatUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorRepeatUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorRepeatUnionRule Create()
        {
            return (ClrCursorRepeatUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalRepeatUnion), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorRepeatUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorRepeatUnionRule>(c => new ClrCursorRepeatUnionRule(c)))
                .toRule(typeof(ClrCursorRepeatUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorRepeatUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (RepeatUnion)rel;
            var traitSet = union.getTraitSet().replace(ClrCursorConvention.Instance);
            var seedRel = union.getSeedRel();
            var iterativeRel = union.getIterativeRel();

            return new ClrCursorRepeatUnion(
                union.getCluster(),
                traitSet,
                convert(seedRel, seedRel.getTraitSet().replace(ClrCursorConvention.Instance)),
                convert(iterativeRel, iterativeRel.getTraitSet().replace(ClrCursorConvention.Instance)),
                union.all,
                union.iterationLimit,
                union.getTransientTable());
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableSpool"/> to a <see cref="ClrCursorTableSpool"/>.
    /// </summary>
    public class ClrCursorTableSpoolRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorTableSpoolRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorTableSpoolRule Create()
        {
            return (ClrCursorTableSpoolRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableSpool), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorTableSpoolRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorTableSpoolRule>(c => new ClrCursorTableSpoolRule(c)))
                .toRule(typeof(ClrCursorTableSpoolRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorTableSpoolRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var spool = (TableSpool)rel;

            return ClrCursorTableSpool.Create(
                convert(spool.getInput(), spool.getInput().getTraitSet().replace(ClrCursorConvention.Instance)),
                spool.readType,
                spool.writeType,
                spool.getTable());
        }

    }

}
