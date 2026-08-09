using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalRepeatUnion"/> to a <see cref="ClrAsyncEnumerableRepeatUnion"/>.
    /// </summary>
    public class ClrAsyncEnumerableRepeatUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableRepeatUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableRepeatUnionRule Create()
        {
            return (ClrAsyncEnumerableRepeatUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalRepeatUnion), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableRepeatUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableRepeatUnionRule>(c => new ClrAsyncEnumerableRepeatUnionRule(c)))
                .toRule(typeof(ClrAsyncEnumerableRepeatUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableRepeatUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (RepeatUnion)rel;
            var traitSet = union.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance);
            var seedRel = union.getSeedRel();
            var iterativeRel = union.getIterativeRel();

            return new ClrAsyncEnumerableRepeatUnion(
                union.getCluster(),
                traitSet,
                convert(seedRel, seedRel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                convert(iterativeRel, iterativeRel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                union.all,
                union.iterationLimit,
                union.getTransientTable());
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableSpool"/> to a <see cref="ClrAsyncEnumerableTableSpool"/>.
    /// </summary>
    public class ClrAsyncEnumerableTableSpoolRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableTableSpoolRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableTableSpoolRule Create()
        {
            return (ClrAsyncEnumerableTableSpoolRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableSpool), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableTableSpoolRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableTableSpoolRule>(c => new ClrAsyncEnumerableTableSpoolRule(c)))
                .toRule(typeof(ClrAsyncEnumerableTableSpoolRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableTableSpoolRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var spool = (TableSpool)rel;

            return ClrAsyncEnumerableTableSpool.Create(
                convert(spool.getInput(), spool.getInput().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                spool.readType,
                spool.writeType,
                spool.getTable());
        }

    }

}
