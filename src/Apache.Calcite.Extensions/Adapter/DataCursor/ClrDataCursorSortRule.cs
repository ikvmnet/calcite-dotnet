using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> to a <see cref="ClrDataCursorSort"/>.
    /// </summary>
    public class ClrDataCursorSortRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorSortRule Create()
        {
            return (ClrDataCursorSortRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(Sort),
                    Convention.NONE,
                    ClrDataCursorConvention.Instance,
                    "ClrDataCursorSortRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorSortRule>(c => new ClrDataCursorSortRule(c)))
                .toRule(typeof(ClrDataCursorSortRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorSortRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var sort = (Sort)rel;

            // a sort carrying an offset or a fetch belongs to ClrDataCursorLimitRule, not to this one
            if (sort.offset != null || sort.fetch != null)
                return null;

            var input = sort.getInput();

            return ClrDataCursorSort.Create(
                convert(input, input.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                sort.getCollation(),
                null,
                null);
        }

    }

}
