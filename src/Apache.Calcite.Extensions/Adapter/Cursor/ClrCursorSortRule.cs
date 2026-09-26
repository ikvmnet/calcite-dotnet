using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> to a <see cref="ClrCursorSort"/>.
    /// </summary>
    public class ClrCursorSortRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorSortRule Create()
        {
            return (ClrCursorSortRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(Sort),
                    Convention.NONE,
                    ClrCursorConvention.Instance,
                    "ClrCursorSortRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorSortRule>(c => new ClrCursorSortRule(c)))
                .toRule(typeof(ClrCursorSortRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorSortRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var sort = (Sort)rel;

            // a sort carrying an offset or a fetch belongs to ClrCursorLimitRule, not to this one
            if (sort.offset != null || sort.fetch != null)
                return null;

            var input = sort.getInput();

            return ClrCursorSort.Create(
                convert(input, input.getTraitSet().replace(ClrCursorConvention.Instance)),
                sort.getCollation(),
                null,
                null);
        }

    }

}
