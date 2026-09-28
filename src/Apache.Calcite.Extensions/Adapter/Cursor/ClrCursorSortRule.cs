using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> with no offset or fetch to a <see cref="ClrCursorSort"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableSortRule</c>. A sort with an offset or fetch is left to
    /// <see cref="ClrCursorLimitRule"/>.
    /// </remarks>
    public class ClrCursorSortRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The rule's configuration.</param>
        public ClrCursorSortRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var sort = (Sort)rel;

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
