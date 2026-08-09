using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> to a <see cref="ClrEnumerableSort"/>.
    /// </summary>
    public class ClrEnumerableSortRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableSortRule Create()
        {
            return (ClrEnumerableSortRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(Sort),
                    Convention.NONE,
                    ClrEnumerableConvention.Instance,
                    "ClrEnumerableSortRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableSortRule>(c => new ClrEnumerableSortRule(c)))
                .toRule(typeof(ClrEnumerableSortRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableSortRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var sort = (Sort)rel;

            // a sort carrying an offset or a fetch belongs to ClrEnumerableLimitRule, not to this one
            if (sort.offset != null || sort.fetch != null)
                return null;

            var input = sort.getInput();

            return ClrEnumerableSort.Create(
                convert(input, input.getTraitSet().replace(ClrEnumerableConvention.Instance)),
                sort.getCollation(),
                null,
                null);
        }

    }

}
