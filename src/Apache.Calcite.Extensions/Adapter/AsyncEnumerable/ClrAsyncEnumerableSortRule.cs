using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> to a <see cref="ClrAsyncEnumerableSort"/>.
    /// </summary>
    public class ClrAsyncEnumerableSortRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableSortRule Create()
        {
            return (ClrAsyncEnumerableSortRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(Sort),
                    Convention.NONE,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrAsyncEnumerableSortRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableSortRule>(c => new ClrAsyncEnumerableSortRule(c)))
                .toRule(typeof(ClrAsyncEnumerableSortRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableSortRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var sort = (Sort)rel;

            // a sort carrying an offset or a fetch belongs to ClrAsyncEnumerableLimitRule, not to this one
            if (sort.offset != null || sort.fetch != null)
                return null!;

            var input = sort.getInput();

            return ClrAsyncEnumerableSort.Create(
                convert(input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                sort.getCollation(),
                null,
                null);
        }

    }

}
