using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;


using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Calling convention that returns results as an
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableConvention"/> node for node and rule for rule, over a sequence that is
    /// awaited. Everything about a <em>row</em> is the same: the same physical type, the same row formats,
    /// the same Rex translation, the same synthetic records.
    ///
    /// <para>Register <see cref="ClrAsyncEnumerableRules.Rules"/> with the planner and ask for this
    /// convention on the root; <see cref="ClrAsyncEnumerablePrograms"/> has the passes that takes. The root
    /// of the plan that comes out is a <see cref="ClrAsyncEnumerableRel"/>, and
    /// <see cref="ClrAsyncEnumerableRelImplementor.ImplementRoot"/> turns it into a lambda to compile.</para>
    ///
    /// <para><b>A plan cannot hold nodes of any other convention.</b> There is no converter to
    /// <c>EnumerableConvention</c> or to <see cref="ClrEnumerableConvention"/> and there will not be: an
    /// <see cref="System.Collections.Generic.IEnumerable{T}"/> behind an awaited interface is a blocking
    /// pull, and the reverse is sync over async. So a query this convention cannot cover whole does not plan
    /// at all, and the planner throws rather than quietly falling back to a plan that would block a thread
    /// per row. Its leaves are <see cref="Schema.IClrAsyncScannableTable"/> and nothing else.</para>
    /// </remarks>
    public sealed class ClrAsyncEnumerableConvention : Convention.Impl
    {

        /// <summary>
        /// The single instance.
        /// </summary>
        public static readonly ClrAsyncEnumerableConvention Instance = new();

        /// <summary>
        /// Cost of a node of this convention versus implementing an equivalent node in a typical calling
        /// convention.
        /// </summary>
        public const double CostMultiplier = 1.0d;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        ClrAsyncEnumerableConvention() :
            base("CLR_ASYNC_ENUMERABLE", typeof(ClrAsyncEnumerableRel))
        {

        }

        /// <inheritdoc />
        public override RelNode enforce(RelNode input, RelTraitSet required)
        {
            var rel = input;
            if (input.getConvention() != this)
            {
                rel = ConventionTraitDef.INSTANCE.convert(input.getCluster().getPlanner(), input, this, true);
                if (rel == null)
                    throw new java.lang.IllegalStateException($"Unable to convert input to {this}, input = {input}");
            }

            var collation = required.getCollation();
            if (collation != null && collation != RelCollations.EMPTY)
                rel = ClrAsyncEnumerableSort.Create(rel, collation, null, null);

            return rel;
        }

        /// <inheritdoc />
        public override bool canConvertConvention(Convention toConvention)
        {
            return false;
        }

        /// <inheritdoc />
        public override bool useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits)
        {
            return true;
        }

        /// <inheritdoc />
        public override RelFactories.Struct getRelFactories()
        {
            return RelFactories.Struct.fromContext(
                Contexts.of([
                    ClrAsyncEnumerableRelFactories.ClrAsyncEnumerableTableScanFactory,
                    ClrAsyncEnumerableRelFactories.ClrAsyncEnumerableProjectFactory,
                    ClrAsyncEnumerableRelFactories.ClrAsyncEnumerableFilterFactory,
                    ClrAsyncEnumerableRelFactories.ClrAsyncEnumerableSortFactory]));
        }

    }

}
