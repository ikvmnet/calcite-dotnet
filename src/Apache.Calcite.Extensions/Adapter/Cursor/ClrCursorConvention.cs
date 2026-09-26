using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Calling convention that returns results as a <see cref="Runtime.IClrCursor"/>: a forward-only
    /// cursor advanced synchronously or with await, as the reader chooses on each advance.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableConvention</c>, differing from it in what a plan is compiled
    /// <em>to</em> and in nothing about a row. A plan is a <see cref="ClrCursorFactory"/>, which opens a
    /// cursor synchronously or with await, and the cursor it opens has both <c>Read</c> and
    /// <c>ReadAsync(token)</c> over one position. That is the shape <c>DbDataReader</c> has, rather than a
    /// sequence's: a sequence states once, at <c>GetEnumerator</c> or <c>GetAsyncEnumerator</c>, whether it
    /// will be pulled or awaited and takes its cancellation at that moment, where a reader over a cursor
    /// never has to choose a mode in advance and never has to throw a per-call token away.
    ///
    /// <para>Register <see cref="ClrCursorRules.Rules"/> with the planner and ask for this convention
    /// on the root, then run <see cref="ClrCursorRules.CalcRules"/> as a second pass, which is what
    /// <c>Programs.standard</c> does with Calcite's own. The root of the plan that comes out is a
    /// <see cref="ClrCursorRel"/>, and <see cref="ClrCursorRelImplementor.ImplementRoot"/> turns it
    /// into the factory. A plan may hold nodes of this convention and of <c>EnumerableConvention</c> —
    /// converters exist in each direction and rows cross untouched — so a statement this convention has
    /// no node for is still planned.</para>
    ///
    /// <para>Everything about a <em>row</em> — the physical type, the row formats, the Rex translation,
    /// and the preference a consumer states for how a row is represented — is in the
    /// <c>Adapter.Enumerable</c> namespace, which mirrors Calcite's <c>adapter.enumerable</c> package, where
    /// the same machinery lives beside <c>EnumerableConvention</c>.</para>
    /// </remarks>
    public sealed class ClrCursorConvention : Convention.Impl
    {

        /// <summary>
        /// The single instance.
        /// </summary>
        public static readonly ClrCursorConvention Instance = new();

        /// <summary>
        /// Cost of a node of this convention versus implementing an equivalent node in a typical calling
        /// convention.
        /// </summary>
        public const double CostMultiplier = 1.0d;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        ClrCursorConvention() :
            base("CLR_CURSOR", typeof(ClrCursorRel))
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
                rel = ClrCursorSort.Create(rel, collation, null, null);

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
                    ClrCursorRelFactories.ClrCursorTableScanFactory,
                    ClrCursorRelFactories.ClrCursorProjectFactory,
                    ClrCursorRelFactories.ClrCursorFilterFactory,
                    ClrCursorRelFactories.ClrCursorSortFactory]));
        }

    }

}
