using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Calling convention that returns results as a <see cref="Runtime.ClrDataCursor"/>: a forward-only
    /// cursor advanced synchronously or with await, as the reader chooses on each advance.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableConvention</c>, as <see cref="Enumerable.ClrEnumerableConvention"/> is,
    /// and it differs from that one in what a plan is compiled <em>to</em> and in nothing about a row. A plan
    /// of <c>ClrEnumerableConvention</c> is a sequence, and a sequence states once — at <c>GetEnumerator</c>
    /// or <c>GetAsyncEnumerator</c> — whether it will be pulled or awaited, and takes its cancellation at
    /// the same moment. A plan of this convention is a <see cref="ClrDataCursorFactory"/>, which opens a
    /// cursor synchronously or with await, and the cursor it opens has both <c>Read</c> and
    /// <c>ReadAsync(token)</c> over one position. That is the shape <c>DbDataReader</c> has, and it is why
    /// this convention exists: a reader over it never has to choose a mode in advance and never has to
    /// throw a per-call token away.
    ///
    /// <para>Register <see cref="ClrDataCursorRules.Rules"/> with the planner and ask for this convention
    /// on the root, then run <see cref="ClrDataCursorRules.CalcRules"/> as a second pass, which is what
    /// <c>Programs.standard</c> does with Calcite's own. The root of the plan that comes out is a
    /// <see cref="ClrDataCursorRel"/>, and <see cref="ClrDataCursorRelImplementor.ImplementRoot"/> turns it
    /// into the factory. A plan may hold nodes of this convention and of <c>EnumerableConvention</c> —
    /// converters exist in each direction and rows cross untouched — so a statement this convention has
    /// no node for is still planned.</para>
    ///
    /// <para>What is shared with <c>ClrEnumerableConvention</c> is everything about a <em>row</em>: the
    /// physical type, the row formats, the Rex translation, and the preference a consumer states for how a
    /// row is represented. Those live in that convention's namespace and are used from here as they stand,
    /// because a row is the same object whichever kind of plan carries it.</para>
    /// </remarks>
    public sealed class ClrDataCursorConvention : Convention.Impl
    {

        /// <summary>
        /// The single instance.
        /// </summary>
        public static readonly ClrDataCursorConvention Instance = new();

        /// <summary>
        /// Cost of a node of this convention versus implementing an equivalent node in a typical calling
        /// convention.
        /// </summary>
        public const double CostMultiplier = 1.0d;

        /// <summary>
        /// Cost of a converter between this convention and <c>EnumerableConvention</c>, against one between
        /// this convention and <c>ClrEnumerableConvention</c>.
        /// </summary>
        /// <remarks>
        /// A crossing to or from Calcite's convention is a Janino compile at prepare and a linq4j
        /// <c>Enumerator</c> bridged per row; a crossing to or from the sequence convention splices one
        /// expression tree into another and changes only what wraps the rows. <c>VolcanoCost</c> compares
        /// row counts and nothing else, and a converter's own cost is its input's row count, so without
        /// this the two routes to a node this convention lacks — Calcite's node under one converter, the
        /// sequence convention's under the other — tie, and the planner keeps whichever it registered
        /// first, which is Calcite's. This is what makes the sequence convention's node the one chosen.
        ///
        /// <para>Between one and two, and both bounds matter. Above one, so that the sequence convention's
        /// node under its converter beats Calcite's node under this one. Below two, so that a node only
        /// Calcite has — a table modification — crosses once, directly, rather than twice by way of the
        /// sequence convention's converter and then this convention's, each of which costs one.</para>
        /// </remarks>
        public const double JavaCrossingCostMultiplier = 1.5d;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        ClrDataCursorConvention() :
            base("CLR_DATA_CURSOR", typeof(ClrDataCursorRel))
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
                rel = ClrDataCursorSort.Create(rel, collation, null, null);

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
                    ClrDataCursorRelFactories.ClrDataCursorTableScanFactory,
                    ClrDataCursorRelFactories.ClrDataCursorProjectFactory,
                    ClrDataCursorRelFactories.ClrDataCursorFilterFactory,
                    ClrDataCursorRelFactories.ClrDataCursorSortFactory]));
        }

    }

}
