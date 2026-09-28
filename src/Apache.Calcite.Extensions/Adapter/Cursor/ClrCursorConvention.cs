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
    /// The counterpart of <c>EnumerableConvention</c>. Rows are represented as Calcite represents them; what
    /// differs is the compiled plan, which is a <see cref="ClrCursorFactory"/> built from
    /// <c>System.Linq.Expressions</c>. The factory opens a cursor synchronously or with await, and the cursor
    /// offers both <c>Read</c> and <c>ReadAsync(token)</c> over one position, as <c>DbDataReader</c> does, so
    /// the reader chooses on each advance and may pass a different token each time.
    ///
    /// <para>To plan into this convention, register <see cref="ClrCursorRules.Rules"/> with the planner,
    /// request this convention on the root, and then run <see cref="ClrCursorRules.CalcRules"/> as a separate
    /// hep pass, as <c>Programs.standard</c> does for Calcite's calc rules. The resulting root is a
    /// <see cref="ClrCursorRel"/>; <see cref="ClrCursorRelImplementor.ImplementRoot"/> turns it into the
    /// factory. A plan may mix nodes of this convention and of <c>EnumerableConvention</c>, with converters in
    /// each direction, so a statement with no node in this convention can still be planned.</para>
    /// </remarks>
    public sealed class ClrCursorConvention : Convention.Impl
    {

        /// <summary>
        /// The single instance.
        /// </summary>
        public static readonly ClrCursorConvention Instance = new();

        /// <summary>
        /// Cost of a node of this convention relative to an equivalent node in a typical calling convention.
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
