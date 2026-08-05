using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

using Apache.Calcite.Linq.Rel;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// Calling convention that returns results as an <see cref="System.Collections.Generic.IEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableConvention</c>, which returns a linq4j <c>Enumerable</c>. A plan runs
    /// as a compiled <see cref="System.Linq.Expressions"/> tree rather than as generated Java source.
    ///
    /// <para>Register <see cref="ClrEnumerableRules.Rules"/> with the planner and ask for this convention on
    /// the root; <see cref="ClrEnumerablePrograms"/> has the passes that takes, and
    /// <see cref="ClrEnumerablePrepare"/> wires the whole of it to a connection. A plan may hold nodes of
    /// both conventions — converters exist in each direction and rows cross untouched.</para>
    /// </remarks>
    public sealed class ClrEnumerableConvention : Convention.Impl
    {

        /// <summary>
        /// The single instance.
        /// </summary>
        public static readonly ClrEnumerableConvention Instance = new();

        /// <summary>
        /// Cost of a node of this convention versus implementing an equivalent node in a typical calling
        /// convention.
        /// </summary>
        public const double CostMultiplier = 1.0d;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        ClrEnumerableConvention() :
            base("CLR_ENUMERABLE", typeof(ClrEnumerableRel))
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
                rel = ClrEnumerableSort.Create(rel, collation, null, null);

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
                    ClrEnumerableRelFactories.ClrEnumerableTableScanFactory,
                    ClrEnumerableRelFactories.ClrEnumerableProjectFactory,
                    ClrEnumerableRelFactories.ClrEnumerableFilterFactory,
                    ClrEnumerableRelFactories.ClrEnumerableSortFactory]));
        }

    }

}
