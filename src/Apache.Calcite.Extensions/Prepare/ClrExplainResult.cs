using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// An <c>EXPLAIN</c>, prepared: the plan rendered as text.
    /// </summary>
    /// <remarks>
    /// Our counterpart of <c>Prepare.PreparedExplain</c>, which — unlike every other prepared result —
    /// implements <c>PreparedResult</c> directly rather than extending <c>PreparedResultImpl</c>. That is
    /// why an <c>EXPLAIN</c> reports no collations: Calcite's driver reads them only off a
    /// <c>PreparedResultImpl</c>.
    ///
    /// <para>The text is rendered when the statement is prepared, not when it is run, because that is where
    /// <c>getCode</c> is called from.</para>
    /// </remarks>
    sealed class ClrExplainResult : ClrPrepareResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="parameterRowType"></param>
        /// <param name="explanation">The rendered plan, or type.</param>
        public ClrExplainResult(RelDataType parameterRowType, string explanation) :
            base(null, parameterRowType, FieldOriginsOfExplain(), com.google.common.collect.ImmutableList.of(), null, null, false)
        {
            Explanation = explanation ?? throw new ArgumentNullException(nameof(explanation));
        }

        /// <summary>
        /// Gets the rendered plan, or type.
        /// </summary>
        public string Explanation { get; }

        /// <inheritdoc />
        /// <remarks>
        /// None. <c>PreparedExplain</c> is not <c>Typed</c>, so Calcite's driver deduces the cursor factory
        /// from the columns alone.
        /// </remarks>
        public override java.lang.reflect.Type? ElementType => null;

        /// <summary>
        /// The origins <c>PreparedExplain.getFieldOrigins</c> reports.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// One entry — the single column an <c>EXPLAIN</c> returns — holding four nulls, which is what the
        /// column metadata reads a catalog, schema, table and column name from.
        /// </remarks>
        static java.util.List FieldOriginsOfExplain()
        {
            return java.util.Collections.singletonList(java.util.Collections.nCopies(4, null));
        }

    }

}
