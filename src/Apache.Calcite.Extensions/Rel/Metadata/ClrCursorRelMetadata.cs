using Apache.Calcite.Extensions.Adapter.Cursor;

using com.google.common.collect;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Metadata handlers for <see cref="ClrCursorConvention"/> nodes, where Calcite keys a handler on the
    /// corresponding <c>Enumerable*</c> class.
    /// </summary>
    /// <remarks>
    /// Calcite chooses a metadata handler by the node's class, so a node of this convention would otherwise
    /// reach the handler for its base class rather than the one Calcite has for its <c>Enumerable*</c>
    /// counterpart. Each handler method here is Calcite's method of the same name with the node class
    /// replaced.
    ///
    /// <para>Where the node can override a method instead, it does, so that it holds under any provider
    /// (<c>ClrCursorLimit.estimateRowCount</c>, for example). The handlers here take effect only when
    /// <see cref="Provider"/> is the cluster's metadata provider. <c>ClrPrepareImpl</c> sets it; a caller
    /// driving its own planner passes it to <c>Programs.standard</c> and to the calc pass.</para>
    /// </remarks>
    public static class ClrCursorRelMetadata
    {

        /// <summary>
        /// A metadata provider that consults the handlers of this class first and then
        /// <c>DefaultRelMetadataProvider.INSTANCE</c>.
        /// </summary>
        public static readonly RelMetadataProvider Provider = ChainedRelMetadataProvider.of(ImmutableList.of(
            ReflectiveRelMetadataProvider.reflectiveSource(new PercentageOriginalRows(), typeof(BuiltInMetadata.CumulativeCost.Handler)),
            ReflectiveRelMetadataProvider.reflectiveSource(new MaxRowCount(), typeof(BuiltInMetadata.MaxRowCount.Handler)),
            ReflectiveRelMetadataProvider.reflectiveSource(new MinRowCount(), typeof(BuiltInMetadata.MinRowCount.Handler)),
            ReflectiveRelMetadataProvider.reflectiveSource(new Collation(), typeof(BuiltInMetadata.Collation.Handler)),
            DefaultRelMetadataProvider.INSTANCE));

        static ImmutableList? CopyOf(java.util.Collection? values)
        {
            return values == null ? null : ImmutableList.copyOf(values);
        }

        /// <summary>
        /// The cumulative-cost handler <c>RelMdPercentageOriginalRows</c> has for the interpreter.
        /// </summary>
        public sealed class PercentageOriginalRows : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.CumulativeCost.DEF;

            /// <summary>
            /// Mirrors <c>getCumulativeCost(EnumerableInterpreter, RelMetadataQuery)</c>: the node's own cost,
            /// excluding its input's.
            /// </summary>
            /// <param name="rel">The node.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The metadata value.</returns>
            public RelOptCost? getCumulativeCost(ClrCursorInterpreter rel, RelMetadataQuery mq)
            {
                return mq.getNonCumulativeCost(rel);
            }

        }

        /// <summary>
        /// The maximum-row-count handler <c>RelMdMaxRowCount</c> has for the limit.
        /// </summary>
        public sealed class MaxRowCount : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.MaxRowCount.DEF;

            /// <summary>
            /// Mirrors <c>getMaxRowCount(EnumerableLimit, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="rel">The node.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The metadata value.</returns>
            public java.lang.Double getMaxRowCount(ClrCursorLimit rel, RelMetadataQuery mq)
            {
                var rowCount = mq.getMaxRowCount(rel.getInput());
                var rows = rowCount == null ? double.PositiveInfinity : rowCount.doubleValue();

                var offset = RelMdUtil.literalValueApproximatedByDouble(rel.Offset, 0D);
                rows = java.lang.Math.max(rows - offset, 0D);

                var limit = RelMdUtil.literalValueApproximatedByDouble(rel.Fetch, rows);
                return java.lang.Double.valueOf(limit < rows ? limit : rows);
            }

        }

        /// <summary>
        /// The minimum-row-count handler <c>RelMdMinRowCount</c> has for the limit.
        /// </summary>
        public sealed class MinRowCount : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.MinRowCount.DEF;

            /// <summary>
            /// Mirrors <c>getMinRowCount(EnumerableLimit, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="rel">The node.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The metadata value.</returns>
            public java.lang.Double getMinRowCount(ClrCursorLimit rel, RelMetadataQuery mq)
            {
                var rowCount = mq.getMinRowCount(rel.getInput());
                var rows = rowCount == null ? 0D : rowCount.doubleValue();

                var offset = RelMdUtil.literalValueApproximatedByDouble(rel.Offset, rel.Offset == null ? 0D : rows);
                rows = java.lang.Math.max(rows - offset, 0D);

                var limit = RelMdUtil.literalValueApproximatedByDouble(rel.Fetch, rel.Fetch == null ? rows : 0D);
                return java.lang.Double.valueOf(limit < rows ? limit : rows);
            }

        }

        /// <summary>
        /// The collation handlers <c>RelMdCollation</c> has for <c>Enumerable*</c> nodes.
        /// </summary>
        public sealed class Collation : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.Collation.DEF;

            /// <summary>
            /// Mirrors <c>collations(EnumerableMergeJoin, RelMetadataQuery)</c>: a merge join preserves the sort
            /// order of its inputs.
            /// </summary>
            /// <param name="join">The join.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The collations, or <see langword="null"/>.</returns>
            public ImmutableList? collations(ClrCursorMergeJoin join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.mergeJoin(mq, join.getLeft(), join.getRight(), join.analyzeCondition().leftKeys, join.analyzeCondition().rightKeys, join.getJoinType()));
            }

            /// <summary>
            /// Mirrors <c>collations(EnumerableHashJoin, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="join">The join.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The collations, or <see langword="null"/>.</returns>
            public ImmutableList? collations(ClrCursorHashJoin join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.enumerableHashJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
            }

            /// <summary>
            /// Mirrors <c>collations(EnumerableNestedLoopJoin, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="join">The join.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The collations, or <see langword="null"/>.</returns>
            public ImmutableList? collations(ClrCursorNestedLoopJoin join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.enumerableNestedLoopJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
            }

            /// <summary>
            /// Mirrors <c>collations(EnumerableMergeUnion, RelMetadataQuery)</c>: a merge union guarantees its
            /// collation, as a sort does.
            /// </summary>
            /// <param name="mergeUnion">The merge union.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The collations, or <see langword="null"/>.</returns>
            public ImmutableList? collations(ClrCursorMergeUnion mergeUnion, RelMetadataQuery mq)
            {
                var collation = mergeUnion.getTraitSet().getCollation();
                if (collation == null)
                    return null; // should not happen

                return CopyOf(RelMdCollation.sort(collation));
            }

            /// <summary>
            /// Mirrors <c>collations(EnumerableCorrelate, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="join">The join.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The collations, or <see langword="null"/>.</returns>
            public ImmutableList? collations(ClrCursorCorrelate join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.enumerableCorrelate(mq, join.getLeft(), join.getRight(), join.getJoinType()));
            }

            /// <summary>
            /// Mirrors <c>collations(EnumerableLimit, RelMetadataQuery)</c>: the input's collations.
            /// </summary>
            /// <param name="rel">The node.</param>
            /// <param name="mq">The metadata query.</param>
            /// <returns>The metadata value.</returns>
            public ImmutableList? collations(ClrCursorLimit rel, RelMetadataQuery mq)
            {
                return mq.collations(rel.getInput());
            }

        }

    }

}
