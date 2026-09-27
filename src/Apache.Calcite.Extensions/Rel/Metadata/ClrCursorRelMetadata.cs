using Apache.Calcite.Extensions.Adapter.Cursor;

using com.google.common.collect;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// The metadata Calcite answers for an <c>Enumerable*</c> node by a handler keyed on that node's class,
    /// answered for the <see cref="ClrCursorConvention"/> node that ports it.
    /// </summary>
    /// <remarks>
    /// A handler is chosen by the rel's class, so a node of this convention with the same base class as its
    /// Calcite counterpart reaches the handler for that base and not the one Calcite wrote for the node. Each
    /// method here is the Calcite method of the same name with the node's class swapped, and nothing else.
    ///
    /// <para>What an override on the node can carry is carried there instead, because it holds whichever
    /// provider the cluster has: <c>ClrCursorLimit.estimateRowCount</c> is
    /// <c>RelMdRowCount.getRowCount(EnumerableLimit)</c>, reached through the handler for
    /// <c>SingleRel</c>. The rest has no hook on the node and needs <see cref="Provider"/> on the cluster —
    /// <c>ClrPrepareImpl</c> puts it there, and a caller driving its own planner has to do the same.</para>
    /// </remarks>
    public static class ClrCursorRelMetadata
    {

        /// <summary>
        /// The handlers here, ahead of Calcite's own.
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
        /// <c>RelMdPercentageOriginalRows</c>'s cumulative cost for the interpreter.
        /// </summary>
        public sealed class PercentageOriginalRows : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.CumulativeCost.DEF;

            /// <summary>
            /// <c>getCumulativeCost(EnumerableInterpreter, RelMetadataQuery)</c>: the node's own cost, its
            /// input's left out.
            /// </summary>
            /// <param name="rel"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public RelOptCost? getCumulativeCost(ClrCursorInterpreter rel, RelMetadataQuery mq)
            {
                return mq.getNonCumulativeCost(rel);
            }

        }

        /// <summary>
        /// <c>RelMdMaxRowCount</c> for the limit.
        /// </summary>
        public sealed class MaxRowCount : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.MaxRowCount.DEF;

            /// <summary>
            /// <c>getMaxRowCount(EnumerableLimit, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="rel"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
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
        /// <c>RelMdMinRowCount</c> for the limit.
        /// </summary>
        public sealed class MinRowCount : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.MinRowCount.DEF;

            /// <summary>
            /// <c>getMinRowCount(EnumerableLimit, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="rel"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
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
        /// <c>RelMdCollation</c> for the nodes it names.
        /// </summary>
        public sealed class Collation : MetadataHandler
        {

            /// <inheritdoc />
            public MetadataDef getDef() => BuiltInMetadata.Collation.DEF;

            /// <summary>
            /// <c>collations(EnumerableMergeJoin, RelMetadataQuery)</c>: in general a join is not sorted, but a
            /// merge join preserves the sort order of the left and right sides.
            /// </summary>
            /// <param name="join"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public ImmutableList? collations(ClrCursorMergeJoin join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.mergeJoin(mq, join.getLeft(), join.getRight(), join.analyzeCondition().leftKeys, join.analyzeCondition().rightKeys, join.getJoinType()));
            }

            /// <summary>
            /// <c>collations(EnumerableHashJoin, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="join"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public ImmutableList? collations(ClrCursorHashJoin join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.enumerableHashJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
            }

            /// <summary>
            /// <c>collations(EnumerableNestedLoopJoin, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="join"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public ImmutableList? collations(ClrCursorNestedLoopJoin join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.enumerableNestedLoopJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
            }

            /// <summary>
            /// <c>collations(EnumerableMergeUnion, RelMetadataQuery)</c>: a merge union guarantees order, like a
            /// sort.
            /// </summary>
            /// <param name="mergeUnion"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public ImmutableList? collations(ClrCursorMergeUnion mergeUnion, RelMetadataQuery mq)
            {
                var collation = mergeUnion.getTraitSet().getCollation();
                if (collation == null)
                    return null; // should not happen

                return CopyOf(RelMdCollation.sort(collation));
            }

            /// <summary>
            /// <c>collations(EnumerableCorrelate, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="join"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public ImmutableList? collations(ClrCursorCorrelate join, RelMetadataQuery mq)
            {
                return CopyOf(RelMdCollation.enumerableCorrelate(mq, join.getLeft(), join.getRight(), join.getJoinType()));
            }

            /// <summary>
            /// <c>collations(EnumerableLimit, RelMetadataQuery)</c>.
            /// </summary>
            /// <param name="rel"></param>
            /// <param name="mq"></param>
            /// <returns></returns>
            public ImmutableList? collations(ClrCursorLimit rel, RelMetadataQuery mq)
            {
                return mq.collations(rel.getInput());
            }

        }

    }

}
