using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The relational factories of this convention, so that a <c>RelBuilder</c> handed this convention's
    /// context builds nodes of it rather than logical ones.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRelFactories</c>, and the same four: scan, project, filter and sort.
    /// </remarks>
    public static class ClrCursorRelFactories
    {

        /// <summary>
        /// Returns a <see cref="ClrCursorTableScan"/>.
        /// </summary>
        public static readonly RelFactories.TableScanFactory ClrCursorTableScanFactory = new TableScanFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrCursorProject"/>.
        /// </summary>
        public static readonly RelFactories.ProjectFactory ClrCursorProjectFactory = new ProjectFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrCursorFilter"/>.
        /// </summary>
        public static readonly RelFactories.FilterFactory ClrCursorFilterFactory = new FilterFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrCursorSort"/>.
        /// </summary>
        public static readonly RelFactories.SortFactory ClrCursorSortFactory = new SortFactoryImpl();

        sealed class TableScanFactoryImpl : RelFactories.TableScanFactory
        {

            /// <inheritdoc />
            public RelNode createScan(RelOptTable.ToRelContext toRelContext, RelOptTable table)
            {
                return ClrCursorTableScan.Create(toRelContext.getCluster(), table);
            }

        }

        sealed class ProjectFactoryImpl : RelFactories.ProjectFactory
        {

            /// <inheritdoc />
            public RelNode createProject(RelNode input, java.util.List hints, java.util.List childExprs, java.util.List fieldNames, java.util.Set variablesSet)
            {
                if (variablesSet.isEmpty() == false)
                    throw new java.lang.IllegalArgumentException("ClrCursorProject does not allow variables");

                var rowType = RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs, fieldNames, SqlValidatorUtil.F_SUGGESTER);

                return ClrCursorProject.Create(input, childExprs, rowType);
            }

            /// <inheritdoc />
            public RelNode createProject(RelNode input, java.util.List hints, java.util.List childExprs, java.util.List fieldNames)
            {
                return createProject(input, hints, childExprs, fieldNames, com.google.common.collect.ImmutableSet.of());
            }

        }

        sealed class FilterFactoryImpl : RelFactories.FilterFactory
        {

            /// <inheritdoc />
            public RelNode createFilter(RelNode input, RexNode condition, java.util.Set variablesSet)
            {
                return ClrCursorFilter.Create(input, condition);
            }

            /// <inheritdoc />
            public RelNode createFilter(RelNode input, RexNode condition)
            {
                return createFilter(input, condition, com.google.common.collect.ImmutableSet.of());
            }

        }

        sealed class SortFactoryImpl : RelFactories.SortFactory
        {

            /// <inheritdoc />
            public RelNode createSort(RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
            {
                return ClrCursorSort.Create(input, collation, offset, fetch);
            }

            /// <inheritdoc />
            public RelNode createSort(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
            {
                return createSort(input, collation, offset, fetch);
            }

        }

    }

}
