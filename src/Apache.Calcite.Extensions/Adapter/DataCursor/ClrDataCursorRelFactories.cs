using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The relational factories of this convention, so that a <c>RelBuilder</c> handed this convention's
    /// context builds nodes of it rather than logical ones.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRelFactories</c>, and the same four: scan, project, filter and sort.
    /// </remarks>
    public static class ClrDataCursorRelFactories
    {

        /// <summary>
        /// Returns a <see cref="ClrDataCursorTableScan"/>.
        /// </summary>
        public static readonly RelFactories.TableScanFactory ClrDataCursorTableScanFactory = new TableScanFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrDataCursorProject"/>.
        /// </summary>
        public static readonly RelFactories.ProjectFactory ClrDataCursorProjectFactory = new ProjectFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrDataCursorFilter"/>.
        /// </summary>
        public static readonly RelFactories.FilterFactory ClrDataCursorFilterFactory = new FilterFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrDataCursorSort"/>.
        /// </summary>
        public static readonly RelFactories.SortFactory ClrDataCursorSortFactory = new SortFactoryImpl();

        sealed class TableScanFactoryImpl : RelFactories.TableScanFactory
        {

            /// <inheritdoc />
            public RelNode createScan(RelOptTable.ToRelContext toRelContext, RelOptTable table)
            {
                return ClrDataCursorTableScan.Create(toRelContext.getCluster(), table);
            }

        }

        sealed class ProjectFactoryImpl : RelFactories.ProjectFactory
        {

            /// <inheritdoc />
            public RelNode createProject(RelNode input, java.util.List hints, java.util.List childExprs, java.util.List fieldNames, java.util.Set variablesSet)
            {
                if (variablesSet.isEmpty() == false)
                    throw new java.lang.IllegalArgumentException("ClrDataCursorProject does not allow variables");

                var rowType = RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs, fieldNames, SqlValidatorUtil.F_SUGGESTER);

                return ClrDataCursorProject.Create(input, childExprs, rowType);
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
                return ClrDataCursorFilter.Create(input, condition);
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
                return ClrDataCursorSort.Create(input, collation, offset, fetch);
            }

            /// <inheritdoc />
            public RelNode createSort(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
            {
                return createSort(input, collation, offset, fetch);
            }

        }

    }

}
