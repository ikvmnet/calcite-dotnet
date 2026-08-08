
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// The relational factories of this convention, so that a <c>RelBuilder</c> handed this convention's
    /// context builds nodes of it rather than logical ones.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRelFactories</c>, and the same four: scan, project, filter and sort.
    /// </remarks>
    public static class ClrAsyncEnumerableRelFactories
    {

        /// <summary>
        /// Returns a <see cref="ClrAsyncEnumerableTableScan"/>.
        /// </summary>
        public static readonly RelFactories.TableScanFactory ClrAsyncEnumerableTableScanFactory = new TableScanFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrAsyncEnumerableProject"/>.
        /// </summary>
        public static readonly RelFactories.ProjectFactory ClrAsyncEnumerableProjectFactory = new ProjectFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrAsyncEnumerableFilter"/>.
        /// </summary>
        public static readonly RelFactories.FilterFactory ClrAsyncEnumerableFilterFactory = new FilterFactoryImpl();

        /// <summary>
        /// Returns a <see cref="ClrAsyncEnumerableSort"/>.
        /// </summary>
        public static readonly RelFactories.SortFactory ClrAsyncEnumerableSortFactory = new SortFactoryImpl();

        /// <summary>
        /// Implementation of <see cref="RelFactories.TableScanFactory"/> returning a vanilla
        /// <see cref="ClrAsyncEnumerableTableScan"/>.
        /// </summary>
        sealed class TableScanFactoryImpl : RelFactories.TableScanFactory
        {

            /// <inheritdoc />
            public RelNode createScan(RelOptTable.ToRelContext toRelContext, RelOptTable table)
            {
                return ClrAsyncEnumerableTableScan.Create(toRelContext.getCluster(), table);
            }

        }

        /// <summary>
        /// Implementation of <see cref="RelFactories.ProjectFactory"/> returning a vanilla
        /// <see cref="ClrAsyncEnumerableProject"/>.
        /// </summary>
        sealed class ProjectFactoryImpl : RelFactories.ProjectFactory
        {

            /// <inheritdoc />
            public RelNode createProject(RelNode input, java.util.List hints, java.util.List childExprs, java.util.List fieldNames, java.util.Set variablesSet)
            {
                if (variablesSet.isEmpty() == false)
                    throw new java.lang.IllegalArgumentException("ClrAsyncEnumerableProject does not allow variables");

                var rowType = RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs, fieldNames, SqlValidatorUtil.F_SUGGESTER);

                return ClrAsyncEnumerableProject.Create(input, childExprs, rowType);
            }

            /// <inheritdoc />
            /// <remarks>Calcite's deprecated form, whose default is this one with no variables.</remarks>
            public RelNode createProject(RelNode input, java.util.List hints, java.util.List childExprs, java.util.List fieldNames)
            {
                return createProject(input, hints, childExprs, fieldNames, com.google.common.collect.ImmutableSet.of());
            }

        }

        /// <summary>
        /// Implementation of <see cref="RelFactories.FilterFactory"/> returning a vanilla
        /// <see cref="ClrAsyncEnumerableFilter"/>.
        /// </summary>
        sealed class FilterFactoryImpl : RelFactories.FilterFactory
        {

            /// <inheritdoc />
            public RelNode createFilter(RelNode input, RexNode condition, java.util.Set variablesSet)
            {
                return ClrAsyncEnumerableFilter.Create(input, condition);
            }

            /// <inheritdoc />
            /// <remarks>Calcite's deprecated form, whose default is this one with no variables.</remarks>
            public RelNode createFilter(RelNode input, RexNode condition)
            {
                return createFilter(input, condition, com.google.common.collect.ImmutableSet.of());
            }

        }

        /// <summary>
        /// Implementation of <see cref="RelFactories.SortFactory"/> returning a vanilla
        /// <see cref="ClrAsyncEnumerableSort"/>.
        /// </summary>
        sealed class SortFactoryImpl : RelFactories.SortFactory
        {

            /// <inheritdoc />
            public RelNode createSort(RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
            {
                return ClrAsyncEnumerableSort.Create(input, collation, offset, fetch);
            }

            /// <inheritdoc />
            /// <remarks>Calcite's deprecated form, whose default drops the trait set.</remarks>
            public RelNode createSort(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
            {
                return createSort(input, collation, offset, fetch);
            }

        }

    }

}
