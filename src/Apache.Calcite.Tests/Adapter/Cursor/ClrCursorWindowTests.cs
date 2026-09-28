using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;

using FluentAssertions;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests of <see cref="ClrCursorWindow"/> members that executing a plan does not exercise.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>EnumerableWindowTest</c>. The differential tests compare the node's rows
    /// against <c>EnumerableConvention</c>; <c>copy</c> with new constants is called only while the planner
    /// explores, so a correct result does not show that it keeps the constants it is given.
    /// </remarks>
    public class ClrCursorWindowTests
    {

        /// <summary>
        /// A node with no inputs, to serve as the window's input.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        sealed class Leaf(RelOptCluster cluster, RelTraitSet traitSet) : AbstractRelNode(cluster, traitSet)
        {

        }

        [Fact]
        public void ShouldKeepTheConstantsItIsGivenWhenCopied()
        {
            var typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var planner = new org.apache.calcite.plan.hep.HepPlanner(org.apache.calcite.plan.hep.HepProgram.builder().build());
            var cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
            var traitSet = RelTraitSet.createEmpty();
            var input = new Leaf(cluster, traitSet);

            var booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            var rowType = typeFactory.createSqlType(SqlTypeName.ROW);

            var constants = com.google.common.collect.ImmutableList.of(RexLiteral.fromJdbcString(booleanType, SqlTypeName.BOOLEAN, "TRUE"));
            var original = new ClrCursorWindow(cluster, traitSet, input, constants, rowType, com.google.common.collect.ImmutableList.of());

            var replacements = com.google.common.collect.ImmutableList.of(RexLiteral.fromJdbcString(booleanType, SqlTypeName.BOOLEAN, "FALSE"));
            var updated = original.copy(replacements);

            updated.Should().NotBeSameAs(original);
            original.getConstants().size().Should().Be(1);
            original.getConstants().get(0).Should().BeSameAs(constants.get(0));
            updated.getConstants().size().Should().Be(1);
            updated.getConstants().get(0).Should().BeSameAs(replacements.get(0));
        }

    }

}
