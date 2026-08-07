using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;


using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql.type;
using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The parts of <see cref="ClrEnumerableWindow"/> a plan does not reach.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>EnumerableWindowTest</c>. Everything else about the node is compared
    /// against <c>EnumerableConvention</c> by the differential tests; <c>copy</c> is not, because the planner
    /// only calls it while it is exploring and a plan that came out right says nothing about whether the
    /// constants survived.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableWindowTests
    {

        /// <summary>
        /// A node with no inputs, to hang a window on.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        sealed class Leaf(RelOptCluster cluster, RelTraitSet traitSet) : AbstractRelNode(cluster, traitSet)
        {

        }

        [TestMethod]
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
            var original = new ClrEnumerableWindow(cluster, traitSet, input, constants, rowType, com.google.common.collect.ImmutableList.of());

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
