using System.Collections.Generic;

using Apache.Calcite.Geography.Schema;
using Apache.Calcite.Geography.Sql;

using FluentAssertions;

using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Pins what an adapter matches on to push a <c>CLR_ST_GEOG_</c> operator down to a store with its own
    /// geodesic functions.
    /// </summary>
    /// <remarks>
    /// The operators resolve here through <see cref="GeographySchema"/>, as a query over an adapter's schema
    /// would reach them.
    /// </remarks>
    public class GeographyPushdownTests
    {

        static RelNode Plan(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("GEO", new GeographyExecutionTests.GeographyTable());
            GeographySchema.AddTo(rootSchema);

            var planner = Frameworks.getPlanner(
                Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .build());

            return planner.rel(planner.validate(planner.parse(sql))).project();
        }

        /// <summary>
        /// Returns every <c>RexCall</c> in the plan, nested calls included.
        /// </summary>
        /// <remarks>
        /// The tree is walked by hand rather than with <c>RelShuttleImpl</c>, which dispatches on the node's
        /// type and does not reach a node it has no overload for. <c>RelNode.accept(RexShuttle)</c> visits the
        /// expressions each node holds.
        /// </remarks>
        /// <param name="rel">The root of the plan.</param>
        /// <returns>The calls held by every node of the plan, nested calls included.</returns>
        static List<RexCall> Calls(RelNode rel)
        {
            var found = new List<RexCall>();
            var collector = new RexCollector(found);

            void Walk(RelNode node)
            {
                node.accept(collector);

                var inputs = node.getInputs();
                for (var i = 0; i < inputs.size(); i++)
                    Walk((RelNode)inputs.get(i));
            }

            Walk(rel);

            return found;
        }

        /// <summary>
        /// The call reaches the logical plan by name, with its operands, for an adapter to render.
        /// </summary>
        [Fact]
        public void ShouldLeaveTheCallInThePlanForAnAdapterToFind()
        {
            var calls = Calls(Plan("SELECT ID FROM GEO WHERE CLR_ST_GEOG_DWITHIN(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)"));

            var dwithin = calls.Find(c => c.getOperator().getName() == "CLR_ST_GEOG_DWITHIN");

            dwithin.Should().NotBeNull();
            dwithin!.getOperands().size().Should().Be(3);
            dwithin.getOperator().Should().BeAssignableTo<SqlUserDefinedFunction>();

            // The first operand is the column, which an adapter checks before rendering the call against its
            // own table.
            dwithin.getOperands().get(0).Should().BeAssignableTo<RexInputRef>();
        }

        /// <summary>
        /// The operator in a plan resolved through a schema is neither the operator table's instance nor equal
        /// to it; <see cref="GeographyOperatorTable.Matches"/> and <see cref="GeographyOperatorTable.Rebind"/>
        /// relate the two.
        /// </summary>
        /// <remarks>
        /// <c>CalciteCatalogReader.toOp</c> builds a new plain <c>SqlUserDefinedFunction</c> on every lookup.
        /// <c>SqlOperator.equals</c> first compares classes, and the table's operators are
        /// <c>GeographyFunction</c>, a subclass carrying the strictness and symmetry Calcite's simplifications
        /// read, so <c>equals</c> is false as well.
        /// </remarks>
        [Fact]
        public void ShouldNotGiveThePlanTheOperatorTablesOwnInstance()
        {
            var call = Calls(Plan("SELECT CLR_ST_GEOG_DISTANCE(GEOG, GEOG) FROM GEO"))
                .Find(c => c.getOperator().getName() == "CLR_ST_GEOG_DISTANCE");

            call.Should().NotBeNull();

            var declared = GeographyOperatorTable.ClrStGeogDistance;

            call!.getOperator().Should().NotBeSameAs(declared);
            call.getOperator().Equals(declared).Should().BeFalse();

            GeographyOperatorTable.Matches(call.getOperator(), declared).Should().BeTrue();
            GeographyOperatorTable.Rebind(call.getOperator()).Should().BeSameAs(declared);
        }

        sealed class RexCollector(List<RexCall> found) : RexShuttle
        {

            public override RexNode visitCall(RexCall call)
            {
                found.Add(call);

                return base.visitCall(call);
            }

        }

    }

}
