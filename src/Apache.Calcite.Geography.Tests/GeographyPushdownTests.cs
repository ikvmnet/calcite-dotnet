using System.Collections.Generic;

using Apache.Calcite.Geography.Schema;
using Apache.Calcite.Geography.Sql;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.tools;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// What an adapter has to match on to push an <c>ST_GEOG_</c> operator down to a store that can do the
    /// geodesy itself.
    /// </summary>
    /// <remarks>
    /// Pushing down is the point of the package: a geodesic store answers <c>ST_GEOG_DWITHIN</c> in its own
    /// SQL, and the S2 evaluator here is what answers when nothing better can. No adapter exists yet, so
    /// these pin the two facts one would be written against rather than any adapter's behaviour.
    /// </remarks>
    [TestClass]
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
        /// Every call in the plan, which is what an adapter's rule sees.
        /// </summary>
        /// <param name="rel"></param>
        /// <returns></returns>
        /// <remarks>
        /// Walked by hand rather than with <c>RelShuttleImpl</c>, which dispatches on the node's type and so
        /// does not reach a node it has no overload for. <c>RelNode.accept(RexShuttle)</c> rewrites the
        /// expressions a node holds, and the shuttle collects them on the way through.
        /// </remarks>
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
        /// The call survives into the plan, named, with the arguments an adapter would render.
        /// </summary>
        [TestMethod]
        public void ShouldLeaveTheCallInThePlanForAnAdapterToFind()
        {
            var calls = Calls(Plan("SELECT ID FROM GEO WHERE ST_GEOG_DWITHIN(GEOG, ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)"));

            var dwithin = calls.Find(c => c.getOperator().getName() == "ST_GEOG_DWITHIN");

            dwithin.Should().NotBeNull();
            dwithin!.getOperands().size().Should().Be(3);
            dwithin.getOperator().Should().BeAssignableTo<SqlUserDefinedFunction>();

            // the first operand is the column, which is what an adapter checks before deciding it can render
            // the call against the table it owns
            dwithin.getOperands().get(0).Should().BeAssignableTo<RexInputRef>();
        }

        /// <summary>
        /// The operator in the plan is not the instance the operator table holds, so an adapter matches by
        /// name rather than by reference.
        /// </summary>
        /// <remarks>
        /// <c>CalciteCatalogReader.toOp</c> builds a fresh <c>SqlUserDefinedFunction</c> on every lookup and
        /// caches nothing, so reference equality never holds for a schema-registered operator.
        /// <c>SqlOperator.equals</c> compares class, name and kind — not the parameter list — so it holds for
        /// any overload of the name, which is why the safe test is the name and, where an adapter cares which
        /// overload, the operand count.
        /// </remarks>
        [TestMethod]
        public void ShouldNotGiveThePlanTheOperatorTablesOwnInstance()
        {
            var call = Calls(Plan("SELECT ST_GEOG_DISTANCE(GEOG, GEOG) FROM GEO"))
                .Find(c => c.getOperator().getName() == "ST_GEOG_DISTANCE");

            call.Should().NotBeNull();

            var declared = GeographyOperatorTable.StGeogDistance;

            call!.getOperator().Should().NotBeSameAs(declared);
            call.getOperator().Equals(declared).Should().BeTrue();
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
