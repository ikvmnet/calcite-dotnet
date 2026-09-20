using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Prepare;
using Apache.Calcite.Extensions.Prepare.Tests;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;

namespace Apache.Calcite.Extensions.Prepare.Enumerable.Tests
{

    /// <summary>
    /// The rounding a caller asks for on a FETCH or an OFFSET that is not a whole number.
    /// </summary>
    /// <remarks>
    /// A count is a <c>BigDecimal</c> since CALCITE-7624, so it need not be an integer, and what a fraction
    /// means is the caller's to say: <c>CalcitePrepareImpl</c> reads a <c>FetchOffsetRoundingPolicy</c> off
    /// the planner's context and stashes it for the limit to read back through the data context.
    /// <c>ClrEnumerablePreparingStmt</c> does the same, and this is the whole of that path — the reader alone
    /// would find nothing to read.
    /// </remarks>
    [TestClass]
    public class ClrEnumerablePreparingStmtTests
    {

        const string Sql = "SELECT ID FROM SALES ORDER BY ID FETCH NEXT 2.5 ROWS ONLY";

        /// <summary>
        /// Rounds a count down, where the default rounds up.
        /// </summary>
        sealed class Floor : FetchOffsetRoundingPolicy
        {

            public java.math.BigDecimal round(java.math.BigDecimal value) => value.setScale(0, java.math.RoundingMode.FLOOR);

        }

        /// <summary>
        /// A prepare whose planner carries a rounding policy, which is how a caller supplies one.
        /// </summary>
        sealed class WithPolicy(FetchOffsetRoundingPolicy policy) : ClrPrepareImpl
        {

            protected override IReadOnlyList<Func<CalcitePrepare.Context, RelOptPlanner>> CreatePlannerFactories()
            {
                return [context => CreatePlanner(context, Contexts.of(context.config(), policy), null)];
            }

        }

        static List<string> Run(string sql, FetchOffsetRoundingPolicy? policy)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = policy == null ? new ClrPrepareImpl() : new WithPolicy(policy);
                var signature = prepare.PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);

                var rows = new List<string>();
                foreach (var row in signature.Bind(context.getDataContext()))
                    rows.Add(row?.ToString() ?? "<null>");

                return rows;
            });
        }

        /// <summary>
        /// With no policy, a fractional FETCH reaches the row it names.
        /// </summary>
        /// <remarks>
        /// <c>FetchOffsetRoundingPolicy.NONE</c> hands the value on as it is, and the operator counts whole
        /// rows against it, so 2.5 takes three.
        /// </remarks>
        [TestMethod]
        public void ShouldTakeTheRowAFractionalFetchReachesInto()
        {
            Run(Sql, null).Should().HaveCount(3);
        }

        /// <summary>
        /// A caller's policy decides what a fraction means.
        /// </summary>
        [TestMethod]
        public void ShouldRoundAFractionalFetchTheWayTheCallerAsks()
        {
            Run(Sql, new Floor()).Should().HaveCount(2);
        }

    }

}
