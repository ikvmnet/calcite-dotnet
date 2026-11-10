using Apache.Calcite.Extensions;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests of a sort over a one-column table function.
    /// </summary>
    /// <remarks>
    /// <para>Sort, limit, limit-sort, spool and repeat union pass their input's rows through unchanged but build
    /// their physical type with the optimising overload, which turns ARRAY into SCALAR for a one-field row
    /// type. A table function scan whose rows are arrays of one field once kept ARRAY, so the sort above it named
    /// SCALAR while handing on ARRAY rows. Since CALCITE-7846, <c>EnumerableTableFunctionScan</c> reads such rows
    /// as SCALAR, slicing out the one element, and <see cref="ClrCursorTableFunctionScan"/> follows it.</para>
    ///
    /// <para>The table function is <c>Smalls.fibonacciTableWithLimit100</c>, written in Java so that
    /// <c>EnumerableConvention</c> can run it too. The hash join rule is removed so that a merge join, and
    /// therefore a sort over each table function, is the only plan; with hash joins available the shape does
    /// not arise.</para>
    /// </remarks>
    public class ClrCursorSortTests
    {

        const string Sql =
            "SELECT \"A\".\"N\" FROM TABLE(\"FIB\"()) AS \"A\", TABLE(\"FIB\"()) AS \"B\" WHERE \"A\".\"N\" = \"B\".\"N\" ORDER BY 1";

        /// <summary>
        /// Both conventions choose a merge join over sorted table function scans.
        /// </summary>
        [Fact]
        public void ShouldPlanAMergeJoinOverSortedTableFunctionsInBothConventions()
        {
            foreach (var clr in new[] { true, false })
            {
                var plan = ClrCursorConventionDifferentialTests.PlanOfFib(Sql, clr, true);

                plan.Should().Contain("MergeJoin");
                plan.Should().Contain("Sort");
                plan.Should().Contain("TableFunctionScan");
            }
        }

        /// <summary>
        /// Both conventions run the query, and give the same rows.
        /// </summary>
        [Fact]
        public void ShouldGiveWhatCalciteGives()
        {
            var calcite = ClrCursorConventionDifferentialTests.RunFib(Sql, false, true);
            var mine = ClrCursorConventionDifferentialTests.RunFib(Sql, true, true);

            calcite.Should().NotBeEmpty();
            mine.Should().Equal(calcite);
        }

    }

}
