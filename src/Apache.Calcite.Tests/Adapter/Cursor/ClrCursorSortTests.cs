using System;

using Apache.Calcite.Extensions;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests of a plan neither convention can run: a sort over a one-column table function, where the defect is
    /// Calcite's and this convention reproduces it.
    /// </summary>
    /// <remarks>
    /// <para>Sort, limit, limit-sort, spool and repeat union pass their input's rows through unchanged but build
    /// their physical type with the optimising overload, which turns ARRAY into SCALAR for a one-field row
    /// type. A table function scan passes <c>optimize = false</c> and keeps ARRAY, so the sort above it names
    /// SCALAR while handing on ARRAY rows. <c>EnumerableSort</c> does not reconcile the two, and neither does
    /// <see cref="ClrCursorSort"/>, which follows it.</para>
    ///
    /// <para>Calcite fails at run time on a cast. This convention fails while building the plan, because
    /// <c>ClrCursorRelImplementor</c> checks the element type of each node's cursor against its physical row
    /// type, which Java's erased <c>Enumerable</c> cannot express. The fix belongs in <c>EnumerableSort</c>:
    /// either it keeps the input's format when only passing rows through, or it converts the rows to the
    /// format it names.</para>
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
        /// Both conventions choose a merge join over sorted table function scans, so the failures below come from
        /// the nodes and not from planning.
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
        /// <c>EnumerableConvention</c> cannot run the query: it reads the sort's <c>Object[]</c> row as the
        /// <c>long</c> the sort said it was.
        /// </summary>
        /// <remarks>
        /// If this test fails, Calcite has changed <c>EnumerableSort</c>, and <see cref="ClrCursorSort"/> should
        /// follow the change.
        /// </remarks>
        [Fact]
        public void ShouldShowThatCalciteCannotRunTheQuery()
        {
            var act = () => ClrCursorConventionDifferentialTests.RunFib(Sql, false, true);

            act.Should().Throw<InvalidCastException>()
                .WithMessage("*System.Object[]*java.lang.Long*");
        }

        /// <summary>
        /// This convention cannot run it either, and refuses while building the plan, naming the sort, the
        /// element type it handed up and its row type.
        /// </summary>
        [Fact]
        public void ShouldRefuseTheQueryNamingTheNodeAndBothTypes()
        {
            var act = () => ClrCursorConventionDifferentialTests.RunFib(Sql, true, true);

            act.Should().Throw<java.lang.IllegalStateException>()
                .WithInnerException<java.lang.IllegalStateException>()
                .WithMessage("*ClrCursorSort handed up an open of System.Object[] where its row type is java.lang.Long*");
        }

    }

}
