using System;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Holds the one place this convention departs from Calcite deliberately: a pass-through node keeps its
    /// input's row format rather than re-optimising it.
    /// </summary>
    /// <remarks>
    /// <para>Sort, limit, limit-sort, spool and repeat union yield their input's rows unchanged, and Calcite
    /// builds their physical type with the overload that optimises — which turns ARRAY into SCALAR for a
    /// one-field row type. A table function cannot reshape its rows, so it passes <c>optimize = false</c> and
    /// keeps an honest ARRAY; a pass-through node above it then optimises that away without touching the
    /// rows, and a consumer reads field 0 as the row itself.</para>
    ///
    /// <para><b>The oracle is <c>Smalls.fibonacciTableWithLimit100</c></b>, a one-column table function
    /// written in Java, so Janino can name it. Every table function of this project's own is a CLR class,
    /// which is why this could not be measured before.</para>
    ///
    /// <para>The hash join rule is removed so that the merge join is the only way to join, which is what puts
    /// a sort over the table function. With it in place the planner hashes instead and the shape never
    /// arises — which is the whole reason Calcite has not noticed.</para>
    /// </remarks>
    [TestClass]
    public class ClrEnumerableRowFormatTests
    {

        const string Sql =
            "SELECT \"A\".\"N\" FROM TABLE(\"FIB\"()) AS \"A\", TABLE(\"FIB\"()) AS \"B\" WHERE \"A\".\"N\" = \"B\".\"N\" ORDER BY 1";

        static readonly string[] Expected = ["1", "1", "1", "1", "2", "3", "5", "8", "13", "21", "34", "55", "89"];

        /// <summary>
        /// Both conventions choose the same plan, so what follows is a difference in the nodes and not in the
        /// planning.
        /// </summary>
        [TestMethod]
        public void ShouldPlanAMergeJoinOverSortedTableFunctionsInBothConventions()
        {
            foreach (var clr in new[] { true, false })
            {
                var plan = ClrEnumerableDifferentialTests.PlanOfFib(Sql, clr, true);

                plan.Should().Contain("MergeJoin");
                plan.Should().Contain("Sort");
                plan.Should().Contain("TableFunctionScan");
            }
        }

        /// <summary>
        /// This convention answers the query.
        /// </summary>
        [TestMethod]
        public void ShouldRunAMergeJoinOverSortedTableFunctions()
        {
            ClrEnumerableDifferentialTests.RunFib(Sql, true, true).Should().Equal(Expected);
        }

        /// <summary>
        /// <c>EnumerableConvention</c> does not, and that is the demonstration.
        /// </summary>
        /// <remarks>
        /// The sort declares its physical type SCALAR — so its Java row type is <c>long</c> — while yielding
        /// the table function's <c>Object[]</c> rows unchanged, and the merge join's key accessor then reads
        /// the row as the value.
        ///
        /// <para><b>When this test starts failing, Calcite has fixed it and the divergence should go.</b>
        /// Flipping those five nodes back to the optimising overload is a one-line change per node, and
        /// <c>ShouldRunATableFunctionInAJoin</c> is what fails if it is done too early — measured.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldShowThatCalciteCannotRunTheSameQuery()
        {
            var act = () => ClrEnumerableDifferentialTests.RunFib(Sql, false, true);

            act.Should().Throw<InvalidCastException>()
                .WithMessage("*System.Object[]*java.lang.Long*");
        }

    }

}
