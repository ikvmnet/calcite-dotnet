using System;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Holds the one shape neither convention can run, and the defect underneath it, which is Calcite's.
    /// </summary>
    /// <remarks>
    /// <para>Sort, limit, limit-sort, spool and repeat union yield their input's rows unchanged, and build
    /// their physical type with the overload that optimises — which turns ARRAY into SCALAR for a one-field
    /// row type. A table function cannot reshape its rows, so it passes <c>optimize = false</c> and keeps an
    /// honest ARRAY; the sort above it then names SCALAR and hands the ARRAY rows on regardless. Nothing
    /// reconciles the two, in either convention: <c>EnumerableSort</c> names a format it does not produce.
    /// </para>
    ///
    /// <para><b>Every node here carries Calcite's own text, and neither convention answers the query.</b>
    /// Calcite reaches a cast and throws; this convention refuses a node further up, because
    /// <c>ClrEnumerableRelImplementor.RequireRowType</c> checks the element type a CLR sequence has to name
    /// and Java's erased <c>Enumerable</c> does not. Same defect, caught earlier and named.</para>
    ///
    /// <para><b>The fix belongs upstream</b>, in <c>EnumerableSort</c> — either it stops optimising a format
    /// it is only passing through, or it converts the rows into the one it names. Repairing it here was
    /// tried three ways — in the five pass-through nodes, in the table function scan, and once generally in
    /// the implementor — and every one of them was this project inventing a behaviour Calcite has not got.
    /// The measurement said so: across the whole suite the repair fired three times, all in
    /// <c>ClrEnumerableSort</c>, all <c>ARRAY Object[] -&gt; SCALAR</c>.</para>
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
        /// <c>EnumerableConvention</c> cannot run the query: it reads the sort's <c>Object[]</c> row as the
        /// <c>long</c> the sort said it was.
        /// </summary>
        /// <remarks>
        /// <b>When this test starts failing, Calcite has fixed <c>EnumerableSort</c></b>, and whatever it did
        /// is what this convention's sort should then say.
        /// </remarks>
        [TestMethod]
        public void ShouldShowThatCalciteCannotRunTheQuery()
        {
            var act = () => ClrEnumerableDifferentialTests.RunFib(Sql, false, true);

            act.Should().Throw<InvalidCastException>()
                .WithMessage("*System.Object[]*java.lang.Long*");
        }

        /// <summary>
        /// This convention cannot either, and refuses before it builds the plan rather than while running it.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseTheQueryNamingTheNodeAndBothTypes()
        {
            var act = () => ClrEnumerableDifferentialTests.RunFib(Sql, true, true);

            act.Should().Throw<java.lang.IllegalStateException>()
                .WithInnerException<java.lang.IllegalStateException>()
                .WithMessage("*ClrEnumerableSort handed up a sequence of System.Object[] where its row type is java.lang.Long*");
        }

    }

}
