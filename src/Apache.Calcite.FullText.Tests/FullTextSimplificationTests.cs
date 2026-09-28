using Apache.Calcite.FullText.Rel.Rules;

using FluentAssertions;

using org.apache.calcite.plan;
using org.apache.calcite.rel;

using Xunit;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// The rewrites <see cref="FullTextRules"/> makes, checked on plan text; there is no evaluator to compare
    /// rows against.
    /// </summary>
    public class FullTextSimplificationTests
    {

        /// <summary>
        /// Plans a statement, runs the pass over it, and returns the plan text.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="chain">Whether to chain the operator table as well as declaring on the schema.</param>
        /// <returns>The simplified plan.</returns>
        static string Simplified(string sql, bool chain = true)
        {
            return RelOptUtil.toString(Simplify(FullTextFixture.Plan(sql, chain: chain)));
        }

        /// <summary>
        /// Plans a statement and returns the plan text without simplifying it.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="chain">Whether to chain the operator table as well as declaring on the schema.</param>
        /// <returns>The plan as written.</returns>
        static string Written(string sql, bool chain = true)
        {
            return RelOptUtil.toString(FullTextFixture.Plan(sql, chain: chain));
        }

        /// <summary>
        /// Runs <see cref="FullTextRules.Program"/> over a logical plan.
        /// </summary>
        /// <remarks>
        /// <c>Programs.hep</c> builds its own <c>HepPlanner</c> and ignores the planner it is passed, so none
        /// is passed.
        /// </remarks>
        /// <param name="rel">The logical plan to rewrite.</param>
        /// <returns>The plan with every full text simplification applied.</returns>
        static RelNode Simplify(RelNode rel)
        {
            return FullTextRules.Program().run(null!, rel, rel.getTraitSet(), java.util.Collections.emptyList(), java.util.Collections.emptyList());
        }

        /// <summary>
        /// Asking for every one of a list of one keyword, or any of it, is asking for that keyword.
        /// </summary>
        [Theory]
        [InlineData("CLR_FT_CONTAINS_ALL", true)]
        [InlineData("CLR_FT_CONTAINS_ANY", true)]
        [InlineData("CLR_FT_CONTAINS_ALL", false)]
        [InlineData("CLR_FT_CONTAINS_ANY", false)]
        public void ShouldNameASingleKeywordSearchAsContains(string written, bool chain)
        {
            var sql = $"SELECT ID FROM DOCS WHERE {written}(BODY, 'steel')";

            Written(sql, chain).Should().Contain(written);

            var plan = Simplified(sql, chain);
            plan.Should().Contain("CLR_FT_CONTAINS(");
            plan.Should().NotContain(written);
        }

        /// <summary>
        /// A keyword a call already carries adds nothing to it.
        /// </summary>
        [Fact]
        public void ShouldRemoveAKeywordACallAlreadyCarries()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame', 'steel')";

            Simplified(sql).Should().Contain("CLR_FT_CONTAINS_ALL($1, 'steel', 'frame')");
        }

        /// <summary>
        /// Deduplicating down to one keyword names the call as itself.
        /// </summary>
        [Fact]
        public void ShouldNameADeduplicatedSearchOfOneKeywordAsContains()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ANY(BODY, 'steel', 'steel')";

            var plan = Simplified(sql);
            plan.Should().Contain("CLR_FT_CONTAINS(");
            plan.Should().NotContain("CLR_FT_CONTAINS_ANY");
        }

        /// <summary>
        /// A fuzzy term of no edits is the term.
        /// </summary>
        [Fact]
        public void ShouldDropAFuzzyTermOfNoEdits()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_FUZZY('steel', 0))";

            Written(sql).Should().Contain("CLR_FT_FUZZY");
            Simplified(sql).Should().NotContain("CLR_FT_FUZZY").And.Contain("CLR_FT_CONTAINS(");
        }

        /// <summary>
        /// A fuzzy term of some edits is not.
        /// </summary>
        [Fact]
        public void ShouldKeepAFuzzyTermOfSomeEdits()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_FUZZY('bycycle', 2))";

            Simplified(sql).Should().Contain("CLR_FT_FUZZY");
        }

        /// <summary>
        /// A weight of one is dropped.
        /// </summary>
        [Fact]
        public void ShouldDropAWeightOfOne()
        {
            const string sql = "SELECT CLR_FT_WEIGHT(CLR_FT_SCORE(BODY, 'steel'), 1.0) FROM DOCS";

            Written(sql).Should().Contain("CLR_FT_WEIGHT");
            Simplified(sql).Should().NotContain("CLR_FT_WEIGHT").And.Contain("CLR_FT_SCORE");
        }

        /// <summary>
        /// A weight other than one is kept.
        /// </summary>
        [Fact]
        public void ShouldKeepAWeightThatIsNotOne()
        {
            const string sql = "SELECT CLR_FT_WEIGHT(CLR_FT_SCORE(BODY, 'steel'), 0.9) FROM DOCS";

            Simplified(sql).Should().Contain("CLR_FT_WEIGHT");
        }

        /// <summary>
        /// A weight of one over a score of another type is kept, since dropping it would change the
        /// expression's type from <c>DOUBLE</c>.
        /// </summary>
        [Fact]
        public void ShouldKeepAWeightOfOneOverAScoreOfAnotherType()
        {
            const string sql = "SELECT CLR_FT_WEIGHT(CAST(ID AS REAL), 1.0) FROM DOCS";

            Simplified(sql).Should().Contain("CLR_FT_WEIGHT");
        }

        /// <summary>
        /// A conjunction over one searched expression merges into <c>CLR_FT_CONTAINS_ALL</c>.
        /// </summary>
        [Fact]
        public void ShouldMergeAConjunctionOverOneSearchedExpression()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel') AND CLR_FT_CONTAINS(BODY, 'frame')";

            Simplified(sql).Should().Contain("CLR_FT_CONTAINS_ALL($1, 'steel', 'frame')");
        }

        /// <summary>
        /// A disjunction over one searched expression merges into <c>CLR_FT_CONTAINS_ANY</c>.
        /// </summary>
        [Fact]
        public void ShouldMergeADisjunctionOverOneSearchedExpression()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel') OR CLR_FT_CONTAINS(BODY, 'frame')";

            Simplified(sql).Should().Contain("CLR_FT_CONTAINS_ANY($1, 'steel', 'frame')");
        }

        /// <summary>
        /// An all-of already written absorbs the rest of the conjunction.
        /// </summary>
        [Fact]
        public void ShouldMergeAConjunctionIntoAnAllOfAlreadyWritten()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame') AND CLR_FT_CONTAINS(BODY, 'red')";

            Simplified(sql).Should().Contain("CLR_FT_CONTAINS_ALL($1, 'steel', 'frame', 'red')");
        }

        /// <summary>
        /// Two searches of different things are two searches.
        /// </summary>
        [Fact]
        public void ShouldNotMergeCallsOverDifferentSearchedExpressions()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel') AND CLR_FT_CONTAINS(DOC, 'frame')";

            var plan = Simplified(sql);
            plan.Should().NotContain("CLR_FT_CONTAINS_ALL");
            plan.Should().Contain("CLR_FT_CONTAINS($1, 'steel')");
            plan.Should().Contain("CLR_FT_CONTAINS($2, 'frame':VARCHAR)");
        }

        /// <summary>
        /// A conjunction keeps whatever else it was carrying.
        /// </summary>
        [Fact]
        public void ShouldKeepTheRestOfAConjunctionItMerges()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel') AND ID > 3 AND CLR_FT_CONTAINS(BODY, 'frame')";

            var plan = Simplified(sql);
            plan.Should().Contain("CLR_FT_CONTAINS_ALL($1, 'steel', 'frame')");
            plan.Should().Contain(">($0, 3)");
        }

        /// <summary>
        /// An any-of is not merged into a conjunction, nor an all-of into a disjunction.
        /// </summary>
        [Fact]
        public void ShouldNotMergeTheOtherKindOfSearchIntoAConjunction()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ANY(BODY, 'steel', 'iron') AND CLR_FT_CONTAINS(BODY, 'frame')";

            var plan = Simplified(sql);
            plan.Should().Contain("CLR_FT_CONTAINS_ANY($1, 'steel', 'iron')");
            plan.Should().Contain("CLR_FT_CONTAINS($1, 'frame')");
        }

        /// <summary>
        /// A conjunction that merges down to a keyword it already held keeps going until it settles.
        /// </summary>
        /// <remarks>
        /// The merge produces <c>CLR_FT_CONTAINS_ALL($1, 'steel', 'steel', 'frame')</c> and the deduplication
        /// happens on a later visit, so this depends on the hep pass running to a fixed point, and on that
        /// fixed point being reached.
        /// </remarks>
        [Fact]
        public void ShouldSettleWhereAMergeLeavesAKeywordTwice()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel') AND CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame')";

            var plan = Simplified(sql);
            plan.Should().Contain("CLR_FT_CONTAINS_ALL($1, 'steel', 'frame')");
            plan.Should().NotContain("'steel', 'steel'");
        }

        /// <summary>
        /// A phrase of one word is not unwrapped, since whether the text is one token is the store's
        /// analyzer's decision.
        /// </summary>
        [Fact]
        public void ShouldNotUnwrapAPhraseOfOneWord()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_PHRASE('steel'))";

            Simplified(sql).Should().Contain("CLR_FT_PHRASE('steel')");
        }

        /// <summary>
        /// A fusion of identical scores is kept, since fusion preserves an ordering rather than a value.
        /// </summary>
        [Fact]
        public void ShouldNotUnwrapAFusionOfScoresThatAreTheSame()
        {
            const string sql = "SELECT CLR_FT_RRF(CLR_FT_SCORE(BODY, 'steel'), CLR_FT_SCORE(BODY, 'steel')) FROM DOCS";

            Simplified(sql).Should().Contain("CLR_FT_RRF");
        }

    }

}
