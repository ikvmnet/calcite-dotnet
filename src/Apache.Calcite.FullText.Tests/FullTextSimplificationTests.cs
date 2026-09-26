using Apache.Calcite.FullText.Rel.Rules;

using FluentAssertions;

using org.apache.calcite.plan;
using org.apache.calcite.rel;

using Xunit;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// What <see cref="FullTextRules"/> takes out of a plan.
    /// </summary>
    /// <remarks>
    /// Plans only. There is no evaluator behind these operators and there will not be one, so there are no
    /// rows to compare a rewrite against — which is why every rewrite here is one the vocabulary's own
    /// declarations state, rather than one measured against an answer.
    /// </remarks>
    public class FullTextSimplificationTests
    {

        /// <summary>
        /// Plans a statement and runs the pass over it.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="chain">Whether to chain the operator table rather than resolve through the
        /// schema.</param>
        /// <returns></returns>
        static string Simplified(string sql, bool chain = true)
        {
            return RelOptUtil.toString(Simplify(FullTextFixture.Plan(sql, chain: chain)));
        }

        /// <summary>
        /// Plans a statement and leaves it alone.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="chain"></param>
        /// <returns></returns>
        static string Written(string sql, bool chain = true)
        {
            return RelOptUtil.toString(FullTextFixture.Plan(sql, chain: chain));
        }

        /// <summary>
        /// Runs the pass over a logical plan, which is the whole of what a host does with it.
        /// </summary>
        /// <param name="rel"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Programs.hep</c> builds its own <c>HepPlanner</c> and ignores the planner it is handed, so there
        /// is nothing to pass one.
        /// </remarks>
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
        /// A weight of one counts for nothing, which <c>CLR_FT_WEIGHT</c>'s own declaration says.
        /// </summary>
        [Fact]
        public void ShouldDropAWeightOfOne()
        {
            const string sql = "SELECT CLR_FT_WEIGHT(CLR_FT_SCORE(BODY, 'steel'), 1.0) FROM DOCS";

            Written(sql).Should().Contain("CLR_FT_WEIGHT");
            Simplified(sql).Should().NotContain("CLR_FT_WEIGHT").And.Contain("CLR_FT_SCORE");
        }

        /// <summary>
        /// A weight that is not one is what the adapter has to render or decline.
        /// </summary>
        [Fact]
        public void ShouldKeepAWeightThatIsNotOne()
        {
            const string sql = "SELECT CLR_FT_WEIGHT(CLR_FT_SCORE(BODY, 'steel'), 0.9) FROM DOCS";

            Simplified(sql).Should().Contain("CLR_FT_WEIGHT");
        }

        /// <summary>
        /// A weight of one over a score of another type is still a conversion, and stays.
        /// </summary>
        /// <remarks>
        /// <c>CLR_FT_WEIGHT</c> answers <c>DOUBLE</c> and takes any numeric score, an adapter's own vector
        /// distance included, so dropping it where the two differ would hand the expression above a
        /// differently typed value.
        /// </remarks>
        [Fact]
        public void ShouldKeepAWeightOfOneOverAScoreOfAnotherType()
        {
            const string sql = "SELECT CLR_FT_WEIGHT(CAST(ID AS REAL), 1.0) FROM DOCS";

            Simplified(sql).Should().Contain("CLR_FT_WEIGHT");
        }

        /// <summary>
        /// A conjunction over one searched expression is the all-of form a store has an index for.
        /// </summary>
        [Fact]
        public void ShouldMergeAConjunctionOverOneSearchedExpression()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel') AND CLR_FT_CONTAINS(BODY, 'frame')";

            Simplified(sql).Should().Contain("CLR_FT_CONTAINS_ALL($1, 'steel', 'frame')");
        }

        /// <summary>
        /// And a disjunction is the any-of form.
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
        /// The merge and the deduplication are separate rewrites and a merged call is not revisited in the
        /// round that made it, so what makes this land is the hep pass running to a fixed point. Written down
        /// because a rewrite that produces work for another one is the shape that fails to terminate.
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
        /// A phrase is not unwrapped, whatever it holds.
        /// </summary>
        /// <remarks>
        /// A single-token phrase is the same search as the bare token in every store surveyed, and whether
        /// that text is one token is the analyzer's answer. Deciding it here is the in-process approximation
        /// this package exists to refuse.
        /// </remarks>
        [Fact]
        public void ShouldNotUnwrapAPhraseOfOneWord()
        {
            const string sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_PHRASE('steel'))";

            Simplified(sql).Should().Contain("CLR_FT_PHRASE('steel')");
        }

        /// <summary>
        /// A single score fused with nothing is still a rank transform, and stays.
        /// </summary>
        [Fact]
        public void ShouldNotUnwrapAFusionOfScoresThatAreTheSame()
        {
            const string sql = "SELECT CLR_FT_RRF(CLR_FT_SCORE(BODY, 'steel'), CLR_FT_SCORE(BODY, 'steel')) FROM DOCS";

            Simplified(sql).Should().Contain("CLR_FT_RRF");
        }

    }

}
