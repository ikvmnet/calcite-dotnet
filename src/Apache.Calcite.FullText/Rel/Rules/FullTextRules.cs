using System;
using System.Collections.Generic;

using Apache.Calcite.FullText.Sql;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.tools;

namespace Apache.Calcite.FullText.Rel.Rules
{

    /// <summary>
    /// Rules that simplify <c>CLR_FT_*</c> expressions.
    /// </summary>
    /// <remarks>
    /// <para>A host sequences the pass in front of whatever program it runs:</para>
    ///
    /// <code>
    /// Programs.sequence(FullTextRules.Program(), Programs.standard())
    /// </code>
    ///
    /// <para><b>A pass and not rules on the planner.</b> <c>VolcanoCost.isLt</c> compares the row count and
    /// nothing else, so a filter whose condition was simplified is never <em>cheaper</em> than the same
    /// filter unsimplified and the planner keeps whichever it registered first — which is the original. This
    /// is the same argument that keeps <c>Programs.calc</c> a hep pass, and it was measured against the
    /// sibling package before either was written this way.</para>
    ///
    /// <para><b>Every rewrite here is an equality of values</b>, so each is valid wherever an expression can
    /// stand. That includes the two that merge calls, and the reason is worth stating because it is not
    /// obvious: these operators are nullable so that a store can have nothing to say about a row a plan
    /// keeps — an outer join's unmatched side, a row outside the searched partition — and that is a property
    /// of the row rather than of the keyword. So every call over one searched expression is null on the same
    /// rows, and <c>CLR_FT_CONTAINS(x, a) AND CLR_FT_CONTAINS(x, b)</c> is null exactly where
    /// <c>CLR_FT_CONTAINS_ALL(x, a, b)</c> is. The merge needs no filter context to be sound.</para>
    ///
    /// <para><b>What is deliberately not here.</b> <c>CLR_FT_PHRASE('steel')</c> is not unwrapped to
    /// <c>'steel'</c>, though a single-token phrase is the same search in every store surveyed. Whether that
    /// text is one token is the analyzer's answer, not this package's — <c>'red-bicycle'</c> is two tokens
    /// under some and one under others — and deciding it here is the in-process approximation the package
    /// exists to refuse. Nor is <c>CLR_FT_RRF</c> of a single score unwrapped: reciprocal rank fusion is a
    /// rank transform, so it preserves an ordering and not a value, and these rewrites are value
    /// equalities.</para>
    ///
    /// <para><b>No strictness is declared on these operators</b>, unlike the sibling package's. A
    /// <c>Strong.Policy.ANY</c> would let <c>RexSimplify</c> rewrite
    /// <c>CLR_FT_CONTAINS(BODY, 'a') IS NULL</c> into <c>BODY IS NULL</c>, which is a claim about the store:
    /// Cosmos answers false for a missing property and PostgreSQL answers null. The package cannot know
    /// which, so it says nothing.</para>
    /// </remarks>
    public static class FullTextRules
    {

        /// <summary>
        /// Simplifies the condition of a <see cref="Filter"/>.
        /// </summary>
        public static readonly RelOptRule Filter =
            new FullTextRule(Config("FullTextFilterRule", (java.lang.Class)typeof(Filter)));

        /// <summary>
        /// Simplifies the expressions of a <see cref="Project"/>.
        /// </summary>
        public static readonly RelOptRule Project =
            new FullTextRule(Config("FullTextProjectRule", (java.lang.Class)typeof(Project)));

        /// <summary>
        /// Simplifies the condition of a <see cref="Join"/>.
        /// </summary>
        public static readonly RelOptRule Join =
            new FullTextRule(Config("FullTextJoinRule", (java.lang.Class)typeof(Join)));

        /// <summary>
        /// Returns every rule in this set.
        /// </summary>
        /// <returns></returns>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return [Filter, Project, Join];
        }

        /// <summary>
        /// Returns these rules as a pass a host sequences in front of its own program.
        /// </summary>
        /// <returns></returns>
        public static Program Program()
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in Rules())
                rules.add(rule);

            return Programs.hep(rules, true, DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Returns the given expression with every <c>CLR_FT_</c> simplification applied.
        /// </summary>
        /// <param name="rexBuilder"></param>
        /// <param name="node"></param>
        /// <returns>The simplified expression, or the one given where nothing applied.</returns>
        /// <remarks>
        /// Public because the rules are not the only way to want this: an adapter walking a plan to render it
        /// wants the canonical form of a call before it starts matching names.
        /// </remarks>
        public static RexNode Simplify(RexBuilder rexBuilder, RexNode node)
        {
            ArgumentNullException.ThrowIfNull(rexBuilder);
            ArgumentNullException.ThrowIfNull(node);

            return (RexNode)node.accept(new Shuttle(rexBuilder));
        }

        /// <summary>
        /// Returns a configuration matching the given node class.
        /// </summary>
        /// <param name="description"></param>
        /// <param name="relClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>FilterToCalcRule</c>'s configuration is borrowed and re-pointed because there is no neutral one
        /// to start from: <c>RelRule.Config</c> is an interface with no instance of its own, and every
        /// concrete configuration is generated by immutables from some rule's own sub-interface. Only
        /// <c>operandSupplier</c>, <c>description</c> and <c>relBuilderFactory</c> are ever read from it, and
        /// <c>toRule</c> is never called, the rule being constructed directly.
        /// </remarks>
        static RelRule.Config Config(string description, java.lang.Class relClass)
        {
            return ((RelRule.Config)FilterToCalcRule.Config.DEFAULT)
                .withOperandSupplier(new OperandTransform(b => b.operand(relClass).anyInputs()))
                .withDescription(description);
        }

        /// <summary>
        /// A <see cref="RelRule.OperandTransform"/> backed by a delegate.
        /// </summary>
        /// <param name="transform"></param>
        sealed class OperandTransform(Func<RelRule.OperandBuilder, RelRule.Done> transform) : RelRule.OperandTransform
        {

            /// <inheritdoc />
            public object apply(object builder)
            {
                return transform((RelRule.OperandBuilder)builder);
            }

            /// <inheritdoc />
            /// <remarks>
            /// C# does not inherit the defaults of an interface IKVM compiled, so composition is forwarded
            /// rather than left to <see cref="java.util.function.Function"/>.
            /// </remarks>
            public java.util.function.Function andThen(java.util.function.Function after)
            {
                return java.util.function.Function.__DefaultMethods.andThen(this, after);
            }

            /// <inheritdoc cref="andThen" />
            public java.util.function.Function compose(java.util.function.Function before)
            {
                return java.util.function.Function.__DefaultMethods.compose(this, before);
            }

        }

        /// <summary>
        /// Runs the shuttle over whatever expressions the matched node holds.
        /// </summary>
        /// <param name="config"></param>
        sealed class FullTextRule(RelRule.Config config) : RelRule(config)
        {

            /// <inheritdoc />
            public override void onMatch(RelOptRuleCall call)
            {
                var rel = call.rel(0);
                var shuttle = new Shuttle(rel.getCluster().getRexBuilder());
                var rewritten = rel.accept(shuttle);

                if (shuttle.Changed == false || ReferenceEquals(rewritten, rel))
                    return;

                call.transformTo(rewritten);
            }

        }

        /// <summary>
        /// The simplifications themselves.
        /// </summary>
        /// <param name="rexBuilder"></param>
        sealed class Shuttle(RexBuilder rexBuilder) : RexShuttle
        {

            /// <summary>
            /// Whether anything was rewritten.
            /// </summary>
            public bool Changed { get; private set; }

            /// <inheritdoc />
            public override RexNode visitCall(RexCall call)
            {
                var visited = (RexCall)base.visitCall(call);

                var simplified =
                    Fuzzy(visited) ??
                    Weight(visited) ??
                    Keywords(visited) ??
                    Merge(visited);

                if (simplified is null)
                    return visited;

                Changed = true;
                return simplified;
            }

            /// <summary>
            /// Drops a fuzzy term of no edits, which is the term.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// The edit count is Levenshtein in every store that has one, and zero edits is an exact match in
            /// all of them. What is left is the bare keyword, which is what a keyword position means.
            /// </remarks>
            static RexNode? Fuzzy(RexCall call)
            {
                if (FullTextOperatorTable.Matches(call.getOperator(), FullTextOperatorTable.ClrFtFuzzy) == false)
                    return null;

                return Exactly(call.getOperands().get(1), java.math.BigDecimal.ZERO)
                    ? (RexNode)call.getOperands().get(0)
                    : null;
            }

            /// <summary>
            /// Drops a weight of one, which counts for nothing.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <c>CLR_FT_WEIGHT</c>'s own declaration says it: a store with no weighting renders the inner
            /// score and declines only where the weight is not one. The type has to agree, because the
            /// operator answers <c>DOUBLE</c> and the score it wraps need not be one — an adapter's own
            /// vector distance may be anything numeric — and dropping the call would then hand the
            /// expression above a differently typed value.
            /// </remarks>
            static RexNode? Weight(RexCall call)
            {
                if (FullTextOperatorTable.Matches(call.getOperator(), FullTextOperatorTable.ClrFtWeight) == false)
                    return null;

                if (Exactly(call.getOperands().get(1), java.math.BigDecimal.ONE) == false)
                    return null;

                var score = (RexNode)call.getOperands().get(0);

                return call.getType().Equals(score.getType()) ? score : null;
            }

            /// <summary>
            /// Removes a keyword a call already carries, and names a one-keyword call as itself.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// Asking for every one of a list that names a keyword twice is asking for it once, and asking
            /// for any of it likewise. What remains may be a single keyword, and
            /// <c>CLR_FT_CONTAINS_ALL(x, k)</c> and <c>CLR_FT_CONTAINS_ANY(x, k)</c> both mean
            /// <c>CLR_FT_CONTAINS(x, k)</c> — which <c>CLR_FT_CONTAINS</c>'s own declaration says, and is why
            /// a single-keyword search reads better written as itself.
            /// </remarks>
            RexNode? Keywords(RexCall call)
            {
                if (FullTextOperatorTable.Matches(call.getOperator(), FullTextOperatorTable.ClrFtContainsAll) == false &&
                    FullTextOperatorTable.Matches(call.getOperator(), FullTextOperatorTable.ClrFtContainsAny) == false)
                    return null;

                var operands = call.getOperands();
                var searched = (RexNode)operands.get(0);

                var keywords = new List<RexNode>();
                for (var i = 1; i < operands.size(); i++)
                    if (keywords.Exists(k => k.Equals(operands.get(i))) == false)
                        keywords.Add((RexNode)operands.get(i));

                if (keywords.Count == 1)
                    return Call(call.getType(), FullTextOperatorTable.ClrFtContains, [searched, keywords[0]]);

                if (keywords.Count == operands.size() - 1)
                    return null;

                return Call(call.getType(), call.getOperator(), [searched, .. keywords]);
            }

            /// <summary>
            /// Merges the calls of a conjunction or a disjunction that search one thing.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <para>A conjunction of <c>CLR_FT_CONTAINS</c> and <c>CLR_FT_CONTAINS_ALL</c> over one searched
            /// expression is one <c>CLR_FT_CONTAINS_ALL</c>, and a disjunction of <c>CLR_FT_CONTAINS</c> and
            /// <c>CLR_FT_CONTAINS_ANY</c> is one <c>CLR_FT_CONTAINS_ANY</c>. That is the form the stores
            /// offering an all-of or any-of have — Cosmos's <c>FullTextContainsAll</c>, PostgreSQL's
            /// <c>to_tsquery('a &amp; b')</c>, SQL Server's <c>CONTAINS('a AND b')</c> — so it is one index
            /// lookup where the conjunction was two.</para>
            ///
            /// <para><b>One searched expression, compared by digest.</b> Two calls over different expressions
            /// are two searches, and merging them would be a different question. And it is the sameness of
            /// that expression that makes the merge exact under three-valued logic; see the remarks on this
            /// class.</para>
            /// </remarks>
            RexNode? Merge(RexCall call)
            {
                var merged = call.getKind() switch
                {
                    var k when k == SqlKind.AND => FullTextOperatorTable.ClrFtContainsAll,
                    var k when k == SqlKind.OR => FullTextOperatorTable.ClrFtContainsAny,
                    _ => null,
                };

                if (merged is null)
                    return null;

                // group the searchable terms by what they search, keeping the order the query wrote
                var groups = new List<(RexNode Searched, List<RexNode> Keywords, int At)>();
                var operands = call.getOperands();
                var result = new List<RexNode?>();

                for (var i = 0; i < operands.size(); i++)
                {
                    var operand = (RexNode)operands.get(i);
                    result.Add(operand);

                    if (operand is not RexCall term)
                        continue;
                    if (FullTextOperatorTable.Matches(term.getOperator(), FullTextOperatorTable.ClrFtContains) == false &&
                        FullTextOperatorTable.Matches(term.getOperator(), merged) == false)
                        continue;

                    var searched = (RexNode)term.getOperands().get(0);
                    var at = groups.FindIndex(g => g.Searched.Equals(searched));

                    if (at < 0)
                    {
                        groups.Add((searched, KeywordsOf(term), i));
                        continue;
                    }

                    groups[at].Keywords.AddRange(KeywordsOf(term));
                    result[i] = null;
                }

                var changed = false;

                foreach (var group in groups)
                {
                    if (result[group.At] is not RexCall original)
                        continue;
                    if (group.Keywords.Count == original.getOperands().size() - 1)
                        continue;

                    result[group.At] = Call(original.getType(), merged, [group.Searched, .. group.Keywords]);
                    changed = true;
                }

                if (changed == false)
                    return null;

                var kept = new java.util.ArrayList();
                foreach (var operand in result)
                    if (operand is not null)
                        kept.add(operand);

                // composeConjunction rather than a call, because a merge can leave one operand and neither
                // AND nor OR takes one; and it is what decides the nullability of what is left
                return call.getKind() == SqlKind.AND
                    ? RexUtil.composeConjunction(rexBuilder, kept)
                    : RexUtil.composeDisjunction(rexBuilder, kept);

                static List<RexNode> KeywordsOf(RexCall term)
                {
                    var keywords = new List<RexNode>();
                    for (var i = 1; i < term.getOperands().size(); i++)
                        keywords.Add((RexNode)term.getOperands().get(i));

                    return keywords;
                }
            }

            /// <summary>
            /// Determines whether an operand is a literal of exactly the given value.
            /// </summary>
            /// <param name="operand"></param>
            /// <param name="value"></param>
            /// <returns></returns>
            /// <remarks>
            /// <c>compareTo</c> rather than <c>equals</c>, because a <c>BigDecimal</c>'s equality counts the
            /// scale and <c>1</c>, <c>1.0</c> and <c>1.00</c> are three of them.
            /// </remarks>
            static bool Exactly(object operand, java.math.BigDecimal value)
            {
                if (operand is not RexLiteral literal || literal.isNull())
                    return false;

                return literal.getValueAs((java.lang.Class)typeof(java.math.BigDecimal)) is java.math.BigDecimal number &&
                    number.compareTo(value) == 0;
            }

            /// <summary>
            /// Builds a call of the given operator, keeping the type the expression already had.
            /// </summary>
            /// <param name="type"></param>
            /// <param name="op"></param>
            /// <param name="operands"></param>
            /// <returns></returns>
            /// <remarks>
            /// The type is carried over rather than inferred again, because the two routes into a plan do not
            /// type a call alike: this table's operators answer <c>ReturnTypes.BOOLEAN_NULLABLE</c>, and the
            /// ones <c>CalciteCatalogReader.toOp</c> builds around the schema declarations answer whatever
            /// <c>FullTextSchemaFunction</c> says. A rewrite is not the place to change which a plan has.
            /// </remarks>
            RexNode Call(org.apache.calcite.rel.type.RelDataType type, SqlOperator op, RexNode[] operands)
            {
                var list = new java.util.ArrayList(operands.Length);
                foreach (var operand in operands)
                    list.add(operand);

                return rexBuilder.makeCall(type, op, list);
            }

        }

    }

}
