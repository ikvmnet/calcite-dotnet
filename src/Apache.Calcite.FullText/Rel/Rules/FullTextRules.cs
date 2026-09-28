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
    /// Rules that simplify <c>CLR_FT_*</c> expressions in filters, projections and join conditions.
    /// </summary>
    /// <remarks>
    /// <para>The rewrites are:</para>
    /// <list type="bullet">
    /// <item><c>CLR_FT_CONTAINS_ALL</c> or <c>CLR_FT_CONTAINS_ANY</c> with a repeated keyword loses the
    /// repeat, and with a single keyword becomes <c>CLR_FT_CONTAINS</c>.</item>
    /// <item><c>CLR_FT_FUZZY(t, 0)</c> becomes <c>t</c>.</item>
    /// <item><c>CLR_FT_WEIGHT(s, 1)</c> becomes <c>s</c> where <c>s</c> has the call's own type, a nullable
    /// <c>DOUBLE</c>.</item>
    /// <item>Within one <c>AND</c>, <c>CLR_FT_CONTAINS</c> and <c>CLR_FT_CONTAINS_ALL</c> calls over the same
    /// searched expression merge into one <c>CLR_FT_CONTAINS_ALL</c>; within one <c>OR</c>,
    /// <c>CLR_FT_CONTAINS</c> and <c>CLR_FT_CONTAINS_ANY</c> calls merge into one
    /// <c>CLR_FT_CONTAINS_ANY</c>.</item>
    /// </list>
    ///
    /// <para>Each rewrite preserves the value, nulls included: a store's null answer is a property of the
    /// row, so every call over one searched expression is null on the same rows.
    /// <c>CLR_FT_PHRASE</c> of a single word is not unwrapped, because whether text is one token is the
    /// store's analyzer's decision, and <c>CLR_FT_RRF</c> of one score is not unwrapped, because fusion
    /// preserves an ordering rather than a value.</para>
    ///
    /// <para>Run the rules as a separate pass, <see cref="Program"/>, ahead of the host's own program:</para>
    ///
    /// <code>
    /// Programs.sequence(FullTextRules.Program(), Programs.standard())
    /// </code>
    ///
    /// <para>They are not effective on a <c>VolcanoPlanner</c>: <c>VolcanoCost.isLt</c> compares row counts only,
    /// so a simplified filter is never cheaper than the original and the planner keeps whichever it
    /// registered first.</para>
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
        /// <returns><see cref="Filter"/>, <see cref="Project"/> and <see cref="Join"/>.</returns>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return [Filter, Project, Join];
        }

        /// <summary>
        /// Returns these rules as a program for a host to sequence ahead of its own.
        /// </summary>
        /// <returns>
        /// A <c>Programs.hep</c> program over <see cref="Rules"/>, run to a fixed point with Calcite's default
        /// metadata provider. It builds its own <c>HepPlanner</c> and ignores the planner it is passed.
        /// </returns>
        public static Program Program()
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in Rules())
                rules.add(rule);

            return Programs.hep(rules, true, DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Returns the given expression with the <c>CLR_FT_*</c> simplifications applied in one bottom-up pass.
        /// </summary>
        /// <param name="rexBuilder">The builder used to create rewritten calls.</param>
        /// <param name="node">The expression to simplify.</param>
        /// <returns>The simplified expression, or <paramref name="node"/> where nothing applied.</returns>
        /// <remarks>
        /// For an adapter that wants the simplified form of an expression before rendering it, without running
        /// the rules. One pass does not reach a fixed point: a merge that repeats a keyword is not deduplicated
        /// until the expression is simplified again.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="rexBuilder"/> or <paramref name="node"/> is <c>null</c>.</exception>
        public static RexNode Simplify(RexBuilder rexBuilder, RexNode node)
        {
            ArgumentNullException.ThrowIfNull(rexBuilder);
            ArgumentNullException.ThrowIfNull(node);

            return (RexNode)node.accept(new Shuttle(rexBuilder));
        }

        /// <summary>
        /// Returns a rule configuration whose operand matches any node of the given class.
        /// </summary>
        /// <remarks>
        /// Starts from <c>FilterToCalcRule.Config.DEFAULT</c> because <c>RelRule.Config</c> has no instance of
        /// its own; every concrete configuration is generated for some rule's sub-interface. Only the operand
        /// supplier, description and rel builder factory are read from it, and <c>toRule</c> is never called.
        /// </remarks>
        /// <param name="description">The name the rule is reported under in planner traces.</param>
        /// <param name="relClass">The class of node the operand matches, subclasses included.</param>
        /// <returns>A configuration to construct a <see cref="FullTextRule"/> from.</returns>
        static RelRule.Config Config(string description, java.lang.Class relClass)
        {
            return ((RelRule.Config)FilterToCalcRule.Config.DEFAULT)
                .withOperandSupplier(new OperandTransform(b => b.operand(relClass).anyInputs()))
                .withDescription(description);
        }

        /// <summary>
        /// A <see cref="RelRule.OperandTransform"/> backed by a delegate.
        /// </summary>
        /// <param name="transform">Builds the operand from the builder it is given.</param>
        sealed class OperandTransform(Func<RelRule.OperandBuilder, RelRule.Done> transform) : RelRule.OperandTransform
        {

            /// <inheritdoc />
            public object apply(object builder)
            {
                return transform((RelRule.OperandBuilder)builder);
            }

            /// <inheritdoc />
            /// <remarks>
            /// IKVM does not expose a Java default method as a C# default interface member, so this forwards to
            /// the interface's default body.
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
        /// Runs <see cref="Shuttle"/> over the expressions of the matched node, and registers the result where
        /// anything changed.
        /// </summary>
        /// <param name="config">The configuration carrying the rule's operand and description.</param>
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
        /// Applies the simplifications to each call, operands first, taking the first rewrite that applies.
        /// </summary>
        /// <param name="rexBuilder">Builds the calls that replace the rewritten ones.</param>
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
            /// Rewrites <c>CLR_FT_FUZZY(t, 0)</c> to <c>t</c>; zero edits is an exact match in every store
            /// with fuzzy search.
            /// </summary>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
            /// <param name="call">The call, its operands already rewritten.</param>
            static RexNode? Fuzzy(RexCall call)
            {
                if (FullTextOperatorTable.Matches(call.getOperator(), FullTextOperatorTable.ClrFtFuzzy) == false)
                    return null;

                return Exactly(call.getOperands().get(1), java.math.BigDecimal.ZERO)
                    ? (RexNode)call.getOperands().get(0)
                    : null;
            }

            /// <summary>
            /// Rewrites <c>CLR_FT_WEIGHT(s, 1)</c> to <c>s</c>.
            /// </summary>
            /// <remarks>
            /// Only where <c>s</c> has the call's own type. The score may be any numeric type, and the call
            /// answers a nullable <c>DOUBLE</c>, so dropping it otherwise would change the expression's type.
            /// </remarks>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
            /// <param name="call">The call, its operands already rewritten.</param>
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
            /// Removes repeated keywords from <c>CLR_FT_CONTAINS_ALL</c> or <c>CLR_FT_CONTAINS_ANY</c>, and
            /// rewrites a call left with one keyword to <c>CLR_FT_CONTAINS</c>.
            /// </summary>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
            /// <param name="call">The call, its operands already rewritten.</param>
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
            /// Merges the <c>CLR_FT_CONTAINS</c> and <c>CLR_FT_CONTAINS_ALL</c> operands of an <c>AND</c> into
            /// one <c>CLR_FT_CONTAINS_ALL</c>, or the <c>CLR_FT_CONTAINS</c> and <c>CLR_FT_CONTAINS_ANY</c>
            /// operands of an <c>OR</c> into one <c>CLR_FT_CONTAINS_ANY</c>, per searched expression.
            /// </summary>
            /// <remarks>
            /// Only calls whose searched expressions are equal are merged, and the merged call takes the place
            /// of the first of them. The merged form is one search in a store that has an all-of or any-of
            /// query, where the original was several.
            /// </remarks>
            /// <returns>The rewritten expression, or <c>null</c> where nothing merged.</returns>
            /// <param name="call">The call, its operands already rewritten.</param>
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

                // compose rather than makeCall, because a merge can leave a single operand, which AND and OR
                // do not take
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
            /// Determines whether an operand is a non-null literal numerically equal to the given value.
            /// </summary>
            /// <remarks>
            /// Uses <c>compareTo</c>, because <c>BigDecimal.equals</c> also compares scale, so <c>1</c> and
            /// <c>1.0</c> would differ.
            /// </remarks>
            /// <param name="operand">The operand to test; anything other than a <c>RexLiteral</c> answers <c>false</c>.</param>
            /// <param name="value">The value to compare with.</param>
            /// <returns><c>true</c> if <paramref name="operand"/> is a non-null literal equal to <paramref name="value"/>.</returns>
            static bool Exactly(object operand, java.math.BigDecimal value)
            {
                if (operand is not RexLiteral literal || literal.isNull())
                    return false;

                return literal.getValueAs((java.lang.Class)typeof(java.math.BigDecimal)) is java.math.BigDecimal number &&
                    number.compareTo(value) == 0;
            }

            /// <summary>
            /// Builds a call of the given operator with the given type, rather than inferring the type again.
            /// </summary>
            /// <remarks>
            /// Keeping the type of the expression being replaced guarantees the rewrite does not change it.
            /// </remarks>
            /// <param name="type">The type of the expression being replaced.</param>
            /// <param name="op">The operator to call.</param>
            /// <param name="operands">The operands, in order.</param>
            /// <returns>The new call.</returns>
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
