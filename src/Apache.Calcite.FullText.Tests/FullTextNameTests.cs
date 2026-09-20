using System;
using System.Collections.Generic;

using Apache.Calcite.FullText.Sql;

using FluentAssertions;

using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;

using Xunit;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// The names, and that Calcite does not already use one.
    /// </summary>
    /// <remarks>
    /// This package exists so that each adapter does not have to remember to write this test.
    /// </remarks>
    public class FullTextNameTests
    {

        static List<string> CalciteNames()
        {
            var names = new List<string>();

            foreach (var table in new SqlOperatorTable[]
            {
                SqlStdOperatorTable.instance(),
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    java.util.EnumSet.allOf(java.lang.Class.forName("org.apache.calcite.sql.fun.SqlLibrary"))),
            })
            {
                var list = table.getOperatorList();
                for (var i = 0; i < list.size(); i++)
                    names.Add(((SqlOperator)list.get(i)).getName());
            }

            return names;
        }

        /// <summary>
        /// None of these names is one Calcite already uses.
        /// </summary>
        /// <remarks>
        /// <para>Not a tidiness check. A connection chains the operator table its <c>fun</c> property names
        /// <em>before</em> the catalog reader, and overload resolution takes the first candidate whose arity
        /// fits — so the day Calcite gives some library a function called <c>CLR_FT_SCORE</c>, that operator
        /// answers and the schema's declaration stops being reached, silently and only for hosts that set
        /// <c>fun</c>. The failure would be a wrong statement rather than an error, and nothing else here
        /// would notice.</para>
        ///
        /// <para>Measured against every library Calcite ships rather than the few chained elsewhere in this
        /// suite, and case-insensitively, because that is how a name matcher would find one.</para>
        /// </remarks>
        [Fact]
        public void ShouldNotTakeANameCalciteAlreadyUses()
        {
            var calcite = CalciteNames();

            calcite.Should().HaveCountGreaterThan(500, "both tables have to have actually loaded for this to mean anything");

            var ours = FullTextOperatorTable.Instance().getOperatorList();
            for (var i = 0; i < ours.size(); i++)
            {
                var name = ((SqlOperator)ours.get(i)).getName();

                calcite.Should().NotContain(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase),
                    "'{0}' would be shadowed by Calcite's own operator wherever a connection sets fun", name);
            }
        }

        /// <summary>
        /// The near miss is on purpose: <c>CONTAINS</c> is taken, which is why these are prefixed.
        /// </summary>
        /// <remarks>
        /// ISO/IEC 13249-2 spells its full text functions <c>Contains</c> and <c>Score</c>, and neither is
        /// available. <c>SqlStdOperatorTable.CONTAINS</c> is the SQL:2011 period predicate — a
        /// <c>SqlOverlapsOperator</c> over <c>SqlKind.CONTAINS</c>, named literally <c>CONTAINS</c> — and the
        /// parser reserves the word besides. This is what the prefix is for, and pinning it here is what says
        /// so: if Calcite ever drops the period predicate, the argument for the prefix changes and somebody
        /// should notice.
        /// </remarks>
        [Fact]
        public void ShouldFindTheUnprefixedNameAlreadyTaken()
        {
            CalciteNames().Should().Contain("CONTAINS", "the SQL:2011 period predicate is why CLR_FT_CONTAINS carries a prefix");

            SqlStdOperatorTable.CONTAINS.getName().Should().Be("CONTAINS");
            SqlStdOperatorTable.CONTAINS.getKind().Should().Be(SqlKind.CONTAINS);
        }

        /// <summary>
        /// Calcite has no full text of its own, anywhere.
        /// </summary>
        /// <remarks>
        /// The premise of the whole package: there is nothing in the standard operator table or in any of the
        /// library tables to map a full text call onto, so an adapter that wants one has to bring the operator
        /// with it. Checked against the spellings the stores use rather than against ours, since it is a
        /// claim about Calcite and not about this naming.
        /// </remarks>
        [Fact]
        public void ShouldFindNoFullTextOperatorInCalcite()
        {
            var calcite = CalciteNames();

            foreach (var name in new[]
            {
                "FULLTEXTCONTAINS", "FULLTEXTCONTAINSALL", "FULLTEXTCONTAINSANY", "FULLTEXTSCORE", "RRF",
                "FREETEXT", "CONTAINSTABLE", "FREETEXTTABLE",
                "TO_TSVECTOR", "TO_TSQUERY", "PLAINTO_TSQUERY", "PHRASETO_TSQUERY", "WEBSEARCH_TO_TSQUERY",
                "TS_RANK", "TS_RANK_CD", "TS_HEADLINE",
                "MATCH", "AGAINST", "BM25", "SNIPPET", "HIGHLIGHT",
            })
            {
                calcite.Should().NotContain(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase),
                    "Calcite has no full text operator, and '{0}' would be one", name);
            }
        }

        /// <summary>
        /// The names, spelled out, so that changing one is a deliberate act.
        /// </summary>
        /// <remarks>
        /// These are the package's whole public surface. A rename is a breaking change for every query written
        /// against it, so it should not be possible to make one by editing a declaration.
        /// </remarks>
        [Fact]
        public void ShouldCarryExactlyTheseNames()
        {
            var names = new List<string>();
            var operators = FullTextOperatorTable.Instance().getOperatorList();

            for (var i = 0; i < operators.size(); i++)
                names.Add(((SqlOperator)operators.get(i)).getName());

            names.Should().BeEquivalentTo([
                "CLR_FT_CONTAINS", "CLR_FT_CONTAINS_ALL", "CLR_FT_CONTAINS_ANY",
                "CLR_FT_SCORE", "CLR_FT_RRF", "CLR_FT_WEIGHT",
                "CLR_FT_PHRASE", "CLR_FT_PREFIX", "CLR_FT_FUZZY",
            ]);

            // and the name-based recognition agrees with the table, rather than being a second list
            foreach (var name in names)
                FullTextOperatorTable.IsFullText(Named(name)).Should().BeTrue();
        }

        /// <summary>
        /// Anything not one of ours is not recognised, which is what stops a name test from being a rubber
        /// stamp.
        /// </summary>
        [Fact]
        public void ShouldNotRecogniseSomethingElse()
        {
            FullTextOperatorTable.IsFullText(SqlStdOperatorTable.CONTAINS).Should().BeFalse();
            FullTextOperatorTable.IsFullText(SqlStdOperatorTable.LIKE).Should().BeFalse();
            FullTextOperatorTable.IsFullText(null).Should().BeFalse();

            FullTextOperatorTable.IsScoring(FullTextOperatorTable.ClrFtContains).Should().BeFalse();
            FullTextOperatorTable.IsScoring(FullTextOperatorTable.ClrFtContainsAll).Should().BeFalse();
            FullTextOperatorTable.IsScoring(FullTextOperatorTable.ClrFtContainsAny).Should().BeFalse();
            FullTextOperatorTable.IsScoring(FullTextOperatorTable.ClrFtScore).Should().BeTrue();
            FullTextOperatorTable.IsScoring(FullTextOperatorTable.ClrFtRrf).Should().BeTrue();
            FullTextOperatorTable.IsScoring(FullTextOperatorTable.ClrFtWeight).Should().BeTrue();

            FullTextOperatorTable.IsTerm(FullTextOperatorTable.ClrFtPhrase).Should().BeTrue();
            FullTextOperatorTable.IsTerm(FullTextOperatorTable.ClrFtPrefix).Should().BeTrue();
            FullTextOperatorTable.IsTerm(FullTextOperatorTable.ClrFtFuzzy).Should().BeTrue();
            FullTextOperatorTable.IsTerm(FullTextOperatorTable.ClrFtContains).Should().BeFalse();
            FullTextOperatorTable.IsTerm(FullTextOperatorTable.ClrFtScore).Should().BeFalse();
            FullTextOperatorTable.IsTerm(SqlStdOperatorTable.LIKE).Should().BeFalse();
            FullTextOperatorTable.IsTerm(null).Should().BeFalse();

            FullTextOperatorTable.Matches(FullTextOperatorTable.ClrFtScore, FullTextOperatorTable.ClrFtRrf).Should().BeFalse();
            FullTextOperatorTable.Matches(null, FullTextOperatorTable.ClrFtScore).Should().BeFalse();
        }

        static SqlOperator Named(string name)
        {
            var operators = FullTextOperatorTable.Instance().getOperatorList();

            for (var i = 0; i < operators.size(); i++)
                if (((SqlOperator)operators.get(i)).getName() == name)
                    return (SqlOperator)operators.get(i);

            throw new InvalidOperationException($"No operator '{name}'.");
        }

    }

}
