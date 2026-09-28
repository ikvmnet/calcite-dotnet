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
    /// The operator names, and that none collides with a name Calcite already uses.
    /// </summary>
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
        /// A connection chains the libraries its <c>fun</c> property names ahead of the catalog reader, so a
        /// Calcite function of the same name would silently take the place of the schema's declaration.
        /// Checked against every library, case-insensitively, as a name matcher may compare.
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
        /// The unprefixed name <c>CONTAINS</c> is already Calcite's SQL:2011 period predicate.
        /// </summary>
        [Fact]
        public void ShouldFindTheUnprefixedNameAlreadyTaken()
        {
            CalciteNames().Should().Contain("CONTAINS", "the SQL:2011 period predicate is why CLR_FT_CONTAINS carries a prefix");

            SqlStdOperatorTable.CONTAINS.getName().Should().Be("CONTAINS");
            SqlStdOperatorTable.CONTAINS.getKind().Should().Be(SqlKind.CONTAINS);
        }

        /// <summary>
        /// Neither Calcite's standard table nor any library table has a full text operator under the names
        /// the stores use.
        /// </summary>
        /// <remarks>
        /// If this fails, Calcite has gained full text operators that these could map onto.
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
        /// The operator names, spelled out, since renaming one breaks every query written against it.
        /// </summary>
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

            // IsFullText keeps its own list of names, which must agree with the table
            foreach (var name in names)
                FullTextOperatorTable.IsFullText(Named(name)).Should().BeTrue();
        }

        /// <summary>
        /// The recognition helpers answer <c>false</c> for other operators and for <c>null</c>, and separate
        /// predicates, scores and term constructors.
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
