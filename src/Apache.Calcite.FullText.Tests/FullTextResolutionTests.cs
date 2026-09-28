using System;
using System.Linq;

using Apache.Calcite.FullText.Schema;
using Apache.Calcite.FullText.Sql;

using FluentAssertions;

using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.validate;

using Xunit;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// Resolution through the chained operator table and through the schema declarations, and that both give
    /// the same plan.
    /// </summary>
    public class FullTextResolutionTests
    {

        static string Keywords(int count)
        {
            return string.Join(", ", Enumerable.Range(0, count).Select(i => $"'k{i}'"));
        }

        /// <summary>
        /// With no operator table chained, the name resolves through the schema declarations.
        /// </summary>
        [Fact]
        public void ShouldResolveThroughTheSchemaAlone()
        {
            var call = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')"),
                "CLR_FT_CONTAINS");

            call.Should().NotBeNull();
            call!.getOperands().size().Should().Be(2);
        }

        /// <summary>
        /// With no schema declarations, the name resolves through the chained operator table.
        /// </summary>
        [Fact]
        public void ShouldResolveThroughTheChainedOperatorTableAlone()
        {
            var call = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", chain: true, declare: false),
                "CLR_FT_CONTAINS");

            call.Should().NotBeNull();
            call!.getOperands().size().Should().Be(2);
        }

        /// <summary>
        /// With neither route registered, the name does not resolve; the control for the tests above.
        /// </summary>
        [Fact]
        public void ShouldNotResolveWithNeither()
        {
            var act = () => FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", declare: false);

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().Contain("CLR_FT_CONTAINS");
        }

        /// <summary>
        /// With both routes registered, a call over a character column plans as it does through the schema
        /// alone.
        /// </summary>
        /// <remarks>
        /// <see cref="ShouldSearchAnArrayColumnThroughEitherRouteButNotBoth"/> covers the case where both
        /// routes together fail.
        /// </remarks>
        [Fact]
        public void ShouldLetAHostDoBoth()
        {
            const string Sql = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ANY(BODY, 'steel', 'frame')";

            var both = FullTextFixture.Call(FullTextFixture.Plan(Sql, chain: true), "CLR_FT_CONTAINS_ANY");
            var schema = FullTextFixture.Call(FullTextFixture.Plan(Sql), "CLR_FT_CONTAINS_ANY");

            both.Should().NotBeNull();
            schema.Should().NotBeNull();
            both!.ToString().Should().Be(schema!.ToString());
            both.getType().ToString().Should().Be(schema.getType().ToString());
        }

        /// <summary>
        /// Each predicate and score produces the same plan text through either route.
        /// </summary>
        [Fact]
        public void ShouldPlanTheSameEitherWay()
        {
            foreach (var sql in new[]
            {
                "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')",
                "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame')",
                "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ANY(BODY, 'steel', 'frame', 'road')",
                "SELECT ID, CLR_FT_SCORE(BODY, 'steel') AS S FROM DOCS",
                "SELECT ID, CLR_FT_RRF(CLR_FT_SCORE(BODY, 'steel'), CLR_FT_SCORE(DOC, 'frame')) AS S FROM DOCS",
            })
            {
                var chained = FullTextFixture.Plan(sql, chain: true, declare: false);
                var declared = FullTextFixture.Plan(sql);

                org.apache.calcite.plan.RelOptUtil.toString(declared).Should()
                    .Be(org.apache.calcite.plan.RelOptUtil.toString(chained), "'{0}' must not depend on how the name was reached", sql);
            }
        }

        /// <summary>
        /// Through the schema, a call carries a <c>SqlUserDefinedFunction</c> Calcite built rather than the
        /// table's operator, and the name-based helpers still recognise it.
        /// </summary>
        [Fact]
        public void ShouldArriveAsCalcitesOwnOperatorThroughTheSchema()
        {
            var call = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')"),
                "CLR_FT_CONTAINS");

            var op = call!.getOperator();

            op.Should().BeOfType<SqlUserDefinedFunction>("the schema route hands Calcite a shape, and Calcite builds the operator");
            ReferenceEquals(op, FullTextOperatorTable.ClrFtContains).Should().BeFalse();

            FullTextOperatorTable.Matches(op, FullTextOperatorTable.ClrFtContains).Should().BeTrue();
            FullTextOperatorTable.IsFullText(op).Should().BeTrue();
            FullTextOperatorTable.IsScoring(op).Should().BeFalse();

            // the chained route carries the table's own operator, so a reference comparison would succeed only
            // on that route
            var chained = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", chain: true, declare: false),
                "CLR_FT_CONTAINS");

            ReferenceEquals(chained!.getOperator(), FullTextOperatorTable.ClrFtContains).Should().BeTrue();
        }

        /// <summary>
        /// Through the schema, a keyword list arrives as written, without <c>DEFAULT</c> padding.
        /// </summary>
        /// <remarks>
        /// <c>SqlCallBinding.operands</c> pads a call with <c>DEFAULT</c> for optional parameters, which no
        /// store can render; the declarations make every parameter required.
        /// </remarks>
        [Fact]
        public void ShouldNotPadAKeywordListWithDefaults()
        {
            for (var keywords = 1; keywords <= 4; keywords++)
            {
                var call = FullTextFixture.Call(
                    FullTextFixture.Plan($"SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, {Keywords(keywords)})"),
                    "CLR_FT_CONTAINS_ALL");

                call.Should().NotBeNull();
                call!.getOperands().size().Should().Be(keywords + 1, "the call was written with {0} keywords", keywords);
                call.ToString().Should().NotContain("DEFAULT");
            }
        }

        /// <summary>
        /// Every parameter of every declaration is required.
        /// </summary>
        [Fact]
        public void ShouldDeclareEveryParameterRequired()
        {
            var entries = FullTextSchema.Functions().entries().iterator();
            var seen = 0;

            while (entries.hasNext())
            {
                var entry = (java.util.Map.Entry)entries.next();
                var function = (FullTextSchemaFunction)entry.getValue();
                var parameters = function.getParameters();

                parameters.size().Should().Be(function.Arity);

                for (var i = 0; i < parameters.size(); i++)
                {
                    ((org.apache.calcite.schema.FunctionParameter)parameters.get(i)).isOptional()
                        .Should().BeFalse("'{0}' at arity {1} must pad nothing", entry.getKey(), function.Arity);
                }

                seen++;
            }

            seen.Should().BeGreaterThan(0);
        }

        /// <summary>
        /// A variadic operator resolves through the schema up to
        /// <see cref="FullTextSchema.VariadicOperandLimit"/> operands, and beyond it only through the chained
        /// operator table.
        /// </summary>
        [Fact]
        public void ShouldOfferAVariadicOperatorUpToTheLimitAndPastItByChaining()
        {
            var atLimit = $"SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, {Keywords(FullTextSchema.VariadicOperandLimit - 1)})";
            var past = $"SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, {Keywords(FullTextSchema.VariadicOperandLimit)})";

            FullTextFixture.Call(FullTextFixture.Plan(atLimit), "CLR_FT_CONTAINS_ALL")!
                .getOperands().size().Should().Be(FullTextSchema.VariadicOperandLimit);

            var beyond = () => FullTextFixture.Plan(past);
            beyond.Should().Throw<Exception>("a schema function is as variadic as its parameter list, and no more");

            FullTextFixture.Call(FullTextFixture.Plan(past, chain: true), "CLR_FT_CONTAINS_ALL")!
                .getOperands().size().Should().Be(FullTextSchema.VariadicOperandLimit + 1);
        }

        /// <summary>
        /// The schema declares every operator the operator table holds.
        /// </summary>
        [Fact]
        public void ShouldDeclareEveryOperatorOnTheSchema()
        {
            var declared = FullTextSchema.Functions().keySet();
            var operators = FullTextOperatorTable.Instance().getOperatorList();

            operators.size().Should().Be(9);

            for (var i = 0; i < operators.size(); i++)
                declared.contains(((SqlOperator)operators.get(i)).getName()).Should().BeTrue();
        }

        /// <summary>
        /// A term constructor stands where a keyword does, on either route, and constructors mix with plain
        /// keywords in one call.
        /// </summary>
        /// <remarks>
        /// A constructor is typed <c>ANY</c>, and <c>FamilyOperandTypeChecker</c> accepts an <c>ANY</c> operand
        /// against the <c>CHARACTER</c> keyword family.
        /// </remarks>
        [Fact]
        public void ShouldTakeATermConstructorWhereAKeywordGoes()
        {
            foreach (var chain in new[] { false, true })
            {
                foreach (var term in new[]
                {
                    "CLR_FT_PHRASE('red bicycle')",
                    "CLR_FT_PREFIX('bicyc')",
                    "CLR_FT_FUZZY('bycycle', 2)",
                })
                {
                    FullTextFixture.Call(
                            FullTextFixture.Plan($"SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, {term})", chain: chain),
                            "CLR_FT_CONTAINS")
                        .Should().NotBeNull("'{0}' must stand where a keyword does, chained: {1}", term, chain);
                }

                var mixed = FullTextFixture.Call(
                    FullTextFixture.Plan(
                        "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, 'red', CLR_FT_FUZZY('bycycle', 2), CLR_FT_PREFIX('mount'))",
                        chain: chain),
                    "CLR_FT_CONTAINS_ALL");

                mixed.Should().NotBeNull("chained: {0}", chain);
                mixed!.getOperands().size().Should().Be(4);
            }
        }

        /// <summary>
        /// A term constructor nested in another validates on both routes; an adapter has to decline it.
        /// </summary>
        /// <remarks>
        /// <para>The rule that lets a constructor (typed <c>ANY</c>) stand in a <c>CHARACTER</c> keyword
        /// position also lets it stand in a constructor's <c>CHARACTER</c> text position.</para>
        ///
        /// <para>Refusing it on the operator alone would make the routes disagree, because the schema route's
        /// checker is the one <c>CalciteCatalogReader.toOp</c> builds.</para>
        /// </remarks>
        [Fact]
        public void ShouldValidateANestedTermConstructorOnBothRoutes()
        {
            foreach (var chain in new[] { false, true })
            {
                var call = FullTextFixture.Call(
                    FullTextFixture.Plan(
                        "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_FUZZY(CLR_FT_PHRASE('red bicycle'), 1))",
                        chain: chain),
                    "CLR_FT_FUZZY");

                call.Should().NotBeNull(
                    "an ANY-typed operand satisfies any declared family, so this cannot be refused here, chained: {0}",
                    chain);
            }
        }

        /// <summary>
        /// A weighted score can be fused, on either route.
        /// </summary>
        [Fact]
        public void ShouldWeightAScoreBeingFused()
        {
            foreach (var chain in new[] { false, true })
            {
                var call = FullTextFixture.Call(
                    FullTextFixture.Plan(
                        "SELECT ID FROM DOCS ORDER BY CLR_FT_RRF(" +
                        "CLR_FT_WEIGHT(CLR_FT_SCORE(BODY, 'steel'), 0.9), " +
                        "CLR_FT_WEIGHT(CLR_FT_SCORE(DOC, 'frame'), 0.1)) DESC",
                        chain: chain),
                    "CLR_FT_RRF");

                call.Should().NotBeNull("chained: {0}", chain);
                call!.getOperands().size().Should().Be(2);
            }
        }

        /// <summary>
        /// Each Cosmos DB full text construct has an equivalent statement that plans on either route.
        /// </summary>
        [Fact]
        public void ShouldExpressEveryCosmosFullTextConstruct()
        {
            foreach (var (cosmos, ours) in new[]
            {
                ("FullTextContains(c.text, 'red')", "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'red')"),
                ("FullTextContains(c.text, 'red bicycle') as a phrase", "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_PHRASE('red bicycle'))"),
                ("FullTextContains(c.text, {term, distance})", "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, CLR_FT_FUZZY('bycycle', 2))"),
                ("FullTextContainsAll(c.text, 'red', 'bicycle')", "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY, 'red', 'bicycle')"),
                ("FullTextContainsAny(c.text, 'bicycle', 'skateboard')", "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ANY(BODY, 'bicycle', 'skateboard')"),
                ("ORDER BY RANK FullTextScore(c.text, 'bicycle', 'mountain')", "SELECT ID FROM DOCS ORDER BY CLR_FT_SCORE(BODY, 'bicycle', 'mountain') DESC"),
                ("ORDER BY RANK RRF(FullTextScore(..), FullTextScore(..))", "SELECT ID FROM DOCS ORDER BY CLR_FT_RRF(CLR_FT_SCORE(BODY, 'a'), CLR_FT_SCORE(DOC, 'b')) DESC"),
                ("RRF(f1, f2, [0.9, 0.1])", "SELECT ID FROM DOCS ORDER BY CLR_FT_RRF(CLR_FT_WEIGHT(CLR_FT_SCORE(BODY, 'a'), 0.9), CLR_FT_WEIGHT(CLR_FT_SCORE(DOC, 'b'), 0.1)) DESC"),
            })
            {
                foreach (var chain in new[] { false, true })
                {
                    var act = () => FullTextFixture.Plan(ours, chain: chain);
                    act.Should().NotThrow("Cosmos's {0} has to be expressible, chained: {1}", cosmos, chain);
                }
            }
        }

        /// <summary>
        /// The searched position takes a character, an <c>ANY</c> or an array column.
        /// </summary>
        /// <remarks>
        /// The array column is checked through the chained table only; through both routes at once it fails,
        /// as <see cref="ShouldSearchAnArrayColumnThroughEitherRouteButNotBoth"/> shows.
        /// </remarks>
        [Fact]
        public void ShouldSearchAColumnOfAnyType()
        {
            foreach (var column in new[] { "BODY", "DOC", "TAGS" })
            {
                FullTextFixture.Call(
                        FullTextFixture.Plan($"SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS({column}, 'steel')", chain: true, declare: false),
                        "CLR_FT_CONTAINS")
                    .Should().NotBeNull("'{0}' must be searchable through a chained operator table", column);
            }

            // and through a schema declaration, for everything Calcite's routine resolution can compare
            foreach (var column in new[] { "BODY", "DOC" })
            {
                FullTextFixture.Call(
                        FullTextFixture.Plan($"SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS({column}, 'steel')"),
                        "CLR_FT_CONTAINS")
                    .Should().NotBeNull("'{0}' must be searchable through a schema declaration", column);
            }
        }

        /// <summary>
        /// An array column is searchable through either route alone, and not through both at once.
        /// </summary>
        /// <remarks>
        /// <para><c>SqlUtil.lookupSubjectRoutines</c> returns before its type-precedence pass when fewer than
        /// two candidates remain (<c>if (list.size() &lt; 2 || coerce)</c>). One route leaves one candidate;
        /// both leave two, and <c>filterRoutinesByTypePrecedence</c> then compares each parameter type using
        /// the argument's precedence list. <c>ArraySqlType</c>'s list accepts only a comparable <c>ARRAY</c>
        /// and throws <c>IllegalArgumentException</c> for anything else.</para>
        ///
        /// <para>Any parameter type but a matching <c>ARRAY</c> fails the same way, and an <c>ARRAY</c>
        /// parameter would refuse a character column, so no declaration avoids it. This is a Calcite defect;
        /// the test fails if Calcite stops throwing here.</para>
        /// </remarks>
        [Fact]
        public void ShouldSearchAnArrayColumnThroughEitherRouteButNotBoth()
        {
            FullTextFixture.Call(
                    FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(TAGS, 'steel')"),
                    "CLR_FT_CONTAINS")
                .Should().NotBeNull("a schema declaration alone leaves one candidate, so the precedence pass never runs");

            FullTextFixture.Call(
                    FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(TAGS, 'steel')", chain: true, declare: false),
                    "CLR_FT_CONTAINS")
                .Should().NotBeNull("and so does a chained operator table alone");

            var both = () => FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(TAGS, 'steel')", chain: true, declare: true);

            FullTextFixture.Describe(both.Should().Throw<java.lang.IllegalArgumentException>(
                    "two candidates reach a pass that cannot compare an ARRAY argument").Which)
                .Should().Contain("must contain type");
        }

        /// <summary>
        /// The searched position takes a <c>ROW</c> of columns, the form for a store that searches several
        /// columns at once.
        /// </summary>
        [Fact]
        public void ShouldSearchARowOfColumns()
        {
            var call = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(ROW(BODY, DOC), 'steel')", chain: true),
                "CLR_FT_CONTAINS");

            call.Should().NotBeNull();
        }

        /// <summary>
        /// A numeric keyword is accepted or refused alike on both routes, and an <c>ANY</c> column is accepted
        /// as a keyword on both.
        /// </summary>
        [Fact]
        public void ShouldTreatAMistypedKeywordTheSameEitherWay()
        {
            static bool Accepts(string sql, bool chain)
            {
                try
                {
                    FullTextFixture.Plan(sql, chain: chain);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }

            const string Mistyped = "SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 42)";

            Accepts(Mistyped, chain: true).Should().Be(Accepts(Mistyped, chain: false),
                "a call must not be accepted by one route and refused by the other");

            // an ANY operand satisfies any declared family, so a document store's untyped property is fine
            foreach (var chain in new[] { false, true })
            {
                FullTextFixture.Call(
                        FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, DOC)", chain: chain),
                        "CLR_FT_CONTAINS")
                    .Should().NotBeNull("chained: {0}", chain);
            }
        }

        /// <summary>
        /// The arities, on both routes.
        /// </summary>
        [Fact]
        public void ShouldHoldEachOperatorToItsArity()
        {
            foreach (var chain in new[] { false, true })
            {
                // CLR_FT_CONTAINS takes exactly two
                ((Action)(() => FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY)", chain: chain)))
                    .Should().Throw<Exception>();
                ((Action)(() => FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'a', 'b')", chain: chain)))
                    .Should().Throw<Exception>();

                // the variadic ones take at least two
                ((Action)(() => FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS_ALL(BODY)", chain: chain)))
                    .Should().Throw<Exception>();
                ((Action)(() => FullTextFixture.Plan("SELECT ID, CLR_FT_SCORE(BODY) AS S FROM DOCS", chain: chain)))
                    .Should().Throw<Exception>();
            }
        }

        /// <summary>
        /// A predicate is typed as a nullable <c>BOOLEAN</c> and a score as a nullable <c>DOUBLE</c>, on both
        /// routes.
        /// </summary>
        [Fact]
        public void ShouldTypeAPredicateBooleanAndAScoreDouble()
        {
            foreach (var chain in new[] { false, true })
            {
                var predicate = FullTextFixture.Call(
                    FullTextFixture.Plan("SELECT ID, CLR_FT_CONTAINS(BODY, 'steel') AS C FROM DOCS", chain: chain),
                    "CLR_FT_CONTAINS")!.getType();

                predicate.getSqlTypeName().Should().Be(SqlTypeName.BOOLEAN);
                predicate.isNullable().Should().BeTrue();

                var score = FullTextFixture.Call(
                    FullTextFixture.Plan("SELECT ID, CLR_FT_SCORE(BODY, 'steel') AS S FROM DOCS", chain: chain),
                    "CLR_FT_SCORE")!.getType();

                score.getSqlTypeName().Should().Be(SqlTypeName.DOUBLE);
                score.isNullable().Should().BeTrue();
            }
        }

        /// <summary>
        /// A query can order by a score, on both routes.
        /// </summary>
        [Fact]
        public void ShouldOrderByAScore()
        {
            foreach (var chain in new[] { false, true })
            {
                var plan = FullTextFixture.Plan(
                    "SELECT ID FROM DOCS ORDER BY CLR_FT_SCORE(BODY, 'steel') DESC FETCH FIRST 10 ROWS ONLY",
                    chain: chain);

                FullTextFixture.Call(plan, "CLR_FT_SCORE").Should().NotBeNull("chained: {0}", chain);
            }
        }

        /// <summary>
        /// Two scores fuse with <c>CLR_FT_RRF</c>, which is recognised as scoring.
        /// </summary>
        [Fact]
        public void ShouldFuseTwoScores()
        {
            var call = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS ORDER BY CLR_FT_RRF(CLR_FT_SCORE(BODY, 'steel'), CLR_FT_SCORE(DOC, 'frame')) DESC"),
                "CLR_FT_RRF");

            call.Should().NotBeNull();
            call!.getOperands().size().Should().Be(2);

            FullTextOperatorTable.IsScoring(call.getOperator()).Should().BeTrue();
        }

        /// <summary>
        /// A character literal where <c>CLR_FT_RRF</c> expects a score is accepted or refused alike on both
        /// routes.
        /// </summary>
        [Fact]
        public void ShouldTreatAMistypedScoreTheSameEitherWay()
        {
            static bool Accepts(string sql, bool chain)
            {
                try
                {
                    FullTextFixture.Plan(sql, chain: chain);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }

            const string Mistyped = "SELECT ID FROM DOCS ORDER BY CLR_FT_RRF(CLR_FT_SCORE(BODY, 'steel'), 'frame') DESC";

            Accepts(Mistyped, chain: true).Should().Be(Accepts(Mistyped, chain: false),
                "a call must not be accepted by one route and refused by the other");
        }

    }

}
