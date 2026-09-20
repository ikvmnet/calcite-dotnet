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
    /// The two routes a name can be reached by, and that they agree.
    /// </summary>
    /// <remarks>
    /// A host chains the operator table; everybody else gets the declarations on a schema. Both are needed —
    /// a connection builds no validator for anyone to chain anything into — and the point of testing them
    /// side by side is that the plan must not depend on which one answered.
    /// </remarks>
    public class FullTextResolutionTests
    {

        static string Keywords(int count)
        {
            return string.Join(", ", Enumerable.Range(0, count).Select(i => $"'k{i}'"));
        }

        /// <summary>
        /// The whole point of the schema declarations: no operator table, and the name still resolves.
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
        /// And the route a host takes: no declarations, and the name still resolves.
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
        /// Neither route is reachable with neither arrangement, which is what says the tests above are
        /// measuring something.
        /// </summary>
        [Fact]
        public void ShouldNotResolveWithNeither()
        {
            var act = () => FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", declare: false);

            FullTextFixture.Describe(act.Should().Throw<Exception>().Which)
                .Should().Contain("CLR_FT_CONTAINS");
        }

        /// <summary>
        /// Doing both is not a duplicate.
        /// </summary>
        /// <remarks>
        /// Overload resolution takes the first candidate whose arity fits and the chained table comes before
        /// the catalog reader, so the operator answers and the declaration is never reached. Nothing insists
        /// on one answer, so nothing breaks.
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
        /// Every operator, both ways, carrying the same operands and the same type.
        /// </summary>
        /// <remarks>
        /// The claim the whole package rests on: which route resolved a name is not something a plan, or an
        /// adapter reading one, can tell. It holds because a declaration's parameter types are the types
        /// whose families the operator's own checker looks for, so the checker Calcite derives from a
        /// declaration accepts what the operator accepts — rather than the two being kept in step by hand.
        /// </remarks>
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
        /// What arrives in a plan is not the operator that was declared.
        /// </summary>
        /// <remarks>
        /// Calcite reads a schema function's parameter list and builds a <c>SqlUserDefinedFunction</c> of its
        /// own around it, carrying the name and the arity. So an adapter matching a call by identity would
        /// recognise one reached through a chained table and quietly fail to recognise the same call reached
        /// through a connection. <c>FullTextOperatorTable.Matches</c> is the check that works on both, and
        /// this is why it asks for a name.
        /// </remarks>
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

            // and the chained route hands over the operator itself, so identity happens to work there — which
            // is exactly how an identity test passes its tests and fails in production
            var chained = FullTextFixture.Call(
                FullTextFixture.Plan("SELECT ID FROM DOCS WHERE CLR_FT_CONTAINS(BODY, 'steel')", chain: true, declare: false),
                "CLR_FT_CONTAINS");

            ReferenceEquals(chained!.getOperator(), FullTextOperatorTable.ClrFtContains).Should().BeTrue();
        }

        /// <summary>
        /// A keyword list arrives as written, with nothing added to it.
        /// </summary>
        /// <remarks>
        /// <c>SqlCallBinding.operands</c> pads a call out to the whole parameter list with <c>DEFAULT</c>
        /// where there is room under the count range, the position is optional, and the checker's parameters
        /// are fixed. One declaration per arity, every parameter required, makes the second false. No store
        /// renders <c>DEFAULT</c>, so this is the difference between a statement and a refusal.
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
        /// Which is a property of the declarations rather than an accident of these calls.
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
        /// A variadic operator is offered through a schema up to a declared limit, and past it by chaining.
        /// </summary>
        /// <remarks>
        /// Calcite derives a function's operand count range from its parameter list, so a schema function is
        /// exactly as variadic as the number of parameters it declares. The limit is reachable rather than
        /// theoretical, so it is measured on both sides — and the second half is the whole of what chaining
        /// the operator table still buys a host that has the declarations too.
        /// </remarks>
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
        /// The schema offers everything the operator table does.
        /// </summary>
        /// <remarks>
        /// Derived from the table rather than listed again, so this guards the derivation rather than a second
        /// list: an operator added to <see cref="FullTextOperatorTable"/> and reachable through a planner a
        /// host built but not through a connection is the defect the declarations exist to close.
        /// </remarks>
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
        /// A term constructor stands where a keyword does, on either route.
        /// </summary>
        /// <remarks>
        /// <para>The mechanism is one rule of Calcite's: <c>FamilyOperandTypeChecker</c> passes an operand
        /// whose own type family is <c>ANY</c> against whatever family is declared. A constructor is typed
        /// <c>ANY</c>, so it satisfies a <c>CHARACTER</c> keyword position and the position stays
        /// <c>CHARACTER</c> for everything else &#8212; which is what keeps a number out of it.</para>
        ///
        /// <para>Mixing them is the case that matters, and is why these are constructors rather than more
        /// predicates: a call can carry an exact keyword and a fuzzy one at once, which a
        /// <c>CLR_FT_CONTAINS_ALL_FUZZY</c> could not.</para>
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
        /// A nested term constructor validates, and cannot be made not to.
        /// </summary>
        /// <remarks>
        /// <para><b>The rule that makes constructors possible is the rule that stops this being refused.</b>
        /// <c>FamilyOperandTypeChecker</c> passes an operand whose own type family is <c>ANY</c> against any
        /// declared family. That is exactly why a constructor stands in a <c>CHARACTER</c> keyword position
        /// &#8212; and it means a <c>CHARACTER</c> position cannot reject one either. Both, or neither.</para>
        ///
        /// <para>It could be refused by hand, but only on one route: the schema route&#8217;s checker is the
        /// one <c>CalciteCatalogReader.toOp</c> builds, which this package does not write, so a hand-written
        /// refusal on the operator would accept through a connection what it refused through a host. A
        /// divergence between the routes is worse than a call the adapter declines.</para>
        ///
        /// <para>So the combination that tempts &#8212; fuzziness on a phrase &#8212; type-checks here and
        /// belongs to the adapter. Which is no worse than where it sits today: Elasticsearch documents that
        /// it silently ignores fuzziness on a phrase query, and an adapter declining is better than that.</para>
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
        /// A weighted score goes wherever a score goes.
        /// </summary>
        /// <remarks>
        /// Which is what covers Cosmos's <c>RRF(f1, f2, [0.9, 0.1])</c> without adopting a positional array:
        /// the weight travels with the score it belongs to and the two cannot come apart.
        /// </remarks>
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
        /// Everything Cosmos offers is expressible, which is the bar this vocabulary was set.
        /// </summary>
        /// <remarks>
        /// One statement per Cosmos construct, in this package's spelling. The spellings differ deliberately
        /// &#8212; a shared vocabulary that took one store's names would make every other adapter map them
        /// anyway &#8212; so what is pinned is that nothing Cosmos does has become unsayable.
        /// </remarks>
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
        /// The searched position takes anything a store might have made searchable.
        /// </summary>
        /// <remarks>
        /// Not laziness: what a store searches differs in type as well as in name. A relational column is
        /// character, a document property is <c>ANY</c> because a container declares no row type, and an array
        /// of strings is searchable in Cosmos and in PostgreSQL alike. <c>ARRAY</c> is the one that would have
        /// been refused by asking <c>SqlTypeFamily.ANY</c> for its type names, since
        /// <c>SqlTypeName.ALL_TYPES</c> does not list it.
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
        /// <para><b>This is the one way doing both is not harmless.</b>
        /// <c>SqlUtil.lookupSubjectRoutines</c> runs three passes and returns early —
        /// <c>if (list.size() &lt; 2 || coerce)</c> — before the third whenever fewer than two candidates
        /// survive. One route leaves one candidate, so the third pass never runs. Both leave two, it runs,
        /// and it is <c>filterRoutinesByTypePrecedence</c>: it compares each candidate's parameter type using
        /// the <em>argument's</em> precedence list, and <c>ArraySqlType</c>'s accepts only another comparable
        /// <c>ARRAY</c> and throws <c>IllegalArgumentException</c> for anything else.</para>
        ///
        /// <para><b>It is not the <c>ANY</c> parameter that causes it.</b> The throw comes from the
        /// argument's list rejecting whatever the parameter is, so any parameter type but a matching
        /// <c>ARRAY</c> fails identically — and one typed <c>ARRAY</c> would then refuse every character
        /// column. There is no declaration that takes both.</para>
        ///
        /// <para>Pinned rather than worked around. It is a pass that throws where it should decline, which is
        /// Calcite's to fix, and this is the test that will notice if it stops being true. The practical
        /// consequence is the README's: register one route or the other, not both.</para>
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
        /// And a row of them, which is how SQL Server and MySQL name a list of columns.
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
        /// A keyword is text, and a number in that position is refused at validation.
        /// </summary>
        /// <remarks>
        /// The checking that all-<c>ANY</c> operands would have given up. It costs a document store nothing:
        /// <c>FamilyOperandTypeChecker</c> passes an operand whose own family is <c>ANY</c> against any
        /// declared family, which is why <c>CLR_FT_CONTAINS(BODY, DOC)</c> below is accepted while
        /// <c>CLR_FT_CONTAINS(BODY, 42)</c> is not.
        /// </remarks>
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
        /// What each operator is typed as.
        /// </summary>
        /// <remarks>
        /// Nullable, both halves. The failure modes are not symmetrical: declaring a result <c>NOT NULL</c>
        /// licences the planner to rewrite on a guarantee the store does not give, and the answer is wrong,
        /// while declaring it nullable at worst costs a rewrite. A store may have nothing to say about a row a
        /// plan keeps.
        /// </remarks>
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
        /// A score can be ordered by, which is the whole reason it is typed rather than opaque.
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
        /// And fused with another, which is what hybrid search writes.
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
        /// <c>CLR_FT_RRF</c> fuses scores, so a keyword in that position is refused.
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
