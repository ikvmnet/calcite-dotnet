using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Rel.Rules;
using Apache.Calcite.Geography.Schema;
using Apache.Calcite.Geography.Sql;

using FluentAssertions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// What <see cref="GeographyRules"/> takes out of a plan, and that the rows are the same either way.
    /// </summary>
    /// <remarks>
    /// Every rewrite is pinned twice: once as a plan, which is what it is for, and once against the rows the
    /// same statement answers without the pass, which is the only thing that says the rewrite was sound.
    /// </remarks>
    public class GeographySimplificationTests
    {

        /// <summary>
        /// Plans the given query into <c>EnumerableConvention</c>, with or without the pass.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="simplify">Whether to run <see cref="GeographyRules.Program"/> in front.</param>
        /// <param name="chained">Whether to chain the operator table rather than declare on the
        /// schema.</param>
        /// <returns></returns>
        static RelNode Plan(string sql, bool simplify, bool chained = true)
        {
            var schema = Frameworks.createRootSchema(true);
            schema.add("GEO", new GeographyExecutionTests.GeographyTable());
            schema.add("GEO2", new GeographyExecutionTests.GeographyTable());

            var builder = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .executor(RexUtil.EXECUTOR)
                .programs(simplify ? Programs.sequence(GeographyRules.Program(), Programs.standard()) : Programs.standard());

            if (chained)
                builder = builder.operatorTable(GeographyFixture.OperatorTable());
            else
                GeographySchema.AddTo(schema);

            var planner = Frameworks.getPlanner(builder.build());
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            return planner.transform(0, logical.getTraitSet().replace(EnumerableConvention.INSTANCE), logical);
        }

        /// <summary>
        /// The plan as a string, which is what each rewrite is pinned against.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="simplify"></param>
        /// <param name="chained"></param>
        /// <returns></returns>
        static string Explain(string sql, bool simplify, bool chained = true)
        {
            return RelOptUtil.toString(Plan(sql, simplify, chained));
        }

        /// <summary>
        /// Runs the given query, with or without the pass, and returns the rows.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="simplify"></param>
        /// <returns></returns>
        static List<object?[]> Run(string sql, bool simplify)
        {
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:");
            var calcite = (CalciteConnection)connection.unwrap((java.lang.Class)typeof(CalciteConnection));
            var schema = calcite.getRootSchema();
            schema.add("GEO", new GeographyExecutionTests.GeographyTable());
            schema.add("GEO2", new GeographyExecutionTests.GeographyTable());

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .operatorTable(GeographyFixture.OperatorTable())
                .executor(RexUtil.EXECUTOR)
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            var runner = (RelRunner)connection.unwrap((java.lang.Class)typeof(RelRunner));
            using var statement = runner.prepareStatement(simplify ? Simplify(logical) : logical);
            var results = statement.executeQuery();
            var count = results.getMetaData().getColumnCount();

            var rows = new List<object?[]>();

            while (results.next())
            {
                var row = new object?[count];
                for (var i = 0; i < count; i++)
                    row[i] = results.getObject(i + 1)?.ToString();

                rows.Add(row);
            }

            return rows;
        }

        /// <summary>
        /// Runs the pass over a logical plan, which is the whole of what a host does with it.
        /// </summary>
        /// <param name="rel"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Programs.hep</c> builds its own <c>HepPlanner</c> and ignores the planner it is handed, so
        /// there is nothing to pass one.
        /// </remarks>
        static RelNode Simplify(RelNode rel)
        {
            return GeographyRules.Program().run(null!, rel, rel.getTraitSet(), java.util.Collections.emptyList(), java.util.Collections.emptyList());
        }

        /// <summary>
        /// Both crossings are the identity, so a round trip is two dispatches per row and nothing else.
        /// </summary>
        [Fact]
        public void ShouldDropACrossingRoundTrip()
        {
            const string sql = "SELECT CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(GEOG)) FROM GEO";

            Explain(sql, simplify: false).Should().Contain("CLR_ST_GEOG_ASGEOM").And.Contain("CLR_ST_GEOM_ASGEOG");
            Explain(sql, simplify: true).Should().NotContain("CLR_ST_GEO");
        }

        /// <summary>
        /// A crossing that changes the type is not a crossing that converts nothing, and stays.
        /// </summary>
        /// <remarks>
        /// The two routes do not type a call alike: this package's operator answers
        /// <c>createJavaType(Geometry.class)</c>, which is what the column is, and the one
        /// <c>CalciteCatalogReader.toOp</c> builds around the schema declaration answers Calcite's
        /// <c>GEOMETRY</c>. So the inner crossing of the same round trip is a real change of type on the
        /// schema route, and dropping it would hand the outer expression something else.
        /// </remarks>
        [Fact]
        public void ShouldKeepACrossingThatChangesTheType()
        {
            const string sql = "SELECT CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(GEOG)) FROM GEO";

            var plan = Explain(sql, simplify: true, chained: false);

            plan.Should().Contain("CLR_ST_GEOM_ASGEOG");
            plan.Should().NotContain("CLR_ST_GEOG_ASGEOM");
        }

        /// <summary>
        /// Two spellings of one function become one expression, so it is evaluated once.
        /// </summary>
        [Theory]
        [InlineData("CLR_ST_GEOG_ASTEXT", "CLR_ST_GEOG_ASWKT")]
        [InlineData("CLR_ST_GEOG_NUMPOINTS", "CLR_ST_GEOG_NPOINTS")]
        [InlineData("CLR_ST_GEOG_NUMINTERIORRING", "CLR_ST_GEOG_NUMINTERIORRINGS")]
        [InlineData("CLR_ST_GEOG_ENVELOPE", "CLR_ST_GEOG_EXTENT")]
        public void ShouldFoldTheSecondSpellingOfAFunctionIntoTheFirst(string canonical, string synonym)
        {
            var sql = $"SELECT {canonical}(GEOG), {synonym}(GEOG) FROM GEO";

            Explain(sql, simplify: false).Should().Contain(synonym);

            var plan = Explain(sql, simplify: true);
            plan.Should().NotContain(synonym);
            plan.Should().Contain(canonical);

            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// <c>Disjoint</c> is <c>Intersects</c> negated, so a negation of either is the other, and an adapter
        /// sees a bare call rather than one under a <c>NOT</c>.
        /// </summary>
        [Theory]
        [InlineData("CLR_ST_GEOG_DISJOINT", "CLR_ST_GEOG_INTERSECTS")]
        [InlineData("CLR_ST_GEOG_INTERSECTS", "CLR_ST_GEOG_DISJOINT")]
        public void ShouldRewriteTheNegationOfARelationAsItsComplement(string written, string expected)
        {
            var sql = $"SELECT ID FROM GEO WHERE NOT {written}(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0.5 0)'))";

            Explain(sql, simplify: false).Should().Contain("NOT(").And.Contain(written);

            var plan = Explain(sql, simplify: true);
            plan.Should().Contain(expected);
            plan.Should().NotContain("NOT(");

            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// A relation written in terms of another becomes that one, operands the other way round.
        /// </summary>
        [Theory]
        [InlineData("CLR_ST_GEOG_CONTAINS", "CLR_ST_GEOG_WITHIN")]
        [InlineData("CLR_ST_GEOG_COVEREDBY", "CLR_ST_GEOG_COVERS")]
        public void ShouldRewriteARelationAsTheOneItIsWrittenInTermsOf(string written, string expected)
        {
            var sql = $"SELECT ID FROM GEO WHERE {written}(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0.5 0)'))";

            Explain(sql, simplify: false).Should().Contain(written);

            var plan = Explain(sql, simplify: true);
            plan.Should().Contain(expected);
            plan.Should().NotContain(written);

            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// A distance compared against a bound is the predicate a geodesic store can answer from an index.
        /// </summary>
        [Fact]
        public void ShouldRewriteADistanceBoundAsDWithin()
        {
            const string sql = "SELECT ID FROM GEO WHERE CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')) <= 200000.0";

            Explain(sql, simplify: false).Should().Contain("CLR_ST_GEOG_DISTANCE");

            var plan = Explain(sql, simplify: true);
            plan.Should().Contain("CLR_ST_GEOG_DWITHIN");
            plan.Should().NotContain("CLR_ST_GEOG_DISTANCE");

            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// The bound the other way round is the same predicate.
        /// </summary>
        [Fact]
        public void ShouldRewriteADistanceBoundWrittenTheOtherWayRound()
        {
            const string sql = "SELECT ID FROM GEO WHERE 200000.0 >= CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))";

            Explain(sql, simplify: true).Should().Contain("CLR_ST_GEOG_DWITHIN");

            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// A strict bound is a different predicate at the boundary and there is no operator for it.
        /// </summary>
        [Fact]
        public void ShouldNotRewriteAStrictDistanceBound()
        {
            const string sql = "SELECT ID FROM GEO WHERE CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')) < 200000.0";

            Explain(sql, simplify: true).Should().Contain("CLR_ST_GEOG_DISTANCE").And.NotContain("CLR_ST_GEOG_DWITHIN");
        }

        /// <summary>
        /// A join condition is rewritten too, which is a rule of its own and not the filter's.
        /// </summary>
        /// <remarks>
        /// <c>RelNode.accept(RexShuttle)</c> is the whole of what each of the three rules does, and it is
        /// <c>Join</c>, <c>Filter</c> and <c>Project</c> that override it —
        /// <c>AbstractRelNode.accept(RexShuttle)</c> answers <c>this</c>. So a condition in an <c>ON</c>
        /// clause is reached by the join rule and by nothing else.
        /// </remarks>
        [Fact]
        public void ShouldSimplifyAJoinCondition()
        {
            const string sql = "SELECT A.ID FROM GEO A INNER JOIN GEO2 B ON CLR_ST_GEOG_CONTAINS(A.GEOG, B.GEOG)";

            Explain(sql, simplify: false).Should().Contain("CLR_ST_GEOG_CONTAINS");

            var plan = Explain(sql, simplify: true);
            plan.Should().Contain("CLR_ST_GEOG_WITHIN");
            plan.Should().NotContain("CLR_ST_GEOG_CONTAINS");
        }

        /// <summary>
        /// A predicate that rejects nulls turns an outer join into an inner one, which is Calcite's own
        /// rewrite reading a fact this package declares.
        /// </summary>
        /// <remarks>
        /// <c>RelOptUtil.simplifyJoin</c> asks <c>Strong.isNotTrue</c> whether the filter can hold of a row
        /// whose whole right side is null; a <c>Strong.Policy.ANY</c> operator says it cannot. Without the
        /// declaration the join stays outer and the filter stays above it, so both sides are sorted and
        /// merged first. Run on both routes because the declaration lives on the operator object and only one
        /// route carries it — the other gets it back from the pass.
        /// </remarks>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ShouldTurnAnOuterJoinIntoAnInnerOneWhereAGeographyPredicateRejectsNulls(bool chained)
        {
            const string sql =
                "SELECT A.ID FROM GEO A LEFT JOIN GEO2 B ON A.ID = B.ID " +
                "WHERE CLR_ST_GEOG_DWITHIN(B.GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)";

            Explain(sql, simplify: false, chained: false).Should().Contain("joinType=[left]");
            Explain(sql, simplify: true, chained: chained).Should().Contain("joinType=[inner]").And.NotContain("joinType=[left]");
        }

        /// <summary>
        /// A call resolved through a schema carries Calcite's own operator, and the pass puts this package's
        /// back, which is what makes the declarations on it reach the plan at all.
        /// </summary>
        [Fact]
        public void ShouldPutThisPackagesOperatorBackOnACallResolvedThroughASchema()
        {
            var schema = Frameworks.createRootSchema(true);
            GeographySchema.AddTo(schema);

            var resolved = Resolve(schema, "CLR_ST_GEOG_DWITHIN");

            resolved.Should().NotBeSameAs(GeographyOperatorTable.ClrStGeogDWithin);
            Strong.policy(resolved).Should().Be(Strong.Policy.AS_IS);

            GeographyOperatorTable.Rebind(resolved).Should().BeSameAs(GeographyOperatorTable.ClrStGeogDWithin);
        }

        /// <summary>
        /// A name this package did not declare the body of is left alone, whatever it is called.
        /// </summary>
        [Fact]
        public void ShouldNotRebindANameDeclaredOverSomeoneElsesBody()
        {
            var schema = Frameworks.createRootSchema(true);
            schema.add("CLR_ST_GEOG_DWITHIN", org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                (java.lang.Class)typeof(Impostor), "DWithin"));

            GeographyOperatorTable.Rebind(Resolve(schema, "CLR_ST_GEOG_DWITHIN")).Should().BeNull();
        }

        /// <summary>
        /// A body of the right shape and the wrong provenance.
        /// </summary>
        public static class Impostor
        {

            /// <summary>
            /// Answers nothing anyone should believe.
            /// </summary>
            /// <param name="a"></param>
            /// <param name="b"></param>
            /// <param name="distance"></param>
            /// <returns></returns>
            public static java.lang.Boolean? DWithin(org.locationtech.jts.geom.Geometry? a, org.locationtech.jts.geom.Geometry? b, java.lang.Object? distance)
            {
                return java.lang.Boolean.TRUE;
            }

        }

        /// <summary>
        /// The operator a schema hands back for a name.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        static org.apache.calcite.sql.SqlOperator Resolve(SchemaPlus schema, string name)
        {
            var reader = new org.apache.calcite.prepare.CalciteCatalogReader(
                CalciteSchema.from(schema),
                java.util.Collections.emptyList(),
                new JavaTypeFactoryImpl(),
                new org.apache.calcite.config.CalciteConnectionConfigImpl(new java.util.Properties()));

            var found = new java.util.ArrayList();
            reader.lookupOperatorOverloads(
                new org.apache.calcite.sql.SqlIdentifier(name, org.apache.calcite.sql.parser.SqlParserPos.ZERO),
                org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION,
                org.apache.calcite.sql.SqlSyntax.FUNCTION,
                found,
                org.apache.calcite.sql.validate.SqlNameMatchers.withCaseSensitive(true));

            found.size().Should().BeGreaterThan(0);

            return (org.apache.calcite.sql.SqlOperator)found.get(0);
        }

        /// <summary>
        /// A predicate answers null exactly when an argument is null, and says so.
        /// </summary>
        [Fact]
        public void ShouldDeclareAPredicateStrict()
        {
            Strong.policy(GeographyOperatorTable.ClrStGeogDWithin).Should().Be(Strong.Policy.ANY);
            Strong.policy(GeographyOperatorTable.ClrStGeogIntersects).Should().Be(Strong.Policy.ANY);
            Strong.policy(GeographyOperatorTable.ClrStGeogDistance).Should().Be(Strong.Policy.ANY);
        }

        /// <summary>
        /// A reader does not, and must not say it does.
        /// </summary>
        /// <remarks>
        /// <c>Strong.Policy.ANY</c> is read in both directions —
        /// <c>RexSimplify.simplifyIsNull</c> turns <c>f(a) IS NULL</c> into <c>a IS NULL</c> — so a function
        /// that can answer null over a non-null argument must not carry it. <c>CLR_ST_GEOG_X</c> of anything
        /// but a point is null, <c>ST_X</c> being <c>geom instanceof Point ? … : null</c>, and
        /// <c>CLR_ST_GEOG_POINTFROMTEXT</c> of text naming another shape is null.
        /// </remarks>
        [Fact]
        public void ShouldNotDeclareAReaderStrict()
        {
            Strong.policy(GeographyOperatorTable.ClrStGeogX).Should().Be(Strong.Policy.AS_IS);
            Strong.policy(GeographyOperatorTable.ClrStGeogPointFromText).Should().Be(Strong.Policy.AS_IS);
            Strong.policy(GeographyOperatorTable.ClrStGeogPointN).Should().Be(Strong.Policy.AS_IS);
            Strong.policy(GeographyOperatorTable.ClrStGeogStartPoint).Should().Be(Strong.Policy.AS_IS);
        }

        /// <summary>
        /// A symmetrical operator is its own reverse, which is what <c>RexNormalize</c> demands of one.
        /// </summary>
        [Fact]
        public void ShouldDeclareASymmetricalOperatorItsOwnReverse()
        {
            GeographyOperatorTable.ClrStGeogDistance.isSymmetrical().Should().BeTrue();
            GeographyOperatorTable.ClrStGeogDistance.reverse().Should().BeSameAs(GeographyOperatorTable.ClrStGeogDistance);

            GeographyOperatorTable.ClrStGeogWithin.isSymmetrical().Should().BeFalse();
            GeographyOperatorTable.ClrStGeogWithin.reverse().Should().BeNull();
        }

        /// <summary>
        /// The same measurement written both ways round is one expression.
        /// </summary>
        /// <remarks>
        /// The whole of what the symmetry declaration buys, and it needs no pass: <c>RexNormalize</c> builds
        /// the digest of a symmetrical two-operand call from its operands unordered, so the two spellings are
        /// one expression and the calc evaluates it once.
        /// </remarks>
        [Fact]
        public void ShouldEvaluateASymmetricalMeasurementOnceHoweverItIsWritten()
        {
            const string sql =
                "SELECT CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')), " +
                "CLR_ST_GEOG_DISTANCE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), GEOG) FROM GEO";

            Occurrences(Explain(sql, simplify: false), "CLR_ST_GEOG_DISTANCE").Should().Be(1);
        }

        /// <summary>
        /// A constant constructor is parsed once while the plan is built rather than once per row, where the
        /// caller gave the planner an executor.
        /// </summary>
        /// <remarks>
        /// Nothing of this package's does it — <c>CoreRules.FILTER_REDUCE_EXPRESSIONS</c> is registered by
        /// <c>RelOptUtil.registerDefaultRules</c> and <c>ReduceExpressionsRule</c> gives up without an
        /// executor, silently, saying in a comment that there is no mechanism for a warning. A
        /// <c>jdbc:calcite:</c> connection always has one; a <c>Frameworks</c> config has whatever it was
        /// given, which is nothing by default. Pinned here because it is the largest thing a caller can get
        /// wrong about these operators and costs a WKT parse per row.
        /// </remarks>
        [Fact]
        public void ShouldReduceAConstantConstructorWhereTheCallerSetAnExecutor()
        {
            const string sql = "SELECT ID FROM GEO WHERE CLR_ST_GEOG_DWITHIN(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)";

            Explain(sql, simplify: false).Should().NotContain("CLR_ST_GEOG_GEOMFROMTEXT");

            Without(sql).Should().Contain("CLR_ST_GEOG_GEOMFROMTEXT");
        }

        /// <summary>
        /// The same statement planned by a caller who named no executor.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static string Without(string sql)
        {
            var schema = Frameworks.createRootSchema(true);
            schema.add("GEO", new GeographyExecutionTests.GeographyTable());

            var planner = Frameworks.getPlanner(
                Frameworks.newConfigBuilder()
                    .defaultSchema(schema)
                    .operatorTable(GeographyFixture.OperatorTable())
                    .programs(Programs.standard())
                    .build());

            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            return RelOptUtil.toString(planner.transform(0, logical.getTraitSet().replace(EnumerableConvention.INSTANCE), logical));
        }

        /// <summary>
        /// The rows are the same with the pass and without it, which is the only thing that says a rewrite
        /// was sound.
        /// </summary>
        [Theory]
        [InlineData("SELECT CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(GEOG)) FROM GEO")]
        [InlineData("SELECT CLR_ST_GEOG_ASWKT(GEOG), CLR_ST_GEOG_ASTEXT(GEOG) FROM GEO")]
        [InlineData("SELECT ID FROM GEO WHERE NOT CLR_ST_GEOG_DISJOINT(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0.5 0)'))")]
        [InlineData("SELECT ID FROM GEO WHERE CLR_ST_GEOG_CONTAINS(CLR_ST_GEOG_BUFFER(GEOG, 1000.0), GEOG)")]
        [InlineData("SELECT ID FROM GEO WHERE CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')) <= 200000.0")]
        [InlineData("SELECT ID FROM GEO WHERE CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')) <= 1.0")]
        [InlineData("SELECT CLR_ST_GEOG_NPOINTS(GEOG), CLR_ST_GEOG_NUMINTERIORRINGS(GEOG) FROM GEO")]
        [InlineData("SELECT A.ID FROM GEO A LEFT JOIN GEO2 B ON A.ID = B.ID WHERE CLR_ST_GEOG_DWITHIN(B.GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)")]
        public void ShouldAnswerTheSameRowsWithThePassAndWithout(string sql)
        {
            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// How many times a name appears in a plan.
        /// </summary>
        /// <param name="plan"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        static int Occurrences(string plan, string name)
        {
            var count = 0;
            var at = plan.IndexOf(name, StringComparison.Ordinal);

            while (at >= 0)
            {
                count++;
                at = plan.IndexOf(name, at + name.Length, StringComparison.Ordinal);
            }

            return count;
        }

    }

}
