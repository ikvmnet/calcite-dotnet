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
    /// Tests the rewrites <see cref="GeographyRules"/> makes, and the operator declarations Calcite's own
    /// simplifications read.
    /// </summary>
    /// <remarks>
    /// Each rewrite is checked against the plan text, and where it can change a result, against the rows the
    /// same statement returns without the pass.
    /// </remarks>
    public class GeographySimplificationTests
    {

        /// <summary>
        /// Plans a query into <c>EnumerableConvention</c>, with an executor set, with or without the pass.
        /// </summary>
        /// <param name="sql">The query text.</param>
        /// <param name="simplify">Whether to run <see cref="GeographyRules.Program"/> before
        /// <c>Programs.standard</c>.</param>
        /// <param name="chained">Whether to supply the operators as an operator table rather than register
        /// them on the schema.</param>
        /// <returns>The physical plan.</returns>
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
        /// Returns <see cref="Plan"/>'s result as text.
        /// </summary>
        /// <param name="sql">The query text.</param>
        /// <param name="simplify">Whether to run <see cref="GeographyRules.Program"/> before <c>Programs.standard</c>.</param>
        /// <param name="chained">Whether to supply the operators as an operator table rather than register them on the
        /// schema.</param>
        /// <returns>The physical plan as text.</returns>
        static string Explain(string sql, bool simplify, bool chained = true)
        {
            return RelOptUtil.toString(Plan(sql, simplify, chained));
        }

        /// <summary>
        /// Runs a query, with or without the pass over its logical plan, and returns the rows with each value
        /// as a string.
        /// </summary>
        /// <param name="sql">The query text.</param>
        /// <param name="simplify">Whether to run <see cref="GeographyRules.Program"/> over the logical plan before
        /// executing it.</param>
        /// <returns>The rows, each value as its string form or <c>null</c>.</returns>
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
        /// Runs <see cref="GeographyRules.Program"/> over a logical plan.
        /// </summary>
        /// <remarks>
        /// The planner argument is null because <c>Programs.hep</c> builds its own <c>HepPlanner</c> and
        /// ignores the one it is given.
        /// </remarks>
        /// <param name="rel">The logical plan to rewrite.</param>
        /// <returns>The plan after the pass.</returns>
        static RelNode Simplify(RelNode rel)
        {
            return GeographyRules.Program().run(null!, rel, rel.getTraitSet(), java.util.Collections.emptyList(), java.util.Collections.emptyList());
        }

        /// <summary>
        /// A round trip through <c>CLR_ST_GEOM_ASGEOG</c> and <c>CLR_ST_GEOG_ASGEOM</c>, both identities, is
        /// removed.
        /// </summary>
        [Fact]
        public void ShouldDropACrossingRoundTrip()
        {
            const string sql = "SELECT CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(GEOG)) FROM GEO";

            Explain(sql, simplify: false).Should().Contain("CLR_ST_GEOG_ASGEOM").And.Contain("CLR_ST_GEOM_ASGEOG");
            Explain(sql, simplify: true).Should().NotContain("CLR_ST_GEO");
        }

        /// <summary>
        /// A conversion that changes the SQL type is kept.
        /// </summary>
        /// <remarks>
        /// This package's operator returns <c>createJavaType(Geometry.class)</c>, the column's type, while the
        /// operator <c>CalciteCatalogReader.toOp</c> builds from the schema declaration returns Calcite's
        /// <c>GEOMETRY</c>. Resolved through the schema, the inner conversion of the round trip changes the
        /// type, so only the outer one is removed.
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
        /// A synonym is rewritten to the canonical name, so both spellings become one expression evaluated
        /// once.
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
        /// <c>NOT</c> of <c>CLR_ST_GEOG_DISJOINT</c> becomes <c>CLR_ST_GEOG_INTERSECTS</c> and vice versa, so an
        /// adapter sees a bare call.
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
        /// <c>CONTAINS</c> becomes <c>WITHIN</c>, and <c>COVEREDBY</c> becomes <c>COVERS</c>, with the operands
        /// swapped.
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
        /// <c>CLR_ST_GEOG_DISTANCE(a, b) &lt;= d</c> becomes <c>CLR_ST_GEOG_DWITHIN(a, b, d)</c>, which a geodesic
        /// store can answer from an index.
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
        /// <c>d &gt;= CLR_ST_GEOG_DISTANCE(a, b)</c> is rewritten the same way.
        /// </summary>
        [Fact]
        public void ShouldRewriteADistanceBoundWrittenTheOtherWayRound()
        {
            const string sql = "SELECT ID FROM GEO WHERE 200000.0 >= CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'))";

            Explain(sql, simplify: true).Should().Contain("CLR_ST_GEOG_DWITHIN");

            Run(sql, simplify: true).Should().BeEquivalentTo(Run(sql, simplify: false));
        }

        /// <summary>
        /// A strict bound (<c>&lt;</c>) is not rewritten, since <c>CLR_ST_GEOG_DWITHIN</c> includes the boundary.
        /// </summary>
        [Fact]
        public void ShouldNotRewriteAStrictDistanceBound()
        {
            const string sql = "SELECT ID FROM GEO WHERE CLR_ST_GEOG_DISTANCE(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')) < 200000.0";

            Explain(sql, simplify: true).Should().Contain("CLR_ST_GEOG_DISTANCE").And.NotContain("CLR_ST_GEOG_DWITHIN");
        }

        /// <summary>
        /// A join condition is rewritten, by the join rule rather than the filter rule.
        /// </summary>
        /// <remarks>
        /// Each of the three rules applies <c>RelNode.accept(RexShuttle)</c>, which <c>Join</c>, <c>Filter</c>
        /// and <c>Project</c> override and <c>AbstractRelNode</c> implements as returning <c>this</c>, so only
        /// the join rule reaches an <c>ON</c> condition.
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
        /// A null-rejecting geography predicate above a left join lets Calcite make the join inner.
        /// </summary>
        /// <remarks>
        /// <c>RelOptUtil.simplifyJoin</c> asks <c>Strong.isNotTrue</c> whether the filter can hold for a row
        /// whose right side is all null, and an operator with <c>Strong.Policy.ANY</c> says it cannot. The
        /// policy is declared on this package's operator object, which an operator table supplies directly;
        /// a call resolved through a schema gets it only after the pass rebinds the operator, so both routes
        /// are tested.
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
        /// A name resolved through a schema yields an operator Calcite built, without this package's
        /// declarations; <see cref="GeographyOperatorTable.Rebind"/> returns this package's operator for it.
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
        /// A function with one of this package's names but a different implementation is not rebound.
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
        /// A function class with the same signature as <c>CLR_ST_GEOG_DWITHIN</c>'s implementation but not
        /// this package's.
        /// </summary>
        public static class Impostor
        {

            /// <summary>
            /// Returns true for any arguments.
            /// </summary>
            /// <param name="a">Ignored.</param>
            /// <param name="b">Ignored.</param>
            /// <param name="distance">Ignored.</param>
            /// <returns><c>java.lang.Boolean.TRUE</c>.</returns>
            public static java.lang.Boolean? DWithin(org.locationtech.jts.geom.Geometry? a, org.locationtech.jts.geom.Geometry? b, java.lang.Object? distance)
            {
                return java.lang.Boolean.TRUE;
            }

        }

        /// <summary>
        /// Returns the first operator a <c>CalciteCatalogReader</c> over the schema finds for a function name.
        /// </summary>
        /// <param name="schema">The schema whose functions are looked up.</param>
        /// <param name="name">The function name, matched case-sensitively.</param>
        /// <returns>The first operator found; the test fails if there is none.</returns>
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
        /// Predicates and measurements, which return null exactly when an argument is null, declare
        /// <c>Strong.Policy.ANY</c>.
        /// </summary>
        [Fact]
        public void ShouldDeclareAPredicateStrict()
        {
            Strong.policy(GeographyOperatorTable.ClrStGeogDWithin).Should().Be(Strong.Policy.ANY);
            Strong.policy(GeographyOperatorTable.ClrStGeogIntersects).Should().Be(Strong.Policy.ANY);
            Strong.policy(GeographyOperatorTable.ClrStGeogDistance).Should().Be(Strong.Policy.ANY);
        }

        /// <summary>
        /// Accessors and typed readers, which can return null for a non-null argument, do not declare
        /// <c>Strong.Policy.ANY</c>.
        /// </summary>
        /// <remarks>
        /// The policy is read in both directions: <c>RexSimplify.simplifyIsNull</c> turns <c>f(a) IS NULL</c>
        /// into <c>a IS NULL</c>. <c>CLR_ST_GEOG_X</c> of anything but a point is null, as <c>ST_X</c> is, and
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
        /// A symmetrical operator is its own reverse, as <c>RexNormalize</c> requires; an asymmetrical one has
        /// no reverse.
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
        /// A symmetrical measurement written with its operands both ways round is one expression.
        /// </summary>
        /// <remarks>
        /// This needs no pass: <c>RexNormalize</c> orders the operands of a symmetrical two-operand call when
        /// building its digest, so the calc evaluates the two spellings once.
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
        /// A constructor over a constant is evaluated once during planning, rather than once per row, when the
        /// planner has an executor.
        /// </summary>
        /// <remarks>
        /// The reduction is Calcite's: <c>RelOptUtil.registerDefaultRules</c> registers
        /// <c>CoreRules.FILTER_REDUCE_EXPRESSIONS</c>, and <c>ReduceExpressionsRule</c> silently does nothing
        /// without an executor. A <c>jdbc:calcite:</c> connection always has one; a <c>Frameworks</c> config has
        /// none unless one is set, and then the WKT is parsed for every row.
        /// </remarks>
        [Fact]
        public void ShouldReduceAConstantConstructorWhereTheCallerSetAnExecutor()
        {
            const string sql = "SELECT ID FROM GEO WHERE CLR_ST_GEOG_DWITHIN(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)";

            Explain(sql, simplify: false).Should().NotContain("CLR_ST_GEOG_GEOMFROMTEXT");

            Without(sql).Should().Contain("CLR_ST_GEOG_GEOMFROMTEXT");
        }

        /// <summary>
        /// Plans a query as <see cref="Explain"/> does but with no executor, and returns the plan as text.
        /// </summary>
        /// <param name="sql">The query text.</param>
        /// <returns>The physical plan as text.</returns>
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
        /// Each statement returns the same rows with the pass and without it.
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
        /// Counts the non-overlapping occurrences of a name in a plan's text.
        /// </summary>
        /// <param name="plan">The plan text.</param>
        /// <param name="name">The text to count, matched ordinally.</param>
        /// <returns>The number of non-overlapping occurrences.</returns>
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
