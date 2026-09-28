using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests of <c>ClrCursorToEnumerableConverter</c>: plans of this convention running under a node of
    /// Calcite's, so that Janino compiles a call back into a stashed plan.
    /// </summary>
    /// <remarks>
    /// Most mixed plans in the suite cross the other way, through a converter into this convention. Crossing
    /// out depends on two facts about the generated Java source: linq4j writes a class name with every
    /// <c>$</c> as <c>.</c>, which cannot name IKVM's mangled name for a generic instantiation, so the plan is
    /// stashed as an <c>Object</c>; and Janino ignores a class IKVM exposes as package-private, so
    /// <c>JavaPlans</c> is public.
    /// </remarks>
    public class ClrCursorToEnumerableConverterTests
    {

        static ClrCursorToEnumerableConverterTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        /// <param name="parameters">The map the implementors stashed values into, which <c>get</c> answers from.</param>
        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// Plans a statement whose root is in Calcite's convention, with this convention's rules registered
        /// beside Calcite's.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="rootSchema">The schema to plan against.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The physical root, in <c>EnumerableConvention</c>.</returns>
        static RelNode Plan(string sql, SchemaPlus rootSchema, RelOptRule[]? remove = null)
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, remove: remove),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Renders a row as text, so that rows from two plans compare by value.
        /// </summary>
        /// <param name="row">A row, an <c>object[]</c> for a multi-column result or the value itself for one column.</param>
        /// <returns>The fields' text joined with <c>|</c>, with a null written as <c>&lt;null&gt;</c>.</returns>
        static string Render(object? row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Runs a plan that ends in Calcite's convention and returns its rows rendered as text.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="rootSchema">The schema to plan against.</param>
        /// <param name="plan">Receives the physical plan's text.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> Run(string sql, SchemaPlus rootSchema, out string plan, RelOptRule[]? remove = null)
        {
            var physical = Plan(sql, rootSchema, remove);
            plan = RelOptUtil.toString(physical);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();
            foreach (var row in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// A scan only this convention can read, under an aggregate of Calcite's, compiles and returns the same
        /// rows as the same query over a table Calcite can scan.
        /// </summary>
        /// <remarks>
        /// The table implements this project's table SPI, which neither <c>EnumerableTableScan.canHandle</c> nor
        /// the bindable scan accepts, so the scan must be this convention's and the converter must be in the plan.
        /// </remarks>
        [Fact]
        public void ShouldCarryACursorPlanUnderACalciteNode()
        {
            var sql = "SELECT REGION, SUM(AMOUNT) FROM ASALES WHERE ID > 1 GROUP BY REGION ORDER BY REGION";

            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("ASALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var rows = Run(sql, rootSchema, out var plan);

            plan.Should().Contain("ClrCursorToEnumerableConverter").And.Contain("ClrCursorTableScan");

            rows.Should().Equal(Run(sql.Replace("ASALES", "SALES"), rootSchema, out _));
        }

        /// <summary>
        /// A sub-plan of this convention under a correlate of Calcite's reads the outer row.
        /// </summary>
        /// <remarks>
        /// The outer row is a parameter of the Java lambda Calcite's correlate generates, and the sub-plan is
        /// compiled apart from that lambda, so the converter reads the row's fields through Calcite's getter and
        /// passes them in through the <c>DataContext</c>. The scans are this convention's because only this
        /// project's SPI reads the table; this convention's correlate rule is removed, so the only correlate the
        /// planner can build is Calcite's over converters out of this convention.
        /// </remarks>
        [Fact]
        public void ShouldReadACorrelationVariableUnderCalcitesCorrelate()
        {
            var sql = "SELECT ID FROM ASALES S1 WHERE EXISTS (SELECT 1 FROM ASALES S2 WHERE S2.REGION = S1.REGION AND S2.ID > 3) ORDER BY ID";

            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("ASALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var rows = Run(sql, rootSchema, out var plan, remove: [ClrCursorRules.ClrCursorCorrelateRule]);

            plan.Should().Contain("EnumerableCorrelate", plan).And.Contain("ClrCursorToEnumerableConverter", plan);

            rows.Should().Equal(Run(sql.Replace("ASALES", "SALES"), rootSchema, out _, remove: [ClrCursorRules.ClrCursorCorrelateRule]));
            rows.Should().NotBeEmpty();
        }

    }

}
