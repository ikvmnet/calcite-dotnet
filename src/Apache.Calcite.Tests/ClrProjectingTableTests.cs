using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions.Adapter.Clr;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Queries a schema whose rows cannot be the objects, because their members are types Calcite has no
    /// representation for.
    /// </summary>
    /// <remarks>
    /// The other half of <see cref="ClrReflectiveSchemaTests"/>. There the row is the object and nothing is
    /// converted; here every value crosses a boundary on the way in, and the assertions are on the values
    /// themselves rather than against another table — a differential test would only compare the plumbing to
    /// itself, the oracle having to hold the converted values to begin with.
    /// </remarks>
    [TestClass]
    public class ClrProjectingTableTests
    {

        static ClrProjectingTableTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        public enum Grade
        {

            Junior = 1,

            Senior = 2,

        }

        /// <summary>
        /// One of every kind of member that has to be converted.
        /// </summary>
        public record Payment(
            int Id,
            decimal Amount,
            DateTime Taken,
            DateOnly Day,
            TimeOnly At,
            TimeSpan Took,
            Guid Reference,
            Grade Grade,
            int? Approver,
            string Note);

        /// <summary>
        /// A type of settable properties, which can be read and cannot be built back.
        /// </summary>
        public class Settable
        {

            public int Id { get; set; }

            public string Name { get; set; } = "";

        }

        public class PaymentSchema
        {

            public IReadOnlyList<Payment> Payments { get; } =
            [
                new Payment(1, 12.5m, new DateTime(2020, 1, 2, 3, 4, 5), new DateOnly(2020, 1, 2), new TimeOnly(3, 4, 5),
                    TimeSpan.FromSeconds(90), Guid.Parse("00000000-0000-0000-0000-0000000000ff"), Grade.Senior, null, "first"),
                new Payment(2, 1.1m, new DateTime(1970, 1, 1, 0, 0, 0), new DateOnly(1970, 1, 1), new TimeOnly(0, 0, 0),
                    TimeSpan.Zero, Guid.Empty, Grade.Junior, 7, "second"),
            ];

            public IReadOnlyList<Settable> Settables { get; } =
            [
                new Settable { Id = 1, Name = "one" },
                new Settable { Id = 2, Name = "two" },
            ];

        }

        /// <summary>
        /// A type with a member Calcite cannot read gets a table that projects, not a failure.
        /// </summary>
        [TestMethod]
        public void ShouldProjectATypeCalciteCannotReadAsItStands()
        {
            ClrTypeMapper.IsObjectRow(typeof(Payment)).Should().BeFalse();
            ClrReflectiveSchema.Of(typeof(Payment), new PaymentSchema().Payments).Should().BeOfType<ClrProjectingTable>();
        }

        /// <summary>
        /// A type that cannot be built back gets one too, rather than failing when a row is rebuilt.
        /// </summary>
        /// <remarks>
        /// Every member of <see cref="Settable"/> is one Calcite reads as it stands, so the only thing
        /// stopping it being an object row is that <c>JavaRowFormat.CUSTOM.record</c> has no constructor to
        /// call. Deciding that here is what keeps it from being an <c>InvalidOperationException</c> deep in a
        /// projection.
        /// </remarks>
        [TestMethod]
        public void ShouldProjectATypeThatCannotBeBuiltBack()
        {
            ClrTypeMapper.IsObjectRow(typeof(Settable)).Should().BeFalse();
            ClrReflect.Constructor(typeof(Settable)).Should().BeNull();
            ClrReflectiveSchema.Of(typeof(Settable), new PaymentSchema().Settables).Should().BeOfType<ClrProjectingTable>();

            Run("SELECT \"Name\" FROM \"Settables\" WHERE \"Id\" = 2").Should().Equal("two");
        }

        /// <summary>
        /// Each converted value equals the SQL literal it was written from.
        /// </summary>
        /// <remarks>
        /// <b>Calcite is the oracle, not arithmetic repeated in the test.</b> A TIMESTAMP is a number of
        /// milliseconds and a DATE a number of days, so asserting on the rendered value would mean computing
        /// an epoch offset here — the same sum <see cref="ClrValues"/> does, and wrong in the same way if it
        /// is wrong. Comparing to a literal asks Calcite instead: the conversion is right exactly when
        /// Calcite agrees the column holds the value it was written from.
        ///
        /// <para>The second row is the epoch in every one of its columns, so that a zero has to be a zero
        /// rather than an offset that happens to cancel.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldConvertEachValueToWhatItsColumnHolds()
        {
            Matches("\"Amount\" = 12.5", 1);
            Matches("\"Taken\" = TIMESTAMP '2020-01-02 03:04:05'", 1);
            Matches("\"Day\" = DATE '2020-01-02'", 1);
            Matches("\"At\" = TIME '03:04:05'", 1);
            Matches("\"Took\" = INTERVAL '0 00:01:30' DAY TO SECOND", 1);
            Matches("\"Reference\" = '00000000-0000-0000-0000-0000000000ff'", 1);
            Matches("\"Grade\" = 2", 1);
            Matches("\"Note\" = 'first'", 1);

            Matches("\"Taken\" = TIMESTAMP '1970-01-01 00:00:00'", 2);
            Matches("\"Day\" = DATE '1970-01-01'", 2);
            Matches("\"At\" = TIME '00:00:00'", 2);
            Matches("\"Took\" = INTERVAL '0' DAY", 2);
        }

        /// <summary>
        /// Requires that exactly the payment of the given id satisfies a predicate.
        /// </summary>
        static void Matches(string predicate, int id)
        {
            Run($"SELECT \"Id\" FROM \"Payments\" WHERE {predicate}")
                .Should().Equal([id.ToString()], "of {0}", predicate);
        }

        /// <summary>
        /// A nullable value type is null where it has no value, which is the thing Java cannot say.
        /// </summary>
        [TestMethod]
        public void ShouldReadANullableValueTypeAsNull()
        {
            Run("SELECT \"Id\" FROM \"Payments\" WHERE \"Approver\" IS NULL").Should().Equal("1");
            Run("SELECT \"Approver\" FROM \"Payments\" WHERE \"Id\" = 2").Should().Equal("7");
            Run("SELECT \"Approver\" FROM \"Payments\" WHERE \"Id\" = 1").Should().Equal("null");
        }

        /// <summary>
        /// The converted values compare, group and aggregate as their types.
        /// </summary>
        /// <remarks>
        /// The reason <see cref="ClrValues.DecimalScale"/> normalises rather than merely declares: a group key
        /// is compared with <c>BigDecimal.equals</c>, which is scale sensitive, so two decimals that differ
        /// only in trailing zeroes would group apart if each kept the scale it was written with.
        /// </remarks>
        [TestMethod]
        public void ShouldCompareAndAggregateConvertedValues()
        {
            Run("SELECT SUM(\"Amount\") FROM \"Payments\"").Should().Equal("13.600000");
            Run("SELECT COUNT(*) FROM \"Payments\" WHERE \"Taken\" > TIMESTAMP '2000-01-01 00:00:00'").Should().Equal("1");
            Run("SELECT COUNT(*) FROM \"Payments\" WHERE \"Day\" = DATE '2020-01-02'").Should().Equal("1");
            Run("SELECT COUNT(DISTINCT \"Amount\") FROM \"Payments\"").Should().Equal("2");
        }

        static List<string> Run(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var added = rootSchema.add("PAY", new ClrReflectiveSchema(new PaymentSchema()));

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(added)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, false, false, null, null),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new PayDataContext(rootSchema, parameters);
            var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            var rows = new List<string>();
            foreach (var row in bindable.Bind(context))
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// Renders a row as the values it holds, which for a temporal column is the number Calcite keeps.
        /// </summary>
        static string Render(object? row)
        {
            if (row is object?[] array)
                return string.Join("|", array.Select(Cell));

            return Cell(row);
        }

        static string Cell(object? value) => value?.ToString() ?? "null";

        sealed class PayDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

    }

}
