using System;
using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Runs a plan that holds nodes of both calling conventions.
    /// </summary>
    /// <remarks>
    /// A row crosses the boundary untouched, because both conventions ask the same <c>JavaTypeFactory</c> what
    /// a field is. These run a plan where the two are mixed, in each direction, to say so.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableMixedConventionTests
    {

        /// <summary>
        /// A table of three rows.
        /// </summary>
        sealed class PeopleTable : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                list.add(new object[] { java.lang.Integer.valueOf(1), "SMITH" });
                list.add(new object[] { java.lang.Integer.valueOf(2), "JONES" });
                list.add(new object[] { java.lang.Integer.valueOf(3), "BROWN" });

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema"></param>
        sealed class TestDataContext(SchemaPlus rootSchema) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => null!;

        }

        /// <summary>
        /// Plans a query with both conventions available and returns its rows.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="root">Which convention the plan is asked to end in.</param>
        /// <returns></returns>
        static List<object[]> Run(string sql, Convention root)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            // both rule sets, so the planner may put any node in either convention and bridge between them
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // both calc rule sets, because a project of either convention refuses to implement itself
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.ofRules(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            var chosen = planner.transform(0, planner.getEmptyTraitSet().replace(root), logical);
            var physical = planner.transform(1, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var bindable = physical is ClrEnumerableRel clr
                ? ClrInterpretable.ToBindable(parameters, clr, EnumerableRel.Prefer.ARRAY)
                : EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY);

            var rows = new List<object[]>();
            var enumerator = bindable.bind(new TestDataContext(rootSchema)).enumerator();
            while (enumerator.moveNext())
            {
                var current = enumerator.current();
                rows.Add(current as object[] ?? [current]);
            }

            return rows;
        }

        [TestMethod]
        public void ShouldEndInThisConvention()
        {
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1", ClrEnumerableConvention.Instance);

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        [TestMethod]
        public void ShouldEndInCalcitesConvention()
        {
            // the plan is asked to end in EnumerableConvention, so whatever of it lands in this one has to be
            // read back across the boundary
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1", EnumerableConvention.INSTANCE);

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

    }

}
