using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Holds what an aggregate over a column of type ANY compiles to.
    /// </summary>
    /// <remarks>
    /// The answers themselves are asserted by the two differential suites; this asks the other question about
    /// the same plans, which is what they are made of. MIN, MAX and SUM over ANY are the one place in either
    /// convention where this project writes a linq4j tree of its own rather than translating one Calcite
    /// produced — <c>ClrAnyAggImplementors</c> implements <c>AggImplementor</c>, whose contexts hand out a
    /// <c>BlockBuilder</c> and a list of linq4j expressions and admit nothing else — so the claim that it is
    /// all translated away before anything runs is worth measuring rather than asserting.
    ///
    /// <para>A tree that reached the delegate untranslated would in fact throw at
    /// <see cref="LambdaExpression.Compile"/> long before it ran, so this is belt and braces. What it really
    /// guards is the day someone adds a node to those implementors that <c>LixToClrTranslator</c> happens to
    /// carry across as a constant.</para>
    /// </remarks>
    [TestClass]
    public class ClrAnyAggregateTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static ClrAnyAggregateTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of two ANY columns, one holding numbers of two classes and one holding strings.
        /// </summary>
        sealed class AnysTable : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("K", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("V", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                    .add("S", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in AsyncTestRows.Anys)
                    list.add(row);

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
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

        /// <summary>
        /// Plans a statement into one of the two conventions and returns the tree and the rows it gives.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="async">Whether to plan into the asynchronous convention.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>AGGREGATE_REDUCE_FUNCTIONS</c> for the same reason the differential suites register it: AVG has
        /// no implementor in any convention, in any type, and is a <c>$SUM0</c> over a <c>COUNT</c> by the
        /// time a planner sees it.
        /// </remarks>
        static async Task<(LambdaExpression Tree, List<string> Rows)> Plan(string sql, bool async)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("ANYS", new AnysTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in async ? ClrAsyncEnumerableRules.CalcRules() : ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = async ? (Convention)ClrAsyncEnumerableConvention.Instance : ClrEnumerableConvention.Instance;
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var rows = new List<string>();

            LambdaExpression tree;

            if (async)
            {
                var implementor = new ClrAsyncEnumerableRelImplementor(physical.getCluster().getRexBuilder(), parameters);
                tree = implementor.ImplementRoot((ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);

                var plan = (Func<DataContext, IAsyncEnumerable<object>>)tree.Compile();
                await foreach (var row in plan(context))
                    rows.Add(Render(row));
            }
            else
            {
                var implementor = new ClrEnumerableRelImplementor(physical.getCluster().getRexBuilder(), parameters);
                tree = implementor.ImplementRoot((ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

                var plan = (Func<DataContext, IEnumerable<object>>)tree.Compile();
                foreach (var row in plan(context))
                    rows.Add(Render(row));
            }

            return (tree, rows);
        }

        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Every query that reaches one of the ANY implementors, aggregate and window alike.
        /// </summary>
        static readonly string[] Queries =
        [
            "SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS",
            "SELECT K, MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS GROUP BY K",
            "SELECT MIN(S), MAX(S) FROM ANYS",
            "SELECT ANY_VALUE(V), VAR_POP(V), MIN(V) FILTER (WHERE ID > 1) FROM ANYS",
            "SELECT ID, MIN(V) OVER (PARTITION BY K), SUM(V) OVER (ORDER BY ID) FROM ANYS",
        ];

        [TestMethod]
        public Task ShouldCompileASyncAnyAggregateToNoLinq4j() => ShouldCompileToNoLinq4j(false);

        [TestMethod]
        public Task ShouldCompileAnAsyncAnyAggregateToNoLinq4j() => ShouldCompileToNoLinq4j(true);

        static async Task ShouldCompileToNoLinq4j(bool async)
        {
            foreach (var sql in Queries)
            {
                var (tree, rows) = await Plan(sql, async);

                // the tree is only worth reading if it produced something: a plan that yielded nothing would
                // satisfy the assertion below without having exercised an implementor at all
                rows.Should().NotBeEmpty("'{0}' should give rows", sql);

                var found = new Linq4jTrees();
                found.Visit(tree);
                found.Found.Should().BeEmpty("'{0}' should hold no linq4j tree node once it is compiled", sql);
            }
        }

        /// <summary>
        /// Collects every reference to a linq4j tree type in an expression tree.
        /// </summary>
        /// <remarks>
        /// <c>org.apache.calcite.linq4j.tree</c> only, and not linq4j as a whole. A plan legitimately holds
        /// linq4j's <em>runtime</em> — a <c>ScannableTable</c> answers an <c>Enumerable</c> and the scan reads
        /// it — and what is being asked here is whether a tree Calcite's or ours <em>built</em> survived the
        /// translation rather than being turned into <see cref="Expression"/>s.
        /// </remarks>
        sealed class Linq4jTrees : ExpressionVisitor
        {

            public readonly List<string> Found = [];

            /// <inheritdoc />
            public override Expression? Visit(Expression? node)
            {
                switch (node)
                {
                    case null:
                        break;
                    case ConstantExpression constant when constant.Value != null:
                        Check(constant.Value.GetType(), node);
                        Check(node.Type, node);
                        break;
                    case MethodCallExpression call:
                        Check(call.Method.DeclaringType, node);
                        Check(call.Method.ReturnType, node);
                        foreach (var parameter in call.Method.GetParameters())
                            Check(parameter.ParameterType, node);
                        Check(node.Type, node);
                        break;
                    case MemberExpression member:
                        Check(member.Member.DeclaringType, node);
                        Check(node.Type, node);
                        break;
                    case NewExpression created when created.Constructor != null:
                        Check(created.Constructor.DeclaringType, node);
                        Check(node.Type, node);
                        break;
                    default:
                        Check(node.Type, node);
                        break;
                }

                return base.Visit(node);
            }

            void Check(Type? type, Expression node)
            {
                foreach (var each in Expand(type))
                    if (each.FullName is string name && name.StartsWith("org.apache.calcite.linq4j.tree.", StringComparison.Ordinal))
                        Found.Add($"{name} at {node.NodeType}");
            }

            /// <summary>
            /// Yields a type and everything it is written in terms of, so that a linq4j node hiding as an
            /// element or a type argument is seen.
            /// </summary>
            static IEnumerable<Type> Expand(Type? type)
            {
                if (type == null)
                    yield break;

                yield return type;

                if (type.HasElementType)
                    foreach (var each in Expand(type.GetElementType()))
                        yield return each;

                if (type.IsGenericType)
                    foreach (var argument in type.GetGenericArguments())
                        foreach (var each in Expand(argument))
                            yield return each;
            }

        }

    }

}
