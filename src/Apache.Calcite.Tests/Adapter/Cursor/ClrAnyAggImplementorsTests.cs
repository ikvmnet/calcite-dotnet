using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests that a plan aggregating a column of type ANY compiles to an expression tree holding no linq4j
    /// tree node.
    /// </summary>
    /// <remarks>
    /// The differential suites check the rows these plans return; this checks what the compiled plan is made
    /// of. <c>ClrAnyAggImplementors</c> implements Calcite's <c>AggImplementor</c>, whose contexts accept only
    /// a <c>BlockBuilder</c> and linq4j expressions, so it writes linq4j trees of its own that must all be
    /// translated to <see cref="Expression"/>s. Most untranslated nodes would already fail at
    /// <see cref="LambdaExpression.Compile()"/>; this also catches one that <c>LixToClrTranslator</c> carries
    /// across as a constant.
    /// </remarks>
    public class ClrAnyAggImplementorsTests
    {

        /// <summary>
        /// Puts Calcite's JDBC assembly on the boot class path.
        /// </summary>
        static ClrAnyAggImplementorsTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table over <c>AsyncTestRows.Anys</c> with two ANY columns: <c>V</c>, holding numbers of more than
        /// one class, and <c>S</c>, holding strings.
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

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        /// <param name="parameters">The map the implementor stashed values into, which <c>get</c> answers from.</param>
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
        /// Plans and implements a statement, and returns one open's expression tree and the rows it reads.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="async">Whether to take the awaiting open's tree and rows rather than the synchronous
        /// open's. The plan is the same either way.</param>
        /// <returns>The chosen open's lambda, and the rows that open reads, each rendered as text.</returns>
        /// <remarks>
        /// <c>AGGREGATE_REDUCE_FUNCTIONS</c> is registered, as in the differential suites, because AVG has no
        /// implementor and must be rewritten in terms of SUM and COUNT before it is planned.
        /// </remarks>
        static async Task<(LambdaExpression Tree, List<string> Rows)> Plan(string sql, bool async)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("ANYS", new AnysTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
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

            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrCursorConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var rows = new List<string>();

            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters)
                .ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

            LambdaExpression tree = async ? factory.OpenAsyncExpression : factory.OpenExpression;

            if (async)
            {
                await using var cursor = await factory.OpenAsync(context, CancellationToken.None);
                while (await cursor.ReadAsync(CancellationToken.None))
                    rows.Add(Render(cursor.Current!));
            }
            else
            {
                using var cursor = factory.Open(context);
                while (cursor.Read())
                    rows.Add(Render(cursor.Current!));
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

        [Fact]
        public Task ShouldCompileASyncAnyAggregateToNoLinq4j() => ShouldCompileToNoLinq4j(false);

        [Fact]
        public Task ShouldCompileAnAsyncAnyAggregateToNoLinq4j() => ShouldCompileToNoLinq4j(true);

        static async Task ShouldCompileToNoLinq4j(bool async)
        {
            foreach (var sql in Queries)
            {
                var (tree, rows) = await Plan(sql, async);

                // a plan that returned no rows could pass the assertion below without exercising an implementor
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
        /// Only types in <c>org.apache.calcite.linq4j.tree</c> count. A plan legitimately uses linq4j's runtime
        /// types, such as the <c>Enumerable</c> a <c>ScannableTable</c> returns; a tree type in the compiled plan
        /// means a linq4j tree was not translated to <see cref="Expression"/>s.
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
            /// <param name="type">The type to expand; null yields nothing.</param>
            /// <returns>The type itself, then its element type and generic arguments, recursively.</returns>
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
