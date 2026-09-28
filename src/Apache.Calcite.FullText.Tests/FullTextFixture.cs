using System;
using System.Collections.Generic;

using Apache.Calcite.FullText.Schema;
using Apache.Calcite.FullText.Sql;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.plan.volcano;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.sql.validate;
using org.apache.calcite.sql2rel;

using DataContext = org.apache.calcite.DataContext;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// A test schema, and a planner that resolves names through either route.
    /// </summary>
    /// <remarks>
    /// <see cref="Plan"/> chains the catalog reader after the operator tables, as <c>CalcitePrepareImpl</c>
    /// does for a connection. <see cref="FullTextConnectionTests"/> covers the same ground through a real
    /// connection.
    /// </remarks>
    static class FullTextFixture
    {

        /// <summary>
        /// Builds a root schema holding the table and, optionally, the function declarations.
        /// </summary>
        /// <param name="declare">Whether to declare the <c>CLR_FT_*</c> functions on the schema.</param>
        /// <returns>The root schema.</returns>
        public static CalciteSchema RootSchema(bool declare = true)
        {
            var root = CalciteSchema.createRootSchema(false);
            root.add("DOCS", new DocumentTable());

            if (declare)
                FullTextSchema.AddTo(root.plus());

            return root;
        }

        /// <summary>
        /// Plans a statement.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="chain">Whether to chain <see cref="FullTextOperatorTable"/>, as a host would.</param>
        /// <param name="declare">Whether the schema declares the functions, as a connection needs.</param>
        /// <param name="libraries">
        /// Whether to chain every <c>SqlLibrary</c> table ahead of the catalog reader, as a connection setting
        /// <c>fun</c> does.
        /// </param>
        /// <returns>The logical plan.</returns>
        public static RelNode Plan(string sql, bool chain = false, bool declare = true, bool libraries = false)
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var rootSchema = RootSchema(declare);

            var properties = new java.util.Properties();
            properties.setProperty("caseSensitive", "true");

            var catalogReader = new CalciteCatalogReader(
                rootSchema,
                java.util.Collections.emptyList(),
                typeFactory,
                new CalciteConnectionConfigImpl(properties));

            var tables = new List<SqlOperatorTable> { SqlStdOperatorTable.instance() };

            if (libraries)
                tables.Add(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    java.util.EnumSet.allOf(java.lang.Class.forName("org.apache.calcite.sql.fun.SqlLibrary"))));

            if (chain)
                tables.Add(FullTextOperatorTable.Instance());

            tables.Add(catalogReader);

            var operators = SqlOperatorTables.chain(tables.ToArray());
            var parsed = SqlParser.create(sql, SqlParser.config().withUnquotedCasing(Casing.UNCHANGED)).parseQuery();
            var validator = SqlValidatorUtil.newValidator(operators, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);

            var planner = new VolcanoPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

            var cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
            var converter = new SqlToRelConverter(null, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());

            return converter.convertQuery(validator.validate(parsed), false, true).project();
        }

        /// <summary>
        /// Flattens everything a failure carries into one string.
        /// </summary>
        /// <remarks>
        /// Includes suppressed exceptions. <c>EnumerableRelImplementor.implementRoot</c> wraps an exception
        /// thrown during code generation in <c>IllegalStateException("Unable to implement " + plan)</c> and
        /// attaches the original with <c>addSuppressed</c>, so the refusal's message is found only there.
        /// </remarks>
        /// <param name="e">The failure.</param>
        /// <returns>Every message it carries.</returns>
        public static string Describe(Exception e)
        {
            var builder = new System.Text.StringBuilder();
            var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);

            void Walk(Exception? current)
            {
                if (current is null || seen.Add(current) == false)
                    return;

                if (builder.Length > 0)
                    builder.Append(" <- ");

                builder.Append(current.Message);

                Walk(current.InnerException);

                if (current is java.lang.Throwable throwable)
                {
                    Walk(throwable.getCause() as Exception);

                    foreach (var suppressed in throwable.getSuppressed())
                        Walk(suppressed as Exception);
                }
            }

            Walk(e);

            return builder.ToString();
        }

        /// <summary>
        /// Finds the first call to a named function anywhere in a plan.
        /// </summary>
        /// <param name="rel">The plan.</param>
        /// <param name="name">The function's name.</param>
        /// <returns>The call, or <c>null</c> where there is none.</returns>
        public static RexCall? Call(RelNode rel, string name)
        {
            RexCall? found = null;

            var shuttle = new Finder(name, call => found ??= call);
            rel.accept(shuttle);

            return found;
        }

        /// <summary>
        /// A one-row table with one column of each kind a store might search.
        /// </summary>
        /// <remarks>
        /// <c>BODY</c> is character, as in a relational store; <c>DOC</c> is <c>ANY</c>, as a document store
        /// types an undeclared property; <c>TAGS</c> is an array of strings, a type
        /// <c>SqlTypeName.ALL_TYPES</c> does not list.
        /// </remarks>
        public sealed class DocumentTable : AbstractTable, ScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("BODY", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("DOC", typeFactory.createSqlType(SqlTypeName.ANY))
                    .add("TAGS", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
                    .build();
            }

            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                return org.apache.calcite.linq4j.Linq4j.asEnumerable(java.util.Arrays.asList([
                    new object[] { java.lang.Integer.valueOf(1), "a steel frame", "a steel frame", java.util.Arrays.asList(["steel"]) },
                ]));
            }

        }

        sealed class Finder(string name, System.Action<RexCall> found) : RelShuttleImpl
        {

            readonly RexShuttle rex = new Visitor(name, found);

            public override RelNode visit(RelNode other)
            {
                return base.visit(other).accept(rex);
            }

            public override RelNode visit(org.apache.calcite.rel.logical.LogicalProject project)
            {
                return base.visit(project).accept(rex);
            }

            public override RelNode visit(org.apache.calcite.rel.logical.LogicalFilter filter)
            {
                return base.visit(filter).accept(rex);
            }

            sealed class Visitor(string name, System.Action<RexCall> found) : RexShuttle
            {

                public override RexNode visitCall(RexCall call)
                {
                    if (call.getOperator().getName() == name)
                        found(call);

                    return base.visitCall(call);
                }

            }

        }

    }

}
