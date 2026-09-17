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
    /// A schema to resolve names against, and the two ways of reaching them.
    /// </summary>
    /// <remarks>
    /// <c>Plan</c> reproduces by hand the one thing <c>CalcitePrepareImpl</c> does for every statement a
    /// connection prepares: it chains the catalog reader onto the operator table the validator is built with.
    /// Everything else here is what a host embedding Calcite would already have.
    /// <see cref="FullTextConnectionTests"/> makes the same claims against a real connection, where none of
    /// this is written down.
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
        /// Whether to chain every <c>SqlLibrary</c> table, which is what a connection setting <c>fun</c>
        /// does — and which is chained ahead of the catalog reader, so it is where a name Calcite also uses
        /// would shadow the schema's declaration.
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
        /// <b>The suppressed list is not optional here.</b> <c>EnumerableRelImplementor.implementRoot</c>
        /// catches any <c>RuntimeException</c> a node's <c>implement</c> throws, builds an
        /// <c>IllegalStateException("Unable to implement " + plan)</c>, and attaches the original with
        /// <c>addSuppressed</c> — which <c>Throwable.toString</c> does not print. So a refusal raised while
        /// generating code is invisible to an assertion on the message, and a test that read only the message
        /// could not tell our refusal from any other reason a plan failed to implement.
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
        /// A table with one column of each kind a store might declare searchable.
        /// </summary>
        /// <remarks>
        /// <c>BODY</c> is character, which is what a relational store searches; <c>DOC</c> is <c>ANY</c>,
        /// which is how a document store types a property whose type the container does not declare; and
        /// <c>TAGS</c> is an array of strings, which Cosmos and PostgreSQL both allow to be searchable and
        /// which <c>SqlTypeName.ALL_TYPES</c> does not list.
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
