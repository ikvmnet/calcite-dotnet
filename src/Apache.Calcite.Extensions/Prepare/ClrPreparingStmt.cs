using System;

using org.apache.calcite.adapter.java;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.validate;
using org.apache.calcite.sql2rel;
using org.apache.calcite.tools;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Takes a parsed statement to a plan, and hands the plan to a convention to implement.
    /// </summary>
    /// <remarks>
    /// Our counterpart of <c>Prepare</c> and <c>CalcitePrepareImpl.CalcitePreparingStmt</c>, which are one
    /// class here because <c>Prepare</c>'s only subclass in Calcite is that one. The steps are its
    /// <c>prepareSql</c>, in order: convert, checked arithmetic, flatten, decorrelate, trim, optimize,
    /// implement — with the two <c>EXPLAIN</c> exits where Calcite has them.
    ///
    /// <para>It is written rather than derived because <c>Prepare.implement</c> is declared to return a
    /// <c>PreparedResult</c>, an interface whose reason for existing is <c>getBindable</c> — a linq4j
    /// <c>Enumerable</c>. Our conventions compile to a delegate, so there is nothing to hand back through
    /// it. <see cref="ClrPrepareResult"/> is that interface without the member.</para>
    ///
    /// <para>Three things are a convention's own and nothing else is: the convention a plan must end in,
    /// the program that gets it there, and the compiler at the end. They are the three abstract members
    /// below, and they are the whole of what a second convention supplies.</para>
    /// </remarks>
    abstract class ClrPreparingStmt : RelOptTable.ViewExpander
    {

        readonly CalcitePrepare.Context context;
        readonly CalciteCatalogReader catalogReader;
        readonly CalciteSchema schema;
        readonly RelOptCluster cluster;
        readonly RelOptPlanner planner;
        readonly RexBuilder rexBuilder;
        readonly JavaTypeFactory typeFactory;
        readonly SqlRexConvertletTable convertletTable;

        /// <summary>
        /// The values the query reads through the <c>DataContext</c> rather than from the plan.
        /// </summary>
        /// <remarks>
        /// Ours rather than Calcite's. <c>CalcitePreparingStmt.internalParameters</c> is private and written
        /// only inside the <c>implement</c> a subclass replaces, so a port that derived from it stashed into
        /// one map and served the query from another.
        /// </remarks>
        readonly java.util.Map internalParameters = new java.util.LinkedHashMap();

        SqlValidator? validator;
        RelDataType? parameterRowType;
        java.util.List? fieldOrigins;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        protected ClrPreparingStmt(
            CalcitePrepare.Context context,
            CalciteCatalogReader catalogReader,
            CalciteSchema schema,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.catalogReader = catalogReader ?? throw new ArgumentNullException(nameof(catalogReader));
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            this.convertletTable = convertletTable ?? throw new ArgumentNullException(nameof(convertletTable));

            planner = cluster.getPlanner();
            rexBuilder = cluster.getRexBuilder();
            typeFactory = (JavaTypeFactory)cluster.getTypeFactory();
        }

        /// <summary>
        /// Gets the context the statement is prepared against.
        /// </summary>
        protected CalcitePrepare.Context Context => context;

        /// <summary>
        /// Gets the cluster the plan is built in.
        /// </summary>
        protected RelOptCluster Cluster => cluster;

        /// <summary>
        /// Gets the values the query reads through the <c>DataContext</c> rather than from the plan.
        /// </summary>
        public java.util.Map InternalParameters => internalParameters;

        /// <summary>
        /// Gets the validator the statement is validated with.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePreparingStmt.getSqlValidator</c>, which caches so that one statement cannot end up
        /// with two.
        /// </remarks>
        public SqlValidator Validator => validator ??= CreateSqlValidator(catalogReader, c => c);

        /// <summary>
        /// Gets the convention a plan must end in.
        /// </summary>
        protected abstract Convention ResultConvention { get; }

        /// <summary>
        /// Returns the program that takes a logical plan to that convention.
        /// </summary>
        protected abstract Program GetProgram();

        /// <summary>
        /// Returns the traits the root of the plan must satisfy.
        /// </summary>
        protected abstract RelTraitSet GetDesiredRootTraitSet(RelRoot root);

        /// <summary>
        /// Compiles the chosen plan.
        /// </summary>
        /// <param name="root">The root of the plan, which is of <see cref="ResultConvention"/>.</param>
        /// <returns>The compiled statement.</returns>
        protected abstract ClrPrepareResult Implement(RelRoot root);

        /// <summary>
        /// Prepares a parsed statement.
        /// </summary>
        /// <param name="sqlQuery">The statement, which an <c>EXPLAIN</c> is unwrapped from.</param>
        /// <param name="sqlNodeOriginal">The statement as parsed, before that unwrapping.</param>
        /// <param name="needsValidation">Whether the statement still has to be validated.</param>
        /// <returns>The compiled statement, or the rendered plan where it was an <c>EXPLAIN</c>.</returns>
        public ClrPrepareResult PrepareSql(SqlNode sqlQuery, SqlNode sqlNodeOriginal, bool needsValidation)
        {
            ArgumentNullException.ThrowIfNull(sqlQuery);
            ArgumentNullException.ThrowIfNull(sqlNodeOriginal);

            var config = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(((java.lang.Boolean)org.apache.calcite.prepare.Prepare.THREAD_EXPAND.get()).booleanValue())
                .withInSubQueryThreshold(((java.lang.Integer)org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD.get()).intValue())
                .withExplain(sqlQuery.getKind() == SqlKind.EXPLAIN);

            var configHolder = Holder.of(config);
            org.apache.calcite.runtime.Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.run(configHolder);

            var sqlToRelConverter = GetSqlToRelConverter(Validator, catalogReader, (SqlToRelConverter.Config)configHolder.get());

            SqlExplain? sqlExplain = null;
            if (sqlQuery.getKind() == SqlKind.EXPLAIN)
            {
                // dig out the underlying SQL statement
                sqlExplain = (SqlExplain)sqlQuery;
                sqlQuery = sqlExplain.getExplicandum();
                sqlToRelConverter.setDynamicParamCountInExplain(sqlExplain.getDynamicParamCount());
            }

            var root = sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true);

            // all arithmetic on exact types where the conformance asks for it, and all arithmetic producing
            // an INTERVAL whatever the conformance says
            var convertToChecked = context.config().conformance().checkedArithmetic();
            var checkedConv = new ConvertToChecked(root.rel.getCluster().getRexBuilder(), convertToChecked);
            root = root.withRel(checkedConv.visit(root.rel));
            org.apache.calcite.runtime.Hook.CONVERTED.run(root.rel);

            var resultType = Validator.getValidatedNodeType(sqlQuery);
            fieldOrigins = Validator.getFieldOrigins(sqlQuery);
            parameterRowType = Validator.getParameterRowType(sqlQuery);

            // the logical plan, before view expansion, physical storage and decorrelation
            if (sqlExplain != null)
            {
                switch (sqlExplain.getDepth().name())
                {
                    case nameof(SqlExplain.Depth.TYPE):
                        return CreatePreparedExplanation(resultType, parameterRowType, null, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
                    case nameof(SqlExplain.Depth.LOGICAL):
                        return CreatePreparedExplanation(null, parameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
                }
            }

            root = root.withRel(FlattenTypes(root.rel, true));

            // TopDownGeneralDecorrelator cannot run until the sub-queries are gone
            if (context.config().forceDecorrelate() && context.config().topDownGeneralDecorrelationEnabled() == false)
                root = root.withRel(Decorrelate(sqlToRelConverter, sqlQuery, root.rel));

            if (((SqlToRelConverter.Config)configHolder.get()).isTrimUnusedFields())
            {
                root = TrimUnusedFields(root);
                org.apache.calcite.runtime.Hook.TRIMMED.run(root.rel);
            }

            // the physical plan, after decorrelation
            if (sqlExplain != null)
            {
                root = Optimize(root);
                return CreatePreparedExplanation(null, parameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
            }

            root = Optimize(root);

            // a DML rewritten to other DML — UPDATE to MERGE — keeps the rewrite's kind; anything else —
            // CALL to SELECT — keeps the kind it was parsed as
            if (root.kind.belongsTo(SqlKind.DML) == false)
                root = root.withKind(sqlNodeOriginal.getKind());

            org.apache.calcite.runtime.Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);

            return Implement(root);
        }

        /// <summary>
        /// Gets the row type of the statement's dynamic parameters, once it has been validated.
        /// </summary>
        protected RelDataType ParameterRowType =>
            parameterRowType ?? throw new InvalidOperationException("The statement has not been validated.");

        /// <summary>
        /// Gets, per field, the name it originates from, once the statement has been validated.
        /// </summary>
        protected java.util.List FieldOrigins =>
            fieldOrigins ?? throw new InvalidOperationException("The statement has not been validated.");

        /// <summary>
        /// Runs the program, which is what chooses a plan.
        /// </summary>
        /// <param name="root"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Prepare.optimize</c>, less the materializations and lattices. See
        /// <see cref="GetMaterializations"/>.
        /// </remarks>
        RelRoot Optimize(RelRoot root)
        {
            var planner = root.rel.getCluster().getPlanner();
            planner.setExecutor(new RexExecutorImpl(context.getDataContext()));

            var materializations = GetMaterializations();
            var lattices = GetLattices();
            var desiredTraits = GetDesiredRootTraitSet(root);

            return root.withRel(GetProgram().run(planner, root.rel, desiredTraits, materializations, lattices));
        }

        /// <summary>
        /// Returns the materialized views the planner may substitute.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// Empty, where Calcite queries the <c>MaterializationService</c> and populates each through
        /// <c>CalcitePrepareImpl.populateMaterializations</c> — which builds a <c>CalciteMaterializer</c>, a
        /// package-private class of <c>org.apache.calcite.prepare</c> with a package-private constructor.
        /// It cannot be reached and cannot be written again, so a materialized view is not substituted here
        /// however the connection is configured.
        /// </remarks>
        static java.util.List GetMaterializations()
        {
            return com.google.common.collect.ImmutableList.of();
        }

        /// <summary>
        /// Returns the lattices the planner may use.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>Schemas.getLatticeEntries</c>, as Calcite's does. A lattice needs a star table to be useful and
        /// <see cref="GetMaterializations"/> supplies none, so this is empty in practice; it is read rather
        /// than assumed empty because that is a fact about the schema rather than about this class.
        /// </remarks>
        java.util.List GetLattices()
        {
            var entries = org.apache.calcite.schema.Schemas.getLatticeEntries(schema);
            var lattices = new java.util.ArrayList();

            for (var i = entries.iterator(); i.hasNext();)
            {
                var lattice = (CalciteSchema.LatticeEntry)i.next();
                var starTable = lattice.getStarTable();
                var starRelOptTable = RelOptTableImpl.create(catalogReader, starTable.getTable().getRowType(typeFactory), starTable, null);
                lattices.add(new RelOptLattice(lattice.getLattice(), starRelOptTable));
            }

            return lattices;
        }

        /// <summary>
        /// Trims the fields no one reads.
        /// </summary>
        /// <param name="root"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Prepare.trimUnusedFields</c>. It builds a second converter because the trim runs with
        /// <c>trimUnusedFields</c> decided per plan by <c>shouldTrim</c> rather than by the config the query
        /// was converted with.
        /// </remarks>
        RelRoot TrimUnusedFields(RelRoot root)
        {
            var config = SqlToRelConverter.config()
                .withTrimUnusedFields(ShouldTrim(root.rel))
                .withExpand(((java.lang.Boolean)org.apache.calcite.prepare.Prepare.THREAD_EXPAND.get()).booleanValue())
                .withInSubQueryThreshold(((java.lang.Integer)org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD.get()).intValue());

            var converter = GetSqlToRelConverter(Validator, catalogReader, config);
            var ordered = root.collation.getFieldCollations().isEmpty() == false;
            var dml = SqlKind.DML.contains(root.kind);

            return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
        }

        /// <summary>
        /// Returns whether a plan is worth trimming.
        /// </summary>
        /// <param name="rootRel"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Prepare.shouldTrim</c>: trimming a bare projection would leave nothing behind.
        /// </remarks>
        static bool ShouldTrim(RelNode rootRel)
        {
            return rootRel is not org.apache.calcite.rel.logical.LogicalProject;
        }

        /// <summary>
        /// Flattens structured types.
        /// </summary>
        /// <param name="rootRel"></param>
        /// <param name="restructure"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>CalcitePreparingStmt.flattenTypes</c>, which flattens only under a Spark handler and otherwise
        /// hands the plan back. This convention cannot use a Spark handler at all — it has no generated
        /// class to give one — so this is the branch Calcite takes without one.
        /// </remarks>
        static RelNode FlattenTypes(RelNode rootRel, bool restructure)
        {
            return rootRel;
        }

        /// <summary>
        /// Removes correlation from a plan.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePreparingStmt.decorrelate</c>. Reached only when the connection asks for it: a
        /// correlate is a node these conventions implement, and decorrelating removes the node they would
        /// have used.
        /// </remarks>
        static RelNode Decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel)
        {
            return sqlToRelConverter.decorrelate(query, rootRel);
        }

        /// <summary>
        /// Builds the converter from SQL to relational algebra.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePreparingStmt.getSqlToRelConverter</c>. This class is the view expander it is given.
        /// </remarks>
        SqlToRelConverter GetSqlToRelConverter(SqlValidator validator, org.apache.calcite.prepare.Prepare.CatalogReader catalogReader, SqlToRelConverter.Config config)
        {
            config = config.withTopDownGeneralDecorrelationEnabled(context.config().topDownGeneralDecorrelationEnabled());

            return new SqlToRelConverter(this, validator, catalogReader, cluster, convertletTable, config);
        }

        /// <summary>
        /// Renders a plan, for an <c>EXPLAIN</c>.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.PreparedExplain</c>'s <c>getCode</c>, which is where the text is produced rather than
        /// at the point the statement is run.
        /// </remarks>
        ClrPrepareResult CreatePreparedExplanation(
            RelDataType? resultType,
            RelDataType parameterRowType,
            RelRoot? root,
            org.apache.calcite.sql.SqlExplainFormat format,
            org.apache.calcite.sql.SqlExplainLevel detailLevel)
        {
            var explanation = root == null
                ? RelOptUtil.dumpType(requireResultType(resultType))
                : RelOptUtil.dumpPlan("", root.rel, format, detailLevel);

            return new ClrExplainResult(parameterRowType, explanation);

            static RelDataType requireResultType(RelDataType? type) =>
                type ?? throw new java.lang.IllegalStateException("an EXPLAIN of a type needs one");
        }

        /// <summary>
        /// Creates the validator.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.createSqlValidator</c>, which is a private static there.
        /// </remarks>
        SqlValidator CreateSqlValidator(CalciteCatalogReader catalogReader, Func<SqlValidator.Config, SqlValidator.Config> configTransform)
        {
            var connectionConfig = context.config();
            var opTab0 = (SqlOperatorTable)connectionConfig.fun((java.lang.Class)typeof(SqlOperatorTable), org.apache.calcite.sql.fun.SqlStdOperatorTable.instance());

            var list = new java.util.ArrayList();
            list.add(opTab0);
            list.add(catalogReader);

            var opTab = org.apache.calcite.sql.util.SqlOperatorTables.chain(list);

            var config = configTransform(
                SqlValidator.Config.DEFAULT
                    .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                    .withConformance(connectionConfig.conformance())
                    .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                    .withIdentifierExpansion(true));

            return new CalciteSqlValidator(opTab, catalogReader, typeFactory, config);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.expandView</c>. A view's definition is parsed and converted against the
        /// schema path the view was declared with, which is not necessarily the connection's.
        ///
        /// <para>The validator is built with <c>withEmbeddedQuery(true)</c> and is not the cached one: a
        /// view's query is validated in its own right, against its own catalog reader.</para>
        /// </remarks>
        public RelRoot expandView(RelDataType rowType, string queryString, java.util.List schemaPath, java.util.List viewPath)
        {
            var parser = SqlParser.create(queryString, ParserConfig());

            SqlNode sqlNode;
            try
            {
                sqlNode = parser.parseQuery();
            }
            catch (SqlParseException e)
            {
                throw new java.lang.RuntimeException("parse failed", e);
            }

            var viewCatalogReader = catalogReader.withSchemaPath(schemaPath);
            var viewValidator = CreateSqlValidator(viewCatalogReader, c => c.withEmbeddedQuery(true));
            var config = SqlToRelConverter.config().withTrimUnusedFields(true);
            var sqlToRelConverter = GetSqlToRelConverter(viewValidator, viewCatalogReader, config);

            return sqlToRelConverter.convertQuery(sqlNode, true, true);
        }

        /// <summary>
        /// Returns the parser configuration a view's definition is parsed with.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.createParser(String)</c>, which uses the default configuration rather than
        /// the connection's — a view's text was written against Calcite's own dialect.
        /// </remarks>
        static SqlParser.Config ParserConfig()
        {
            return SqlParser.config();
        }

    }

}
