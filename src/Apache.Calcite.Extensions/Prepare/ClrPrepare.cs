using System;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.sql2rel;
using org.apache.calcite.tools;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Takes a statement from a parse tree to a compiled plan.
    /// </summary>
    /// <remarks>
    /// Our counterpart of <c>Prepare</c>, and split from <see cref="ClrPreparingStmt"/> where Calcite splits
    /// them. This half is the algorithm — convert, checked arithmetic, flatten, decorrelate, trim, optimize,
    /// implement, with the two <c>EXPLAIN</c> exits where Calcite has them — and knows nothing of a cluster,
    /// a schema or a convertlet table. Those belong to the class below it.
    ///
    /// <para>The split is worth keeping even though Calcite has exactly one subclass of <c>Prepare</c>. What
    /// it separates is real: this class cannot reach a <c>CalciteSchema</c> or a <c>RelOptCluster</c>, and it
    /// is where a preparing statement backed by something other than a <c>CalcitePrepare.Context</c> would
    /// part company. It also keeps the port diffable — when <c>Prepare.prepareSql</c> changes upstream, the
    /// change lands in one file here rather than having to be found inside a larger one.</para>
    ///
    /// <para>It is written rather than derived because <c>Prepare.implement</c> is declared to return a
    /// <c>PreparedResult</c>, an interface whose reason for existing is <c>getBindable</c> — a linq4j
    /// <c>Enumerable</c>. Our conventions compile to a delegate, so there is nothing to hand back through
    /// it. <see cref="ClrPrepareResult"/> is that interface without the member.</para>
    /// </remarks>
    public abstract class ClrPrepare
    {

        readonly CalcitePrepare.Context context;
        readonly CalciteCatalogReader catalogReader;
        readonly Convention resultConvention;

        RelDataType? parameterRowType;
        java.util.List? fieldOrigins;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="catalogReader">How a name in the statement is resolved to a table.</param>
        /// <param name="resultConvention">The convention a plan must end in.</param>
        protected ClrPrepare(CalcitePrepare.Context context, CalciteCatalogReader catalogReader, Convention resultConvention)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.catalogReader = catalogReader ?? throw new ArgumentNullException(nameof(catalogReader));
            this.resultConvention = resultConvention ?? throw new ArgumentNullException(nameof(resultConvention));
        }

        /// <summary>
        /// Gets the context the statement is prepared against.
        /// </summary>
        protected CalcitePrepare.Context Context => context;

        /// <summary>
        /// Gets how a name in the statement is resolved to a table.
        /// </summary>
        protected CalciteCatalogReader CatalogReader => catalogReader;

        /// <summary>
        /// Gets the convention a plan must end in.
        /// </summary>
        protected Convention ResultConvention => resultConvention;

        /// <summary>
        /// Gets the validator the statement is validated with.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.getSqlValidator</c>, abstract there as here: building one needs a type factory, which
        /// this half does not have.
        /// </remarks>
        protected internal abstract SqlValidator SqlValidator { get; }

        /// <summary>
        /// Gets or sets the row type of the statement's dynamic parameters.
        /// </summary>
        /// <remarks>
        /// Settable by the subclass because <c>prepare_</c> assigns it directly for a plan that was never
        /// validated, as Calcite's does.
        /// </remarks>
        protected RelDataType ParameterRowType
        {
            get => parameterRowType ?? throw new InvalidOperationException("The statement has not been validated.");
            set => parameterRowType = value;
        }

        /// <summary>
        /// Gets or sets, per field, the name it originates from.
        /// </summary>
        protected java.util.List FieldOrigins
        {
            get => fieldOrigins ?? throw new InvalidOperationException("The statement has not been validated.");
            set => fieldOrigins = value;
        }

        /// <summary>
        /// Returns the program that takes a logical plan to <see cref="ResultConvention"/>.
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
        protected abstract ClrPrepareResult Implement(RelRoot root);

        /// <summary>
        /// Renders a plan or a type, for an <c>EXPLAIN</c>.
        /// </summary>
        protected abstract ClrPrepareResult CreatePreparedExplanation(
            RelDataType? resultType,
            RelDataType parameterRowType,
            RelRoot? root,
            SqlExplainFormat format,
            SqlExplainLevel detailLevel);

        /// <summary>
        /// Builds the converter from SQL to relational algebra.
        /// </summary>
        protected abstract SqlToRelConverter GetSqlToRelConverter(
            SqlValidator validator,
            org.apache.calcite.prepare.Prepare.CatalogReader catalogReader,
            SqlToRelConverter.Config config);

        /// <summary>
        /// Flattens structured types.
        /// </summary>
        protected abstract RelNode FlattenTypes(RelNode rootRel, bool restructure);

        /// <summary>
        /// Removes correlation from a plan.
        /// </summary>
        protected abstract RelNode Decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel);

        /// <summary>
        /// Returns the materialized views the planner may substitute.
        /// </summary>
        protected abstract java.util.List GetMaterializations();

        /// <summary>
        /// Returns the lattices the planner may use.
        /// </summary>
        protected abstract java.util.List GetLattices();

        /// <summary>
        /// Prepares a parsed statement.
        /// </summary>
        /// <param name="sqlQuery">The statement, which an <c>EXPLAIN</c> is unwrapped from.</param>
        /// <param name="sqlNodeOriginal">The statement as parsed, before that unwrapping.</param>
        /// <param name="needsValidation">Whether the statement still has to be validated.</param>
        /// <returns>The compiled statement, or the rendered plan where it was an <c>EXPLAIN</c>.</returns>
        /// <remarks>
        /// <c>Prepare.prepareSql</c>, step for step.
        /// </remarks>
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

            var sqlToRelConverter = GetSqlToRelConverter(SqlValidator, catalogReader, (SqlToRelConverter.Config)configHolder.get());

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

            var resultType = SqlValidator.getValidatedNodeType(sqlQuery);
            FieldOrigins = SqlValidator.getFieldOrigins(sqlQuery);
            ParameterRowType = SqlValidator.getParameterRowType(sqlQuery);

            // the logical plan, before view expansion, physical storage and decorrelation
            if (sqlExplain != null)
            {
                switch (sqlExplain.getDepth().name())
                {
                    case nameof(SqlExplain.Depth.TYPE):
                        return CreatePreparedExplanation(resultType, ParameterRowType, null, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
                    case nameof(SqlExplain.Depth.LOGICAL):
                        return CreatePreparedExplanation(null, ParameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
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
                return CreatePreparedExplanation(null, ParameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
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
        /// Runs the program, which is what chooses a plan.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.optimize</c>. The planner is read off the root rather than held, which is why a plan
        /// handed in from elsewhere is optimized by whichever planner built it.
        /// </remarks>
        protected RelRoot Optimize(RelRoot root)
        {
            var planner = root.rel.getCluster().getPlanner();
            planner.setExecutor(new RexExecutorImpl(context.getDataContext()));

            var materializations = GetMaterializations();
            var lattices = GetLattices();
            var desiredTraits = GetDesiredRootTraitSet(root);

            return root.withRel(GetProgram().run(planner, root.rel, desiredTraits, materializations, lattices));
        }

        /// <summary>
        /// Trims the fields no one reads.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.trimUnusedFields</c>. It builds a second converter because the trim runs with
        /// <c>trimUnusedFields</c> decided per plan by <see cref="ShouldTrim"/> rather than by the config the
        /// query was converted with.
        /// </remarks>
        protected RelRoot TrimUnusedFields(RelRoot root)
        {
            var config = SqlToRelConverter.config()
                .withTrimUnusedFields(ShouldTrim(root.rel))
                .withExpand(((java.lang.Boolean)org.apache.calcite.prepare.Prepare.THREAD_EXPAND.get()).booleanValue())
                .withInSubQueryThreshold(((java.lang.Integer)org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD.get()).intValue());

            var converter = GetSqlToRelConverter(SqlValidator, catalogReader, config);
            var ordered = root.collation.getFieldCollations().isEmpty() == false;
            var dml = SqlKind.DML.contains(root.kind);

            return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
        }

        /// <summary>
        /// Returns whether a plan is worth trimming.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.shouldTrim</c>: trimming a bare projection would leave nothing behind.
        /// </remarks>
        protected static bool ShouldTrim(RelNode rootRel)
        {
            return rootRel is not org.apache.calcite.rel.logical.LogicalProject;
        }

        /// <summary>
        /// Returns which modification a DML statement performs.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.mapTableModOp</c>, which is protected there and belongs to this half rather than to a
        /// convention's.
        /// </remarks>
        protected static TableModify.Operation? MapTableModOp(bool isDml, SqlKind sqlKind)
        {
            if (isDml == false)
                return null;

            return sqlKind.name() switch
            {
                nameof(SqlKind.INSERT) => TableModify.Operation.INSERT,
                nameof(SqlKind.DELETE) => TableModify.Operation.DELETE,
                nameof(SqlKind.MERGE) => TableModify.Operation.MERGE,
                nameof(SqlKind.UPDATE) => TableModify.Operation.UPDATE,
                _ => null,
            };
        }

    }

}
