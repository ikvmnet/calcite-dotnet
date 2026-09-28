using System;

using org.apache.calcite.avatica;
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
    /// The counterpart of Calcite's <c>Prepare</c>: conversion to relational algebra, <c>EXPLAIN</c>
    /// handling, flattening, decorrelation, field trimming, optimization and implementation, in Calcite's
    /// order. A subclass supplies the validator, converter and implementation.
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
        /// <param name="catalogReader">Resolves the names in the statement.</param>
        /// <param name="resultConvention">The convention the root of the plan must be in.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
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
        /// Gets the catalog reader that resolves the names in the statement.
        /// </summary>
        protected CalciteCatalogReader CatalogReader => catalogReader;

        /// <summary>
        /// Gets the convention the root of the plan must be in.
        /// </summary>
        protected Convention ResultConvention => resultConvention;

        /// <summary>
        /// Prepares this object for a statement whose runtime context is <paramref name="runtimeContextClass"/>.
        /// </summary>
        /// <param name="runtimeContextClass">The class of the runtime context.</param>
        protected abstract void Init(java.lang.Class runtimeContextClass);

        /// <summary>
        /// Gets the validator the statement is validated with.
        /// </summary>
        protected abstract SqlValidator SqlValidator { get; }

        /// <summary>
        /// Gets or sets the row type of the statement's dynamic parameters.
        /// </summary>
        /// <exception cref="InvalidOperationException">Read before the statement was validated.</exception>
        protected RelDataType ParameterRowType
        {
            get => parameterRowType ?? throw new InvalidOperationException("The statement has not been validated.");
            set => parameterRowType = value;
        }

        /// <summary>
        /// Gets or sets the origin of each result field.
        /// </summary>
        /// <exception cref="InvalidOperationException">Read before the statement was validated.</exception>
        protected java.util.List FieldOrigins
        {
            get => fieldOrigins ?? throw new InvalidOperationException("The statement has not been validated.");
            set => fieldOrigins = value;
        }

        /// <summary>
        /// Returns the program that takes a logical plan to <see cref="ResultConvention"/>.
        /// </summary>
        /// <returns>The program set by <c>Hook.PROGRAM</c> if there is one; otherwise
        /// <c>Programs.standard</c> followed by a hep pass of <c>ClrCursorRules.CalcRules()</c>.</returns>
        /// <remarks>
        /// The counterpart of <c>Prepare.getProgram</c>. <c>Programs.standard</c> runs unchanged, including its
        /// own calc pass of <c>RelOptRules.CALC_RULES</c>, which names <c>EnumerableConvention</c>'s nodes; the
        /// appended pass is the same list with the cursor convention's nodes in their place. The calc rules
        /// cannot be planner rules: <c>VolcanoCost.isLt</c> compares row counts only, so a calc is never
        /// cheaper than the project it came from, and <c>VolcanoPlanner.addRule</c> does not register a
        /// <c>TransformationRule</c>'s operand against a <c>PhysicalNode</c>.
        ///
        /// <para>Both passes are given
        /// <see cref="Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider"/> rather than
        /// <c>DefaultRelMetadataProvider.INSTANCE</c>. Each hep pass installs its provider as the thread's
        /// metadata provider, so the provider given to <c>standard</c> is also the one its Volcano pass costs
        /// with.</para>
        /// </remarks>
        protected virtual Program GetProgram()
        {
            // Hook.PROGRAM lets a caller replace the whole program
            var holder = Holder.empty();
            org.apache.calcite.runtime.Hook.PROGRAM.run(holder);
            if (holder.get() is Program holderValue)
                return holderValue;

            var calcRules = new java.util.ArrayList();
            foreach (var rule in Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.CalcRules())
                calcRules.add(rule);

            return Programs.sequence(
                Programs.standard(Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider),
                Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider));
        }

        /// <summary>
        /// Returns the traits the root of the plan must satisfy.
        /// </summary>
        /// <param name="root">The logical plan.</param>
        /// <returns>The root's traits with <see cref="ResultConvention"/> and the root's collation.</returns>
        protected virtual RelTraitSet GetDesiredRootTraitSet(RelRoot root)
        {
            return root.rel.getTraitSet()
                .replace(ResultConvention)
                .replace(root.collation)
                .simplify();
        }

        /// <summary>
        /// Compiles the chosen plan.
        /// </summary>
        /// <param name="root">The root of the chosen plan, in <see cref="ResultConvention"/>.</param>
        /// <returns>The prepared result.</returns>
        protected abstract IPreparedResult Implement(RelRoot root);

        /// <summary>
        /// Creates the result of an <c>EXPLAIN</c>, which renders a plan or a type.
        /// </summary>
        /// <param name="resultType">The type to render, or <see langword="null"/> to render a plan.</param>
        /// <param name="parameterRowType">The statement's dynamic parameters.</param>
        /// <param name="root">The plan to render, or <see langword="null"/> to render a type.</param>
        /// <param name="format">The output format.</param>
        /// <param name="detailLevel">How much detail to render.</param>
        /// <returns>The prepared explanation.</returns>
        protected abstract IPreparedResult CreatePreparedExplanation(
            RelDataType? resultType,
            RelDataType parameterRowType,
            RelRoot? root,
            SqlExplainFormat format,
            SqlExplainLevel detailLevel);

        /// <summary>
        /// Creates the converter from SQL to relational algebra.
        /// </summary>
        /// <param name="validator">The validator.</param>
        /// <param name="catalogReader">The catalog reader.</param>
        /// <param name="config">The converter configuration.</param>
        /// <returns>The converter.</returns>
        protected abstract SqlToRelConverter GetSqlToRelConverter(
            SqlValidator validator,
            org.apache.calcite.prepare.Prepare.CatalogReader catalogReader,
            SqlToRelConverter.Config config);

        /// <summary>
        /// Flattens structured types.
        /// </summary>
        /// <param name="rootRel">The plan.</param>
        /// <param name="restructure">Whether to restructure the result into its original structured type.</param>
        /// <returns>The flattened plan.</returns>
        public abstract RelNode FlattenTypes(RelNode rootRel, bool restructure);

        /// <summary>
        /// Removes correlation from a plan.
        /// </summary>
        /// <param name="sqlToRelConverter">The converter that produced the plan.</param>
        /// <param name="query">The statement.</param>
        /// <param name="rootRel">The plan.</param>
        /// <returns>The decorrelated plan.</returns>
        protected abstract RelNode Decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel);

        /// <summary>
        /// Returns the materializations the planner may substitute.
        /// </summary>
        /// <returns>A list of <c>Prepare.Materialization</c>.</returns>
        protected abstract java.util.List GetMaterializations();

        /// <summary>
        /// Returns the lattices the planner may use.
        /// </summary>
        /// <returns>A list of <c>CalciteSchema.LatticeEntry</c>.</returns>
        protected abstract java.util.List GetLattices();

        /// <summary>
        /// Prepares a parsed statement that was not rewritten before it arrived.
        /// </summary>
        /// <param name="sqlQuery">The statement. An <c>EXPLAIN</c> is prepared as an explanation of the
        /// statement it wraps.</param>
        /// <param name="runtimeContextClass">The class of the runtime context.</param>
        /// <param name="validator">The validator.</param>
        /// <param name="needsValidation">Whether the statement still has to be validated.</param>
        /// <returns>The compiled statement, or the explanation for an <c>EXPLAIN</c>.</returns>
        public IPreparedResult PrepareSql(SqlNode sqlQuery, java.lang.Class runtimeContextClass, SqlValidator validator, bool needsValidation)
        {
            return PrepareSql(sqlQuery, sqlQuery, runtimeContextClass, validator, needsValidation);
        }

        /// <summary>
        /// Prepares a parsed statement.
        /// </summary>
        /// <param name="sqlQuery">The statement, possibly rewritten. An <c>EXPLAIN</c> is prepared as an
        /// explanation of the statement it wraps.</param>
        /// <param name="sqlNodeOriginal">The statement as parsed. A non-DML result takes its kind from it.</param>
        /// <param name="runtimeContextClass">The class of the runtime context.</param>
        /// <param name="validator">The validator.</param>
        /// <param name="needsValidation">Whether the statement still has to be validated.</param>
        /// <returns>The compiled statement, or the explanation for an <c>EXPLAIN</c>.</returns>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        public IPreparedResult PrepareSql(SqlNode sqlQuery, SqlNode sqlNodeOriginal, java.lang.Class runtimeContextClass, SqlValidator validator, bool needsValidation)
        {
            ArgumentNullException.ThrowIfNull(sqlQuery);
            ArgumentNullException.ThrowIfNull(sqlNodeOriginal);
            ArgumentNullException.ThrowIfNull(runtimeContextClass);
            ArgumentNullException.ThrowIfNull(validator);

            Init(runtimeContextClass);

            var config = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(((java.lang.Boolean)org.apache.calcite.prepare.Prepare.THREAD_EXPAND.get()).booleanValue())
                .withInSubQueryThreshold(((java.lang.Integer)org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD.get()).intValue())
                .withExplain(sqlQuery.getKind() == SqlKind.EXPLAIN);

            var configHolder = Holder.of(config);
            org.apache.calcite.runtime.Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.run(configHolder);

            var sqlToRelConverter = GetSqlToRelConverter(validator, catalogReader, (SqlToRelConverter.Config)configHolder.get());

            SqlExplain? sqlExplain = null;
            if (sqlQuery.getKind() == SqlKind.EXPLAIN)
            {
                // dig out the underlying SQL statement
                sqlExplain = (SqlExplain)sqlQuery;
                sqlQuery = sqlExplain.getExplicandum();
                sqlToRelConverter.setDynamicParamCountInExplain(sqlExplain.getDynamicParamCount());
            }

            var root = sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true);

            // checked arithmetic on exact types where the conformance asks for it, and always for arithmetic
            // producing an INTERVAL
            var convertToChecked = context.config().conformance().checkedArithmetic();
            var checkedConv = new ConvertToChecked(root.rel.getCluster().getRexBuilder(), convertToChecked);
            root = root.withRel(checkedConv.visit(root.rel));
            org.apache.calcite.runtime.Hook.CONVERTED.run(root.rel);

            var resultType = validator.getValidatedNodeType(sqlQuery);
            FieldOrigins = validator.getFieldOrigins(sqlQuery);
            ParameterRowType = validator.getParameterRowType(sqlQuery);

            // EXPLAIN of the type, or of the logical plan before flattening and decorrelation
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

            // TopDownGeneralDecorrelator runs inside the program, after sub-queries are removed
            if (context.config().forceDecorrelate() && context.config().topDownGeneralDecorrelationEnabled() == false)
                root = root.withRel(Decorrelate(sqlToRelConverter, sqlQuery, root.rel));

            if (((SqlToRelConverter.Config)configHolder.get()).isTrimUnusedFields())
            {
                root = TrimUnusedFields(root);
                org.apache.calcite.runtime.Hook.TRIMMED.run(root.rel);
            }

            // EXPLAIN of the physical plan
            if (sqlExplain != null)
            {
                root = Optimize(root, GetMaterializations(), GetLattices());
                return CreatePreparedExplanation(null, ParameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
            }

            root = Optimize(root, GetMaterializations(), GetLattices());

            // DML rewritten to other DML (UPDATE to MERGE) keeps the rewritten kind; anything else (CALL to
            // SELECT) keeps the kind it was parsed as
            if (root.kind.belongsTo(SqlKind.DML) == false)
                root = root.withKind(sqlNodeOriginal.getKind());

            return Implement(root);
        }

        /// <summary>
        /// Runs the program from <see cref="GetProgram"/> to choose the physical plan.
        /// </summary>
        /// <param name="root">The logical plan.</param>
        /// <param name="materializations">The materializations; see the remarks.</param>
        /// <param name="lattices">The lattices, as <c>CalciteSchema.LatticeEntry</c>.</param>
        /// <returns>The chosen plan.</returns>
        /// <remarks>
        /// Sets a <c>RexExecutorImpl</c> on the planner first, as Calcite does, so that constant reduction
        /// works. No materializations are passed to the program: <c>Prepare.Materialization</c>'s fields are
        /// not accessible through IKVM, so they cannot be converted.
        /// </remarks>
        protected RelRoot Optimize(RelRoot root, java.util.List materializations, java.util.List lattices)
        {
            ArgumentNullException.ThrowIfNull(materializations);
            ArgumentNullException.ThrowIfNull(lattices);

            var planner = root.rel.getCluster().getPlanner();
            planner.setExecutor(new RexExecutorImpl(context.getDataContext()));

            // Calcite converts each Materialization to a RelOptMaterialization here; Materialization's fields
            // are package private or private and unreachable, so the list stays empty
            var materializationList = new java.util.ArrayList(materializations.size());

            var latticeList = new java.util.ArrayList(lattices.size());
            for (var i = lattices.iterator(); i.hasNext();)
            {
                var lattice = (CalciteSchema.LatticeEntry)i.next();
                var starTable = lattice.getStarTable();
                var starRelOptTable = org.apache.calcite.prepare.RelOptTableImpl.create(
                    catalogReader,
                    starTable.getTable().getRowType(context.getTypeFactory()),
                    starTable,
                    null);

                latticeList.add(new RelOptLattice(lattice.getLattice(), starRelOptTable));
            }

            var desiredTraits = GetDesiredRootTraitSet(root);

            var program = GetProgram();

            return root.withRel(program.run(planner, root.rel, desiredTraits, materializationList, latticeList));
        }

        /// <summary>
        /// Removes fields that nothing reads.
        /// </summary>
        /// <param name="root">The plan.</param>
        /// <returns>The trimmed plan.</returns>
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
        /// Returns whether to trim a plan: always when <c>Prepare.THREAD_TRIM</c> is set, and otherwise when it
        /// has fewer than two joins.
        /// </summary>
        static bool ShouldTrim(RelNode rootRel)
        {
            return ((java.lang.Boolean)org.apache.calcite.prepare.Prepare.THREAD_TRIM.get()).booleanValue()
                || RelOptUtil.countJoins(rootRel) < 2;
        }

        /// <summary>
        /// Returns which modification a DML statement performs.
        /// </summary>
        /// <param name="isDml">Whether the statement is DML.</param>
        /// <param name="sqlKind">The statement's kind.</param>
        /// <returns>The operation, or <see langword="null"/> if the statement is not DML or not a kind that
        /// maps to one.</returns>
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


        /// <summary>
        /// What preparing a statement produces.
        /// </summary>
        public interface IPreparedResult
        {

            /// <summary>
            /// Gets the code preparation generated, or for an explanation the rendered plan or type.
            /// </summary>
            string Code { get; }

            /// <summary>
            /// Gets whether the statement modifies data, in which case the result is one row of one column
            /// holding the number of rows affected.
            /// </summary>
            bool IsDml { get; }

            /// <summary>
            /// Gets which modification a DML statement performs, or <see langword="null"/> if it is not DML.
            /// </summary>
            TableModify.Operation? TableModOp { get; }

            /// <summary>
            /// Gets, per result field, the origin of the field as a four-element list of database, schema, table
            /// and column.
            /// </summary>
            java.util.List FieldOrigins { get; }

            /// <summary>
            /// Gets a record type whose fields are the statement's dynamic parameters.
            /// </summary>
            RelDataType ParameterRowType { get; }

            /// <summary>
            /// Returns the plan that produces the rows.
            /// </summary>
            /// <param name="cursorFactory">How a row is read back.</param>
            /// <returns>The compiled plan.</returns>
            Apache.Calcite.Extensions.Runtime.IClrCursorFactory GetBindable(Meta.CursorFactory cursorFactory);

        }

        /// <summary>
        /// Base class of a prepared result that came from a plan.
        /// </summary>
        public abstract class PreparedResultImpl : IPreparedResult
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="rowType">The result's row type.</param>
            /// <param name="parameterRowType">The row type of the dynamic parameters.</param>
            /// <param name="fieldOrigins">The origin of each result field.</param>
            /// <param name="collations">The collations the result is known to carry.</param>
            /// <param name="rootRel">The root of the plan.</param>
            /// <param name="tableModOp">The DML operation, or <see langword="null"/>.</param>
            /// <param name="isDml">Whether the statement is DML.</param>
            /// <exception cref="ArgumentNullException">An argument other than <paramref name="tableModOp"/> is
            /// <see langword="null"/>.</exception>
            protected PreparedResultImpl(
                RelDataType rowType,
                RelDataType parameterRowType,
                java.util.List fieldOrigins,
                java.util.List collations,
                RelNode rootRel,
                TableModify.Operation? tableModOp,
                bool isDml)
            {
                ArgumentNullException.ThrowIfNull(collations);

                RowType = rowType ?? throw new ArgumentNullException(nameof(rowType));
                ParameterRowType = parameterRowType ?? throw new ArgumentNullException(nameof(parameterRowType));
                FieldOrigins = fieldOrigins ?? throw new ArgumentNullException(nameof(fieldOrigins));
                Collations = com.google.common.collect.ImmutableList.copyOf(collations);
                RootRel = rootRel ?? throw new ArgumentNullException(nameof(rootRel));
                TableModOp = tableModOp;
                IsDml = isDml;
            }

            /// <summary>
            /// Gets the row type of the result.
            /// </summary>
            public RelDataType RowType { get; }

            /// <summary>
            /// Gets the physical row type of the result, which is <see cref="RowType"/>.
            /// </summary>
            public RelDataType PhysicalRowType => RowType;

            /// <inheritdoc />
            public RelDataType ParameterRowType { get; }

            /// <inheritdoc />
            public java.util.List FieldOrigins { get; }

            /// <summary>
            /// Gets the collations the result is known to carry.
            /// </summary>
            public java.util.List Collations { get; }

            /// <summary>
            /// Gets the root of the plan.
            /// </summary>
            public RelNode RootRel { get; }

            /// <inheritdoc />
            public TableModify.Operation? TableModOp { get; }

            /// <inheritdoc />
            public bool IsDml { get; }

            /// <inheritdoc />
            public abstract string Code { get; }

            /// <inheritdoc />
            public abstract Apache.Calcite.Extensions.Runtime.IClrCursorFactory GetBindable(Meta.CursorFactory cursorFactory);

            /// <summary>
            /// Gets the CLR type of one row, which decides how a row is read back.
            /// </summary>
            /// <remarks>
            /// The counterpart of <c>Typed.getElementType</c>, which Calcite's <c>PreparedResultImpl</c>
            /// implements.
            /// </remarks>
            public abstract System.Type ElementType { get; }

        }

        /// <summary>
        /// Base class of a prepared <c>EXPLAIN</c> statement.
        /// </summary>
        public abstract class PreparedExplain : IPreparedResult
        {

            readonly RelDataType? rowType;
            readonly RelRoot? root;
            readonly SqlExplainFormat format;
            readonly SqlExplainLevel detailLevel;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="rowType">The type to render, if the <c>EXPLAIN</c> is of a type.</param>
            /// <param name="parameterRowType">The statement's dynamic parameters.</param>
            /// <param name="root">The plan to render, if the <c>EXPLAIN</c> is of a plan.</param>
            /// <param name="format">How the plan is rendered.</param>
            /// <param name="detailLevel">How much of the plan is rendered.</param>
            /// <exception cref="ArgumentNullException"><paramref name="parameterRowType"/>,
            /// <paramref name="format"/> or <paramref name="detailLevel"/> is <see langword="null"/>.</exception>
            protected PreparedExplain(
                RelDataType? rowType,
                RelDataType parameterRowType,
                RelRoot? root,
                SqlExplainFormat format,
                SqlExplainLevel detailLevel)
            {
                this.rowType = rowType;
                this.root = root;
                this.format = format ?? throw new ArgumentNullException(nameof(format));
                this.detailLevel = detailLevel ?? throw new ArgumentNullException(nameof(detailLevel));

                ParameterRowType = parameterRowType ?? throw new ArgumentNullException(nameof(parameterRowType));
            }

            /// <inheritdoc />
            public string Code =>
                root == null
                    ? rowType == null ? "rowType is null" : RelOptUtil.dumpType(rowType)
                    : RelOptUtil.dumpPlan("", root.rel, format, detailLevel);

            /// <inheritdoc />
            public RelDataType ParameterRowType { get; }

            /// <inheritdoc />
            public java.util.List FieldOrigins =>
                java.util.Collections.singletonList(java.util.Collections.nCopies(4, null));

            /// <inheritdoc />
            public bool IsDml => false;

            /// <inheritdoc />
            public TableModify.Operation? TableModOp => null;

            /// <inheritdoc />
            public abstract Apache.Calcite.Extensions.Runtime.IClrCursorFactory GetBindable(Meta.CursorFactory cursorFactory);

        }

    }

}
