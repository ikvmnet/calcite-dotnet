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
        /// Prepares this object for a statement whose runtime context is <paramref name="runtimeContextClass"/>.
        /// </summary>
        protected abstract void Init(java.lang.Class runtimeContextClass);

        /// <summary>
        /// Gets the validator the statement is validated with.
        /// </summary>
        protected abstract SqlValidator SqlValidator { get; }

        /// <summary>
        /// Gets or sets the row type of the statement's dynamic parameters.
        /// </summary>
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
        protected virtual Program GetProgram()
        {
            // allow a test to override the default program
            var holder = Holder.empty();
            org.apache.calcite.runtime.Hook.PROGRAM.run(holder);
            if (holder.get() is Program holderValue)
                return holderValue;

            return GetStandardProgram();
        }

        /// <summary>
        /// Returns this convention's counterpart of <c>Programs.standard</c>.
        /// </summary>
        /// <remarks>
        /// <c>Prepare.getProgram</c> ends in the constant <c>Programs.standard()</c>, there being one calling
        /// convention to plan into. There are two here and their last two passes differ, so the constant is a
        /// method. Everything else about <c>getProgram</c> is unchanged — the hook is read here rather than at
        /// the call site, and <see cref="Optimize"/> calls nothing else.
        /// </remarks>
        protected abstract Program GetStandardProgram();

        /// <summary>
        /// Returns the traits the root of the plan must satisfy.
        /// </summary>
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
        /// <param name="root">The root of the plan, which is of <see cref="ResultConvention"/>.</param>
        protected abstract IPreparedResult Implement(RelRoot root);

        /// <summary>
        /// Renders a plan or a type, for an <c>EXPLAIN</c>.
        /// </summary>
        protected abstract IPreparedResult CreatePreparedExplanation(
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
        public abstract RelNode FlattenTypes(RelNode rootRel, bool restructure);

        /// <summary>
        /// Removes correlation from a plan.
        /// </summary>
        protected abstract RelNode Decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel);

        /// <summary>
        /// Returns the materialized views the planner may substitute.
        /// </summary>
        protected abstract java.util.List GetMaterializations();

        /// <summary>
        /// Returns the lattices the planner may use, as <c>CalciteSchema.LatticeEntry</c>.
        /// </summary>
        protected abstract java.util.List GetLattices();

        /// <summary>
        /// Prepares a parsed statement that was not rewritten before it arrived.
        /// </summary>
        /// <param name="sqlQuery">The statement, which an <c>EXPLAIN</c> is unwrapped from.</param>
        /// <param name="needsValidation">Whether the statement still has to be validated.</param>
        /// <returns>The compiled statement, or the rendered plan where it was an <c>EXPLAIN</c>.</returns>
        public IPreparedResult PrepareSql(SqlNode sqlQuery, java.lang.Class runtimeContextClass, SqlValidator validator, bool needsValidation)
        {
            return PrepareSql(sqlQuery, sqlQuery, runtimeContextClass, validator, needsValidation);
        }

        /// <summary>
        /// Prepares a parsed statement.
        /// </summary>
        /// <param name="sqlQuery">The statement, which an <c>EXPLAIN</c> is unwrapped from.</param>
        /// <param name="sqlNodeOriginal">The statement as parsed, before that unwrapping.</param>
        /// <param name="needsValidation">Whether the statement still has to be validated.</param>
        /// <returns>The compiled statement, or the rendered plan where it was an <c>EXPLAIN</c>.</returns>
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

            // all arithmetic on exact types where the conformance asks for it, and all arithmetic producing
            // an INTERVAL whatever the conformance says
            var convertToChecked = context.config().conformance().checkedArithmetic();
            var checkedConv = new ConvertToChecked(root.rel.getCluster().getRexBuilder(), convertToChecked);
            root = root.withRel(checkedConv.visit(root.rel));
            org.apache.calcite.runtime.Hook.CONVERTED.run(root.rel);

            var resultType = validator.getValidatedNodeType(sqlQuery);
            FieldOrigins = validator.getFieldOrigins(sqlQuery);
            ParameterRowType = validator.getParameterRowType(sqlQuery);

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
                root = Optimize(root, GetMaterializations(), GetLattices());
                return CreatePreparedExplanation(null, ParameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel());
            }

            root = Optimize(root, GetMaterializations(), GetLattices());

            // a DML rewritten to other DML — UPDATE to MERGE — keeps the rewrite's kind; anything else —
            // CALL to SELECT — keeps the kind it was parsed as
            if (root.kind.belongsTo(SqlKind.DML) == false)
                root = root.withKind(sqlNodeOriginal.getKind());

            return Implement(root);
        }

        /// <summary>
        /// Runs the program, which is what chooses a plan.
        /// </summary>
        protected RelRoot Optimize(RelRoot root, java.util.List materializations, java.util.List lattices)
        {
            ArgumentNullException.ThrowIfNull(materializations);
            ArgumentNullException.ThrowIfNull(lattices);

            var planner = root.rel.getCluster().getPlanner();
            planner.setExecutor(new RexExecutorImpl(context.getDataContext()));

            // Calcite converts each Materialization to a RelOptMaterialization here, and that loop cannot be
            // written: Prepare.Materialization's fields are package private and starRelOptTable is private,
            // so IKVM makes every one of them unreachable. Nothing is lost, because GetMaterializations
            // answers an empty list for a reason of its own — CalciteMaterializer cannot be reached either.
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
        /// Trims the fields no one reads.
        /// </summary>
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
        static bool ShouldTrim(RelNode rootRel)
        {
            return ((java.lang.Boolean)org.apache.calcite.prepare.Prepare.THREAD_TRIM.get()).booleanValue()
                || RelOptUtil.countJoins(rootRel) < 2;
        }

        /// <summary>
        /// Returns which modification a DML statement performs.
        /// </summary>
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
            /// Gets the code preparation generated.
            /// </summary>
            string Code { get; }

            /// <summary>
            /// Gets whether the statement modifies data, in which case the result is one row of one column
            /// holding the number of rows affected.
            /// </summary>
            bool IsDml { get; }

            /// <summary>
            /// Gets which modification a DML statement performs, or <see langword="null"/> where it is not one.
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
            Apache.Calcite.Extensions.Runtime.IClrBindableBase GetBindable(Meta.CursorFactory cursorFactory);

        }

        /// <summary>
        /// A prepared result that came from a plan: the storage every such result has.
        /// </summary>
        public abstract class PreparedResultImpl : IPreparedResult
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
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
            /// Gets the physical row type of the prepared statement, which the validator's need not be
            /// identical to — its field names may have been made unique.
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
            public abstract Apache.Calcite.Extensions.Runtime.IClrBindableBase GetBindable(Meta.CursorFactory cursorFactory);

            /// <summary>
            /// Gets the type of one row, which decides how a row is read back.
            /// </summary>
            /// <remarks>
            /// <c>Typed.getElementType</c>, which <c>PreparedResultImpl</c> declares abstract and the
            /// interface does not carry.
            /// </remarks>
            public abstract System.Type ElementType { get; }

        }

        /// <summary>
        /// An <c>EXPLAIN PLAN</c> statement, prepared. It is always good to have an explanation prepared.
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
            /// <param name="rowType">The type to render, where the <c>EXPLAIN</c> is of a type.</param>
            /// <param name="parameterRowType">The statement's dynamic parameters.</param>
            /// <param name="root">The plan to render, where the <c>EXPLAIN</c> is of a plan.</param>
            /// <param name="format">How the plan is rendered.</param>
            /// <param name="detailLevel">How much of the plan is rendered.</param>
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
            public abstract Apache.Calcite.Extensions.Runtime.IClrBindableBase GetBindable(Meta.CursorFactory cursorFactory);

        }

    }

}
