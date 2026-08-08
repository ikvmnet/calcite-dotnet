using System;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Prepare.AsyncEnumerable;
using Apache.Calcite.Extensions.Runtime;
using Apache.Calcite.Extensions.Prepare.Enumerable;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql2rel;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Parses, plans and compiles a statement into the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>CalcitePrepareImpl</c>'s driver — <c>prepare_</c> and <c>prepare2_</c> — and
    /// nothing more. <c>Prepare</c> and <c>CalcitePrepareImpl.CalcitePreparingStmt</c> are reused as they
    /// are, so validation, sql-to-rel, field trimming, view expansion, materialization registration and
    /// <c>optimize</c> are Calcite's, untouched.
    ///
    /// <para>What is replaced is the outer driver, because its one exit is a <c>Bindable</c>: it calls
    /// <c>preparedResult.getBindable(cursorFactory)</c> and puts the result in a <c>CalciteSignature</c>,
    /// whose only row-producing member binds it to a linq4j <c>Enumerable</c>. A plan of this convention is
    /// a compiled delegate, so it leaves through <see cref="ClrSignature"/> instead.</para>
    ///
    /// <para>Nothing of Calcite's prepare is inherited. <c>Prepare.implement</c> is declared to return a
    /// <c>PreparedResult</c>, whose reason for existing is <c>getBindable</c> — a linq4j <c>Enumerable</c> —
    /// and these conventions compile to a delegate. So the pipeline is <see cref="ClrPreparingStmt"/> and
    /// the factory methods below are ours, which is also what lets a second convention reuse them:
    /// everything here is the same for both, and only the preparing statement differs.</para>
    ///
    /// <para>The type-and-column metadata below is ported rather than reused: every piece of it is a
    /// private static of <c>CalcitePrepareImpl</c>.</para>
    /// </remarks>
    sealed class ClrPrepareImpl
    {

        /// <summary>
        /// Plans and compiles one statement.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="sql">The statement's text.</param>
        /// <param name="elementType">What a caller wants a row to be. <c>Object[]</c> asks for an array.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <returns>The planned statement.</returns>
        /// <remarks>
        /// <c>CalcitePrepareImpl.prepare_</c> loops over <c>createPlannerFactories</c>, trying each planner
        /// until one does not throw <c>CannotPlanException</c>. There is one factory here — Calcite's own
        /// list holds exactly one too — so the loop is a single call, and a plan that cannot be found
        /// throws rather than falling back.
        /// </remarks>
        public ClrSignature Prepare(CalcitePrepare.Context context, string sql, java.lang.reflect.Type elementType, long maxRowCount)
        {
            return Prepare(context, sql, elementType, maxRowCount, false);
        }

        /// <summary>
        /// Plans and compiles one statement into one of the two conventions.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="sql">The statement's text.</param>
        /// <param name="elementType">What a caller wants a row to be. <c>Object[]</c> asks for an array.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <param name="async">Whether to prepare into the asynchronous convention.</param>
        /// <returns>The planned statement.</returns>
        /// <remarks>
        /// The whole of what the two conventions differ by here is which rules go on the planner and which
        /// preparing statement is built, which is what <c>ClrPreparingStmt</c> already scopes: the
        /// convention, the program, the root traits and the compiler are a convention's own and nothing else
        /// is. Parsing, DDL, the statement kind, the parameter and column metadata and the cursor factory
        /// are shared, and this is the same method for both rather than a second copy of it.
        ///
        /// <para><b>An asynchronous plan has no fallback.</b> That convention has no converter to any other,
        /// so a query touching a table that cannot produce rows asynchronously does not plan and the planner
        /// throws. A caller that wants the synchronous plan in that case asks for it, rather than being
        /// handed one that would block a thread per row while looking asynchronous.</para>
        /// </remarks>
        public ClrSignature Prepare(CalcitePrepare.Context context, string sql, java.lang.reflect.Type elementType, long maxRowCount, bool async)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(sql);

            var typeFactory = context.getTypeFactory();
            var catalogReader = new CalciteCatalogReader(
                context.getRootSchema(),
                context.getDefaultSchemaPath(),
                typeFactory,
                context.config());

            var planner = CreatePlanner(context, async);
            var preparingStmt = GetPreparingStmt(context, elementType, catalogReader, planner, async);

            return Prepare2(context, sql, elementType, maxRowCount, catalogReader, preparingStmt);
        }

        /// <summary>
        /// Plans and compiles a relational expression that was built rather than parsed.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="rel">The plan to run.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <returns>The planned statement.</returns>
        /// <remarks>
        /// <c>CalcitePrepareImpl.prepare2_</c>'s <c>query.rel</c> branch, which Calcite reaches through
        /// <c>RelRunner</c>. The result is described from the plan's own row type, and the statement is
        /// always a SELECT — a plan carries no statement kind.
        /// </remarks>
        public ClrSignature PrepareRel(CalcitePrepare.Context context, org.apache.calcite.rel.RelNode rel, long maxRowCount)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(rel);

            var typeFactory = context.getTypeFactory();
            var catalogReader = new CalciteCatalogReader(
                context.getRootSchema(),
                context.getDefaultSchemaPath(),
                typeFactory,
                context.config());

            // the plan arrives in its caller's cluster, and Optimize runs on that cluster's planner —
            // Prepare.optimize reads the planner off the root rather than being handed one. So the rules go
            // on that planner rather than on one of our own, and the preparing statement shares the cluster,
            // or field trimming would build nodes the planner has never seen. PlannerRulesProgram installs
            // nothing by design, so if they are not here they are nowhere.
            var cluster = rel.getCluster();
            var planner = cluster.getPlanner();

            RelOptUtil.registerDefaultRules(planner, context.config().materializationsEnabled(), false);

            foreach (var rule in ClrEnumerableRules.Rules())
                planner.addRule(rule);

            var preparingStmt = new ClrEnumerablePreparingStmt(
                context,
                catalogReader,
                context.getRootSchema(),
                cluster,
                CreateConvertletTable(),
                ClrEnumerablePrefer.Array);

            var x = rel.getRowType();
            var preparedResult = preparingStmt.PrepareRel(rel, x);

            return Describe(context, org.apache.calcite.plan.RelOptUtil.toString(rel), x, maxRowCount,
                preparingStmt, preparedResult, Meta.StatementType.SELECT);
        }

        /// <summary>
        /// Creates the planner, with Calcite's default rules and this convention's.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite's planner with this convention's rules added. Calcite's own stay on it, so a statement
        /// this convention has no node for is still planned and run — implemented in
        /// <c>EnumerableConvention</c>, with a converter carrying its rows. That is how a table
        /// modification works here.
        /// </remarks>
        RelOptPlanner CreatePlanner(CalcitePrepare.Context context, bool async = false)
        {
            var planner = new org.apache.calcite.plan.volcano.VolcanoPlanner(null, Contexts.of(context.config()));
            planner.setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

            if (((java.lang.Boolean)CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()).booleanValue())
                planner.addRelTraitDef(org.apache.calcite.rel.RelCollationTraitDef.INSTANCE);

            planner.setTopDownOpt(context.config().topDownOpt());

            // enableBindable is false, as it is on a connection that has not asked for the interpreter.
            // Calcite reads that flag only to choose BindableConvention as its own result convention, which
            // is not a choice available here
            RelOptUtil.registerDefaultRules(planner, context.config().materializationsEnabled(), false);

            if (async)
                foreach (var rule in Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableRules.Rules())
                    planner.addRule(rule);
            else
                foreach (var rule in ClrEnumerableRules.Rules())
                    planner.addRule(rule);

            return planner;
        }

        /// <summary>
        /// Factory method for default convertlet table.
        /// </summary>
        SqlRexConvertletTable CreateConvertletTable()
        {
            return StandardConvertletTable.INSTANCE;
        }

        /// <summary>
        /// Factory method for cluster.
        /// </summary>
        RelOptCluster CreateCluster(RelOptPlanner planner, RexBuilder rexBuilder)
        {
            return RelOptCluster.create(planner, rexBuilder);
        }

        /// <summary>
        /// Factory method for SQL parser configuration.
        /// </summary>
        SqlParser.Config ParserConfig()
        {
            return SqlParser.config();
        }

        /// <summary>
        /// Executes a DDL statement.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.executeDdl</c>: the parser factory off the configuration, then its
        /// executor. It reads nothing else of the prepare.
        /// </remarks>
        void ExecuteDdl(CalcitePrepare.Context context, SqlNode node)
        {
            var config = context.config();
            var parserFactory = (SqlParserImplFactory)config.parserFactory((java.lang.Class)typeof(SqlParserImplFactory), org.apache.calcite.sql.parser.impl.SqlParserImpl.FACTORY);
            parserFactory.getDdlExecutor().executeDdl(context, node);
        }

        /// <summary>
        /// Creates the preparing statement.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="elementType"></param>
        /// <param name="catalogReader"></param>
        /// <param name="planner"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>CalcitePrepareImpl.getPreparingStmt</c>, deciding the row format the same way — an
        /// <c>Object[]</c> element type asks for an array and anything else asks for a custom row.
        /// </remarks>
        ClrPreparingStmt GetPreparingStmt(CalcitePrepare.Context context, java.lang.reflect.Type elementType, CalciteCatalogReader catalogReader, RelOptPlanner planner, bool async = false)
        {
            var typeFactory = context.getTypeFactory();
            var prefer = elementType == (java.lang.reflect.Type)(java.lang.Class)typeof(object[])
                ? ClrEnumerablePrefer.Array
                : ClrEnumerablePrefer.Custom;

            var cluster = CreateCluster(planner, new RexBuilder(typeFactory));

            if (async)
                return new ClrAsyncEnumerablePreparingStmt(
                    context,
                    catalogReader,
                    context.getRootSchema(),
                    cluster,
                    CreateConvertletTable(),
                    prefer);

            return new ClrEnumerablePreparingStmt(
                context,
                catalogReader,
                context.getRootSchema(),
                cluster,
                CreateConvertletTable(),
                prefer);
        }

        /// <summary>
        /// Parses, plans, compiles and describes one statement.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.prepare2_</c>, in its order, less the <c>queryable</c> and <c>rel</c>
        /// branches — this pipeline is reached with SQL text and nothing else.
        /// </remarks>
        ClrSignature Prepare2(CalcitePrepare.Context context, string sql, java.lang.reflect.Type elementType, long maxRowCount, CalciteCatalogReader catalogReader, ClrPreparingStmt preparingStmt)
        {
            var typeFactory = context.getTypeFactory();
            var config = context.config();

            var parseConfig = ParserConfig()
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuoting(config.quoting())
                .withConformance((org.apache.calcite.sql.validate.SqlConformance)config.conformance())
                .withCaseSensitive(config.caseSensitive());

            var parserFactory = (SqlParserImplFactory)config.parserFactory((java.lang.Class)typeof(SqlParserImplFactory), null);
            if (parserFactory != null)
                parseConfig = parseConfig.withParserFactory(parserFactory);

            SqlNode sqlNode;
            try
            {
                sqlNode = SqlParser.create(sql, parseConfig).parseStmt();
            }
            catch (SqlParseException e)
            {
                throw new java.lang.RuntimeException("parse failed: " + e.getMessage(), e);
            }

            var statementType = GetStatementType(sqlNode.getKind());

            org.apache.calcite.runtime.Hook.PARSE_TREE.run(new object[] { sql, sqlNode });

            // a DDL statement has already taken effect once this returns: it is executed here rather than
            // planned, exactly as Calcite does, and there is nothing left to bind
            if (sqlNode.getKind().belongsTo(SqlKind.DDL))
            {
                ExecuteDdl(context, sqlNode);

                return new ClrSignature(
                    sql,
                    com.google.common.collect.ImmutableList.of(),
                    new java.util.LinkedHashMap(),
                    null,
                    com.google.common.collect.ImmutableList.of(),
                    Meta.CursorFactory.OBJECT,
                    null,
                    com.google.common.collect.ImmutableList.of(),
                    -1,
                    null,
                    Meta.StatementType.OTHER_DDL);
            }

            var preparedResult = preparingStmt.PrepareSql(sqlNode, sqlNode, true);

            RelDataType x;
            switch (sqlNode.getKind().name())
            {
                case nameof(SqlKind.INSERT):
                case nameof(SqlKind.DELETE):
                case nameof(SqlKind.UPDATE):
                case nameof(SqlKind.MERGE):
                case nameof(SqlKind.EXPLAIN):
                    // getValidatedNodeType is wrong for DML, which is Calcite's own note
                    x = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
                    break;
                default:
                    x = preparingStmt.SqlValidator.getValidatedNodeType(sqlNode);
                    break;
            }

            return Describe(context, sql, x, maxRowCount, preparingStmt, preparedResult, statementType);
        }

        /// <summary>
        /// Describes a prepared statement for a caller: its parameters, its columns, how a row is read back,
        /// and the plan that produces the rows.
        /// </summary>
        /// <remarks>
        /// The tail of <c>prepare2_</c>, shared by both entry points because a statement is described the
        /// same way however its plan was arrived at.
        /// </remarks>
        ClrSignature Describe(
            CalcitePrepare.Context context,
            string sql,
            RelDataType x,
            long maxRowCount,
            ClrPreparingStmt preparingStmt,
            ClrPrepareResult preparedResult,
            Meta.StatementType statementType)
        {
            var typeFactory = context.getTypeFactory();

            var parameters = new java.util.ArrayList();
            for (var i = preparedResult.ParameterRowType.getFieldList().iterator(); i.hasNext();)
            {
                var field = (RelDataTypeField)i.next();
                var type = field.getType();
                parameters.add(
                    new AvaticaParameter(
                        false,
                        GetPrecision(type),
                        GetScale(type),
                        GetTypeOrdinal(type),
                        GetTypeName(type),
                        GetClassName(type),
                        field.getName()));
            }

            var jdbcType = MakeStruct(typeFactory, x);
            var columns = GetColumnMetaDataList((JavaTypeFactory)typeFactory, x, jdbcType, preparedResult.FieldOrigins);
            var cursorFactory = Meta.CursorFactory.deduce(columns, preparedResult.ElementType as java.lang.Class);

            // an EXPLAIN never reaches Implement, so there is no plan behind it — the rendered text is the
            // result, and the cursor factory decides whether the one row is an array or the text itself
            IClrBindableBase? bindable = preparedResult switch
            {
                ClrEnumerablePrepareResult clr => clr.Bindable,
                ClrAsyncEnumerablePrepareResult clr => clr.Bindable,
                ClrExplainResult explain => new ClrExplainBindable(explain.Explanation, cursorFactory),
                _ => throw new java.lang.IllegalStateException($"{preparedResult.GetType()} has no plan."),
            };

            return new ClrSignature(
                sql,
                parameters,
                preparingStmt.InternalParameters,
                x,
                columns,
                cursorFactory,
                context.getRootSchema(),
                preparedResult.Collations,
                maxRowCount,
                bindable,
                statementType);
        }

        /// <summary>
        /// Deduces the broad type of statement from its kind.
        /// </summary>
        /// <param name="kind"></param>
        /// <returns></returns>
        static Meta.StatementType GetStatementType(SqlKind kind) => kind.name() switch
        {
            nameof(SqlKind.INSERT) => Meta.StatementType.IS_DML,
            nameof(SqlKind.DELETE) => Meta.StatementType.IS_DML,
            nameof(SqlKind.UPDATE) => Meta.StatementType.IS_DML,
            nameof(SqlKind.MERGE) => Meta.StatementType.IS_DML,
            _ => Meta.StatementType.SELECT,
        };

        /// <summary>
        /// Builds one <see cref="ColumnMetaData"/> per field.
        /// </summary>
        static java.util.List GetColumnMetaDataList(JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType, java.util.List originList)
        {
            var columns = new java.util.ArrayList();
            var fields = jdbcType.getFieldList();

            for (int i = 0; i < fields.size(); i++)
            {
                var field = (RelDataTypeField)fields.get(i);
                var type = field.getType();
                var fieldType = x.isStruct() ? ((RelDataTypeField)x.getFieldList().get(i)).getType() : type;
                columns.add(MetaData(typeFactory, columns.size(), field.getName(), type, fieldType, (java.util.List)originList.get(i)));
            }

            return columns;
        }

        /// <summary>
        /// Builds one <see cref="ColumnMetaData"/>.
        /// </summary>
        static ColumnMetaData MetaData(JavaTypeFactory typeFactory, int ordinal, string fieldName, RelDataType type, RelDataType? fieldType, java.util.List? origins)
        {
            var avaticaType = AvaticaTypeOf(typeFactory, type, fieldType);

            return new ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                type.isNullable() ? java.sql.DatabaseMetaData.columnNullable : java.sql.DatabaseMetaData.columnNoNulls,
                SqlTypeName.UNSIGNED_TYPES.contains(type.getSqlTypeName()) == false,
                type.getPrecision(),
                fieldName,
                Origin(origins, 0),
                Origin(origins, 2),
                GetPrecision(type),
                GetScale(type),
                Origin(origins, 1),
                null,
                avaticaType,
                true,
                false,
                false,
                avaticaType.columnClassName());
        }

        /// <summary>
        /// Returns the Avatica type of a field, descending into a component or a struct.
        /// </summary>
        static ColumnMetaData.AvaticaType AvaticaTypeOf(JavaTypeFactory typeFactory, RelDataType type, RelDataType? fieldType)
        {
            string typeName;
            if (type is org.apache.calcite.sql.type.MeasureSqlType)
            {
                type = type.getMeasureElementType() ?? throw new java.lang.IllegalStateException("measure type");
                typeName = "MEASURE<" + GetTypeName(type) + ">";
            }
            else
            {
                typeName = GetTypeName(type);
            }

            if (type.getComponentType() != null)
            {
                var componentType = AvaticaTypeOf(typeFactory, type.getComponentType(), null);
                var clazz = typeFactory.getJavaClass(type.getComponentType());
                var rep = ColumnMetaData.Rep.of(clazz) ?? throw new java.lang.IllegalStateException($"no Rep for {clazz}");

                return ColumnMetaData.array(componentType, typeName, rep);
            }

            var typeOrdinal = GetTypeOrdinal(type);
            if (typeOrdinal == java.sql.Types.STRUCT)
            {
                var columns = new java.util.ArrayList(type.getFieldList().size());
                for (var i = type.getFieldList().iterator(); i.hasNext();)
                {
                    var field = (RelDataTypeField)i.next();
                    columns.add(MetaData(typeFactory, field.getIndex(), field.getName(), field.getType(), null, null));
                }

                return ColumnMetaData.@struct(columns);
            }

            // GEOMETRY is reported as a string, which is Calcite's own fall-through
            if (typeOrdinal == ExtraSqlTypes.GEOMETRY)
                typeOrdinal = java.sql.Types.VARCHAR;

            var scalarClazz = typeFactory.getJavaClass(fieldType ?? type);
            var scalarRep = ColumnMetaData.Rep.of(scalarClazz) ?? throw new java.lang.IllegalStateException($"no Rep for {scalarClazz}");

            return ColumnMetaData.scalar(typeOrdinal, typeName, scalarRep);
        }

        /// <summary>
        /// Reads one of a field's origins, counting from the end.
        /// </summary>
        static string? Origin(java.util.List? origins, int offsetFromEnd)
        {
            return origins == null || offsetFromEnd >= origins.size()
                ? null
                : (string)origins.get(origins.size() - 1 - offsetFromEnd);
        }

        /// <summary>
        /// Returns the JDBC type ordinal of a type.
        /// </summary>
        static int GetTypeOrdinal(RelDataType type)
        {
            if (type.getSqlTypeName().name() == nameof(SqlTypeName.MEASURE))
            {
                var measureElementType = type.getMeasureElementType() ?? throw new java.lang.IllegalStateException("measureElementType");
                return measureElementType.getSqlTypeName().getJdbcOrdinal();
            }

            return type.getSqlTypeName().getJdbcOrdinal();
        }

        /// <summary>
        /// Returns the class a column is reported as. CALCITE-2613: always <c>Object</c>.
        /// </summary>
        static string GetClassName(RelDataType type)
        {
            return ((java.lang.Class)typeof(java.lang.Object)).getName();
        }

        /// <summary>
        /// Returns a type's scale, or zero where it has none.
        /// </summary>
        static int GetScale(RelDataType type)
        {
            return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED ? 0 : type.getScale();
        }

        /// <summary>
        /// Returns a type's precision, or zero where it has none.
        /// </summary>
        static int GetPrecision(RelDataType type)
        {
            return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ? 0 : type.getPrecision();
        }

        /// <summary>
        /// Returns the type name in string form, without precision, scale or nullability.
        /// </summary>
        /// <remarks>
        /// "DECIMAL" not "DECIMAL(7, 2)"; "INTEGER" not "JavaType(int)".
        /// </remarks>
        static string GetTypeName(RelDataType type)
        {
            var sqlTypeName = type.getSqlTypeName();

            return sqlTypeName.name() switch
            {
                nameof(SqlTypeName.ARRAY) => type.toString(),
                nameof(SqlTypeName.MULTISET) => type.toString(),
                nameof(SqlTypeName.MAP) => type.toString(),
                nameof(SqlTypeName.ROW) => type.toString(),
                nameof(SqlTypeName.MEASURE) => type.toString(),
                nameof(SqlTypeName.INTERVAL_YEAR_MONTH) => "INTERVAL_YEAR_TO_MONTH",
                nameof(SqlTypeName.INTERVAL_DAY_HOUR) => "INTERVAL_DAY_TO_HOUR",
                nameof(SqlTypeName.INTERVAL_DAY_MINUTE) => "INTERVAL_DAY_TO_MINUTE",
                nameof(SqlTypeName.INTERVAL_DAY_SECOND) => "INTERVAL_DAY_TO_SECOND",
                nameof(SqlTypeName.INTERVAL_HOUR_MINUTE) => "INTERVAL_HOUR_TO_MINUTE",
                nameof(SqlTypeName.INTERVAL_HOUR_SECOND) => "INTERVAL_HOUR_TO_SECOND",
                nameof(SqlTypeName.INTERVAL_MINUTE_SECOND) => "INTERVAL_MINUTE_TO_SECOND",
                _ => sqlTypeName.getName(),
            };
        }

        /// <summary>
        /// Wraps a type in a one-field struct where it is not one already.
        /// </summary>
        static RelDataType MakeStruct(RelDataTypeFactory typeFactory, RelDataType type)
        {
            return type.isStruct() ? type : typeFactory.builder().add("$0", type).build();
        }

    }

}
