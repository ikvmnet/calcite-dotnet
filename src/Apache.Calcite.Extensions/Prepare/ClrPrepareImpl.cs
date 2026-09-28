using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Prepare.Cursor;
using Apache.Calcite.Extensions.Rel.Metadata;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.validate;
using org.apache.calcite.sql2rel;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Parses, plans and compiles a statement into the
    /// <see cref="Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>CalcitePrepareImpl</c>. The planner carries Calcite's rules as well as
    /// the cursor convention's, so a node the cursor convention cannot implement is planned in
    /// <c>EnumerableConvention</c> and connected by a converter. <c>Apache.Calcite.Data</c> prepares every
    /// statement through this class.
    /// </remarks>
    public class ClrPrepareImpl : IClrPrepare
    {

        /// <summary>
        /// The statements <see cref="SimplePrepare"/> answers without planning.
        /// </summary>
        static readonly HashSet<string> SIMPLE_SQLS =
        [
            "SELECT 1",
            "select 1",
            "SELECT 1 FROM DUAL",
            "select 1 from dual",
            "values 1",
            "VALUES 1",
        ];

        /// <summary>
        /// Plans and compiles one query.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="query">The statement's text, or a relational expression built rather than parsed.</param>
        /// <param name="elementType">The type the caller wants a row to be; <c>object[]</c> asks for an
        /// array.</param>
        /// <param name="maxRowCount">The maximum number of rows to return, or a negative number for no
        /// limit.</param>
        /// <returns>The planned statement.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="context"/> or <paramref name="query"/> is
        /// <see langword="null"/>.</exception>
        /// <remarks>
        /// A DDL statement is executed here and returns a signature with no plan. The six forms of
        /// <c>SELECT 1</c> and <c>VALUES 1</c> that Calcite short-circuits are answered without planning.
        /// The returned plan can be opened and read either synchronously or asynchronously.
        /// </remarks>
        public IClrPrepare.Signature PrepareSql(CalcitePrepare.Context context, IClrPrepare.Query query, System.Type elementType, long maxRowCount)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(query);

            return Prepare_(context, query, elementType, maxRowCount);
        }

        /// <summary>
        /// Tries each planner factory in turn, and rethrows the last <c>CannotPlanException</c> if none can
        /// plan the statement.
        /// </summary>
        IClrPrepare.Signature Prepare_(CalcitePrepare.Context context, IClrPrepare.Query query, System.Type elementType, long maxRowCount)
        {
            if (query.Sql is { } simpleSql && SIMPLE_SQLS.Contains(simpleSql))
                return SimplePrepare(context, simpleSql);

            var typeFactory = context.getTypeFactory();
            var catalogReader = new CalciteCatalogReader(
                context.getRootSchema(),
                context.getDefaultSchemaPath(),
                typeFactory,
                context.config());

            var plannerFactories = CreatePlannerFactories();
            if (plannerFactories.Count == 0)
                throw new InvalidOperationException("no planner factories");

            Exception? exception = null;

            foreach (var plannerFactory in plannerFactories)
            {
                var planner = plannerFactory(context) ?? throw new InvalidOperationException("factory returned null planner");

                try
                {
                    var preparingStmt = GetPreparingStmt(context, elementType, catalogReader, planner);
                    return Prepare2_(context, query, elementType, maxRowCount, catalogReader, preparingStmt);
                }
                catch (RelOptPlanner.CannotPlanException e)
                {
                    exception = e;
                }
            }

            throw exception!;
        }

        /// <summary>
        /// Creates a planner with Calcite's default rules and the cursor convention's.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <returns>The planner.</returns>
        protected virtual RelOptPlanner CreatePlanner(CalcitePrepare.Context context)
        {
            return CreatePlanner(context, null, null);
        }

        /// <summary>
        /// Creates a query planner over a given planner context and cost model, and initializes it with a
        /// default set of rules.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="externalContext">The planner's context, or <see langword="null"/> for one over the
        /// connection configuration.</param>
        /// <param name="costFactory">The cost model, or <see langword="null"/> for the planner's own.</param>
        /// <returns>The planner.</returns>
        /// <remarks>
        /// Registers rules through <see cref="Apache.Calcite.Extensions.Plan.ClrRelOptUtil.RegisterDefaultRules"/>,
        /// then runs <c>Hook.PLANNER</c> so that a caller can add or remove rules.
        /// </remarks>
        protected virtual RelOptPlanner CreatePlanner(
            CalcitePrepare.Context context,
            org.apache.calcite.plan.Context? externalContext,
            RelOptCostFactory? costFactory)
        {
            externalContext ??= Contexts.of(context.config());

            var planner = new org.apache.calcite.plan.volcano.VolcanoPlanner(costFactory, externalContext);
            planner.setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

            if (((java.lang.Boolean)CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()).booleanValue())
                planner.addRelTraitDef(org.apache.calcite.rel.RelCollationTraitDef.INSTANCE);

            planner.setTopDownOpt(context.config().topDownOpt());

            Apache.Calcite.Extensions.Plan.ClrRelOptUtil.RegisterDefaultRules(
                planner,
                context.config().materializationsEnabled());

            // lets a caller add or remove rules, as in Calcite
            org.apache.calcite.runtime.Hook.PLANNER.run(planner);

            return planner;
        }

        /// <summary>
        /// Creates the planner factories to try, in order.
        /// </summary>
        /// <returns>The factories. By default, one that calls <see cref="CreatePlanner(CalcitePrepare.Context)"/>.</returns>
        protected virtual IReadOnlyList<Func<CalcitePrepare.Context, RelOptPlanner>> CreatePlannerFactories()
        {
            return [context => CreatePlanner(context)];
        }

        /// <summary>
        /// Creates the convertlet table used to convert SQL to relational expressions.
        /// </summary>
        /// <returns><c>StandardConvertletTable.INSTANCE</c> by default.</returns>
        protected virtual SqlRexConvertletTable CreateConvertletTable()
        {
            return StandardConvertletTable.INSTANCE;
        }

        /// <summary>
        /// Creates the cluster a plan is built in.
        /// </summary>
        /// <param name="planner">The planner.</param>
        /// <param name="rexBuilder">The expression builder.</param>
        /// <returns>A cluster whose metadata queries use
        /// <see cref="ClrCursorRelMetadata.Provider"/>.</returns>
        protected virtual RelOptCluster CreateCluster(RelOptPlanner planner, RexBuilder rexBuilder)
        {
            var cluster = RelOptCluster.create(planner, rexBuilder);
            cluster.setMetadataQuerySupplier(ClrRelMetadataProvider.QuerySupplier(ClrCursorRelMetadata.Provider));
            cluster.invalidateMetadataQuery();
            return cluster;
        }

        /// <summary>
        /// Creates a SQL parser with the configuration from <see cref="ParserConfig"/>.
        /// </summary>
        /// <param name="sql">The text to parse.</param>
        /// <returns>The parser.</returns>
        protected virtual SqlParser CreateParser(string sql)
        {
            return CreateParser(sql, ParserConfig());
        }

        /// <summary>
        /// Creates a SQL parser with the given configuration.
        /// </summary>
        /// <param name="sql">The text to parse.</param>
        /// <param name="parserConfig">The parser configuration.</param>
        /// <returns>The parser.</returns>
        protected virtual SqlParser CreateParser(string sql, SqlParser.Config parserConfig)
        {
            return SqlParser.create(sql, parserConfig);
        }

        /// <summary>
        /// Returns the base parser configuration, to which the connection's casing, quoting, conformance and
        /// case sensitivity are applied.
        /// </summary>
        /// <returns><c>SqlParser.config()</c> by default.</returns>
        protected virtual SqlParser.Config ParserConfig()
        {
            return SqlParser.config();
        }

        /// <summary>
        /// Executes a DDL statement.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to execute against.</param>
        /// <param name="node">The parsed statement.</param>
        /// <remarks>
        /// Unlike Calcite's <c>executeDdl</c>, this takes a lock, because a root schema here may be shared by
        /// connections used concurrently, and DDL modifies the schema's <c>TreeMap</c>-backed name maps that
        /// planning reads. If the context carries a root lock (see <c>PrepareContext.RootLock</c>), a read
        /// lock the caller holds is released, the write lock is taken for the statement, and the read lock is
        /// taken again before returning. Otherwise the statement runs under the mutable root schema's
        /// monitor.
        /// </remarks>
        public virtual void ExecuteDdl(CalcitePrepare.Context context, SqlNode node)
        {
            var config = context.config();
            var parserFactory = (SqlParserImplFactory)config.parserFactory((java.lang.Class)typeof(SqlParserImplFactory), org.apache.calcite.sql.parser.impl.SqlParserImpl.FACTORY);

            var rootLock = (context as PrepareContext)?.RootLock;
            if (rootLock is null)
            {
                lock (context.getMutableRootSchema())
                    parserFactory.getDdlExecutor().executeDdl(context, node);

                return;
            }

            var hadRead = rootLock.IsReadLockHeld;
            if (hadRead)
                rootLock.ExitReadLock();

            rootLock.EnterWriteLock();
            try
            {
                parserFactory.getDdlExecutor().executeDdl(context, node);
            }
            finally
            {
                rootLock.ExitWriteLock();
                if (hadRead)
                    rootLock.EnterReadLock();
            }
        }

        /// <summary>
        /// Creates the object that prepares one statement.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="elementType">The type the caller wants a row to be; <c>object[]</c> selects
        /// <see cref="ClrCursorPrefer.Array"/>, anything else <see cref="ClrCursorPrefer.Custom"/>.</param>
        /// <param name="catalogReader">The catalog reader.</param>
        /// <param name="planner">The planner.</param>
        /// <returns>The preparing statement.</returns>
        protected virtual PreparingStmt GetPreparingStmt(CalcitePrepare.Context context, System.Type elementType, CalciteCatalogReader catalogReader, RelOptPlanner planner)
        {
            var typeFactory = context.getTypeFactory();
            var prefer = elementType == typeof(object[])
                ? ClrCursorPrefer.Array
                : ClrCursorPrefer.Custom;

            var cluster = CreateCluster(planner, new RexBuilder(typeFactory));

            return new ClrCursorPreparingStmt(
                this,
                context,
                catalogReader,
                typeFactory,
                context.getRootSchema(),
                prefer,
                cluster,
                CreateConvertletTable());
        }

        /// <summary>
        /// Prepares one of <see cref="SIMPLE_SQLS"/> without planning, as <c>CalcitePrepareImpl.simplePrepare</c>
        /// does.
        /// </summary>
        static IClrPrepare.Signature SimplePrepare(CalcitePrepare.Context context, string sql)
        {
            var typeFactory = context.getTypeFactory();
            var x = typeFactory.builder().add(SqlUtil.deriveAliasFromOrdinal(0), SqlTypeName.INTEGER).build();
            var origins = java.util.Collections.nCopies(x.getFieldCount(), null);
            var columns = GetColumnMetaDataList(typeFactory, x, x, origins);
            var cursorFactory = Meta.CursorFactory.deduce(columns, null);

            return new IClrPrepare.Signature(
                sql,
                com.google.common.collect.ImmutableList.of(),
                com.google.common.collect.ImmutableMap.of(),
                x,
                // no dynamic parameters
                null,
                columns,
                cursorFactory,
                context.getRootSchema(),
                com.google.common.collect.ImmutableList.of(),
                -1,
                // Calcite's row is a java.lang.Integer 1; a one-column result is the value, not a one-element row
                new ClrSimpleBindable(java.lang.Integer.valueOf(1)),
                Meta.StatementType.SELECT);
        }

        /// <summary>
        /// Parses, plans, compiles and describes one statement.
        /// </summary>
        IClrPrepare.Signature Prepare2_(CalcitePrepare.Context context, IClrPrepare.Query query, System.Type elementType, long maxRowCount, CalciteCatalogReader catalogReader, PreparingStmt preparingStmt)
        {
            var typeFactory = context.getTypeFactory();
            var config = context.config();

            RelDataType x;
            ClrPrepare.IPreparedResult preparedResult;
            Meta.StatementType statementType;

            if (query.Sql is { } sql)
            {
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
                    sqlNode = CreateParser(sql, parseConfig).parseStmt();
                }
                catch (SqlParseException e)
                {
                    throw new java.lang.RuntimeException("parse failed: " + e.getMessage(), e);
                }

                statementType = GetStatementType(sqlNode.getKind());

                org.apache.calcite.runtime.Hook.PARSE_TREE.run(new object[] { sql, sqlNode });

                // DDL is executed here rather than planned, as in Calcite, and the signature has no plan
                if (sqlNode.getKind().belongsTo(SqlKind.DDL))
                {
                    ExecuteDdl(context, sqlNode);

                    return new IClrPrepare.Signature(
                        sql,
                        com.google.common.collect.ImmutableList.of(),
                        com.google.common.collect.ImmutableMap.of(),
                        null,
                        // no dynamic parameters
                        null,
                        com.google.common.collect.ImmutableList.of(),
                        Meta.CursorFactory.OBJECT,
                        null,
                        com.google.common.collect.ImmutableList.of(),
                        -1,
                        null,
                        Meta.StatementType.OTHER_DDL);
                }

                var validator = preparingStmt.CreateSqlValidator(catalogReader, c => c);

                preparedResult = preparingStmt.PrepareSql(sqlNode, (java.lang.Class)typeof(java.lang.Object), validator, true);

                switch (sqlNode.getKind().name())
                {
                    case nameof(SqlKind.INSERT):
                    case nameof(SqlKind.DELETE):
                    case nameof(SqlKind.UPDATE):
                    case nameof(SqlKind.MERGE):
                    case nameof(SqlKind.EXPLAIN):
                        // as in Calcite: getValidatedNodeType does not give the result type of DML
                        x = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
                        break;
                    default:
                        x = validator.getValidatedNodeType(sqlNode);
                        break;
                }
            }
            else
            {
                var rel = query.Rel ?? throw new java.lang.IllegalStateException("a query is text or a plan");

                x = rel.getRowType();
                preparedResult = preparingStmt.PrepareRel(rel, x);
                statementType = GetStatementType(preparedResult);
            }

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

            // only a PreparedResultImpl has an element type; for an EXPLAIN the cursor factory is deduced from
            // the columns alone. Meta.CursorFactory.deduce takes a java.lang.Class.
            var rowClass = (preparedResult as ClrPrepare.PreparedResultImpl)?.ElementType;
            var resultClazz = rowClass is null ? null : ikvm.runtime.Util.getFriendlyClassFromType(rowClass);
            var cursorFactory = Meta.CursorFactory.deduce(columns, resultClazz);

            return new IClrPrepare.Signature(
                query.Sql,
                parameters,
                preparingStmt.InternalParameters,
                jdbcType,
                preparedResult.ParameterRowType,
                columns,
                cursorFactory,
                context.getRootSchema(),
                (preparedResult as ClrPrepare.PreparedResultImpl)?.Collations ?? (java.util.List)com.google.common.collect.ImmutableList.of(),
                maxRowCount,
                preparedResult.GetBindable(cursorFactory),
                statementType);
        }

        /// <summary>
        /// Deduces the broad type of statement from its kind.
        /// </summary>
        /// <param name="kind">The kind of the statement's root node.</param>
        /// <returns><c>IS_DML</c> for an <c>INSERT</c>, <c>DELETE</c>, <c>UPDATE</c> or <c>MERGE</c>; <c>SELECT</c> for anything else.</returns>
        static Meta.StatementType GetStatementType(SqlKind kind) => kind.name() switch
        {
            nameof(SqlKind.INSERT) => Meta.StatementType.IS_DML,
            nameof(SqlKind.DELETE) => Meta.StatementType.IS_DML,
            nameof(SqlKind.UPDATE) => Meta.StatementType.IS_DML,
            nameof(SqlKind.MERGE) => Meta.StatementType.IS_DML,
            _ => Meta.StatementType.SELECT,
        };

        /// <summary>
        /// Deduces the broad type of statement from a prepared result.
        /// </summary>
        static Meta.StatementType GetStatementType(ClrPrepare.IPreparedResult preparedResult)
        {
            return preparedResult.IsDml ? Meta.StatementType.IS_DML : Meta.StatementType.SELECT;
        }

        /// <summary>
        /// Creates the validator a statement is validated with, over the connection's function libraries and
        /// the catalog.
        /// </summary>
        static SqlValidator CreateSqlValidator(CalcitePrepare.Context context, CalciteCatalogReader catalogReader, Func<SqlValidator.Config, SqlValidator.Config> configTransform)
        {
            var opTab0 = (SqlOperatorTable)context.config().fun((java.lang.Class)typeof(SqlOperatorTable), org.apache.calcite.sql.fun.SqlStdOperatorTable.instance());

            var list = new java.util.ArrayList();
            list.add(opTab0);
            list.add(catalogReader);

            var opTab = org.apache.calcite.sql.util.SqlOperatorTables.chain(list);
            var typeFactory = context.getTypeFactory();
            var connectionConfig = context.config();

            var config = configTransform(
                SqlValidator.Config.DEFAULT
                    .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                    .withConformance(connectionConfig.conformance())
                    .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                    .withIdentifierExpansion(true));

            return new CalciteSqlValidator(opTab, catalogReader, typeFactory, config);
        }

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
            var avaticaType = AvaticaType(typeFactory, type, fieldType);

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
        static ColumnMetaData.AvaticaType AvaticaType(JavaTypeFactory typeFactory, RelDataType type, RelDataType? fieldType)
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
                var componentType = AvaticaType(typeFactory, type.getComponentType(), null);
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

            // GEOMETRY is reported as VARCHAR, as in Calcite
            if (typeOrdinal == ExtraSqlTypes.GEOMETRY)
                typeOrdinal = java.sql.Types.VARCHAR;

            var scalarClazz = typeFactory.getJavaClass(fieldType ?? type);
            var scalarRep = ColumnMetaData.Rep.of(scalarClazz) ?? throw new java.lang.IllegalStateException($"no Rep for {scalarClazz}");

            return ColumnMetaData.scalar(typeOrdinal, typeName, scalarRep);
        }

        /// <summary>
        /// Reads one element of a field's origin list, counting from the end (0 column, 1 table, 2 schema).
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
        /// Returns the class name a column is reported as, which is always <c>java.lang.Object</c>, as in
        /// Calcite.
        /// </summary>
        static string GetClassName(RelDataType type)
        {
            return ((java.lang.Class)typeof(java.lang.Object)).getName();
        }

        /// <summary>
        /// Returns a type's scale, or zero if it has none.
        /// </summary>
        static int GetScale(RelDataType type)
        {
            return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED ? 0 : type.getScale();
        }

        /// <summary>
        /// Returns a type's precision, or zero if it has none.
        /// </summary>
        static int GetPrecision(RelDataType type)
        {
            return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ? 0 : type.getPrecision();
        }

        /// <summary>
        /// Returns the type name in string form, without precision, scale or nullability.
        /// </summary>
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
        /// Wraps a type in a one-field struct if it is not one already.
        /// </summary>
        static RelDataType MakeStruct(RelDataTypeFactory typeFactory, RelDataType type)
        {
            return type.isStruct() ? type : typeFactory.builder().add("$0", type).build();
        }


        /// <summary>
        /// Prepares one statement against a Calcite schema; the counterpart of Calcite's
        /// <c>CalcitePreparingStmt</c>.
        /// </summary>
        public abstract class PreparingStmt : ClrPrepare, RelOptTable.ViewExpander
        {

            readonly RelOptPlanner planner;
            readonly RexBuilder rexBuilder;
            readonly ClrPrepareImpl prepare;
            readonly CalciteSchema schema;
            readonly RelDataTypeFactory typeFactory;
            readonly SqlRexConvertletTable convertletTable;
            readonly ClrCursorPrefer prefer;
            readonly RelOptCluster cluster;

            /// <summary>
            /// The values stashed during implementation, which the plan reads through the <c>DataContext</c>.
            /// </summary>
            readonly java.util.Map internalParameters = new java.util.LinkedHashMap();

            int expansionDepth;

            SqlValidator? validator;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="prepare">The owning <see cref="ClrPrepareImpl"/>, which creates view parsers.</param>
            /// <param name="context">The schema, type factory and configuration to plan against.</param>
            /// <param name="catalogReader">The catalog reader.</param>
            /// <param name="typeFactory">The type factory.</param>
            /// <param name="schema">The root schema, whose lattices the planner may use.</param>
            /// <param name="prefer">The row representation the caller prefers.</param>
            /// <param name="cluster">The cluster the plan is built in; its planner chooses the plan.</param>
            /// <param name="resultConvention">The convention the root of the plan must be in.</param>
            /// <param name="convertletTable">The convertlet table.</param>
            /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
            /// <remarks>
            /// Override <see cref="CreateSqlValidator"/> to customize validation.
            /// </remarks>
            protected PreparingStmt(
                ClrPrepareImpl prepare,
                CalcitePrepare.Context context,
                CalciteCatalogReader catalogReader,
                RelDataTypeFactory typeFactory,
                CalciteSchema schema,
                ClrCursorPrefer prefer,
                RelOptCluster cluster,
                Convention resultConvention,
                SqlRexConvertletTable convertletTable) :
                base(context, catalogReader, resultConvention)
            {
                this.prepare = prepare ?? throw new ArgumentNullException(nameof(prepare));
                this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
                this.prefer = prefer;
                this.cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
                this.planner = cluster.getPlanner();
                this.rexBuilder = cluster.getRexBuilder();
                this.typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
                this.convertletTable = convertletTable ?? throw new ArgumentNullException(nameof(convertletTable));
            }

            /// <summary>
            /// Gets the cluster the plan is built in.
            /// </summary>
            protected RelOptCluster Cluster => cluster;

            /// <summary>
            /// Gets the planner that chooses the plan.
            /// </summary>
            protected RelOptPlanner Planner => planner;

            /// <summary>
            /// Gets the row representation the caller prefers.
            /// </summary>
            protected ClrCursorPrefer Prefer => prefer;

            /// <summary>
            /// Gets the type factory the statement is prepared with.
            /// </summary>
            protected RelDataTypeFactory TypeFactory => typeFactory;

            /// <summary>
            /// Gets the values stashed during implementation, which the plan reads through the <c>DataContext</c>.
            /// </summary>
            public java.util.Map InternalParameters => internalParameters;

            /// <inheritdoc />
            protected override SqlValidator SqlValidator => validator ??= CreateSqlValidator(CatalogReader, c => c);

            /// <summary>
            /// Prepares a relational expression that was built rather than parsed.
            /// </summary>
            /// <param name="rel">The relational expression. It is planned by its own cluster's planner.</param>
            /// <param name="resultType">The row type to describe the result as.</param>
            /// <returns>The compiled statement.</returns>
            /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
            /// <remarks>
            /// The counterpart of Calcite's <c>prepare_(Supplier, RelDataType)</c>: there is no validation, so
            /// no field origins or parameters, and no materializations or lattices are offered.
            /// </remarks>
            public ClrPrepare.IPreparedResult PrepareRel(RelNode rel, RelDataType resultType)
            {
                ArgumentNullException.ThrowIfNull(rel);
                ArgumentNullException.ThrowIfNull(resultType);

                Init((java.lang.Class)typeof(java.lang.Object));

                var rowType = rel.getRowType();
                var fields = org.apache.calcite.util.Pair.zip(
                    org.apache.calcite.util.ImmutableIntList.identity(rowType.getFieldCount()),
                    rowType.getFieldNames());

                var collation = rel is org.apache.calcite.rel.core.Sort sort
                    ? sort.collation
                    : RelCollations.EMPTY;

                var root = new RelRoot(rel, resultType, SqlKind.SELECT, fields, collation, com.google.common.collect.ImmutableList.of());

                // no validation, so no field origins and no parameters, as in Calcite
                var jdbcType = MakeStruct(rexBuilder.getTypeFactory(), resultType);
                FieldOrigins = java.util.Collections.nCopies(jdbcType.getFieldCount(), null);
                ParameterRowType = rexBuilder.getTypeFactory().builder().build();

                root = root.withRel(FlattenTypes(root.rel, true));
                root = TrimUnusedFields(root);

                // no materializations or lattices, as in Calcite's prepare_(Supplier, RelDataType)
                root = Optimize(root, com.google.common.collect.ImmutableList.of(), com.google.common.collect.ImmutableList.of());

                return Implement(root);
            }

            /// <inheritdoc />
            protected override void Init(java.lang.Class runtimeContextClass)
            {

            }

            /// <inheritdoc />
            protected override java.util.List GetMaterializations()
            {
                return com.google.common.collect.ImmutableList.of();
            }

            /// <inheritdoc />
            protected override java.util.List GetLattices()
            {
                return org.apache.calcite.schema.Schemas.getLatticeEntries(schema);
            }

            /// <inheritdoc />
            public override RelNode FlattenTypes(RelNode rootRel, bool restructure)
            {
                return rootRel;
            }

            /// <inheritdoc />
            protected override RelNode Decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel)
            {
                if (Context.config().topDownGeneralDecorrelationEnabled())
                {
                    // Calcite writes sqlToRelConverter.config(), which Java resolves to the static
                    // SqlToRelConverter.config(), so the relational builder is the default configuration's
                    var relBuilder = SqlToRelConverter.config().getRelBuilderFactory().create(rootRel.getCluster(), null);

                    return org.apache.calcite.sql2rel.TopDownGeneralDecorrelator.decorrelateQuery(rootRel, relBuilder);
                }

                return sqlToRelConverter.decorrelate(query, rootRel);
            }

            /// <inheritdoc />
            protected override SqlToRelConverter GetSqlToRelConverter(SqlValidator validator, org.apache.calcite.prepare.Prepare.CatalogReader catalogReader, SqlToRelConverter.Config config)
            {
                config = config.withTopDownGeneralDecorrelationEnabled(Context.config().topDownGeneralDecorrelationEnabled());

                return new SqlToRelConverter(this, validator, catalogReader, cluster, convertletTable, config);
            }

            /// <inheritdoc />
            protected override ClrPrepare.IPreparedResult CreatePreparedExplanation(
                RelDataType? resultType,
                RelDataType parameterRowType,
                RelRoot? root,
                SqlExplainFormat format,
                SqlExplainLevel detailLevel)
            {
                return new ClrPreparedExplain(resultType, parameterRowType, root, format, detailLevel);
            }

            /// <summary>
            /// Creates the validator.
            /// </summary>
            /// <param name="catalogReader">The catalog reader, which must be a <c>CalciteCatalogReader</c>.</param>
            /// <param name="configTransform">Adjusts the validator configuration.</param>
            /// <returns>The validator.</returns>
            /// <remarks>
            /// <c>protected internal</c> because <see cref="ClrPrepareImpl"/> calls it, as Java's protected
            /// also grants package access.
            /// </remarks>
            protected internal virtual SqlValidator CreateSqlValidator(org.apache.calcite.prepare.Prepare.CatalogReader catalogReader, Func<SqlValidator.Config, SqlValidator.Config> configTransform)
            {
                return ClrPrepareImpl.CreateSqlValidator(Context, (CalciteCatalogReader)catalogReader, configTransform);
            }

            /// <inheritdoc />
            public RelRoot expandView(RelDataType rowType, string queryString, java.util.List schemaPath, java.util.List viewPath)
            {
                expansionDepth++;

                var parser = prepare.CreateParser(queryString);

                SqlNode sqlNode;
                try
                {
                    sqlNode = parser.parseQuery();
                }
                catch (SqlParseException e)
                {
                    throw new java.lang.RuntimeException("parse failed", e);
                }

                var viewCatalogReader = CatalogReader.withSchemaPath(schemaPath);
                var viewValidator = CreateSqlValidator(viewCatalogReader, c => c.withEmbeddedQuery(true));
                var config = SqlToRelConverter.config().withTrimUnusedFields(true);
                var sqlToRelConverter = GetSqlToRelConverter(viewValidator, viewCatalogReader, config);
                var root = sqlToRelConverter.convertQuery(sqlNode, true, true);

                --expansionDepth;

                return root;
            }

        }

        /// <summary>
        /// A prepared <c>EXPLAIN</c>, whose plan returns the rendered text as its one row.
        /// </summary>
        sealed class ClrPreparedExplain : ClrPrepare.PreparedExplain
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            public ClrPreparedExplain(
                RelDataType? resultType,
                RelDataType parameterRowType,
                RelRoot? root,
                SqlExplainFormat format,
                SqlExplainLevel detailLevel) :
                base(resultType, parameterRowType, root, format, detailLevel)
            {

            }

            /// <inheritdoc />
            public override IClrCursorFactory GetBindable(Meta.CursorFactory cursorFactory)
            {
                return new ClrExplainBindable(Code, cursorFactory);
            }

        }

    }

}
