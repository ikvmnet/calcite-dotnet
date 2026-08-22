using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Prepare.AsyncEnumerable;
using Apache.Calcite.Extensions.Prepare.Enumerable;
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
    /// Parses, plans and compiles a statement into the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
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
        /// <param name="query">The statement's text, or a plan that was built rather than parsed.</param>
        /// <param name="elementType">What a caller wants a row to be. <c>Object[]</c> asks for an array.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <returns>The planned statement.</returns>
        public IClrPrepare.Signature PrepareSql(CalcitePrepare.Context context, IClrPrepare.Query query, System.Type elementType, long maxRowCount)
        {
            return PrepareSql(context, query, elementType, maxRowCount, false);
        }

        /// <summary>
        /// Plans and compiles one query into one of the two conventions.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="query">The statement's text, or a plan that was built rather than parsed.</param>
        /// <param name="elementType">What a caller wants a row to be. <c>Object[]</c> asks for an array.</param>
        /// <param name="maxRowCount">The row limit, or a negative number for none.</param>
        /// <param name="async">Whether to prepare into the asynchronous convention.</param>
        /// <returns>The planned statement.</returns>
        public IClrPrepare.Signature PrepareSql(CalcitePrepare.Context context, IClrPrepare.Query query, System.Type elementType, long maxRowCount, bool async)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(query);

            return Prepare_(context, query, elementType, maxRowCount, async);
        }

        /// <summary>
        /// Tries each planner in turn, and rethrows the last failure when none can plan the statement.
        /// </summary>
        IClrPrepare.Signature Prepare_(CalcitePrepare.Context context, IClrPrepare.Query query, System.Type elementType, long maxRowCount, bool async)
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
                    var preparingStmt = GetPreparingStmt(context, elementType, catalogReader, planner, async);
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
        /// Creates the planner, with Calcite's default rules and both of this project's conventions'.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
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
        /// <returns></returns>
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

            // lets a test add or remove rules, as it does upstream
            org.apache.calcite.runtime.Hook.PLANNER.run(planner);

            return planner;
        }

        /// <summary>
        /// Creates the planner factories to try, in order.
        /// </summary>
        /// <returns></returns>
        protected virtual IReadOnlyList<Func<CalcitePrepare.Context, RelOptPlanner>> CreatePlannerFactories()
        {
            return [context => CreatePlanner(context)];
        }

        /// <summary>
        /// Factory method for default convertlet table.
        /// </summary>
        protected virtual SqlRexConvertletTable CreateConvertletTable()
        {
            return StandardConvertletTable.INSTANCE;
        }

        /// <summary>
        /// Factory method for cluster.
        /// </summary>
        protected virtual RelOptCluster CreateCluster(RelOptPlanner planner, RexBuilder rexBuilder)
        {
            var cluster = RelOptCluster.create(planner, rexBuilder);
            cluster.setMetadataQuerySupplier(ClrRelMetadataProvider.QuerySupplier(DefaultRelMetadataProvider.INSTANCE));
            cluster.invalidateMetadataQuery();
            return cluster;
        }

        /// <summary>
        /// Factory method for default SQL parser.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        protected virtual SqlParser CreateParser(string sql)
        {
            return CreateParser(sql, ParserConfig());
        }

        /// <summary>
        /// Factory method for SQL parser with a given configuration.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parserConfig"></param>
        /// <returns></returns>
        protected virtual SqlParser CreateParser(string sql, SqlParser.Config parserConfig)
        {
            return SqlParser.create(sql, parserConfig);
        }

        /// <summary>
        /// Factory method for SQL parser configuration.
        /// </summary>
        protected virtual SqlParser.Config ParserConfig()
        {
            return SqlParser.config();
        }

        /// <summary>
        /// Executes a DDL statement.
        /// </summary>
        public virtual void ExecuteDdl(CalcitePrepare.Context context, SqlNode node)
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
        protected virtual PreparingStmt GetPreparingStmt(CalcitePrepare.Context context, System.Type elementType, CalciteCatalogReader catalogReader, RelOptPlanner planner, bool async = false)
        {
            var typeFactory = context.getTypeFactory();
            var prefer = elementType == typeof(object[])
                ? ClrEnumerablePrefer.Array
                : ClrEnumerablePrefer.Custom;

            var cluster = CreateCluster(planner, new RexBuilder(typeFactory));

            if (async)
                return new ClrAsyncEnumerablePreparingStmt(
                    this,
                    context,
                    catalogReader,
                    typeFactory,
                    context.getRootSchema(),
                    prefer,
                    cluster,
                    CreateConvertletTable());

            return new ClrEnumerablePreparingStmt(
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
        /// Quickly prepares a simple statement, circumventing the usual preparation process.
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
                null,
                columns,
                cursorFactory,
                context.getRootSchema(),
                com.google.common.collect.ImmutableList.of(),
                -1,
                // Calcite's one row is Linq4j.asEnumerable(ImmutableList.of(1)), so the value is a
                // java.lang.Integer; a one-column result is the value rather than a one-element row
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

                // a DDL statement has already taken effect once this returns: it is executed here rather than
                // planned, exactly as Calcite does, and there is nothing left to bind
                if (sqlNode.getKind().belongsTo(SqlKind.DDL))
                {
                    ExecuteDdl(context, sqlNode);

                    return new IClrPrepare.Signature(
                        sql,
                        com.google.common.collect.ImmutableList.of(),
                        com.google.common.collect.ImmutableMap.of(),
                        null,
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
                        // getValidatedNodeType is wrong for DML, which is Calcite's own note
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

            // Typed is on PreparedResultImpl and not on the interface, so this tests for it before reading;
            // an EXPLAIN has no element type and the factory is deduced from the columns alone. This is also
            // the one place a row class leaves .NET: Meta.CursorFactory.deduce is Avatica's and takes a
            // java.lang.Class, and everything above holds the CLR type.
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
        /// Deduces the broad type of statement from a prepared result.
        /// </summary>
        static Meta.StatementType GetStatementType(ClrPrepare.IPreparedResult preparedResult)
        {
            return preparedResult.IsDml ? Meta.StatementType.IS_DML : Meta.StatementType.SELECT;
        }

        /// <summary>
        /// Builds the validator a statement is validated with.
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


        /// <summary>
        /// A statement being prepared against a Calcite schema.
        /// </summary>
        public abstract class PreparingStmt : ClrPrepare, RelOptTable.ViewExpander
        {

            readonly RelOptPlanner planner;
            readonly RexBuilder rexBuilder;
            readonly ClrPrepareImpl prepare;
            readonly CalciteSchema schema;
            readonly RelDataTypeFactory typeFactory;
            readonly SqlRexConvertletTable convertletTable;
            readonly ClrEnumerablePrefer prefer;
            readonly RelOptCluster cluster;

            /// <summary>
            /// The values the query reads through the <c>DataContext</c> rather than from the plan.
            /// </summary>
            readonly java.util.Map internalParameters = new java.util.LinkedHashMap();

            int expansionDepth;

            SqlValidator? validator;

            /// <summary>
            /// Initializes a new instance. Override this and <see cref="CreateSqlValidator"/> to supply
            /// custom validation logic.
            /// </summary>
            protected PreparingStmt(
                ClrPrepareImpl prepare,
                CalcitePrepare.Context context,
                CalciteCatalogReader catalogReader,
                RelDataTypeFactory typeFactory,
                CalciteSchema schema,
                ClrEnumerablePrefer prefer,
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
            /// Gets the representation a consumer of the plan would prefer its rows to arrive in.
            /// </summary>
            protected ClrEnumerablePrefer Prefer => prefer;

            /// <summary>
            /// Gets the type factory the statement is prepared with.
            /// </summary>
            protected RelDataTypeFactory TypeFactory => typeFactory;

            /// <summary>
            /// Gets the values the query reads through the <c>DataContext</c> rather than from the plan.
            /// </summary>
            public java.util.Map InternalParameters => internalParameters;

            /// <inheritdoc />
            protected override SqlValidator SqlValidator => validator ??= CreateSqlValidator(CatalogReader, c => c);

            /// <summary>
            /// Prepares a plan that was built rather than parsed.
            /// </summary>
            /// <param name="rel">The plan to run.</param>
            /// <param name="resultType">The row type a caller wants the result described as.</param>
            /// <returns>The compiled statement.</returns>
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

                // no validation happened, so there is nothing to say about where a field came from or what
                // parameters the statement takes — which is what Calcite records here too
                var jdbcType = MakeStruct(rexBuilder.getTypeFactory(), resultType);
                FieldOrigins = java.util.Collections.nCopies(jdbcType.getFieldCount(), null);
                ParameterRowType = rexBuilder.getTypeFactory().builder().build();

                root = root.withRel(FlattenTypes(root.rel, true));
                root = TrimUnusedFields(root);

                // empty for both, as Calcite's prepare_(Supplier, RelDataType) passes them: a plan that arrived
                // built is not a candidate for substitution
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
                    // Calcite writes this as sqlToRelConverter.config(), which reads as the converter's own
                    // configuration and is not: SqlToRelConverter has no instance config() and Java resolves
                    // the call to the static factory through the instance reference. So the builder is the
                    // default one, and C# has to say so
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
            /// Creates the validator. Override this and this class to supply custom validation logic.
            /// </summary>
            /// <remarks>
            /// <c>protected internal</c> rather than <c>protected</c>: Java's protected is also package
            /// access, and <see cref="Prepare2_"/> is the caller that relies on it.
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
        /// An <c>EXPLAIN</c> statement, prepared and ready to execute.
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
            public override IClrBindableBase GetBindable(Meta.CursorFactory cursorFactory)
            {
                return new ClrExplainBindable(Code, cursorFactory);
            }

        }

    }

}
