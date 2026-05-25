using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data;

using java.util;
using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.model;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Represents an active session with the Calcite engine, encapsulating the root schema, type factory,  and connection configuration.
    /// </summary>
    internal sealed class CalciteSession
    {

        /// <summary>
        /// Maps each <see cref="CalciteConnectionStringBuilder"/> key constant to the
        /// corresponding <see cref="CalciteConnectionProperty"/>, which is the authoritative
        /// source of the camelCase property name Calcite expects.
        /// </summary>
        static readonly Dictionary<string, CalciteConnectionProperty> KeyToProperty = new(StringComparer.OrdinalIgnoreCase)
        {
            [CalciteConnectionStringBuilder.ApproximateDecimalKey] = CalciteConnectionProperty.APPROXIMATE_DECIMAL,
            [CalciteConnectionStringBuilder.ApproximateDistinctCountKey] = CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT,
            [CalciteConnectionStringBuilder.ApproximateTopNKey] = CalciteConnectionProperty.APPROXIMATE_TOP_N,
            [CalciteConnectionStringBuilder.CaseSensitiveKey] = CalciteConnectionProperty.CASE_SENSITIVE,
            [CalciteConnectionStringBuilder.ConformanceKey] = CalciteConnectionProperty.CONFORMANCE,
            [CalciteConnectionStringBuilder.CreateMaterializationsKey] = CalciteConnectionProperty.CREATE_MATERIALIZATIONS,
            [CalciteConnectionStringBuilder.DefaultNullCollationKey] = CalciteConnectionProperty.DEFAULT_NULL_COLLATION,
            [CalciteConnectionStringBuilder.DruidFetchKey] = CalciteConnectionProperty.DRUID_FETCH,
            [CalciteConnectionStringBuilder.ForceDecorrelateKey] = CalciteConnectionProperty.FORCE_DECORRELATE,
            [CalciteConnectionStringBuilder.FunKey] = CalciteConnectionProperty.FUN,
            [CalciteConnectionStringBuilder.LexKey] = CalciteConnectionProperty.LEX,
            [CalciteConnectionStringBuilder.MaterializationsEnabledKey] = CalciteConnectionProperty.MATERIALIZATIONS_ENABLED,
            [CalciteConnectionStringBuilder.ParserFactoryKey] = CalciteConnectionProperty.PARSER_FACTORY,
            [CalciteConnectionStringBuilder.QuotingKey] = CalciteConnectionProperty.QUOTING,
            [CalciteConnectionStringBuilder.QuotedCasingKey] = CalciteConnectionProperty.QUOTED_CASING,
            [CalciteConnectionStringBuilder.UnquotedCasingKey] = CalciteConnectionProperty.UNQUOTED_CASING,
            [CalciteConnectionStringBuilder.SchemaKey] = CalciteConnectionProperty.SCHEMA,
            [CalciteConnectionStringBuilder.SchemaFactoryKey] = CalciteConnectionProperty.SCHEMA_FACTORY,
            [CalciteConnectionStringBuilder.SchemaTypeKey] = CalciteConnectionProperty.SCHEMA_TYPE,
            [CalciteConnectionStringBuilder.SparkKey] = CalciteConnectionProperty.SPARK,
            [CalciteConnectionStringBuilder.TimeZoneKey] = CalciteConnectionProperty.TIME_ZONE,
            [CalciteConnectionStringBuilder.TypeSystemKey] = CalciteConnectionProperty.TYPE_SYSTEM,
            [CalciteConnectionStringBuilder.TypeCoercionKey] = CalciteConnectionProperty.TYPE_COERCION,
        };

        readonly CalciteSchema _rootSchema;
        readonly SchemaPlus _rootSchemaPlus;
        readonly JavaTypeFactory _typeFactory;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        readonly Func<CalcitePrepare> _prepareFactory;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="prepareFactory">
        /// Optional factory that produces the <see cref="CalcitePrepare"/> instance used for each
        /// query. When <see langword="null"/>, <see cref="CalcitePrepare.DEFAULT_FACTORY"/> is used.
        /// </param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        public CalciteSession(CalciteConnectionStringBuilder options, Func<CalcitePrepare>? prepareFactory = null)
        {
            ArgumentNullException.ThrowIfNull(options);

            try
            {
                _prepareFactory = prepareFactory ?? (() => (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply());
                _rootSchema = BuildRootSchema(options, out var modelDefaultSchema);
                _rootSchemaPlus = _rootSchema.plus();
                _config = new CalciteConnectionConfigImpl(BuildEngineProperties(options));
                _typeFactory = new JavaTypeFactoryImpl();
                _defaultSchemaPath = string.IsNullOrEmpty(modelDefaultSchema ?? options.Schema) ? [] : [modelDefaultSchema ?? options.Schema];
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite.", e);
            }
        }

        /// <summary>
        /// Builds the root schema based on the provided connection options, applying any specified model and determining the default schema path.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="defaultSchema"></param>
        /// <returns></returns>
        CalciteSchema BuildRootSchema(CalciteConnectionStringBuilder options, out string? defaultSchema)
        {
            var rootSchema = CalciteSchema.createRootSchema(addMetadataSchema: true);
            defaultSchema = null;

            if (string.IsNullOrEmpty(options.Model) == false)
                ApplyModel(rootSchema, options.Model, out defaultSchema);

            return rootSchema;
        }

        /// <summary>
        /// Applies a Calcite model to the root schema, either from an inline JSON definition or a file path.
        /// </summary>
        /// <param name="rootSchema">The root schema to which the model will be applied.</param>
        /// <param name="model">Either an inline JSON model definition (prefixed with "inline:" or starting with "{") or a file path to a
        /// model definition.</param>
        /// <param name="defaultSchema">When this method returns, contains the default schema name defined in the model, or <see langword="null"/>
        /// if no default schema is defined.</param>
        /// <exception cref="FileNotFoundException">Thrown when the specified model file does not exist.</exception>
        /// <exception cref="CalciteException">Thrown when the model fails to load.</exception>
        void ApplyModel(CalciteSchema rootSchema, string model, out string? defaultSchema)
        {
            try
            {
                if (model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) || model.TrimStart().StartsWith("{"))
                {
                    var inline = model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) ? model.Substring("inline:".Length) : model;
                    var handler = new ModelHandler(rootSchema.plus(), "inline:" + inline);
                    defaultSchema = handler.defaultSchemaName();
                }
                else
                {
                    if (!File.Exists(model))
                        throw new FileNotFoundException("Model file was not found.", model);

                    var handler = new ModelHandler(rootSchema.plus(), model);
                    defaultSchema = handler.defaultSchemaName();
                }

            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to load Calcite model.", e);
            }
        }

        /// <summary>
        /// Builds a Java Properties object from connection string options, mapping keys to their camel-cased property
        /// names and excluding the Model key.
        /// </summary>
        /// <param name="options">The connection string builder containing the options to convert.</param>
        /// <returns>A Properties object populated with the connection string options.</returns>
        Properties BuildEngineProperties(CalciteConnectionStringBuilder options)
        {
            var props = new Properties();

            foreach (var key in options.EnumerateKeys())
            {
                if (string.Equals(key, CalciteConnectionStringBuilder.ModelKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (options.TryGetValue(key, out var v) && v is not null)
                    props.setProperty(KeyToProperty.TryGetValue(key, out var prop) ? prop.camelName() : key, v.ToString());
            }

            return props;
        }

        /// <summary>
        /// Gets the root schema for the current context.
        /// </summary>
        public SchemaPlus RootSchema => _rootSchemaPlus;

        /// <summary>
        /// Gets the factory used to create Java type representations.
        /// </summary>
        public JavaTypeFactory TypeFactory => _typeFactory;

        /// <summary>
        /// Gets the configuration settings for the Calcite connection.
        /// </summary>
        public CalciteConnectionConfig Config => _config;

        /// <summary>
        /// Prepares the SQL statement and outputs the relevant objects capturing it.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="signature"></param>
        /// <param name="dataContext"></param>
        /// <param name="registration"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        void Prepare(CalciteExecuteRequest request, out CalcitePrepare.CalciteSignature signature, out DataContext dataContext, out CancellationTokenRegistration registration, CancellationToken cancellationToken)
        {
            var cancelFlag = new AtomicBoolean(false);
            var boundParameters = ParameterBinder.Bind(request.Parameters);
            dataContext = new StatementDataContext(_rootSchema.plus(), _typeFactory, cancelFlag, request.CommandTimeoutSeconds * 1000L, boundParameters);
            var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, dataContext, _defaultSchemaPath);

            var prepare = _prepareFactory();
            var query = CalcitePrepare.Query.of(request.Sql);

            CalcitePrepare.Dummy.push(ctx);
            try
            {
                signature = prepare.prepareSql(ctx, query, (java.lang.Class)typeof(java.lang.Object[]), -1);
            }
            finally
            {
                CalcitePrepare.Dummy.pop(ctx);
            }

            registration = cancellationToken.Register(() => cancelFlag.set(true));
        }

        /// <summary>
        /// Calls <c>Hook.addThread</c> for each entry, binding it to the current thread for the
        /// duration of execution. Returns the list of <c>Closeable</c> handles that must be passed
        /// to <see cref="DeactivateHooks"/> when execution ends, or <see langword="null"/> when
        /// <paramref name="hooks"/> is <see langword="null"/>.
        /// </summary>
        List<org.apache.calcite.runtime.Hook.Closeable>? ActivateHooks(IEnumerable<CalciteHookEntry>? hooks)
        {
            if (hooks is null)
                return null;

            var closeables = new List<org.apache.calcite.runtime.Hook.Closeable>();
            foreach (var entry in hooks)
                closeables.Add(entry.Hook.addThread(org.apache.calcite.runtime.Hook.propertyJ(entry.Value)));

            return closeables;
        }

        /// <summary>
        /// Closes each handle returned by <see cref="ActivateHooks"/>, deregistering the hooks
        /// from the current thread. Safe to call with a <see langword="null"/> list.
        /// </summary>
        void DeactivateHooks(List<org.apache.calcite.runtime.Hook.Closeable>? closeables)
        {
            if (closeables is not null)
                foreach (var c in closeables)
                    c?.close();
        }

        /// <summary>
        /// Prepares and executes a SQL statement asynchronously, returning a <see cref="CalciteResult"/>.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        public Task<CalciteResult> ExecuteReaderAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);
            try
            {
                Prepare(request, out var signature, out var dataContext, out var registration, cancellationToken);

                Enumerable? enumerable = null;
                try
                {
                    var statementType = (Meta.StatementType.__Enum)signature.statementType.ordinal();
                    if (!IsDdl(statementType))
                        enumerable = signature.enumerable(dataContext);
                }
                catch
                {
                    registration.Dispose();
                    throw;
                }

                return Task.FromResult(new CalciteResult(signature, registration, enumerable?.enumerator(), 0));
            }
            catch (CalciteException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new CalciteException("Failed to execute Calcite statement.", e);
            }
            finally
            {
                DeactivateHooks(closeables);
            }
        }

        /// <summary>
        /// Prepares and executes a SQL statement asynchronously, returning a <see cref="CalciteResult"/>.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        public Task<CalciteResult> ExecuteNonQueryAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);
            try
            {
                Prepare(request, out var signature, out var dataContext, out var registration, cancellationToken);

                long recordsAffected;
                try
                {
                    var statementType = (Meta.StatementType.__Enum)signature.statementType.ordinal();

                    if (IsDdl(statementType))
                    {
                        // DDL: already executed as a side-effect of prepareSql; nothing to enumerate.
                        recordsAffected = 0;
                    }
                    else if (statementType == Meta.StatementType.__Enum.SELECT)
                    {
                        // SELECT has no affected row count by ADO.NET convention.
                        recordsAffected = -1;
                    }
                    else
                    {
                        // DML (INSERT/UPDATE/DELETE/MERGE): drain the enumerator to trigger execution.
                        // Because prepareSql is called with elementType=Object[], the prefer hint is
                        // ARRAY and cursorFactory is CursorFactory.ARRAY. Calcite therefore yields a
                        // single Object[] row whose only element [0] is the ROWCOUNT BIGINT column
                        // defined by RelOptUtil.createDmlRowType.
                        recordsAffected = 0;
                        using var e = signature.enumerable(dataContext).enumerator();
                        if (e.moveNext())
                        {
                            var cur = e.current();
                            if (cur is object[] row && row.Length > 0)
                                recordsAffected = ToInt64(row[0]);
                            else if (cur != null)
                                recordsAffected = ToInt64(cur);
                        }
                    }
                }
                catch
                {
                    registration.Dispose();
                    throw;
                }

                return Task.FromResult(new CalciteResult(signature, registration, enumerator: null, recordsAffected));
            }
            catch (CalciteException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new CalciteException("Failed to execute Calcite statement.", e);
            }
            finally
            {
                DeactivateHooks(closeables);
            }
        }

        static bool IsDdl(Meta.StatementType.__Enum t) => t switch
        {
            Meta.StatementType.__Enum.CREATE => true,
            Meta.StatementType.__Enum.ALTER => true,
            Meta.StatementType.__Enum.DROP => true,
            Meta.StatementType.__Enum.OTHER_DDL => true,
            _ => false,
        };

        static long ToInt64(object? value) => value switch
        {
            null => 0,
            java.lang.Long l => l.longValue(),
            java.lang.Integer i => i.intValue(),
            java.lang.Number n => n.longValue(),
            IConvertible c => c.ToInt64(null),
            _ => Convert.ToInt64(value.ToString()),
        };

        public void Dispose()
        {
            _disposed = true;
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CalciteSession));
        }

    }

}
