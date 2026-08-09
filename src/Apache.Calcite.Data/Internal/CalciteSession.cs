using java.util;
using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.model;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Represents an active session with the Calcite engine, encapsulating the root schema, type factory,  and connection configuration.
    /// </summary>
    internal sealed class CalciteSession
    {

        /// <summary>
        /// Registers Calcite's JDBC driver, which a view needs and nothing else does.
        /// </summary>
        /// <remarks>
        /// <c>ViewTableMacro.apply</c> reads <c>MaterializedViewTable.MATERIALIZATION_CONNECTION</c>, and
        /// that field's initializer is <c>DriverManager.getConnection("jdbc:calcite:")</c> — so expanding
        /// any view, however it was declared, goes through the JDBC driver. Under IKVM nothing had
        /// registered one, and every view failed at validation.
        ///
        /// <para>Both halves are needed. Constructing the <c>Driver</c> runs its static initializer, which
        /// is what calls <c>register()</c>; and the assembly has to be on the boot class path first,
        /// because <c>UnregisteredDriver</c> resolves its factory by name through <c>Class.forName</c> and
        /// cannot see a class that is only in a referenced assembly.</para>
        /// </remarks>
        static CalciteSession()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.Driver).Assembly);
            new org.apache.calcite.jdbc.Driver();
        }

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
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        /// <remarks>
        /// Every statement is planned into <c>ClrEnumerableConvention</c> and run as a compiled expression
        /// tree. Calcite's own rules stay on the planner, so a statement that convention has no node for is
        /// still planned and run — implemented in <c>EnumerableConvention</c>, with a converter carrying its
        /// rows.
        /// </remarks>
        public CalciteSession(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            try
            {
                _rootSchema = BuildRootSchema(options, out var modelDefaultSchema);
                _rootSchemaPlus = _rootSchema.plus();
                _config = new CalciteConnectionConfigImpl(BuildEngineProperties(options));
                _typeFactory = new JavaTypeFactoryImpl();
                var defaultSchema = modelDefaultSchema ?? options.Schema;
                _defaultSchemaPath = string.IsNullOrEmpty(defaultSchema) ? [] : [defaultSchema];
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
                    var handler = new ClrModelHandler(rootSchema.plus(), "inline:" + inline);
                    defaultSchema = handler.defaultSchemaName();
                }
                else
                {
                    if (!File.Exists(model))
                        throw new FileNotFoundException("Model file was not found.", model);

                    var handler = new ClrModelHandler(rootSchema.plus(), model);
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
        /// Parses and plans <paramref name="request"/>, returning the compiled <see cref="ClrSignature"/>.
        /// No execution state is created here.
        /// </summary>
        /// <remarks>
        /// The context is still pushed onto <c>CalcitePrepare.Dummy</c>'s thread-local stack, because
        /// Calcite's own parse-to-rel reads it from there.
        /// </remarks>
        ClrSignature Plan(CalciteExecuteRequest request, bool async = false)
        {
            var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, _defaultSchemaPath);

            CalcitePrepare.Dummy.push(ctx);
            try
            {
                return new ClrPrepareImpl().Prepare(ctx, request.Sql, (java.lang.Class)typeof(java.lang.Object[]), -1, async);
            }
            finally
            {
                CalcitePrepare.Dummy.pop(ctx);
            }
        }

        /// <summary>
        /// Puts this session's context on <c>CalcitePrepare.Dummy</c>'s stack until the returned handle is
        /// disposed.
        /// </summary>
        /// <remarks>
        /// <see cref="Plan"/> does this around planning because Calcite's parse-to-rel reads the context
        /// from there. Anything else that makes Calcite reach for a context has to do the same, and
        /// describing a view is one: <c>ClrViewTableMacro</c> is the connection's configuration only
        /// because <c>Schemas.makeContext</c> can find the context here, and metadata expands views outside
        /// any planning.
        ///
        /// <para>The context reports no object path, so <c>push</c> cannot raise
        /// <c>CyclicDefinitionException</c> against another of ours.</para>
        /// </remarks>
        public IDisposable PushContext()
        {
            ThrowIfDisposed();

            var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, _defaultSchemaPath);
            CalcitePrepare.Dummy.push(ctx);

            return new ContextScope(ctx);
        }

        /// <summary>Pops the context <see cref="PushContext"/> pushed.</summary>
        sealed class ContextScope(CalcitePrepare.Context context) : IDisposable
        {

            bool _popped;

            public void Dispose()
            {
                if (_popped)
                    return;

                _popped = true;
                CalcitePrepare.Dummy.pop(context);
            }

        }

        /// <summary>
        /// Creates the execution-time <see cref="DataContext"/> for a planned <paramref name="signature"/>.
        /// Mirrors the work done by <c>CalciteConnectionImpl.enumerable()</c> just before it calls
        /// <c>signature.enumerable(dataContext)</c>: bound parameters, stashed compile-time values
        /// from <c>signature.internalParameters</c>, cancel flag, and timeout are assembled into a
        /// single <see cref="StatementDataContext"/>.
        /// </summary>
        void Bind(CalciteExecuteRequest request, ClrSignature signature, out DataContext dataContext, out AtomicBoolean cancelFlag)
        {
            cancelFlag = new AtomicBoolean(false);
            var boundParameters = ParameterBinder.Bind(request.Parameters);
            dataContext = new StatementDataContext(_rootSchema.plus(), _typeFactory, cancelFlag, request.CommandTimeoutSeconds * 1000L, boundParameters, signature.InternalParameters);
        }

        /// <summary>
        /// Calls <c>Hook.addThread</c> for each entry, binding it to the current thread for the
        /// duration of execution. Returns the list of <c>Closeable</c> handles that must be passed
        /// to <see cref="DeactivateHooks"/> when execution ends, or <see langword="null"/> when
        /// <paramref name="hooks"/> is <see langword="null"/>.
        /// </summary>
        static List<Hook.Closeable>? ActivateHooks(IEnumerable<CalciteHookEntry>? hooks)
        {
            if (hooks is null)
                return null;

            var closeables = new List<Hook.Closeable>();
            foreach (var entry in hooks)
                if (entry.Consumer is { } consumer)
                    closeables.Add(entry.Hook.addThread(consumer));

            return closeables;
        }

        /// <summary>
        /// Closes each handle returned by <see cref="ActivateHooks"/>, deregistering the hooks
        /// from the current thread. Safe to call with a <see langword="null"/> list.
        /// </summary>
        static void DeactivateHooks(List<Hook.Closeable>? closeables)
        {
            if (closeables is not null)
                foreach (var c in closeables)
                    c?.close();
        }

        /// <summary>
        /// Prepares and executes a query, returning a <see cref="CalciteResult"/> whose enumerator streams
        /// the result rows. For DDL statements the enumerator is <see langword="null"/>.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <returns>A <see cref="CalciteResult"/> holding the signature and a row enumerator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="CalciteException">Thrown when planning or execution fails.</exception>
        /// <remarks>
        /// Prepares into <c>ClrEnumerableConvention</c>, which reads a <c>ScannableTable</c>, a
        /// <c>QueryableTable</c> and the rest of Calcite's table SPI, and falls through to
        /// <c>EnumerableConvention</c> across a converter for anything it has no node for.
        /// </remarks>
        public CalciteEnumerableResult ExecuteReader(CalciteExecuteRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();

            var closeables = ActivateHooks(request.Hooks);

            try
            {
                var signature = Plan(request);
                Bind(request, signature, out var dataContext, out _);

                IEnumerator<object>? enumerator = null;
                if (!IsDdl(signature.StatementType))
                    enumerator = signature.Bind(dataContext).GetEnumerator();

                return new CalciteEnumerableResult(signature, enumerator, 0);
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
        /// Prepares and executes a query into the asynchronous convention, returning a
        /// <see cref="CalciteResult"/> whose enumerator streams the result rows.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <param name="cancellationToken">Token used to cancel execution. It is given to the plan's
        /// enumerator, which is the only place a token can enter an
        /// <see cref="IAsyncEnumerable{T}"/>.</param>
        /// <returns>A <see cref="CalciteResult"/> holding the signature and an asynchronous row enumerator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="CalciteException">Thrown when planning or execution fails, <b>including where the
        /// query cannot be planned asynchronously at all</b>.</exception>
        /// <remarks>
        /// <b>There is no fallback.</b> The planner this goes to carries the asynchronous convention's rules
        /// and not the synchronous one's, so a query touching a table that is not an
        /// <c>IClrAsyncScannableTable</c> cannot be planned and this throws. Preparing the synchronous plan
        /// instead would hand back a reader that looks asynchronous and blocks a thread per row, which is the
        /// one thing this convention exists to refuse -- and a caller cannot tell the difference from the
        /// outside, so the failure has to be visible.
        ///
        /// <para><c>ClrEnumerableToClrAsyncEnumerableConverter</c> is what a fallback would be built from,
        /// and it exists; what does not exist is a decision to register both rule sets here. Doing so would
        /// make the mixed plan one the planner chose and costed rather than a second plan substituted behind
        /// the caller's back — but it would also let a plan block a thread per row without saying so, which
        /// is why it is not the default.</para>
        ///
        /// <para>A caller that wants the synchronous plan asks for it: <see cref="ExecuteReader"/>.</para>
        /// </remarks>
        public Task<CalciteResult> ExecuteReaderAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);

            try
            {
                var signature = Plan(request, async: true);
                Bind(request, signature, out var dataContext, out _);

                IAsyncEnumerator<object>? enumerator = null;
                if (!IsDdl(signature.StatementType))
                    enumerator = signature.BindAsync(dataContext).GetAsyncEnumerator(cancellationToken);

                return Task.FromResult<CalciteResult>(new CalciteAsyncEnumerableResult(signature, enumerator, 0));
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
        /// Prepares and executes a DML, DDL, or SELECT statement and returns the number of rows affected.
        /// For SELECT the affected-row count is <c>-1</c> by ADO.NET convention; for DDL it is <c>0</c>;
        /// for DML it is the row count reported by Calcite.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <param name="cancellationToken">Token used to cancel execution.</param>
        /// <returns>A <see cref="CalciteResult"/> with <c>RecordsAffected</c> set and no row enumerator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="CalciteException">Thrown when planning or execution fails.</exception>
        /// <remarks>
        /// Synchronous, and <see cref="ExecuteNonQueryAsync"/> is this method in a completed task. There is
        /// no asynchronous DML and there cannot be one: a table modification is not a node either of these
        /// conventions implements, and only the synchronous one reaches Calcite's across a converter -- there
        /// is no converter to <c>EnumerableConvention</c> from the asynchronous one and cannot be -- so a
        /// write is planned into <c>ClrEnumerableConvention</c> whichever entry point asked for it.
        /// </remarks>
        public CalciteEnumerableResult ExecuteNonQuery(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);
            try
            {
                var signature = Plan(request);
                Bind(request, signature, out var dataContext, out var cancelFlag);

                var statementType = signature.StatementType;

                long recordsAffected;
                if (IsDdl(statementType))
                {
                    // DDL: already executed as a side-effect of prepareSql; nothing to enumerate.
                    recordsAffected = 0;
                }
                else if (statementType == Meta.StatementType.SELECT)
                {
                    // SELECT has no affected row count by ADO.NET convention.
                    recordsAffected = -1;
                }
                else
                {
                    // DML (INSERT/UPDATE/DELETE/MERGE): drain the enumerator to trigger execution.
                    // Wire the cancellation token to the Calcite cancel flag only here, where we
                    // are synchronously enumerating and need Calcite's check-points to be able to
                    // interrupt the loop. The registration is scoped to this block only.
                    // Because prepareSql is called with elementType=Object[], the prefer hint is
                    // ARRAY and cursorFactory is CursorFactory.ARRAY. Calcite therefore yields a
                    // single Object[] row whose only element [0] is the ROWCOUNT BIGINT column
                    // defined by RelOptUtil.createDmlRowType.
                    recordsAffected = 0;
                    using var _ = cancellationToken.Register(() => cancelFlag.set(true));
                    using var e = signature.Bind(dataContext).GetEnumerator();
                    if (e.MoveNext())
                    {
                        var cur = e.Current;
                        if (cur is object[] row && row.Length > 0)
                            recordsAffected = ToInt64(row[0]);
                        else if (cur != null)
                            recordsAffected = ToInt64(cur);
                    }
                }

                return new CalciteEnumerableResult(signature, null, recordsAffected);
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
        /// Prepares and executes a DML, DDL, or SELECT statement and returns the number of rows affected.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <param name="cancellationToken">Token used to cancel execution.</param>
        /// <returns>A <see cref="CalciteResult"/> with <c>RecordsAffected</c> set and no row enumerator.</returns>
        /// <remarks>
        /// <see cref="ExecuteNonQuery"/> in a completed task, for the reason that method gives: there is no
        /// asynchronous DML to prepare. It is here so that a caller writing asynchronously has the method it
        /// expects, not because anything about it awaits.
        /// </remarks>
        public Task<CalciteResult> ExecuteNonQueryAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult<CalciteResult>(ExecuteNonQuery(request, cancellationToken));
        }

        /// <summary>Returns <see langword="true"/> when <paramref name="t"/> represents a DDL statement type.</summary>
        static bool IsDdl(Meta.StatementType t) => t.name() switch
        {
            nameof(Meta.StatementType.CREATE) => true,
            nameof(Meta.StatementType.ALTER) => true,
            nameof(Meta.StatementType.DROP) => true,
            nameof(Meta.StatementType.OTHER_DDL) => true,
            _ => false,
        };

        /// <summary>Converts a Calcite row-count value (Java boxed number or CLR primitive) to <see cref="long"/>.</summary>
        static long ToInt64(object? value) => value switch
        {
            null => 0,
            java.lang.Long l => l.longValue(),
            java.lang.Integer i => i.intValue(),
            java.lang.Number n => n.longValue(),
            IConvertible c => c.ToInt64(null),
            _ => Convert.ToInt64(value.ToString()),
        };

        /// <summary>Marks the session as disposed. Further calls to execute methods will throw <see cref="ObjectDisposedException"/>.</summary>
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
