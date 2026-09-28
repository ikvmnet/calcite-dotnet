using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;
using Apache.Calcite.Extensions.Runtime;

using java.util;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The per-connection part of a Calcite connection: the configuration, the type factory and the type
    /// mappings, over a root schema the data source holds. Plans and executes statements.
    /// </summary>
    internal sealed class CalciteSession
    {

        /// <summary>
        /// Registers Calcite's JDBC driver, which expanding a view requires.
        /// </summary>
        /// <remarks>
        /// <c>ViewTableMacro.apply</c> reads <c>MaterializedViewTable.MATERIALIZATION_CONNECTION</c>, whose
        /// initializer calls <c>DriverManager.getConnection("jdbc:calcite:")</c>, so every view expansion needs
        /// the driver registered. Constructing a <c>Driver</c> runs the static initializer that registers it.
        /// The assembly is added to the boot class path first because <c>UnregisteredDriver</c> loads its
        /// factory by name through <c>Class.forName</c>, which under IKVM does not see a class that is only in
        /// a referenced assembly.
        /// </remarks>
        static CalciteSession()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.Driver).Assembly);
            new org.apache.calcite.jdbc.Driver();
        }

        readonly CalciteDataSourceRoot _root;
        readonly bool _ownsRoot;
        readonly CalciteSchema _rootSchema;
        readonly SchemaPlus _rootSchemaPlus;
        readonly JavaTypeFactory _typeFactory;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        readonly Func<ClrPrepareImpl> _prepareFactory;
        readonly ClrTypeRegistry _registry;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="root">The root schema to plan against, built by the data source.</param>
        /// <param name="ownsRoot">Whether this session is the only user of <paramref name="root"/> and so
        /// disposes it. <see langword="true"/> under <c>Pooling=false</c>, where the root was built for this
        /// connection alone.</param>
        /// <param name="typeFactory">Type factory, or <see langword="null"/> to build one from the
        /// configuration. See the remarks for what the plan requires of one.</param>
        /// <param name="prepareFactory">Prepare factory, or <see langword="null"/> for <see cref="ClrPrepareImpl"/>.</param>
        /// <param name="typeResolvers">The type-mapping chain to bind, or default for the built-in one.</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> or <paramref name="root"/> is
        /// <see langword="null"/>.</exception>
        /// <exception cref="CalciteException">The session could not be initialized.</exception>
        /// <remarks>
        /// <para>
        /// The per-connection part of <c>CalciteConnectionImpl</c>'s constructor: the configuration, the
        /// prepare factory, and a type factory over the type system the <c>TypeSystem</c> key names (resolved
        /// by <see cref="ClrPlugin"/>), wrapped to convert ragged unions to varying types where the conformance
        /// asks for it. The root, <c>DUAL</c> and the model belong to <see cref="CalciteDataSourceRoot"/> and are
        /// shared. Calcite makes the same split when it opens an internal connection over an existing root
        /// (<c>CalciteMetaImpl.connect(schema.root(), null)</c>) with a new type factory. Each session needs
        /// its own factory because <c>JavaTypeFactoryImpl.syntheticTypes</c> is an unsynchronized
        /// <c>HashMap</c> written while planning aggregates and windows.
        /// </para>
        /// <para>
        /// An injected <paramref name="typeFactory"/> bypasses the configured type system and the ragged-union
        /// wrapper, as in Calcite. It must behave as <c>JavaTypeFactoryImpl</c> does in two respects the
        /// interface does not state: <c>createSyntheticType</c> must return a
        /// <c>JavaTypeFactoryImpl.SyntheticRecordType</c>, which aggregate and window implementation test for
        /// (as <c>EnumerableAggregateBase</c> does) and <c>ClrTypes.Resolve</c> must be able to map to a CLR
        /// type; and <c>getJavaClass</c> must answer from the same set of Java classes, which the plan boxes
        /// and unboxes against.
        /// </para>
        /// <para>
        /// Every statement is planned into <c>ClrCursorConvention</c> and runs as a compiled expression tree
        /// that opens a cursor, which may be opened and advanced either synchronously or with await. Calcite's
        /// own rules stay on the planner, so a statement with a node the cursor convention does not implement
        /// runs that part in <c>EnumerableConvention</c> under a converter.
        /// </para>
        /// </remarks>
        public CalciteSession(CalciteConnectionStringBuilder options, CalciteDataSourceRoot root, bool ownsRoot, JavaTypeFactory? typeFactory = null, Func<ClrPrepareImpl>? prepareFactory = null, System.Collections.Immutable.ImmutableArray<IClrTypeResolver> typeResolvers = default)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(root);

            try
            {
                var cfg = new CalciteConnectionConfigImpl(CalciteEngineProperties.Build(options));

                _prepareFactory = prepareFactory ?? (static () => new ClrPrepareImpl());
                if (typeFactory != null)
                {
                    _typeFactory = typeFactory;
                }
                else
                {
                    var typeSystem = ClrPlugin.Resolve(options.TypeSystem, RelDataTypeSystem.DEFAULT);
                    if (cfg.conformance().shouldConvertRaggedUnionTypesToVarying())
                        typeSystem = new RaggedUnionDelegatingTypeSystem(typeSystem);

                    _typeFactory = new JavaTypeFactoryImpl(typeSystem);
                }

                _root = root;
                _ownsRoot = ownsRoot;
                _root.Acquire();
                _rootSchema = root.Schema;
                _config = cfg;
                _rootSchemaPlus = _rootSchema.plus();

                // the chain is bound to this session's type factory, whose answers decide which Java class
                // holds each Calcite type, so it is read once here and not again
                _registry = new ClrTypeRegistry(_typeFactory, typeResolvers.IsDefaultOrEmpty ? new ClrTypeMapper().Resolvers : typeResolvers);

                var defaultSchema = root.DefaultSchemaName ?? options.Schema;
                _defaultSchemaPath = string.IsNullOrEmpty(defaultSchema) ? [] : [defaultSchema];
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite.", e);
            }
        }

        /// <summary>
        /// A type system that converts ragged union types to varying types. Mirrors the anonymous
        /// <c>DelegatingTypeSystem</c> subclass in <c>CalciteConnectionImpl</c>'s constructor.
        /// </summary>
        sealed class RaggedUnionDelegatingTypeSystem : DelegatingTypeSystem
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="typeSystem">The type system to delegate to.</param>
            public RaggedUnionDelegatingTypeSystem(RelDataTypeSystem typeSystem) :
                base(typeSystem)
            {

            }

            /// <inheritdoc />
            public override bool shouldConvertRaggedUnionTypesToVarying()
            {
                return true;
            }

        }

        /// <summary>
        /// Gets the root schema, shared with every other session on the same data source root.
        /// </summary>
        public SchemaPlus RootSchema => _rootSchemaPlus;


        /// <summary>
        /// Gets this session's type factory.
        /// </summary>
        public JavaTypeFactory TypeFactory => _typeFactory;

        /// <summary>
        /// Gets the type mappings this session reads and writes values through, bound to
        /// <see cref="TypeFactory"/>.
        /// </summary>
        public ClrTypeRegistry Registry => _registry;

        /// <summary>
        /// Gets the Calcite configuration derived from the connection string.
        /// </summary>
        public CalciteConnectionConfig Config => _config;

        /// <summary>
        /// Parses and plans <paramref name="request"/>, returning the compiled <see cref="IClrPrepare.Signature"/>.
        /// No execution state is created here. DDL takes effect during this call.
        /// </summary>
        /// <remarks>
        /// The prepare context is pushed onto <c>CalcitePrepare.Dummy</c>'s thread-local stack for the call,
        /// because Calcite's parse-to-rel reads it from there.
        /// </remarks>
        IClrPrepare.Signature Plan(CalciteExecuteRequest request)
        {
            // the root's read lock, from the snapshot the context takes to the signature: the root may be
            // shared with connections altering it by DDL. DDL releases this read lock and takes the write
            // side inside the prepare, then takes the read lock again before returning
            _root.Lock.EnterReadLock();
            try
            {
                var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, _defaultSchemaPath, _root.Lock);

                CalcitePrepare.Dummy.push(ctx);
                try
                {
                    return _prepareFactory().PrepareSql(ctx, IClrPrepare.Query.Of(request.Sql), typeof(object[]), -1);
                }
                finally
                {
                    CalcitePrepare.Dummy.pop(ctx);
                }
            }
            finally
            {
                _root.Lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Takes the root's read lock and returns a handle that releases it on dispose, for code that reads the
        /// root outside planning.
        /// </summary>
        /// <returns>The handle.</returns>
        public ReadLockHold ReadRoot()
        {
            _root.Lock.EnterReadLock();
            return new ReadLockHold(_root.Lock);
        }

        /// <summary>
        /// A held read lock, released on dispose. Dispose exactly once, on the thread that took it.
        /// </summary>
        public readonly struct ReadLockHold : IDisposable
        {

            readonly System.Threading.ReaderWriterLockSlim _lock;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="lock">The lock held.</param>
            public ReadLockHold(System.Threading.ReaderWriterLockSlim @lock)
            {
                _lock = @lock;
            }

            /// <inheritdoc />
            public void Dispose()
            {
                _lock.ExitReadLock();
            }

        }

        /// <summary>
        /// Creates the execution-time <see cref="DataContext"/> for a planned <paramref name="signature"/>.
        /// Mirrors what <c>CalciteConnectionImpl.enumerable()</c> does before it calls
        /// <c>signature.enumerable(dataContext)</c>: the bound parameters, the values planning stashed in
        /// <c>signature.internalParameters</c>, the cancel flag and the timeout go into one
        /// <see cref="StatementDataContext"/> over <c>signature.rootSchema</c>, the snapshot the statement was
        /// planned against rather than the live root.
        /// </summary>
        /// <remarks>
        /// The context turns <paramref name="cancellationToken"/> into the <c>CANCEL_FLAG</c> that Calcite's
        /// own operators poll.
        /// </remarks>
        /// <exception cref="ClrTypeMappingException">A parameter value cannot be converted to its
        /// placeholder's type.</exception>
        void Bind(CalciteExecuteRequest request, IClrPrepare.Signature signature, CancellationToken cancellationToken, out StatementDataContext dataContext)
        {
            var boundParameters = ParameterBinder.Bind(request.Parameters, _registry, signature);
            dataContext = new StatementDataContext(signature.RootSchema, _typeFactory, _config, _defaultSchemaPath, cancellationToken, request.CommandTimeoutSeconds * 1000L, boundParameters, signature.InternalParameters);
        }

        /// <summary>
        /// Attaches each hook to the current thread with <c>Hook.addThread</c>. Returns the handles to pass
        /// to <see cref="DeactivateHooks"/>, or <see langword="null"/> when <paramref name="hooks"/> is
        /// <see langword="null"/>.
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
        /// Closes each handle returned by <see cref="ActivateHooks"/>, detaching the hooks from the current
        /// thread. Accepts <see langword="null"/>.
        /// </summary>
        static void DeactivateHooks(List<Hook.Closeable>? closeables)
        {
            if (closeables is not null)
                foreach (var c in closeables)
                    c?.close();
        }

        /// <summary>
        /// Plans a statement and opens its cursor synchronously, returning a result that reads its rows. A
        /// DDL statement takes effect while it is planned and its result has no rows.
        /// </summary>
        /// <param name="request">The SQL text, parameters, timeout and hooks.</param>
        /// <returns>The result, which owns the cursor, the data context and the cancellation source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
        /// <exception cref="CalciteException">Planning, binding or opening fails.</exception>
        /// <remarks>
        /// Opening runs the plan's acquisition on this thread (a sort drains its input, a leaf runs its query);
        /// where a table can only be read asynchronously, this blocks with the synchronization context
        /// suppressed. The result's <c>ReadAsync</c> still awaits wherever the plan can suspend. Hooks are
        /// attached to this thread while the statement is planned and opened, and detached before this
        /// returns.
        /// </remarks>
        public CalciteResult ExecuteReader(CalciteExecuteRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();

            var closeables = ActivateHooks(request.Hooks);

            try
            {
                var signature = Plan(request);

                // a source of the statement's own, so that a token given later to ReadAsync has something to
                // cancel; the statement's cancel flag is tied to the same source
                var cancellation = new CancellationTokenSource();
                Bind(request, signature, cancellation.Token, out var dataContext);

                // the result owns the context and the source from here, since the rows outlive this call
                IClrCursor? cursor = null;
                if (!IsDdl(signature.StatementType))
                    cursor = signature.Open(dataContext);

                return new CalciteCursorResult(signature, _registry, cursor, 0, dataContext, cancellation);
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
        /// Plans a statement and opens its cursor with await, returning a result that reads its rows. A DDL
        /// statement takes effect while it is planned and its result has no rows.
        /// </summary>
        /// <param name="request">The SQL text, parameters, timeout and hooks.</param>
        /// <param name="cancellationToken">Checked before planning, and linked into the statement's
        /// cancellation source, which the plan is opened with and which later <c>ReadAsync</c> calls can
        /// cancel.</param>
        /// <returns>The result, which owns the cursor, the data context and the cancellation source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already cancelled.</exception>
        /// <exception cref="CalciteException">Planning, binding or opening fails.</exception>
        /// <remarks>
        /// Planning is synchronous and completes before the first await. Opening awaits the plan's
        /// acquisition, so a table that implements <c>ScanAsync</c> is scanned asynchronously; tables read
        /// through Calcite's own interfaces complete synchronously.
        /// </remarks>
        public async Task<CalciteResult> ExecuteReaderAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();

            // planning and opening can do real work (an adapter leaf opens a connection and sends a query),
            // so a token that is already cancelled stops here
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);

            try
            {
                var signature = Plan(request);

                // linked to the caller's token; the plan is opened with this source's token and the context's
                // cancel flag is tied to it, so both observe one cancellation, which a token given later to
                // ReadAsync can also trigger
                var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                Bind(request, signature, cancellation.Token, out var dataContext);

                // the result owns the context and the source from here, since the rows outlive this call
                IClrCursor? cursor = null;
                if (!IsDdl(signature.StatementType))
                    cursor = await signature.OpenAsync(dataContext, cancellation.Token).ConfigureAwait(false);

                return new CalciteCursorResult(signature, _registry, cursor, 0, dataContext, cancellation);
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
        /// Plans and executes a statement for its affected-row count: -1 for a query, 0 for DDL, and for DML
        /// the count the statement's single row reports.
        /// </summary>
        /// <param name="request">The SQL text, parameters, timeout and hooks.</param>
        /// <param name="cancellationToken">Checked before planning, and tied to the statement's cancel flag.</param>
        /// <returns>A result with no rows and <c>RecordsAffected</c> set.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already cancelled.</exception>
        /// <exception cref="CalciteException">Planning or execution fails.</exception>
        /// <remarks>
        /// A query is planned and not run. DDL takes effect while it is planned. DML is run by reading the
        /// first row of its cursor, synchronously: the modification is Calcite's <c>EnumerableTableModify</c>
        /// under a converter, which has nothing to await.
        /// </remarks>
        public CalciteCursorResult ExecuteNonQuery(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);
            try
            {
                var signature = Plan(request);
                Bind(request, signature, cancellationToken, out var dataContext);
                using var _ = dataContext;

                var statementType = signature.StatementType;

                long recordsAffected;
                if (IsDdl(statementType))
                {
                    // DDL has already taken effect during planning
                    recordsAffected = 0;
                }
                else if (statementType == Meta.StatementType.SELECT)
                {
                    // a query has no affected-row count in ADO.NET
                    recordsAffected = -1;
                }
                else
                {
                    // DML runs when its first row is read. RelOptUtil.createDmlRowType gives DML one ROWCOUNT
                    // column, and Meta.CursorFactory.deduce answers OBJECT for a single column, so the row is
                    // the boxed count itself; the array branch is for a plan that produces an array row
                    recordsAffected = 0;

                    var cur = FirstRow(signature.Open(dataContext));

                    if (cur is object[] row && row.Length > 0)
                        recordsAffected = ToInt64(row[0]);
                    else if (cur != null)
                        recordsAffected = ToInt64(cur);
                }

                return new CalciteCursorResult(signature, _registry, null, recordsAffected);
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
        /// Returns the first row of <paramref name="cursor"/>, or <see langword="null"/> where there is
        /// none, and closes it.
        /// </summary>
        static object? FirstRow(IClrCursor cursor)
        {
            using (cursor)
                return cursor.Read() ? cursor.Current : null;
        }

        /// <summary>
        /// Runs <see cref="ExecuteNonQuery"/> and returns its result as a completed task.
        /// </summary>
        /// <param name="request">The SQL text, parameters, timeout and hooks.</param>
        /// <param name="cancellationToken">Checked before planning, and tied to the statement's cancel flag.</param>
        /// <returns>A completed task whose result has no rows and <c>RecordsAffected</c> set.</returns>
        /// <remarks>
        /// Runs synchronously on the calling thread, for the reason <see cref="ExecuteNonQuery"/> gives.
        /// Exceptions are thrown directly rather than through the task.
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

        /// <summary>Converts a row count, as a Java boxed number or a CLR primitive, to <see cref="long"/>.</summary>
        static long ToInt64(object? value) => value switch
        {
            null => 0,
            java.lang.Long l => l.longValue(),
            java.lang.Integer i => i.intValue(),
            java.lang.Number n => n.longValue(),
            IConvertible c => c.ToInt64(null),
            _ => Convert.ToInt64(value.ToString()),
        };

        /// <summary>
        /// Marks the session as disposed, so that further calls to execute methods throw
        /// <see cref="ObjectDisposedException"/>, and releases this session's hold on the root, retiring it
        /// first where the root was built for this session alone.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            if (_ownsRoot)
                _root.Retire();

            _root.Release();
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CalciteSession));
        }

    }

}
