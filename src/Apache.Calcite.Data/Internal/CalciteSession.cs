using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

using java.util;
using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The connection-lived half of a Calcite connection: the type factory, the configuration and the
    /// convention, over a root schema the data source holds.
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

        readonly CalciteDataSourceRoot _root;
        readonly bool _ownsRoot;
        readonly CalciteSchema _rootSchema;
        readonly SchemaPlus _rootSchemaPlus;
        readonly JavaTypeFactory _typeFactory;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        readonly bool _synchronous;
        readonly Func<ClrPrepareImpl> _prepareFactory;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="root">The root schema to plan against, built by the data source.</param>
        /// <param name="ownsRoot">Whether this session is the only user of <paramref name="root"/> and so
        /// disposes it. <see langword="true"/> under <c>Pooling=false</c>, where the root was built for this
        /// connection alone.</param>
        /// <param name="typeFactory">Type factory, or null. See the remarks for what the conventions require of one.</param>
        /// <param name="prepareFactory">Prepare factory, or null for <see cref="ClrPrepareImpl"/>.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        /// <remarks>
        /// This is the half of <c>CalciteConnectionImpl</c>'s constructor that is the connection's rather than
        /// the data source's: the config, the prepare factory, and the type factory resolved from the
        /// <c>typeSystem</c> property (by <see cref="ClrPlugin"/>, this provider naming a plugin in .NET rather
        /// than in Java) under the conformance's ragged-union wrapper. The root, <c>DUAL</c> and the model
        /// are <see cref="CalciteDataSourceRoot"/>'s, built once per data source and shared by every connection
        /// it opens, which is the split Calcite itself makes when it opens an internal connection over an
        /// existing root — <c>CalciteMetaImpl.connect(schema.root(), null)</c> — and pairs it with a fresh
        /// type factory. The pairing is not a nicety: <c>JavaTypeFactoryImpl.syntheticTypes</c> is a plain
        /// <c>HashMap</c> written by every grouped aggregate and window, so a factory shared by connections
        /// used concurrently would race, where a root shared by them is read.
        ///
        /// <para>An injected <paramref name="typeFactory"/> bypasses both the configured type system and the
        /// ragged-union wrapper, as upstream's does. Both conventions require more of it than its interface
        /// says, and the requirement is stated rather than typed because no type expresses it. Every grouped
        /// aggregate and every window builds its accumulator from <c>createSyntheticType</c> and then
        /// matches the result against <c>JavaTypeFactoryImpl.SyntheticRecordType</c>, a nested class of
        /// the implementation — Calcite's own coupling, <c>EnumerableAggregateBase</c> having the same
        /// <c>instanceof</c> — and <c>ClrTypes.Resolve</c> has to answer a CLR type for whatever comes
        /// back, throwing where it cannot. Calcite survives further, writing the type's name into Java
        /// source for Janino to resolve. The same goes for <c>getJavaClass</c>, whose closed set of Java
        /// classes is what the conventions box and unbox against.</para>
        ///
        /// <para>Naming <c>JavaTypeFactoryImpl</c> here would not enforce any of that — a subclass
        /// overriding <c>createSyntheticType</c> satisfies the parameter and still breaks the plan —
        /// while <c>_typeFactory</c>, <see cref="TypeFactory"/>, <c>PrepareContext</c> and everything in
        /// <c>Apache.Calcite.Extensions</c> that consumes one are declared against the interface. A
        /// narrower door onto a pipeline typed the other way buys nothing and reads as though it did.</para>
        ///
        /// <para>Every query is planned into one of the two Clr conventions and run as a compiled expression
        /// tree — which one is the connection's choice, the way Calcite's own connection can ask for the
        /// bindable convention. The default is <c>ClrAsyncEnumerableConvention</c>;
        /// <see cref="CalciteConnectionStringBuilder.Synchronous"/> asks for <c>ClrEnumerableConvention</c>
        /// instead. Either way Calcite's own rules stay on the planner, so a statement the chosen convention
        /// has no node for is still planned and run — implemented in <c>EnumerableConvention</c>, with a
        /// converter carrying its rows.</para>
        /// </remarks>
        public CalciteSession(CalciteConnectionStringBuilder options, CalciteDataSourceRoot root, bool ownsRoot, JavaTypeFactory? typeFactory = null, Func<ClrPrepareImpl>? prepareFactory = null)
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
                _rootSchema = root.Schema;
                _config = cfg;
                _rootSchemaPlus = _rootSchema.plus();

                _synchronous = options.Synchronous ?? false;
                var defaultSchema = root.DefaultSchemaName ?? options.Schema;
                _defaultSchemaPath = string.IsNullOrEmpty(defaultSchema) ? [] : [defaultSchema];
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite.", e);
            }
        }

        /// <summary>
        /// The anonymous <c>DelegatingTypeSystem</c> subclass of <c>CalciteConnectionImpl</c>'s constructor,
        /// named because C# has no anonymous classes. One override, nothing else.
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
        /// Parses and plans <paramref name="request"/>, returning the compiled <see cref="IClrPrepare.Signature"/>.
        /// No execution state is created here.
        /// </summary>
        /// <remarks>
        /// The context is still pushed onto <c>CalcitePrepare.Dummy</c>'s thread-local stack, because
        /// Calcite's own parse-to-rel reads it from there.
        /// </remarks>
        IClrPrepare.Signature Plan(CalciteExecuteRequest request, bool async = false)
        {
            var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, _defaultSchemaPath);

            CalcitePrepare.Dummy.push(ctx);
            try
            {
                return _prepareFactory().PrepareSql(ctx, IClrPrepare.Query.Of(request.Sql), typeof(object[]), -1, async);
            }
            finally
            {
                CalcitePrepare.Dummy.pop(ctx);
            }
        }

        /// <summary>
        /// Creates the execution-time <see cref="DataContext"/> for a planned <paramref name="signature"/>.
        /// Mirrors the work done by <c>CalciteConnectionImpl.enumerable()</c> just before it calls
        /// <c>signature.enumerable(dataContext)</c>: bound parameters, stashed compile-time values
        /// from <c>signature.internalParameters</c>, cancel flag, and timeout are assembled into a
        /// single <see cref="StatementDataContext"/> over <c>signature.rootSchema</c> — the snapshot
        /// the statement was planned against, not the live root, so a statement executes against what
        /// it planned against. Null for DDL, as upstream's is.
        /// </summary>
        void Bind(CalciteExecuteRequest request, IClrPrepare.Signature signature, out DataContext dataContext, out AtomicBoolean cancelFlag)
        {
            cancelFlag = new AtomicBoolean(false);
            var boundParameters = ParameterBinder.Bind(request.Parameters);
            dataContext = new StatementDataContext(signature.RootSchema, _typeFactory, _config, _defaultSchemaPath, cancelFlag, request.CommandTimeoutSeconds * 1000L, boundParameters, signature.InternalParameters);
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
        /// <see cref="ExecuteReaderAsync"/> without a token: the plan is the connection's, not the entry
        /// point's, so both entry points prepare the same one. In the default mode the reader this hands a
        /// synchronous caller blocks per row wherever the plan genuinely suspends —
        /// <c>CalciteAsyncEnumerableResult.Read</c> says how that is made safe — and completes synchronously
        /// everywhere else.
        /// </remarks>
        public CalciteResult ExecuteReader(CalciteExecuteRequest request)
        {
            return ExecuteReaderCore(request, CancellationToken.None);
        }

        /// <summary>
        /// Prepares and executes a query, returning a <see cref="CalciteResult"/> whose enumerator streams
        /// the result rows.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <param name="cancellationToken">Token used to cancel execution. It is given to the plan's
        /// enumerator, which is the only place a token can enter an
        /// <see cref="IAsyncEnumerable{T}"/>. In synchronous mode it is observed only before planning.</param>
        /// <returns>A <see cref="CalciteResult"/> holding the signature and a row enumerator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="CalciteException">Thrown when planning or execution fails.</exception>
        /// <remarks>
        /// <see cref="ExecuteReaderCore"/> in a completed task — planning is synchronous work and nothing
        /// here awaits. Nothing is read until the first <c>ReadAsync</c>.
        /// </remarks>
        public Task<CalciteResult> ExecuteReaderAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult(ExecuteReaderCore(request, cancellationToken));
        }

        /// <summary>
        /// Prepares and executes a query into the connection's convention.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <param name="cancellationToken">Token given to an asynchronous plan's enumerator.</param>
        /// <returns>A <see cref="CalciteResult"/> holding the signature and a row enumerator.</returns>
        /// <remarks>
        /// <b>Which convention is the connection's choice, not the entry point's</b> — the way Calcite's own
        /// connection can ask for the bindable convention. The default is <c>ClrAsyncEnumerableConvention</c>,
        /// so that <c>ReadAsync</c> is asynchronous wherever the schema can be: an
        /// <c>IClrAsyncScannableTable</c> is scanned asynchronously, a table of Calcite's SPI is read the way
        /// Calcite reads it and wrapped in a sequence that completes synchronously — a state machine and no
        /// thread — and a statement the convention has no node for is implemented in
        /// <c>EnumerableConvention</c> with a converter carrying its rows. Nothing on the asynchronous
        /// surface ever parks a thread waiting for a row.
        ///
        /// <para><see cref="CalciteConnectionStringBuilder.Synchronous"/> demands
        /// <c>ClrEnumerableConvention</c> of the root instead, for both entry points, and <c>ReadAsync</c>
        /// answers with completed tasks. It is a choice of root and not of rule set: the prepare pipeline
        /// registers both conventions whichever mode is asked for, so a query touching a table that can
        /// <em>only</em> produce rows asynchronously is still planned, reached across a converter, and
        /// <c>Read</c> blocks there. Registering one convention and not the other would refuse a schema
        /// whose own rules target the other, which nothing here can rule out.</para>
        /// </remarks>
        CalciteResult ExecuteReaderCore(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();

            var closeables = ActivateHooks(request.Hooks);

            try
            {
                var signature = Plan(request, async: !_synchronous);
                Bind(request, signature, out var dataContext, out _);

                if (_synchronous)
                {
                    IEnumerator<object>? enumerator = null;
                    if (!IsDdl(signature.StatementType))
                        enumerator = signature.Bind(dataContext).GetEnumerator();

                    return new CalciteEnumerableResult(signature, enumerator, 0);
                }
                else
                {
                    IAsyncEnumerator<object>? enumerator = null;
                    if (!IsDdl(signature.StatementType))
                        enumerator = signature.BindAsync(dataContext).GetAsyncEnumerator(cancellationToken);

                    return new CalciteAsyncEnumerableResult(signature, enumerator, 0);
                }
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
        /// Synchronous, and <see cref="ExecuteNonQueryAsync"/> is this method in a completed task. The plan
        /// is the connection's here as everywhere: a table modification is not a node either Clr convention
        /// implements, so the modify itself is Calcite's <c>EnumerableTableModify</c> in both modes, and
        /// under the asynchronous root its count row crosses <c>EnumerableToClrAsyncEnumerableConverter</c>
        /// and completes synchronously — the drain never truly waits, but it blocks with the synchronization
        /// context suppressed all the same, because correctness must not depend on what the sub-plan happens
        /// to be. There is still no asynchronous DML in the node-level sense — the modify cannot suspend —
        /// and the asynchronous root does not pretend otherwise; what it keeps is one plan per statement per
        /// connection, whichever entry point asked.
        /// </remarks>
        public CalciteEnumerableResult ExecuteNonQuery(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var closeables = ActivateHooks(request.Hooks);
            try
            {
                var signature = Plan(request, async: !_synchronous);
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
                    // are enumerating and need Calcite's check-points to be able to interrupt the
                    // loop. The registration is scoped to this block only.
                    // RelOptUtil.createDmlRowType gives DML one ROWCOUNT column, and
                    // Meta.CursorFactory.deduce answers OBJECT for a single column before it looks at
                    // the element type -- measured -- so the row is the boxed count itself and not an
                    // array holding it. The array branch below is for a plan that says otherwise.
                    recordsAffected = 0;
                    using var _ = cancellationToken.Register(() => cancelFlag.set(true));

                    var cur = _synchronous
                        ? FirstRow(signature.Bind(dataContext))
                        : FirstRow(signature.BindAsync(dataContext), cancellationToken);

                    if (cur is object[] row && row.Length > 0)
                        recordsAffected = ToInt64(row[0]);
                    else if (cur != null)
                        recordsAffected = ToInt64(cur);
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
        /// Returns the first row of <paramref name="source"/>, or <see langword="null"/> where there is
        /// none.
        /// </summary>
        static object? FirstRow(IEnumerable<object> source)
        {
            using var e = source.GetEnumerator();
            return e.MoveNext() ? e.Current : null;
        }

        /// <summary>
        /// Returns the first row of <paramref name="source"/>, or <see langword="null"/> where there is
        /// none, blocking for it with the synchronization context suppressed before the plan runs — the
        /// operators capture the context at suspension, inside the call, so the suppression has to precede
        /// it. <c>CalciteAsyncEnumerableResult.Read</c> has the measurement.
        /// </summary>
        static object? FirstRow(IAsyncEnumerable<object> source, CancellationToken cancellationToken)
        {
            var context = SynchronizationContext.Current;
            if (context is null)
                return First(source, cancellationToken);

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                return First(source, cancellationToken);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }

            static object? First(IAsyncEnumerable<object> source, CancellationToken cancellationToken)
            {
                var e = source.GetAsyncEnumerator(cancellationToken);
                try
                {
                    return WaitMove(e.MoveNextAsync()) ? e.Current : null;
                }
                finally
                {
                    Wait(e.DisposeAsync());
                }
            }

            static bool WaitMove(ValueTask<bool> task)
            {
                return task.IsCompletedSuccessfully ? task.Result : task.AsTask().GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Waits for a disposal the caller cannot await.
        /// </summary>
        static void Wait(ValueTask task)
        {
            if (task.IsCompleted)
                task.GetAwaiter().GetResult();
            else
                task.AsTask().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Prepares and executes a DML, DDL, or SELECT statement and returns the number of rows affected.
        /// </summary>
        /// <param name="request">The execute request containing SQL text, parameters, timeout, and hooks.</param>
        /// <param name="cancellationToken">Token used to cancel execution.</param>
        /// <returns>A <see cref="CalciteResult"/> with <c>RecordsAffected</c> set and no row enumerator.</returns>
        /// <remarks>
        /// <see cref="ExecuteNonQuery"/> in a completed task, for the reason that method gives: a modify
        /// cannot suspend, so there is nothing here to await. It is here so that a caller writing
        /// asynchronously has the method it expects.
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

        /// <summary>
        /// Marks the session as disposed, so that further calls to execute methods throw
        /// <see cref="ObjectDisposedException"/>, and disposes the root where this session owns it.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            if (_ownsRoot)
                _root.Dispose();
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CalciteSession));
        }

    }

}
