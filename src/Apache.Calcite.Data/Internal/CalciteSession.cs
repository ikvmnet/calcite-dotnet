using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using java.util.concurrent.atomic;

using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Represents an active session with the Calcite engine, encapsulating the root schema, type factory,  and connection configuration.
    /// </summary>
    internal sealed class CalciteSession
    {

        readonly CalciteSchema _rootSchema;
        readonly JavaTypeFactory _typeFactory;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        public CalciteSession(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            try
            {
                var builder = new RootSchemaBuilder(options);
                _rootSchema = builder.Build();
                _config = new CalciteConnectionConfigImpl(builder.BuildEngineProperties());
                _typeFactory = new JavaTypeFactoryImpl();
                _defaultSchemaPath = string.IsNullOrEmpty(options.Schema) ? [] : [options.Schema];
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite.", e);
            }
        }

        /// <summary>
        /// Gets the root schema for the current context.
        /// </summary>
        /// <remarks>The root schema serves as the entry point for accessing all available database
        /// objects and sub-schemas. Use this property to navigate or query the schema hierarchy.</remarks>
        public SchemaPlus RootSchema => _rootSchema.plus();

        /// <summary>
        /// Gets the factory used to create Java type representations.
        /// </summary>
        public JavaTypeFactory TypeFactory => _typeFactory;

        /// <summary>
        /// Gets the configuration settings for the Calcite connection.
        /// </summary>
        public CalciteConnectionConfig Config => _config;

        /// <summary>
        /// Prepares and executes a SQL statement asynchronously, returning a <see cref="CalciteResult"/>.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="CalciteException"></exception>
        public Task<CalciteResult> ExecuteAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                var cancelFlag = new AtomicBoolean(false);
                var boundParameters = ParameterBinder.Bind(request.Parameters);
                var dataContext = new StatementDataContext(_rootSchema.plus(), _typeFactory, cancelFlag, request.CommandTimeoutSeconds * 1000L, boundParameters);
                var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, dataContext, _defaultSchemaPath);

                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                var query = CalcitePrepare.Query.of(request.Sql);

                CalcitePrepare.Dummy.push(ctx);
                CalcitePrepare.CalciteSignature signature;
                try
                {
                    signature = prepare.prepareSql(ctx, query, (java.lang.Class)typeof(java.lang.Object[]), -1);
                }
                finally
                {
                    CalcitePrepare.Dummy.pop(ctx);
                }

                var registration = cancellationToken.Register(() => cancelFlag.set(true));

                Enumerable enumerable;
                try
                {
                    enumerable = signature.enumerable(dataContext);
                }
                catch
                {
                    registration.Dispose();
                    throw;
                }

                return Task.FromResult(new CalciteResult(signature, enumerable.enumerator(), registration));
            }
            catch (CalciteException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new CalciteException("Failed to execute Calcite statement.", e);
            }
        }

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
