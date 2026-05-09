using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using java.util.concurrent.atomic;

using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;

using Apache.Calcite.AdoNet.Protocol;

namespace Apache.Calcite.AdoNet.Engine
{

    /// <summary>
    /// Native engine binding that prepares and executes SQL through Calcite's planner and runtime.
    /// </summary>
    /// <remarks>
    /// Replaces the responsibilities of <c>CalciteConnectionImpl</c>, <c>CalciteStatement</c>, and
    /// <c>CalciteResultSet</c> from the JDBC driver. It does not consume any class from
    /// <c>org.apache.calcite.jdbc</c> that descends from Avatica/JDBC; instead it directly uses
    /// <see cref="CalcitePrepare"/>, <see cref="CalciteSchema"/>, and the resulting
    /// <see cref="CalcitePrepare.CalciteSignature"/> the same way the JDBC driver does internally.
    /// </remarks>
    internal sealed class CalciteEngineClient : ICalciteClient
    {

        readonly CalciteSchema _rootSchema;
        readonly JavaTypeFactory _typeFactory;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        bool _disposed;

        public CalciteEngineClient(CalciteConnectionStringBuilder options)
        {
            if (options is null)
                throw new ArgumentNullException(nameof(options));

            try
            {
                var builder = new RootSchemaBuilder(options);
                _rootSchema = builder.Build();
                _config = new CalciteConnectionConfigImpl(builder.BuildEngineProperties());
                _typeFactory = new JavaTypeFactoryImpl();
                _defaultSchemaPath = string.IsNullOrEmpty(options.Schema)
                    ? System.Array.Empty<string>()
                    : new[] { options.Schema! };
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite engine.", e);
            }
        }

        public Task<ICalciteResult> ExecuteAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            if (request is null)
                throw new ArgumentNullException(nameof(request));

            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                var cancelFlag = new AtomicBoolean(false);
                var dataContext = new StatementDataContext(_rootSchema.plus(), _typeFactory, cancelFlag, request.CommandTimeoutSeconds * 1000L);
                var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, dataContext, _defaultSchemaPath);

                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                var query = CalcitePrepare.Query.of(request.Sql);

                CalcitePrepare.Dummy.push(ctx);
                CalcitePrepare.CalciteSignature signature;
                try
                {
                    signature = prepare.prepareSql(ctx, query, java.lang.Class.forName("[Ljava.lang.Object;"), -1);
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

                var columns = ColumnAdapter.MapColumns(signature);
                var enumerator = enumerable.enumerator();
                var result = new CalciteEngineResult(columns, signature, enumerator, registration);
                return Task.FromResult<ICalciteResult>(result);
            }
            catch (CalciteException)
            {
                throw;
            }
            catch (System.Exception e)
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
                throw new ObjectDisposedException(nameof(CalciteEngineClient));
        }

    }

}
