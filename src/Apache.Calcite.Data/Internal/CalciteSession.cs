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
        /// Prepares the SQL statement and outputs the relevant objects capturing it.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="signature"></param>
        /// <param name="dataContext"></param>
        /// <param name="registration"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        void Prepare(CalciteExecuteRequest request, out CalcitePrepare.CalciteSignature signature, out StatementDataContext dataContext, out CancellationTokenRegistration registration, CancellationToken cancellationToken)
        {
            var cancelFlag = new AtomicBoolean(false);
            var boundParameters = ParameterBinder.Bind(request.Parameters);
            dataContext = new StatementDataContext(_rootSchema.plus(), _typeFactory, cancelFlag, request.CommandTimeoutSeconds * 1000L, boundParameters);
            var ctx = new PrepareContext(_typeFactory, _rootSchema, _config, dataContext, _defaultSchemaPath);

            var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
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
