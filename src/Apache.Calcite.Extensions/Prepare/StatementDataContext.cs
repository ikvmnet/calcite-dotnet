using System;
using System.Collections.Generic;
using System.Threading;

using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.prepare;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;
using org.apache.calcite.sql.advise;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.validate;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// The <see cref="DataContext"/> a statement executes against.
    /// </summary>
    /// <remarks>
    /// Holds the statement's runtime values in the form Calcite's generated code reads them: the query start
    /// time as the timestamp variables, the connection's time zone as a <c>java.util.TimeZone</c>, its locale
    /// as a <c>java.util.Locale</c>, the timeout as a <c>java.lang.Long</c> of milliseconds, the bound
    /// parameter values, and the values stashed during planning.
    ///
    /// <para>Calcite code observes cancellation through <c>DataContext.Variable.CANCEL_FLAG</c>, an
    /// <see cref="AtomicBoolean"/> that some tables poll (<c>EnumerableDefaults</c> operators do not). The
    /// statement's <see cref="CancellationToken"/> sets the flag through a registration, because
    /// <c>AtomicBoolean.get()</c> is final and cannot be made to read the token. Disposing releases the
    /// registration. The flag is set however the rows are read, so it cancels Calcite sub-plans under a
    /// synchronously read plan too.</para>
    /// </remarks>
    internal sealed class StatementDataContext : DataContext, IDisposable
    {

        readonly CancellationTokenRegistration _cancelRegistration;

        readonly CalciteSchema? _rootSchema;
        readonly JavaTypeFactory _typeFactory;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        readonly Dictionary<string, object?> _map;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rootSchema">The schema the statement was planned against, or <see langword="null"/>
        /// for a DDL signature, which has no plan to run.</param>
        /// <param name="typeFactory">The type factory the statement was planned with.</param>
        /// <param name="config">The connection configuration, which supplies the time zone and locale.</param>
        /// <param name="defaultSchemaPath">The default schema path, which the SQL advisor resolves against.</param>
        /// <param name="cancellationToken">The statement's cancellation token, which sets the cancel
        /// flag.</param>
        /// <param name="queryTimeoutMillis">The query timeout in milliseconds, or zero for none.</param>
        /// <param name="parameters">Bound positional query parameters, addressed as <c>?0</c>, <c>?1</c>, ….</param>
        /// <param name="internalParameters">The values stashed during planning, which the compiled plan reads
        /// by name, or <see langword="null"/>.</param>
        public StatementDataContext(
            CalciteSchema? rootSchema,
            JavaTypeFactory typeFactory,
            CalciteConnectionConfig config,
            IReadOnlyList<string> defaultSchemaPath,
            CancellationToken cancellationToken,
            long queryTimeoutMillis,
            IReadOnlyList<object?> parameters,
            java.util.Map? internalParameters = null)
        {
            _rootSchema = rootSchema;
            _typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));

            var cancelFlag = new AtomicBoolean(false);
            _cancelRegistration = cancellationToken.CanBeCanceled
                ? cancellationToken.Register(static state => ((AtomicBoolean)state!).set(true), cancelFlag)
                : default;

            _config = config ?? throw new ArgumentNullException(nameof(config));
            _defaultSchemaPath = defaultSchemaPath ?? [];

            // the query start time, which CURRENT_TIMESTAMP and the other timestamp variables report for the
            // whole statement; Hook.CURRENT_TIME lets a test set it
            var timeHolder = Holder.of(java.lang.Long.valueOf(java.lang.System.currentTimeMillis()));
            Hook.CURRENT_TIME.run(timeHolder);
            var time = ((java.lang.Long)timeHolder.get()).longValue();

            var timeZone = java.util.TimeZone.getTimeZone(_config.timeZone());
            var timeFrameSet = _typeFactory.getTypeSystem().deriveTimeFrameSet(TimeFrames.CORE);
            var localOffset = (long)timeZone.getOffset(time);
            var currentOffset = localOffset;
            var sysOffset = (long)java.util.TimeZone.getDefault().getOffset(time);

            var localeName = _config.locale();
            var locale = localeName != null ? Util.parseLocale(localeName) : java.util.Locale.ROOT;

            var streamHolder = Holder.of(new object[] { java.lang.System.@in, java.lang.System.@out, java.lang.System.err });
            Hook.STANDARD_STREAMS.run(streamHolder);
            var streams = (object[])streamHolder.get();

            _map = new Dictionary<string, object?>
            {
                { DataContext.Variable.UTC_TIMESTAMP.camelName, java.lang.Long.valueOf(time) },
                { DataContext.Variable.CURRENT_TIMESTAMP.camelName, java.lang.Long.valueOf(time + currentOffset) },
                { DataContext.Variable.LOCAL_TIMESTAMP.camelName, java.lang.Long.valueOf(time + localOffset) },
                { DataContext.Variable.SYS_TIMESTAMP.camelName, java.lang.Long.valueOf(time + sysOffset) },
                { DataContext.Variable.TIME_ZONE.camelName, timeZone },
                { DataContext.Variable.TIME_FRAME_SET.camelName, timeFrameSet },
                { DataContext.Variable.USER.camelName, "sa" },
                { DataContext.Variable.SYSTEM_USER.camelName, java.lang.System.getProperty("user.name") },
                { DataContext.Variable.LOCALE.camelName, locale },
                { DataContext.Variable.STDIN.camelName, streams[0] },
                { DataContext.Variable.STDOUT.camelName, streams[1] },
                { DataContext.Variable.STDERR.camelName, streams[2] },
                { DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag },
                { DataContext.Variable.TIMEOUT.camelName, java.lang.Long.valueOf(queryTimeoutMillis) },
            };

            // Calcite reads positional dynamic parameters from the same map as "?0", "?1", ...
            for (var i = 0; i < parameters.Count; i++)
                _map["?" + i] = parameters[i] ?? DummyValue;

            if (internalParameters != null)
            {
                for (var i = internalParameters.entrySet().iterator(); i.hasNext();)
                {
                    var entry = (java.util.Map.Entry)i.next();
                    _map[(string)entry.getKey()] = entry.getValue() ?? DummyValue;
                }
            }
        }

        /// <summary>
        /// Stored in the map for a value that is present and null, to distinguish it from an absent name.
        /// </summary>
        static object DummyValue => org.apache.calcite.avatica.AvaticaSite.DUMMY_VALUE;

        /// <inheritdoc />
        public SchemaPlus? getRootSchema() => _rootSchema?.plus();

        /// <inheritdoc />
        public JavaTypeFactory getTypeFactory() => _typeFactory;

        /// <summary>
        /// Returns the query provider a plan runs a <c>Queryable</c> through.
        /// </summary>
        /// <remarks>
        /// Calcite's <c>DataContextImpl</c> returns its connection, which is a <c>QueryProvider</c>; with no
        /// connection, <c>Linq4j.DEFAULT_PROVIDER</c> delegates to <c>queryable.enumerator()</c> in the same
        /// way.
        /// </remarks>
        public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;

        /// <inheritdoc />
        public object? get(string name)
        {
            lock (_map)
            {
                _map.TryGetValue(name, out var o);

                if (ReferenceEquals(o, DummyValue))
                    return null;

                if (o is null && DataContext.Variable.SQL_ADVISOR.camelName == name)
                    return GetSqlAdvisor();

                return o;
            }
        }

        /// <summary>
        /// Builds the SQL advisor returned for the <c>sqlAdvisor</c> variable.
        /// </summary>
        SqlAdvisor GetSqlAdvisor()
        {
            var rootSchema = _rootSchema ?? throw new java.lang.IllegalStateException("rootSchema");

            var schemaPath = new java.util.ArrayList(_defaultSchemaPath.Count);
            foreach (var s in _defaultSchemaPath)
                schemaPath.add(s);

            var validator = new SqlAdvisorValidator(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.instance(),
                new CalciteCatalogReader(rootSchema, schemaPath, _typeFactory, _config),
                _typeFactory,
                SqlValidator.Config.DEFAULT);

            // duplicates the parser configuration in ClrPrepareImpl.Prepare2_, as Calcite's copy does
            var parserConfig = SqlParser.config()
                .withQuotedCasing(_config.quotedCasing())
                .withUnquotedCasing(_config.unquotedCasing())
                .withQuoting(_config.quoting())
                .withConformance((SqlConformance)_config.conformance())
                .withCaseSensitive(_config.caseSensitive());

            return new SqlAdvisor(validator, parserConfig);
        }

        /// <summary>
        /// Releases the registration that sets the cancel flag from the statement's token.
        /// </summary>
        public void Dispose()
        {
            _cancelRegistration.Dispose();
        }

    }

}
