using System;
using System.Collections.Generic;

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
    internal sealed class StatementDataContext : DataContext
    {

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
        /// <param name="cancelFlag">The flag a running plan polls at its check points.</param>
        /// <param name="queryTimeoutMillis">The query timeout in milliseconds, or zero for none.</param>
        /// <param name="parameters">Bound positional query parameters, addressed as <c>?0</c>, <c>?1</c>, ….</param>
        /// <param name="internalParameters">The values planning stashed, which the compiled plan reads back
        /// by name.</param>
        public StatementDataContext(
            CalciteSchema? rootSchema,
            JavaTypeFactory typeFactory,
            CalciteConnectionConfig config,
            IReadOnlyList<string> defaultSchemaPath,
            AtomicBoolean cancelFlag,
            long queryTimeoutMillis,
            IReadOnlyList<object?> parameters,
            java.util.Map? internalParameters = null)
        {
            _rootSchema = rootSchema;
            _typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _defaultSchemaPath = defaultSchemaPath ?? [];

            // the time the query started, which the SQL standard requires CURRENT_TIMESTAMP and friends to
            // report unchanged for its whole duration; the hook is how a test moves the clock
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

            // Calcite addresses positional dynamic parameters as "?0", "?1", … and puts them in the same map
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
        /// Stands in the map for a value that is present and null, so that an absent name and a null one are
        /// told apart.
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
        /// <c>DataContextImpl</c> answers the connection, which is itself a <c>QueryProvider</c>.
        /// There is no connection here, and <c>Linq4j.DEFAULT_PROVIDER</c> is the same delegation to
        /// <c>queryable.enumerator()</c> without one.
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
        /// Builds the SQL advisor the <c>sqlAdvisor</c> variable answers.
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

            // this duplicates ClrPrepareImpl.Prepare2, as Calcite's own comment says of its copy
            var parserConfig = SqlParser.config()
                .withQuotedCasing(_config.quotedCasing())
                .withUnquotedCasing(_config.unquotedCasing())
                .withQuoting(_config.quoting())
                .withConformance((SqlConformance)_config.conformance())
                .withCaseSensitive(_config.caseSensitive());

            return new SqlAdvisor(validator, parserConfig);
        }

    }

}
