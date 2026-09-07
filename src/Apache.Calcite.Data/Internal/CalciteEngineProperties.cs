using System;
using System.Collections.Generic;

using java.util;

using org.apache.calcite.config;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Turns a connection string into the <see cref="Properties"/> Calcite's own connection is configured by.
    /// </summary>
    internal static class CalciteEngineProperties
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
            [CalciteConnectionStringBuilder.TypeCoercionKey] = CalciteConnectionProperty.TYPE_COERCION,
        };

        /// <summary>
        /// Builds a Java Properties object from connection string options, mapping keys to their camel-cased property
        /// names and excluding the keys that are the provider's rather than the engine's.
        /// </summary>
        /// <param name="options">The connection string builder containing the options to convert.</param>
        /// <returns>A Properties object populated with the connection string options.</returns>
        public static Properties Build(CalciteConnectionStringBuilder options)
        {
            var props = new Properties();

            foreach (var key in options.EnumerateKeys())
            {
                if (string.Equals(key, CalciteConnectionStringBuilder.ModelKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                // a provider option, not an engine one: it chooses the convention the session plans into
                if (string.Equals(key, CalciteConnectionStringBuilder.SynchronousKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                // provider options, not engine ones: whether connections share a root schema, and for how
                // long the provider keeps one nobody is using
                if (string.Equals(key, CalciteConnectionStringBuilder.PoolingKey, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (string.Equals(key, CalciteConnectionStringBuilder.ConnectionIdleLifetimeKey, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (string.Equals(key, CalciteConnectionStringBuilder.ConnectionPruningIntervalKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                // a provider option, not an engine one: it names a .NET type, resolved by ClrPlugin, and
                // nothing in calcite-core reads the engine property but the constructor line this ports
                if (string.Equals(key, CalciteConnectionStringBuilder.TypeSystemKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (options.TryGetValue(key, out var v) && v is not null)
                    props.setProperty(KeyToProperty.TryGetValue(key, out var prop) ? prop.camelName() : key, v.ToString());
            }

            return props;
        }

    }

}
