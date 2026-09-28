using System;
using System.Collections.Generic;

using java.util;

using org.apache.calcite.config;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Turns a connection string into the <see cref="Properties"/> that configure Calcite's
    /// <c>CalciteConnectionConfigImpl</c>.
    /// </summary>
    internal static class CalciteEngineProperties
    {

        /// <summary>
        /// Maps each <see cref="CalciteConnectionStringBuilder"/> key to its <see cref="CalciteConnectionProperty"/>,
        /// whose <c>camelName()</c> is the property name Calcite reads.
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
            [CalciteConnectionStringBuilder.TopDownGeneralDecorrelationEnabledKey] = CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED,
            [CalciteConnectionStringBuilder.TypeCoercionKey] = CalciteConnectionProperty.TYPE_COERCION,
        };

        /// <summary>
        /// Builds Calcite connection properties from connection string options.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <returns>The properties.</returns>
        /// <remarks>
        /// A known key is renamed to Calcite's camelCase property name; an unknown key is passed through as
        /// written, so any Calcite property can be set. <c>Model</c>, <c>Pooling</c>,
        /// <c>Connection Idle Lifetime</c>, <c>Connection Pruning Interval</c> and <c>TypeSystem</c> are the
        /// provider's own and are left out.
        /// </remarks>
        public static Properties Build(CalciteConnectionStringBuilder options)
        {
            var props = new Properties();

            foreach (var key in options.EnumerateKeys())
            {
                if (string.Equals(key, CalciteConnectionStringBuilder.ModelKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                // provider options, not engine ones: whether connections share a root schema, and for how
                // long the provider keeps one nobody is using
                if (string.Equals(key, CalciteConnectionStringBuilder.PoolingKey, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (string.Equals(key, CalciteConnectionStringBuilder.ConnectionIdleLifetimeKey, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (string.Equals(key, CalciteConnectionStringBuilder.ConnectionPruningIntervalKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                // a provider option, not an engine one: it names a .NET type, which ClrPlugin resolves when
                // the session builds its type factory
                if (string.Equals(key, CalciteConnectionStringBuilder.TypeSystemKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (options.TryGetValue(key, out var v) && v is not null)
                    props.setProperty(KeyToProperty.TryGetValue(key, out var prop) ? prop.camelName() : key, v.ToString());
            }

            return props;
        }

    }

}
