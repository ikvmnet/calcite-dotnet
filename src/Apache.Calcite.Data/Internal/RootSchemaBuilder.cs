using System;
using System.Collections.Generic;
using System.IO;

using java.util;

using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.model;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Builds and configures a Calcite root <see cref="CalciteSchema"/> from the connection options.
    /// </summary>
    /// <remarks>
    /// This is a native re-implementation of the model/schema-bootstrap that
    /// <c>org.apache.calcite.jdbc.Driver</c> performs. It uses Calcite's own
    /// <see cref="ModelHandler"/> and <see cref="CalciteSchema"/> APIs — it does not go through the
    /// JDBC driver, <c>CalciteConnectionImpl</c>, or any <c>java.sql</c> classes.
    /// </remarks>
    internal sealed class RootSchemaBuilder
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
            [CalciteConnectionStringBuilder.TypeSystemKey] = CalciteConnectionProperty.TYPE_SYSTEM,
            [CalciteConnectionStringBuilder.TypeCoercionKey] = CalciteConnectionProperty.TYPE_COERCION,
        };

        readonly CalciteConnectionStringBuilder _options;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public RootSchemaBuilder(CalciteConnectionStringBuilder options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public CalciteSchema Build()
        {
            var rootSchema = CalciteSchema.createRootSchema(addMetadataSchema: true);
            ApplyModel(rootSchema.plus());
            return rootSchema;
        }

        public Properties BuildEngineProperties()
        {
            var props = new Properties();

            foreach (var key in _options.EnumerateKeys())
            {
                if (string.Equals(key, CalciteConnectionStringBuilder.ModelKey, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (_options.TryGetValue(key, out var v) && v is not null)
                    props.setProperty(KeyToProperty.TryGetValue(key, out var prop) ? prop.camelName() : key, v.ToString());
            }

            return props;
        }

        void ApplyModel(SchemaPlus root)
        {
            var model = _options.Model;
            if (string.IsNullOrEmpty(model))
                return;

            try
            {
                if (model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) || model.TrimStart().StartsWith("{"))
                {
                    var inline = model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) ? model.Substring("inline:".Length) : model;
                    new ModelHandler(root, "inline:" + inline);
                }
                else
                {
                    if (File.Exists(model) == false)
                        throw new FileNotFoundException("Model file was not found.", model);

                    new ModelHandler(root, model);
                }
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to load Calcite model.", e);
            }
        }

    }

}
