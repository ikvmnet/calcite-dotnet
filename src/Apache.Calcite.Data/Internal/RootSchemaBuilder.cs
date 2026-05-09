using System;
using System.Collections.Generic;
using System.IO;

using java.util;

using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.model;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data
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

        readonly CalciteConnectionStringBuilder _options;

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
                    continue; // model is materialized into the schema, not propagated

                if (_options.TryGetValue(key, out var v) && v is not null)
                    props.setProperty(MapToCalciteProperty(key), v.ToString());
            }

            if (string.IsNullOrEmpty(_options.Schema) == false)
                props.setProperty(CalciteConnectionProperty.SCHEMA.camelName(), _options.Schema);

            if (_options.CaseSensitive is bool cs)
                props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), cs ? "true" : "false");

            if (string.IsNullOrEmpty(_options.Conformance) == false)
                props.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(), _options.Conformance);

            return props;
        }

        void ApplyModel(SchemaPlus root)
        {
            var model = _options.Model;
            if (string.IsNullOrEmpty(model))
                return;

            try
            {
                if (model!.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) || model.TrimStart().StartsWith("{"))
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

        static string MapToCalciteProperty(string key)
        {
            // The connection-string keys defined on CalciteConnectionStringBuilder happen to use
            // PascalCase that matches Calcite's camelCase property names when first-lowered.
            if (string.IsNullOrEmpty(key))
                return key;

            return char.ToLowerInvariant(key[0]) + key.Substring(1);
        }

    }

}
