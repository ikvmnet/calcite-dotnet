using System;
using System.IO;

using com.fasterxml.jackson.core;
using com.fasterxml.jackson.databind;
using com.fasterxml.jackson.dataformat.yaml;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.config;
using org.apache.calcite.model;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.util;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A Calcite model, read and parsed once: the JSON or YAML document the <c>Model</c> key names, or the
    /// one the <c>SchemaFactory</c> and <c>SchemaType</c> keys stand for.
    /// </summary>
    /// <remarks>
    /// Calcite's <c>ModelHandler</c> reads a model and applies it to a root in its constructor. Here the two
    /// are apart: this class is the reading, a port of that constructor's first half, and
    /// <see cref="CalciteModelAssembler"/> is the applying, done for every root a data source assembles. The
    /// document is read once per data source, so every root is built from the same text even if a model file
    /// changes; <see cref="CalciteDataSource.Clear"/> reads it again.
    /// </remarks>
    internal sealed class CalciteModel
    {

        static readonly ObjectMapper JsonMapper = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        static readonly ObjectMapper YamlMapper = new YAMLMapper();

        CalciteModel(string uri, JsonRoot root)
        {
            Uri = uri;
            Root = root;
        }

        /// <summary>
        /// Gets the model's URI as Calcite's <c>ModelHandler</c> receives it: <c>inline:</c> followed by the
        /// document, or the path of a file. Adapters receive it as the <c>modelUri</c> operand.
        /// </summary>
        public string Uri { get; }

        /// <summary>
        /// Gets the parsed document.
        /// </summary>
        public JsonRoot Root { get; }

        /// <summary>
        /// Gets the default schema the model names, or <see langword="null"/> where it names none.
        /// </summary>
        public string? DefaultSchemaName => Root.defaultSchema;

        /// <summary>
        /// Returns the model a connection string asks for: the <c>Model</c> key where it is set, otherwise an
        /// inline model with one custom schema over the factory the <c>SchemaFactory</c> or <c>SchemaType</c>
        /// key names, taking every <c>schema.</c>-prefixed key as an operand, otherwise
        /// <see langword="null"/>.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="config">The configuration built from them.</param>
        /// <returns>The model, read and parsed, or <see langword="null"/> where there is none.</returns>
        /// <exception cref="CalciteException">The model could not be read or parsed. A missing file is reported
        /// as an inner <see cref="FileNotFoundException"/>.</exception>
        /// <remarks>
        /// Mirrors the model resolution in Calcite's <c>Driver.createHandler</c>.
        /// </remarks>
        public static CalciteModel? Resolve(CalciteConnectionStringBuilder options, CalciteConnectionConfig config)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(config);

            var uri = ModelUri(options, config);
            if (uri is null)
                return null;

            try
            {
                return Load(uri);
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to load Calcite model.", e);
            }
        }

        /// <summary>
        /// Returns the URI of the model a connection string asks for, synthesizing an inline one for the
        /// <c>SchemaFactory</c> and <c>SchemaType</c> keys.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="config">The configuration built from them.</param>
        /// <returns>The URI, or <see langword="null"/> where there is no model.</returns>
        static string? ModelUri(CalciteConnectionStringBuilder options, CalciteConnectionConfig config)
        {
            var model = options.Model;
            if (string.IsNullOrEmpty(model) == false)
                return model;

            var schemaFactory = (SchemaFactory?)config.schemaFactory((java.lang.Class)typeof(SchemaFactory), null);
            var info = CalciteEngineProperties.Build(options);
            var schemaName = Util.first(config.schema(), "adhoc");
            if (schemaFactory == null)
            {
                var schemaType = config.schemaType();
                if (schemaType != null)
                {
                    switch (schemaType.name())
                    {
                        case nameof(JsonSchema.Type.JDBC):
                            schemaFactory = JdbcSchema.Factory.INSTANCE;
                            break;
                        case nameof(JsonSchema.Type.MAP):
                            schemaFactory = AbstractSchema.Factory.INSTANCE;
                            break;
                        default:
                            break;
                    }
                }
            }

            if (schemaFactory == null)
                return null;

            var json = new JsonBuilder();
            var root = json.map();
            root.put("version", "1.0");
            root.put("defaultSchema", schemaName);
            var schemaList = json.list();
            root.put("schemas", schemaList);
            var schema = json.map();
            schemaList.add(schema);
            schema.put("type", "custom");
            schema.put("name", schemaName);
            schema.put("factory", ikvm.runtime.Util.getClassFromObject(schemaFactory).getName());
            var operandMap = json.map();
            schema.put("operand", operandMap);
            for (var it = Util.toMap(info).entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                var key = (string)entry.getKey();
                if (key.StartsWith("schema.", StringComparison.Ordinal))
                    operandMap.put(key.Substring("schema.".Length), entry.getValue());
            }

            return "inline:" + json.toJsonString(root);
        }

        /// <summary>
        /// Reads and parses a model.
        /// </summary>
        /// <param name="model">Inline JSON or YAML prefixed with <c>inline:</c>, inline JSON starting with
        /// <c>{</c>, or the path of a model file, read as YAML where it ends in <c>.yaml</c> or <c>.yml</c>
        /// and as JSON otherwise.</param>
        /// <returns>The model.</returns>
        /// <exception cref="FileNotFoundException">The model file does not exist.</exception>
        /// <exception cref="java.lang.SecurityException"><c>calcite.model.baseDirectory</c> is set and the file
        /// resolves outside it.</exception>
        /// <remarks>
        /// <para>
        /// Calcite's <c>ModelHandler</c> constructor, as far as the parse: the same Jackson configuration, the
        /// same choice between JSON and YAML, and the same confinement to <c>calcite.model.baseDirectory</c>.
        /// A model that starts with <c>{</c> and has no <c>inline:</c> prefix is taken as inline JSON, as the
        /// <c>Model</c> key has always accepted.
        /// </para>
        /// <para>
        /// Where a model file cannot be read or parsed, Calcite throws a message that names only the path and
        /// logs the cause, so that a server does not show its clients a file's contents or whether it exists.
        /// Here the caller is the process that named the file, so the cause is thrown as it is.
        /// </para>
        /// </remarks>
        public static CalciteModel Load(string model)
        {
            ArgumentNullException.ThrowIfNull(model);

            if (model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) || model.TrimStart().StartsWith('{'))
            {
                var inline = model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) ? model.Substring("inline:".Length) : model;

                // trim here is to correctly autodetect if it is json or not in case of leading spaces
                var trimmed = inline.Trim();
                var mapper = trimmed.StartsWith("/*", StringComparison.Ordinal) || trimmed.StartsWith('{') ? JsonMapper : YamlMapper;
                return new CalciteModel("inline:" + inline, (JsonRoot)mapper.readValue(trimmed, (java.lang.Class)typeof(JsonRoot)));
            }
            else
            {
                var path = ModelFile((string)CalciteSystemProperty.MODEL_BASE_DIRECTORY.value(), model);
                if (File.Exists(path) == false)
                    throw new FileNotFoundException("Model file was not found.", path);

                var mapper = model.EndsWith(".yaml", StringComparison.Ordinal) || model.EndsWith(".yml", StringComparison.Ordinal) ? YamlMapper : JsonMapper;
                return new CalciteModel(model, (JsonRoot)mapper.readValue(new java.io.File(path), (java.lang.Class)typeof(JsonRoot)));
            }
        }

        /// <summary>
        /// Resolves a model path, enforcing <c>calcite.model.baseDirectory</c> where it is set. A port of
        /// <c>ModelHandler.modelFile</c>.
        /// </summary>
        /// <remarks>
        /// An empty base directory means no restriction. Otherwise a relative path resolves under the base
        /// directory, and a path that resolves, lexically, outside it is refused.
        /// </remarks>
        /// <param name="baseDirectory">The value of <c>calcite.model.baseDirectory</c>, empty where unset.</param>
        /// <param name="uri">The model's path.</param>
        /// <returns>The path to read.</returns>
        /// <exception cref="java.lang.SecurityException">The path resolves outside the base directory.</exception>
        static string ModelFile(string baseDirectory, string uri)
        {
            if (baseDirectory.Length == 0)
                return uri;

            var basePath = System.IO.Path.TrimEndingDirectorySeparator(System.IO.Path.GetFullPath(baseDirectory));
            var path = System.IO.Path.GetFullPath(System.IO.Path.Combine(basePath, uri));
            var inside = path.Length == basePath.Length
                ? string.Equals(path, basePath, StringComparison.Ordinal)
                : path.StartsWith(basePath + System.IO.Path.DirectorySeparatorChar, StringComparison.Ordinal);
            if (inside == false)
                throw new java.lang.SecurityException($"Model file '{uri}' resolves outside the configured base directory (system property 'calcite.model.baseDirectory')");

            return path;
        }

    }

}
