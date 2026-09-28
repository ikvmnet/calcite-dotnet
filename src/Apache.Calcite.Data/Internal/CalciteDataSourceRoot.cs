using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using com.google.common.collect;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.model;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.util;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A root schema built from a connection string: Calcite's root, <c>DUAL</c> where the conformance has
    /// one, the model, and any schemas a <see cref="CalciteDataSource"/> was asked to hold.
    /// </summary>
    /// <remarks>
    /// The part of Calcite's JDBC connection that lives as long as the data source: Calcite builds the root
    /// and <c>DUAL</c> in <c>CalciteConnectionImpl</c>'s constructor and then applies the model in the
    /// driver's <c>onConnectionInit</c>. <see cref="Build"/> runs the same two steps in the same order, so a
    /// model can replace <c>DUAL</c>. Calcite does this once per connection; here it is done once per data
    /// source, and every connection the data source opens shares the result.
    /// </remarks>
    internal sealed class CalciteDataSourceRoot
    {

        readonly CalciteSchema _schema;
        readonly string? _defaultSchemaName;
        bool _disposed;

        CalciteDataSourceRoot(CalciteSchema schema, string? defaultSchemaName)
        {
            _schema = schema;
            _defaultSchemaName = defaultSchemaName;
        }

        /// <summary>
        /// Builds a root schema.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="configure">Steps to run over the root after the model, in order.</param>
        /// <param name="rootSchema">Root schema to build on, or <see langword="null"/> to create one.
        /// <c>DUAL</c> and the model are added to it either way, as Calcite does with a root handed to its
        /// connection.</param>
        /// <exception cref="CalciteException">Thrown when the root or the model could not be built.</exception>
        public static CalciteDataSourceRoot Build(CalciteConnectionStringBuilder options, IReadOnlyList<Action<SchemaPlus>> configure, CalciteSchema? rootSchema = null)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(configure);

            try
            {
                var cfg = new CalciteConnectionConfigImpl(CalciteEngineProperties.Build(options));

                var root = rootSchema ?? CalciteSchema.createRootSchema(true);

                if (cfg.conformance().isSupportedDualTable())
                {
                    SchemaPlus schemaPlus = root.plus();
                    // Dual table contains one row with a value X
                    schemaPlus.add(
                        "DUAL", ViewTable.viewMacro(schemaPlus, "VALUES ('X')",
                        ImmutableList.of(), null, java.lang.Boolean.valueOf(false)));
                }

                // the model after DUAL, as Calcite's driver applies it after the connection's constructor
                string? defaultSchemaName = null;
                var model = Model(options, cfg);
                if (model != null)
                    ApplyModel(root, model, out defaultSchemaName);

                foreach (var step in configure)
                    step(root.plus());

                return new CalciteDataSourceRoot(root, defaultSchemaName);
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite.", e);
            }
        }

        /// <summary>
        /// Returns the model to load: the <c>Model</c> key where it is set, otherwise an inline model with one
        /// custom schema over the factory the <c>SchemaFactory</c> or <c>SchemaType</c> key names, taking every
        /// <c>schema.</c>-prefixed key as an operand, otherwise <see langword="null"/>. Mirrors the model
        /// resolution in Calcite's <c>Driver.createHandler</c>.
        /// </summary>
        static string? Model(CalciteConnectionStringBuilder options, CalciteConnectionConfig config)
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

            if (schemaFactory != null)
            {
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

            return null;
        }

        /// <summary>
        /// Applies a Calcite model to the root schema, from inline JSON or a file.
        /// </summary>
        /// <param name="rootSchema">The root schema to apply the model to.</param>
        /// <param name="model">Inline JSON, prefixed with <c>inline:</c> or starting with <c>{</c>, or the path
        /// of a model file.</param>
        /// <param name="defaultSchema">When this method returns, the model's default schema name, or
        /// <see langword="null"/> where it names none.</param>
        /// <exception cref="CalciteException">The model file does not exist or the model fails to load. A missing
        /// file is reported as an inner <see cref="FileNotFoundException"/>.</exception>
        static void ApplyModel(CalciteSchema rootSchema, string model, out string? defaultSchema)
        {
            try
            {
                if (model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) || model.TrimStart().StartsWith("{"))
                {
                    var inline = model.StartsWith("inline:", StringComparison.OrdinalIgnoreCase) ? model.Substring("inline:".Length) : model;
                    var handler = new ModelHandler(rootSchema.plus(), "inline:" + inline);
                    defaultSchema = handler.defaultSchemaName();
                }
                else
                {
                    if (!File.Exists(model))
                        throw new FileNotFoundException("Model file was not found.", model);

                    var handler = new ModelHandler(rootSchema.plus(), model);
                    defaultSchema = handler.defaultSchemaName();
                }
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to load Calcite model.", e);
            }
        }

        /// <summary>
        /// Gets the root schema.
        /// </summary>
        public CalciteSchema Schema => _schema;

        /// <summary>
        /// Gets the default schema the model named, or <see langword="null"/> where it named none.
        /// </summary>
        public string? DefaultSchemaName => _defaultSchemaName;

        readonly object _sync = new();
        int _users;
        bool _retired;
        long _idleSince = Environment.TickCount64;

        /// <summary>
        /// Gets the lock a statement plans under and DDL alters the root under.
        /// </summary>
        /// <remarks>
        /// Calcite does not guard a root against concurrent use: a <c>CalciteSchema</c> keeps its tables and
        /// sub-schemas in <c>NameMap</c>s over <c>TreeMap</c>s, and DDL writes into them. Planning (snapshot,
        /// validation, optimization, implementation) takes the read lock, so many statements plan at once;
        /// DDL takes the write lock. The lock is thread-affine, so it covers planning, which runs on one
        /// thread, and not execution, which may resume on others across awaits. A running plan resolves
        /// tables again from its snapshot, and the snapshot shares its table map with the live root, so a
        /// table lookup during execution is not protected against concurrent DDL.
        /// </remarks>
        public ReaderWriterLockSlim Lock { get; } = new(LockRecursionPolicy.SupportsRecursion);

        /// <summary>
        /// Gets the number of sessions holding this root.
        /// </summary>
        public int Users
        {
            get { lock (_sync) return _users; }
        }

        /// <summary>
        /// Gets the <see cref="Environment.TickCount64"/> at which this root last became unused: its
        /// construction, or the last <see cref="Release"/> that left no session holding it.
        /// </summary>
        public long IdleSince
        {
            get { lock (_sync) return _idleSince; }
        }

        /// <summary>
        /// Counts a session onto this root.
        /// </summary>
        public void Acquire()
        {
            lock (_sync)
                _users++;
        }

        /// <summary>
        /// Counts a session off this root, disposing it where it has been retired and this was the last.
        /// </summary>
        public void Release()
        {
            bool dispose;
            lock (_sync)
            {
                _users--;
                if (_users == 0)
                    _idleSince = Environment.TickCount64;

                dispose = _users == 0 && _retired;
            }

            if (dispose)
                Dispose();
        }

        /// <summary>
        /// Marks this root as no longer wanted, disposing it now where no session holds it and otherwise when
        /// the last session releases it.
        /// </summary>
        /// <remarks>
        /// A data source retires its root on <see cref="CalciteDataSource.Clear"/>, on disposal, and when the
        /// provider evicts or prunes it; a session retires a root built for it alone. Connections still open
        /// on the root keep working until they are disposed.
        /// </remarks>
        public void Retire()
        {
            bool dispose;
            lock (_sync)
            {
                _retired = true;
                dispose = _users == 0;
            }

            if (dispose)
                Dispose();
        }

        /// <summary>
        /// Disposes every schema on the root that can be disposed, sub-schemas first.
        /// </summary>
        /// <remarks>
        /// Calcite has no disposal hook for a schema. A schema that implements <see cref="IDisposable"/> is
        /// disposed here, when its root has been <see cref="Retire">retired</see> and no session holds it, so
        /// an adapter holding a client can release it.
        /// </remarks>
        void Dispose()
        {
            lock (_sync)
            {
                if (_disposed)
                    return;

                _disposed = true;
            }

            DisposeSchemas(_schema);
            Lock.Dispose();
        }

        static void DisposeSchemas(CalciteSchema schema)
        {
            for (var it = schema.getSubSchemaMap().values().iterator(); it.hasNext();)
                DisposeSchemas((CalciteSchema)it.next());

            if (schema.schema is IDisposable disposable)
                disposable.Dispose();
        }

    }

}
