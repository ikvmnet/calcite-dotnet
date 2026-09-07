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
    /// This is the half of Calcite's connection that lives as long as the data source rather than the
    /// connection. Calcite's JDBC connection builds it in two steps — the root and <c>DUAL</c> in
    /// <c>CalciteConnectionImpl</c>'s constructor, the model in the driver's <c>onConnectionInit</c> —
    /// and this class is those two steps, in that order, so that a model can overwrite <c>DUAL</c> and never
    /// the reverse. What Calcite builds once per connection is built here once per data source, and every
    /// connection the data source opens shares it.
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
        /// <param name="rootSchema">Root schema to build on, or null to create one. Used verbatim, as
        /// upstream uses one handed to its connection, and <c>DUAL</c> and the model are applied to it all
        /// the same.</param>
        /// <exception cref="CalciteException">Thrown when the root or the model could not be built.</exception>
        public static CalciteDataSourceRoot Build(CalciteConnectionStringBuilder options, IReadOnlyList<Action<SchemaPlus>> configure, CalciteSchema? rootSchema = null)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(configure);

            try
            {
                var cfg = new CalciteConnectionConfigImpl(CalciteEngineProperties.Build(options));

                var root = rootSchema ?? CalciteSchema.createRootSchema(true);

                // Add dual table metadata when isSupportedDualTable return true
                if (cfg.conformance().isSupportedDualTable())
                {
                    SchemaPlus schemaPlus = root.plus();
                    // Dual table contains one row with a value X
                    schemaPlus.add(
                        "DUAL", ViewTable.viewMacro(schemaPlus, "VALUES ('X')",
                        ImmutableList.of(), null, java.lang.Boolean.valueOf(false)));
                }

                // the driver's ModelHandler step, run after the constructor's work as upstream runs it
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
        /// The model to read, which is the <c>Model</c> key where there is one, and otherwise a model written
        /// around the schema factory the <c>SchemaFactory</c> or <c>SchemaType</c> key names, holding every
        /// <c>schema.</c>-prefixed key as an operand. <c>Driver.createHandler().model</c>.
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
        /// Applies a Calcite model to the root schema, either from an inline JSON definition or a file path.
        /// </summary>
        /// <param name="rootSchema">The root schema to which the model will be applied.</param>
        /// <param name="model">Either an inline JSON model definition (prefixed with "inline:" or starting with "{") or a file path to a
        /// model definition.</param>
        /// <param name="defaultSchema">When this method returns, contains the default schema name defined in the model, or <see langword="null"/>
        /// if no default schema is defined.</param>
        /// <exception cref="FileNotFoundException">Thrown when the specified model file does not exist.</exception>
        /// <exception cref="CalciteException">Thrown when the model fails to load.</exception>
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
        /// Calcite's connection is driven by one thread and its root by one connection, so nothing in Calcite
        /// guards the root: a <c>CalciteSchema</c> keeps its tables and sub-schemas in <c>NameMap</c>s over
        /// <c>TreeMap</c>s, and a DDL statement writes into them while a statement planning reads them. A
        /// root shared by connections used concurrently needs the reader-writer shape: planning — the
        /// snapshot, validation, optimisation, implementation — under the read lock, many at a time, and DDL
        /// under the write lock, alone. The lock is thread-affine, so it spans planning, which is one thread's
        /// work, and not execution: a table is resolved again from the snapshot when a plan runs, the
        /// snapshot shares its table map with the live root by Calcite's own javadoc, and an asynchronous
        /// read cannot hold a lock across its awaits. That lookup against a concurrent DDL is the exposure
        /// that stays open, and it is Calcite's.
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
        /// Gets when this root last had no session holding it, as <see cref="Environment.TickCount64"/> —
        /// its construction, or the last <see cref="Release"/> that left it with none.
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
        /// Marks this root as done with, disposing it now where no session holds it and otherwise when
        /// the last one lets go.
        /// </summary>
        /// <remarks>
        /// This is what a data source does to a root it drops — on <see cref="CalciteDataSource.Clear"/>,
        /// on its own disposal, and when the provider evicts or prunes it — and what a session does to a
        /// root built for it alone. A connection still open keeps working: the root goes when the last
        /// session on it is disposed, the way a pooled connector Npgsql has cleared is closed when it is
        /// returned rather than while it is busy.
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
        /// Calcite has no disposal hook for a schema: a <c>SchemaFactory</c> builds one and nothing ever
        /// tells it the schema is done with, so an adapter holding a client holds it for the life of the
        /// process. A root has a lifetime here, so the schemas on it get one too — a schema that implements
        /// <see cref="IDisposable"/> is disposed when its root is, which is when it has been
        /// <see cref="Retire">retired</see> and no session holds it.
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
