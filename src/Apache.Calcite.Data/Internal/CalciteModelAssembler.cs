using System;
using System.Collections.Generic;

using com.google.common.collect;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.materialize;
using org.apache.calcite.model;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Applies a <see cref="CalciteModel"/> to a root schema: a port of the <c>visit</c> methods of Calcite's
    /// <c>ModelHandler</c>, run once for every root a data source assembles.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite applies the model to each connection's own root, so everything a model declares is created
    /// against the root it will be planned with: a view is analysed against that root, a lattice and a
    /// materialization belong to it, and a table that connection creates is visible to them. This does the
    /// same, element by element and in Calcite's order.
    /// </para>
    /// <para>
    /// One divergence: what a factory makes is made once per data source and added to every root, rather
    /// than made again for each. That is the schema a <c>custom</c> schema's factory returns, the
    /// <c>JdbcSchema</c> a <c>jdbc</c> schema becomes, and the table a <c>custom</c> table's factory
    /// returns. Those hold an adapter's clients and caches, and a data source shares them between its
    /// connections. A factory is therefore given the first root's schema as the parent, and an adapter that
    /// keeps that parent and reads it, rather than building expressions from its path as
    /// <c>JdbcSchema</c> does, sees the first root.
    /// </para>
    /// <para>
    /// <c>ModelHandler</c> dispatches through <c>accept</c> methods on the model's classes, which take a
    /// <c>ModelHandler</c>; here the elements are matched by type in the order its <c>visitChildren</c>
    /// methods visit them.
    /// </para>
    /// <para>
    /// <c>ModelHandler</c> passes every class a model names through <c>ClassNameFilter</c>, which refuses a
    /// class unless <c>calcite.model.classes.allowed</c> names it. No class is filtered here: a model names
    /// the classes the application asks for, and the application chooses its model.
    /// </para>
    /// </remarks>
    internal sealed class CalciteModelAssembler
    {

        readonly CalciteModel _model;
        readonly object _sync = new();
        readonly Dictionary<object, object> _made = new(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="model">The model to apply.</param>
        public CalciteModelAssembler(CalciteModel model)
        {
            _model = model ?? throw new ArgumentNullException(nameof(model));
        }

        /// <summary>
        /// Gets the schemas the model's factories have made, which belong to the data source rather than to
        /// any one root.
        /// </summary>
        public IReadOnlyList<Schema> Schemas
        {
            get
            {
                lock (_sync)
                {
                    var schemas = new List<Schema>();
                    foreach (var made in _made.Values)
                        if (made is Schema schema)
                            schemas.Add(schema);

                    return schemas;
                }
            }
        }

        /// <summary>
        /// Applies the model to a root: its types, then its schemas, each with everything it declares.
        /// </summary>
        /// <param name="rootSchema">The root.</param>
        /// <exception cref="java.lang.RuntimeException">An element could not be created, as Calcite reports it:
        /// <c>Error instantiating</c> the element, with the cause.</exception>
        public void Apply(SchemaPlus rootSchema)
        {
            ArgumentNullException.ThrowIfNull(rootSchema);

            // visit(JsonRoot)
            for (var it = _model.Root.types.iterator(); it.hasNext();)
                AddType(rootSchema, null, (JsonType)it.next());

            for (var it = _model.Root.schemas.iterator(); it.hasNext();)
            {
                switch (it.next())
                {
                    // JsonCustomSchema extends JsonMapSchema, so it is matched first
                    case JsonCustomSchema custom:
                        AddCustomSchema(rootSchema, custom);
                        break;
                    case JsonMapSchema map:
                        AddMapSchema(rootSchema, map);
                        break;
                    case JsonJdbcSchema jdbc:
                        AddJdbcSchema(rootSchema, jdbc);
                        break;
                    case var other:
                        throw new java.lang.RuntimeException("Unknown schema " + other);
                }
            }
        }

        /// <summary>
        /// Returns what a factory made for an element of the model, making it on first use.
        /// </summary>
        /// <param name="element">The model's element: a custom or JDBC schema, or a custom table.</param>
        /// <param name="make">Calls the factory.</param>
        /// <returns>The schema or table, the same for every root.</returns>
        object Made(object element, Func<object> make)
        {
            lock (_sync)
            {
                if (_made.TryGetValue(element, out var made) == false)
                    _made.Add(element, made = make());

                return made;
            }
        }

        /// <summary>
        /// <c>visit(JsonMapSchema)</c>: an empty schema of this root's own, holding what the model declares in
        /// it.
        /// </summary>
        /// <param name="parentSchema">The root.</param>
        /// <param name="jsonSchema">The schema's element.</param>
        void AddMapSchema(SchemaPlus parentSchema, JsonMapSchema jsonSchema)
        {
            Mutable(parentSchema, "schema");
            var schema = parentSchema.add(jsonSchema.name, new AbstractSchema());
            if (jsonSchema.path != null)
                schema.setPath(StringListList(jsonSchema.path));

            Populate(jsonSchema, schema);
        }

        /// <summary>
        /// <c>visit(JsonCustomSchema)</c>: the schema the factory made, once per data source.
        /// </summary>
        /// <param name="parentSchema">The root.</param>
        /// <param name="jsonSchema">The schema's element.</param>
        void AddCustomSchema(SchemaPlus parentSchema, JsonCustomSchema jsonSchema)
        {
            try
            {
                Mutable(parentSchema, "sub-schema");
                var schema = (Schema)Made(jsonSchema, () =>
                {
                    var schemaFactory = (SchemaFactory)AvaticaUtils.instantiatePlugin((java.lang.Class)typeof(SchemaFactory), jsonSchema.factory);
                    return schemaFactory.create(parentSchema, jsonSchema.name, OperandMap(jsonSchema, jsonSchema.operand));
                });
                var schemaPlus = parentSchema.add(jsonSchema.name, schema);
                Populate(jsonSchema, schemaPlus);
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonSchema, Throwable(e));
            }
        }

        /// <summary>
        /// <c>visit(JsonJdbcSchema)</c>: a <c>JdbcSchema</c>, once per data source. Calcite does not wrap a
        /// failure here.
        /// </summary>
        /// <param name="parentSchema">The root.</param>
        /// <param name="jsonSchema">The schema's element.</param>
        void AddJdbcSchema(SchemaPlus parentSchema, JsonJdbcSchema jsonSchema)
        {
            Mutable(parentSchema, "jdbc schema");
            var schema = (JdbcSchema)Made(jsonSchema, () =>
            {
                var dataSource = JdbcSchema.dataSource(jsonSchema.jdbcUrl, jsonSchema.jdbcDriver, jsonSchema.jdbcUser, jsonSchema.jdbcPassword);
                if (string.IsNullOrEmpty(jsonSchema.sqlDialectFactory))
                    return JdbcSchema.create(parentSchema, jsonSchema.name, dataSource, jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema);

                var factory = (SqlDialectFactory)AvaticaUtils.instantiatePlugin((java.lang.Class)typeof(SqlDialectFactory), jsonSchema.sqlDialectFactory);
                return JdbcSchema.create(parentSchema, jsonSchema.name, dataSource, factory, jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema);
            });
            var schemaPlus = parentSchema.add(jsonSchema.name, schema);
            Populate(jsonSchema, schemaPlus);
        }

        /// <summary>
        /// <c>populateSchema</c> and the <c>visitChildren</c> of <c>JsonSchema</c> and <c>JsonMapSchema</c>:
        /// the cache setting, then lattices, materializations, tables, functions and types, in that order.
        /// </summary>
        /// <param name="jsonSchema">The schema's element.</param>
        /// <param name="schema">The schema as added to the root.</param>
        void Populate(JsonSchema jsonSchema, SchemaPlus schema)
        {
            if (jsonSchema.cache != null)
                schema.setCacheEnabled(jsonSchema.cache.booleanValue());

            for (var it = jsonSchema.lattices.iterator(); it.hasNext();)
                AddLattice(schema, jsonSchema.name, (JsonLattice)it.next());

            for (var it = jsonSchema.materializations.iterator(); it.hasNext();)
                AddMaterialization(schema, jsonSchema.name, (JsonMaterialization)it.next());

            if (jsonSchema is not JsonMapSchema map)
                return;

            for (var it = map.tables.iterator(); it.hasNext();)
            {
                switch (it.next())
                {
                    case JsonCustomTable table:
                        AddCustomTable(schema, table);
                        break;
                    case JsonView view:
                        AddView(schema, jsonSchema.name, view);
                        break;
                    case var other:
                        throw new java.lang.RuntimeException("Unknown table " + other);
                }
            }

            for (var it = map.functions.iterator(); it.hasNext();)
                AddFunction(schema, (JsonFunction)it.next());

            for (var it = map.types.iterator(); it.hasNext();)
                AddType(schema, jsonSchema.name, (JsonType)it.next());
        }

        /// <summary>
        /// <c>visit(JsonCustomTable)</c>: the table the factory made, once per data source. Its columns are
        /// visited and ignored, as in Calcite.
        /// </summary>
        /// <param name="schema">The schema the table belongs to.</param>
        /// <param name="jsonTable">The table's element.</param>
        void AddCustomTable(SchemaPlus schema, JsonCustomTable jsonTable)
        {
            try
            {
                Mutable(schema, "table");
                var table = (org.apache.calcite.schema.Table)Made(jsonTable, () =>
                {
                    var tableFactory = (TableFactory)AvaticaUtils.instantiatePlugin((java.lang.Class)typeof(TableFactory), jsonTable.factory);
                    return tableFactory.create(schema, jsonTable.name, OperandMap(null, jsonTable.operand), null);
                });
                schema.add(jsonTable.name, table);
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonTable, Throwable(e));
            }
        }

        /// <summary>
        /// <c>visit(JsonView)</c>: a view macro over this root, its path the view's own or the schema's.
        /// </summary>
        /// <param name="schema">The schema the view belongs to.</param>
        /// <param name="schemaName">The schema's name, the view's default path.</param>
        /// <param name="jsonView">The view's element.</param>
        static void AddView(SchemaPlus schema, string schemaName, JsonView jsonView)
        {
            try
            {
                Mutable(schema, "view");
                var path = jsonView.path ?? ImmutableList.of(schemaName);
                var viewPath = ImmutableList.builder().addAll(path).add(jsonView.name).build();
                schema.add(jsonView.name, ViewTable.viewMacro(schema, jsonView.getSql(), path, viewPath, jsonView.modifiable));
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonView, Throwable(e));
            }
        }

        /// <summary>
        /// <c>visit(JsonFunction)</c>.
        /// </summary>
        /// <param name="schema">The schema the functions belong to.</param>
        /// <param name="jsonFunction">The function's element.</param>
        static void AddFunction(SchemaPlus schema, JsonFunction jsonFunction)
        {
            // "name" is not required - a class can have several functions
            try
            {
                Mutable(schema, "function");
                AddFunctions(schema, jsonFunction.name, jsonFunction.className, jsonFunction.methodName, false);
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonFunction, Throwable(e));
            }
        }

        /// <summary>
        /// <c>ModelHandler.addFunctions</c>, without its class filter: adds to a schema the table function,
        /// table macro, scalar functions or aggregate function a class defines.
        /// </summary>
        /// <param name="schema">The schema to add to.</param>
        /// <param name="functionName">The function's name, or <see langword="null"/> to use the method's.</param>
        /// <param name="className">The class to look in for methods that may be user-defined functions.</param>
        /// <param name="methodName">The method's name; <see langword="null"/> means the class is the function,
        /// and <c>*</c> adds every method.</param>
        /// <param name="upCase">Whether to upper-case the names, so that they can be called without quotes.</param>
        /// <exception cref="java.lang.RuntimeException">The class does not exist, or defines no function.</exception>
        /// <remarks>
        /// The class is loaded with <c>ModelHandler</c>'s class loader and without initializing it, as Calcite
        /// loads it, and is tried as a table function, a table macro, scalar functions and an aggregate
        /// function, in that order.
        /// </remarks>
        static void AddFunctions(SchemaPlus schema, string? functionName, string className, string? methodName, bool upCase)
        {
            java.lang.Class clazz;
            try
            {
                // Defer class initialization until it is checked to be a valid function (see below)
                clazz = java.lang.Class.forName(className, false, ((java.lang.Class)typeof(ModelHandler)).getClassLoader());
            }
            catch (java.lang.ClassNotFoundException)
            {
                throw new java.lang.RuntimeException("UDF class '" + className + "' not found");
            }

            var methodNameOrDefault = methodName ?? "eval";
            var actualFunctionName = functionName ?? methodNameOrDefault;
            if (upCase)
                actualFunctionName = actualFunctionName.ToUpperInvariant();

            var tableFunction = TableFunctionImpl.create(clazz, methodNameOrDefault);
            if (tableFunction != null)
            {
                schema.add(functionName ?? methodNameOrDefault, tableFunction);
                return;
            }

            // Must look for TableMacro before ScalarFunction. Both have an "eval"
            // method.
            var macro = TableMacroImpl.create(clazz);
            if (macro != null)
            {
                schema.add(actualFunctionName, macro);
                return;
            }

            if (methodName == "*")
            {
                for (var it = ScalarFunctionImpl.functions(clazz).entries().iterator(); it.hasNext();)
                {
                    var entry = (java.util.Map.Entry)it.next();
                    var name = (string)entry.getKey();
                    if (upCase)
                        name = name.ToUpperInvariant();

                    schema.add(name, (Function)entry.getValue());
                }

                return;
            }
            else
            {
                var function = ScalarFunctionImpl.create(clazz, methodNameOrDefault);
                if (function != null)
                {
                    schema.add(actualFunctionName, function);
                    return;
                }
            }

            if (methodName == null)
            {
                var aggFunction = AggregateFunctionImpl.create(clazz);
                if (aggFunction != null)
                {
                    schema.add(actualFunctionName, aggFunction);
                    return;
                }
            }

            throw new java.lang.RuntimeException("Not a valid function class: " + clazz
                + ". Scalar functions and table macros have an 'eval' method; "
                + "aggregate functions have 'init' and 'add' methods, and optionally "
                + "'initAdd', 'merge' and 'result' methods.");
        }

        /// <summary>
        /// <c>visit(JsonType)</c>: a type of this schema, resolved each time it is used.
        /// </summary>
        /// <param name="schema">The schema the type belongs to: a model schema, or the root.</param>
        /// <param name="schemaName">The schema's name, or <see langword="null"/> for the root.</param>
        /// <param name="jsonType">The type.</param>
        static void AddType(SchemaPlus schema, string? schemaName, JsonType jsonType)
        {
            try
            {
                Mutable(schema, "type");
                schema.add(jsonType.name, new ModelType(schema, schemaName, jsonType));
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonType, Throwable(e));
            }
        }

        /// <summary>
        /// The lambda <c>visit(JsonType)</c> adds: a SQL type, or a record of attributes.
        /// </summary>
        /// <param name="schema">The schema the type belongs to, in which an attribute's type is looked up.</param>
        /// <param name="schemaName">The schema's name, for the message where that lookup fails.</param>
        /// <param name="jsonType">The type's element.</param>
        /// <remarks>
        /// An attribute naming a type the schema defines never reaches the schema lookup: Calcite requires
        /// <c>SqlTypeName.get</c> to answer for the attribute's type before it would fall back to the schema,
        /// so such an attribute fails with <c>SqlTypeName.get for</c> the name. Reproduced.
        /// </remarks>
        sealed class ModelType(SchemaPlus schema, string? schemaName, JsonType jsonType) : RelProtoDataType
        {

            /// <inheritdoc />
            public object apply(object obj)
            {
                var typeFactory = (RelDataTypeFactory)obj;
                if (jsonType.type != null)
                    return typeFactory.createSqlType(SqlTypeName.get(jsonType.type)
                        ?? throw new java.lang.NullPointerException("SqlTypeName.get for " + jsonType.type));

                var builder = typeFactory.builder();
                for (var it = jsonType.attributes.iterator(); it.hasNext();)
                {
                    var jsonTypeAttribute = (JsonTypeAttribute)it.next();
                    var typeName = SqlTypeName.get(jsonTypeAttribute.type)
                        ?? throw new java.lang.NullPointerException("SqlTypeName.get for " + jsonTypeAttribute.type);
                    var type = typeFactory.createSqlType(typeName);
                    if (type == null)
                    {
                        var proto = schema.getType(jsonTypeAttribute.type)
                            ?? throw new java.lang.NullPointerException("type " + jsonTypeAttribute.type + " is not found in schema " + schemaName);
                        type = (RelDataType)proto.apply(typeFactory);
                    }

                    builder.add(jsonTypeAttribute.name, type);
                }

                return builder.build();
            }

        }

        /// <summary>
        /// <c>visit(JsonMaterialization)</c>: a materialized view over this root, registered with Calcite's
        /// <c>MaterializationService</c> as Calcite registers one per connection.
        /// </summary>
        /// <param name="schema">The schema the materialization belongs to.</param>
        /// <param name="schemaName">The schema's name, for the message where it is not mutable.</param>
        /// <param name="jsonMaterialization">The materialization's element.</param>
        static void AddMaterialization(SchemaPlus schema, string schemaName, JsonMaterialization jsonMaterialization)
        {
            try
            {
                if (schema.isMutable() == false)
                    throw new java.lang.RuntimeException("Cannot define materialization; parent schema '" + schemaName + "' is not a SemiMutableSchema");

                var calciteSchema = CalciteSchema.from(schema);

                string viewName;
                bool existing;
                if (jsonMaterialization.view == null)
                {
                    // If the user did not supply a view name, that means the materialized
                    // view is pre-populated. Generate a synthetic view name.
                    viewName = "$" + schema.tables().getNames(org.apache.calcite.schema.lookup.LikePattern.any()).size();
                    existing = true;
                }
                else
                {
                    viewName = jsonMaterialization.view;
                    existing = false;
                }

                var viewPath = calciteSchema.path(viewName);
                schema.add(viewName, MaterializedViewTable.create(calciteSchema, jsonMaterialization.getSql(), jsonMaterialization.viewSchemaPath, viewPath, jsonMaterialization.table, existing));
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonMaterialization, Throwable(e));
            }
        }

        /// <summary>
        /// <c>visit(JsonLattice)</c> and <c>populateLattice</c>: a lattice over this root, with its default
        /// measures and then its tiles.
        /// </summary>
        /// <param name="schema">The schema the lattice belongs to.</param>
        /// <param name="schemaName">The schema's name, for the message where it is not mutable.</param>
        /// <param name="jsonLattice">The lattice's element.</param>
        void AddLattice(SchemaPlus schema, string schemaName, JsonLattice jsonLattice)
        {
            try
            {
                if (schema.isMutable() == false)
                    throw new java.lang.RuntimeException("Cannot define lattice; parent schema '" + schemaName + "' is not a SemiMutableSchema");

                var calciteSchema = CalciteSchema.from(schema);
                var latticeBuilder = Lattice.builder(calciteSchema, jsonLattice.getSql())
                    .auto(jsonLattice.auto)
                    .algorithm(jsonLattice.algorithm);
                if (jsonLattice.rowCountEstimate != null)
                    latticeBuilder.rowCountEstimate(jsonLattice.rowCountEstimate.doubleValue());
                if (jsonLattice.statisticProvider != null)
                    latticeBuilder.statisticProvider(jsonLattice.statisticProvider);

                for (var it = jsonLattice.defaultMeasures.iterator(); it.hasNext();)
                    latticeBuilder.addMeasure(Measure(latticeBuilder, (JsonMeasure)it.next()));

                for (var it = jsonLattice.tiles.iterator(); it.hasNext();)
                {
                    var jsonTile = (JsonTile)it.next();
                    var tileBuilder = Lattice.Tile.builder();
                    for (var mit = jsonTile.measures.iterator(); mit.hasNext();)
                        tileBuilder.addMeasure(Measure(latticeBuilder, (JsonMeasure)mit.next()));
                    for (var dit = jsonTile.dimensions.iterator(); dit.hasNext();)
                        tileBuilder.addDimension(latticeBuilder.resolveColumn(dit.next()));

                    latticeBuilder.addTile(tileBuilder.build());
                }

                schema.add(jsonLattice.name, latticeBuilder.build());
            }
            catch (Exception e)
            {
                throw new java.lang.RuntimeException("Error instantiating " + jsonLattice, Throwable(e));
            }
        }

        /// <summary>
        /// <c>visit(JsonMeasure)</c>, short of placing the measure: the model has no <c>distinct</c> for one.
        /// </summary>
        /// <param name="latticeBuilder">The lattice the measure's columns are resolved in.</param>
        /// <param name="jsonMeasure">The measure's element.</param>
        /// <returns>The measure, for the lattice or a tile to add.</returns>
        static Lattice.Measure Measure(Lattice.Builder latticeBuilder, JsonMeasure jsonMeasure)
        {
            return latticeBuilder.resolveMeasure(jsonMeasure.agg, false, jsonMeasure.args);
        }

        /// <summary>
        /// <c>operandMap</c>: an operand with the extra entries Calcite adds where the model does not set
        /// them — the model's URI, the directory to read files from, and a custom schema's tables.
        /// </summary>
        /// <param name="jsonSchema">The custom schema the operand is for, or <see langword="null"/> for a
        /// table.</param>
        /// <param name="operand">The operand the model gives, or <see langword="null"/>.</param>
        /// <returns>The operand with the extra entries, or an empty map where the model gives none.</returns>
        java.util.Map OperandMap(JsonSchema? jsonSchema, java.util.Map? operand)
        {
            if (operand == null)
                return ImmutableMap.of();

            var builder = ImmutableMap.builder();
            builder.putAll(operand);
            foreach (ModelHandler.ExtraOperand extraOperand in ModelHandler.ExtraOperand.values())
            {
                if (operand.containsKey(extraOperand.camelName))
                    continue;

                switch (extraOperand.name())
                {
                    case nameof(ModelHandler.ExtraOperand.MODEL_URI):
                        builder.put(extraOperand.camelName, _model.Uri);
                        break;
                    case nameof(ModelHandler.ExtraOperand.BASE_DIRECTORY):
                        java.io.File? f = null;
                        if (_model.Uri.StartsWith("inline:", StringComparison.Ordinal) == false)
                            f = new java.io.File(_model.Uri).getParentFile();

                        builder.put(extraOperand.camelName, f ?? new java.io.File(""));
                        break;
                    case nameof(ModelHandler.ExtraOperand.TABLES):
                        if (jsonSchema is JsonCustomSchema custom)
                            builder.put(extraOperand.camelName, custom.tables);
                        break;
                    default:
                        break;
                }
            }

            return builder.build();
        }

        /// <summary>
        /// <c>currentMutableSchema</c>'s check.
        /// </summary>
        /// <param name="schema">The schema an element is about to be added to.</param>
        /// <param name="elementType">What the element is, for the message.</param>
        static void Mutable(SchemaPlus schema, string elementType)
        {
            if (schema.isMutable() == false)
                throw new java.lang.RuntimeException("Cannot define " + elementType + "; parent schema '" + schema.getName() + "' is not mutable");
        }

        /// <summary>
        /// <c>stringListList</c>: a schema path as a list of lists of names.
        /// </summary>
        /// <param name="path">The path as the model gives it: names, or lists of names.</param>
        /// <returns>The path, each element a list of names.</returns>
        static ImmutableList StringListList(java.util.List path)
        {
            var builder = ImmutableList.builder();
            for (var it = path.iterator(); it.hasNext();)
                builder.add(StringList(it.next()));

            return builder.build();
        }

        static ImmutableList StringList(object s)
        {
            if (s is string str)
                return ImmutableList.of(str);

            if (s is java.util.List list)
            {
                var builder = ImmutableList.builder();
                for (var it = list.iterator(); it.hasNext();)
                {
                    var o = it.next();
                    if (o is string element)
                        builder.add(element);
                    else
                        throw new java.lang.RuntimeException("Invalid path element " + o + "; was expecting string");
                }

                return builder.build();
            }

            throw new java.lang.RuntimeException("Invalid path element " + s + "; was expecting string or list of string");
        }

        /// <summary>
        /// Returns an exception as the <c>Throwable</c> a Java exception's cause has to be.
        /// </summary>
        /// <param name="e">The exception.</param>
        /// <returns>The exception, or IKVM's mapping of it.</returns>
        static java.lang.Throwable Throwable(Exception e)
        {
            return (java.lang.Throwable)ikvm.runtime.Util.mapException(e);
        }

    }

}
