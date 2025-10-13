using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

using Apache.Calcite.Data.Impl;
using Apache.Calcite.Data.Schema;

using java.lang;
using java.util;

using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;
using org.apache.calcite.util;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Wrapper around user-defined schema used internally.
    /// </summary>
    public abstract class CalciteSchema
    {

        readonly CalciteSchema? _parent;
        readonly SchemaRef _schema;
        readonly string _name;

        readonly NameMapRef<TableEntry> _tableMap;
        readonly LazyReferenceRef<Lookup, LookupRef<TableEntry>> _tables = new LazyReference();
        readonly NameMultimapRef<FunctionEntry> _functionMap;
        readonly NameMapRef<TypeEntry> _typeMap;
        readonly NameMapRef<LatticeEntry> _latticeMap;
        readonly NameSet _functionNames;
        readonly NameMapRef<FunctionEntry> _nullaryFunctionMap;
        readonly NameMapRef<CalciteSchema> _subschemaMap;
        readonly LazyReferenceRef<Lookup, LookupRef<CalciteSchema>> _subschemas = new LazyReference();
        readonly ImmutableList<string>? _path;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="subSchemaMap"></param>
        /// <param name="tableMap"></param>
        /// <param name="latticeMap"></param>
        /// <param name="typeMap"></param>
        /// <param name="functionMap"></param>
        /// <param name="functionNames"></param>
        /// <param name="nullaryFunctionMap"></param>
        /// <param name="path"></param>
        protected CalciteSchema(
            CalciteSchema? parent,
            org.apache.calcite.schema.Schema schema,
            string name,
            NameMapRef<CalciteSchema>? subSchemaMap,
            NameMapRef<TableEntry>? tableMap,
            NameMapRef<LatticeEntry>? latticeMap,
            NameMapRef<TypeEntry>? typeMap,
            NameMultimapRef<FunctionEntry>? functionMap,
            NameSet? functionNames,
            NameMapRef<FunctionEntry>? nullaryFunctionMap,
            ImmutableList<string>? path)
        {
            _parent = parent;
            _schema = (schema ?? throw new ArgumentNullException(nameof(schema))).AsRef();
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _subschemaMap = subSchemaMap ?? new NameMap();
            _tableMap = tableMap ?? new NameMap();
            _latticeMap = latticeMap ?? new NameMap();
            _typeMap = typeMap ?? new NameMap();
            _path = path;

            if (functionMap.HasValue == false)
            {
                _functionMap = new NameMultimap();
                _functionNames = new NameSet();
                _nullaryFunctionMap = new NameMap();
            }
            else
            {
                _functionMap = functionMap.Value;
                _functionNames = functionNames ?? throw new ArgumentNullException(nameof(functionNames));
                _nullaryFunctionMap = nullaryFunctionMap ?? throw new ArgumentNullException(nameof(nullaryFunctionMap));
            }
        }

        /// <summary>
        /// Gets the name of the schema.
        /// </summary>
        public string Name => _name;

        /// <summary>
        /// Gets the parent schema.
        /// </summary>
        public CalciteSchema? Parent => _parent;

        /// <summary>
        /// Gets the underlying schema.
        /// </summary>
        public SchemaRef Schema => _schema;

        /// <summary>
        /// Gets the set of tables.
        /// </summary>
        public LookupRef<TableEntry> Tables => _tables
            .GetOrCompute(() =>
                LookupRef.Concat(
                    LookupRef.Of(_tableMap),
                    _schema.Tables.Map((s, n) => CreateTableEntry(n, s))));

        /// <summary>
        /// Gets the set of sub-schemas.
        /// </summary>
        public LookupRef<CalciteSchema> Subschemas => _subschemas
            .GetOrCompute(() =>
                LookupRef.Concat(
                    LookupRef.Of(_subschemaMap),
                    _schema.Subschemas.Map((s, n) => CreateSubSchema(s, n))));

        /// <summary>
        /// Returns a type with a given name that is defined implicitly (that is, by the underlying <see cref="org.apache.calcite.schema.Schema"/> object, not explicitly by a call to <see cref="Add(string, RelProtoDataType)"/>, or null.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="caseSensitive"></param>
        /// <returns></returns>
        protected abstract TypeEntry GetImplicitType(string name, bool caseSensitive);

        protected abstract TableEntry GetImplicitTableBasedOnNullaryFunction(string tableName, bool caseSensitive);

        protected abstract void AddImplicitFunctionsToSet(ISet<Function> target, string name, bool caseSensitive);

        protected abstract void AddImplicitFuncNamesToSet(ISet<string> target);

        protected abstract void AddImplicitTypeNamesToSet(ISet<string> target);

        protected abstract void AddImplicitTablesBasedOnNullaryFunctionsToDictionary(IDictionary<string, Table> target);

        protected abstract CalciteSchema Snapshot(CalciteSchema? parent, SchemaVersion version);

        protected abstract bool IsCacheEnabled { get; }

        public abstract void SetCache(bool cache);

        /// <summary>
        /// Creates a sub-schema with a given name that is defined implicitly.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        protected abstract CalciteSchema CreateSubSchema(org.apache.calcite.schema.Schema schema, string name);

        /// <summary>
        /// Creates a <see cref="TableEntryImpl"/>.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        protected virtual TableEntry CreateTableEntry(string name, org.apache.calcite.schema.Table table)
        {
            return new TableEntryImpl(this, name, table, ImmutableList<string>.Empty);
        }

        /// <summary>
        /// Creates a TypeEntryImpl.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="relProtoDataType"></param>
        /// <returns></returns>
        protected virtual TypeEntry CreateTypeEntry(string name, RelProtoDataType relProtoDataType)
        {
            return new TypeEntryImpl(this, name, relProtoDataType);
        }

        /// <summary>
        /// Defines a table within this schema.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public TableEntry Add(string tableName, org.apache.calcite.schema.Table table)
        {
            return Add(tableName, table, ImmutableList<string>.Empty);
        }

        /// <summary>
        /// Defines a table within this schema.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="table"></param>
        /// <param name="sqls"></param>
        /// <returns></returns>
        public TableEntry Add(string tableName, org.apache.calcite.schema.Table table, ImmutableList<string> sqls)
        {
            var entry = new TableEntryImpl(this, tableName, table, sqls);
            _tableMap.Put(tableName, entry);
            return entry;
        }

        /// <summary>
        /// Defines a type within this schema.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public TypeEntry Add(string name, RelProtoDataType type)
        {
            var entry = new TypeEntryImpl(this, name, type);
            _typeMap.Put(name, entry);
            return entry;
        }

        /// <summary>
        /// Gets the root schema.
        /// </summary>
        public CalciteSchema Root => GetRoot();

        /// <summary>
        /// Implements the getter for <see cref="Root"/>.
        /// </summary>
        /// <returns></returns>
        CalciteSchema GetRoot()
        {
            for (var schema = this; ;)
            {
                if (schema._parent == null)
                    return schema;

                schema = schema._parent;
            }
        }

        /// <summary>
        /// Gets whether this is the root schema.
        /// </summary>
        public bool IsRoot => _parent == null;

        /// <summary>
        /// Gets the path of an object in this schema.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public IReadOnlyList<string> GetPath(string? name)
        {
            var list = new List<string>();
            if (name is not null)
                list.Insert(0, name);

            for (var s = this; s != null; s = s._parent)
            {
                if (s._parent != null || s._name != (""))
                    list.Insert(0, s._name);
            }

            return list;
        }

        /// <summary>
        /// Gets the named sub-schema.
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="caseSensitive"></param>
        /// <returns></returns>
        public CalciteSchema? GetSubSchema(string schemaName, bool caseSensitive)
        {
            return caseSensitive ? Subschemas.Get(schemaName) : (CalciteSchema?)Named.entityOrNull(Subschemas.GetIgnoreCase(schemaName));
        }

        /// <summary>
        /// Adds a child schema of this schema.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public abstract CalciteSchema Add(string name, org.apache.calcite.schema.Schema schema);

        /// <summary>
        /// Gets a table with the specified name. Does not look for views.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="caseSensitive"></param>
        /// <returns></returns>
        public TableEntry? GetTable(string tableName, bool caseSensitive)
        {
            return LookupRef.Get(Tables, tableName, caseSensitive);
        }

        /// <summary>
        /// Returns a table that materializes the given SQL statement.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public TableEntry? GetTableBySql(string sql)
        {
            foreach (var i in _tableMap.Map)
                if (i.Value.Sqls.Contains(sql))
                    return i.Value;

            return null;
        }

        /// <summary>
        /// Gets a <see cref="SchemaPlus"/> version of this schema.
        /// </summary>
        /// <returns></returns>
        public SchemaPlus Plus() => new SchemaPlusImpl(this);

        /// <summary>
        /// Gets the default path resolving functions from this schema.
        /// </summary>
        /// <returns></returns>
        public IReadOnlyList<string> Path => _path ?? GetPath(null);

        /// <summary>
        /// Returns a collection of sub-schemas, both explicit (defined using <see cref="Add(string, org.apache.calcite.schema.Schema)"/> and implicit.
        /// </summary>
        /// <returns></returns>
        public IReadOnlyDictionary<string, CalciteSchema> GetSubSchemaMap()
        {
            return _subschemaMap.Map;
        }

        /// <summary>
        /// Gets the collection of lattices.
        /// </summary>
        /// <returns></returns>
        public IReadOnlyDictionary<string, LatticeEntry> GetLatticeMap()
        {
            return _latticeMap.Map;
        }

        /// <summary>
        /// Gets the set of table names filtered by the given pattern. Includes implicit and explicit tables and functions with zero parameters.
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public IReadOnlySet<string> GetTableNames(LikePattern pattern)
        {
            return Tables.GetNames(pattern);
        }

        /// <summary>
        /// Gets the set of all table names. Includes implicit and explicit tables and functions with zero parameters.
        /// </summary>
        /// <returns></returns>
        public IReadOnlySet<string> GetTableNames()
        {
            return GetTableNames(LikePattern.any());
        }

        /// <summary>
        /// Gets the set of all type names.
        /// </summary>
        /// <returns></returns>
        public IReadOnlySet<string> GetTypeNames()
        {
            var set = new HashSet<string>();

            foreach (var i in _typeMap.Map)
                set.Add(i.Key);

            AddImplicitTypeNamesToSet(set);

            return set;
        }

        /// <summary>
        /// Gets a type, explicit and implicit, with a given name. Never null.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="caseSensitive"></param>
        /// <returns></returns>
        public TypeEntry GetType(string name, bool caseSensitive)
        {
            return _typeMap.Range(name, caseSensitive).Select(i => i.Value).FirstOrDefault(GetImplicitType(name, caseSensitive));
        }

        /// <summary>
        /// Gets a collection of all functions, explicit and implicit, with a given name. Never null.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="caseSensitivity"></param>
        /// <returns></returns>
        public IReadOnlySet<Function> GetFunctions(string name, bool caseSensitivity)
        {
            var builder = new HashSet<Function>();

            foreach (var key in _functionMap.Range(name, caseSensitivity))
                builder.Add(key.Function);

            AddImplicitFunctionsToSet(builder, name, caseSensitivity);

            return builder;
        }

        /// <summary>
        /// Gets the set of all type names.
        /// </summary>
        /// <returns></returns>
        public IReadOnlySet<string> GetFunctionNames()
        {
            var builder = new HashSet<string>();

            foreach (var i in _functionMap.Map)
                builder.Add(i.Key);

            AddImplicitTypeNamesToSet(builder);

            return builder;
        }

        /// <summary>
        /// Returns tables derived from explicit and implicit functions that take zero parameters.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public IReadOnlyDictionary<string, Table> GetTablesBasedOnNullaryFunctions()
        {
            var builder = new Dictionary<string, Table>();

            foreach (var i in _nullaryFunctionMap.Map)
            {
                var function = i.Value.Function;
                if (function is TableMacro macro)
                {
                    if (function.getParameters().isEmpty() == false)
                        throw new InvalidOperationException();

                    var table = macro.apply(com.google.common.collect.ImmutableList.of());
                    builder.Add(i.Key, table);
                }
            }

            AddImplicitTablesBasedOnNullaryFunctionsToDictionary(builder);

            return builder;
        }

        /// <summary>
        /// Returns a tables derived from explicit and implicit functions that take zero parameters.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="caseSensitive"></param>
        /// <returns></returns>
        public TableEntry? GetTableBasedOnNullaryFunction(string tableName, bool caseSensitive)
        {
            foreach (var entry in _nullaryFunctionMap.Range(tableName, caseSensitive))
            {
                var function = entry.Value.Function;
                if (function is TableMacro macro)
                {
                    var table = macro.apply(com.google.common.collect.ImmutableList.of());
                    return CreateTableEntry(tableName, table);
                }
            }

            return GetImplicitTableBasedOnNullaryFunction(tableName, caseSensitive);
        }

        /// <summary>
        /// Creates a root schema.
        /// </summary>
        /// <remarks>
        /// When <paramref name="addMetadataSchema"/> argument is true adds a "metadata" schema containing definitions of tables, columns etc. to root schema. By default, creates a <see cref="CachingCalciteSchema"/>.
        /// </remarks>
        /// <param name="addMetadataSchema"></param>
        /// <returns></returns>
        public static CalciteSchema CreateRootSchema(bool addMetadataSchema)
        {
            return CreateRootSchema(addMetadataSchema, true);
        }

        /// <summary>
        /// Creates a root schema.
        /// </summary>
        /// <remarks>
        /// When <paramref name="addMetadataSchema"/> argument is true adds a "metadata" schema containing definitions of tables, columns etc. to root schema.
        /// </remarks>
        /// <param name="addMetadataSchema"></param>
        /// <param name="cache"></param>
        /// <returns></returns>
        public static CalciteSchema CreateRootSchema(bool addMetadataSchema, bool cache)
        {
            return CreateRootSchema(addMetadataSchema, cache, "");
        }

        /// <summary>
        /// Creates a root schema.
        /// </summary>
        /// <remarks>
        /// When <paramref name="addMetadataSchema"/> argument is true adds a "metadata" schema containing definitions of tables, columns etc. to root schema.
        /// </remarks>
        /// <param name="addMetadataSchema"></param>
        /// <param name="cache"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static CalciteSchema CreateRootSchema(bool addMetadataSchema, bool cache, string name)
        {
            return CreateRootSchema(addMetadataSchema, cache, name, new CalciteConnectionImpl.RootSchema());
        }

        /// <summary>
        /// Creates a root schema.
        /// </summary>
        /// <remarks>
        /// When <paramref name="addMetadataSchema"/> argument is true adds a "metadata" schema containing definitions of tables, columns etc. to root schema.
        /// </remarks>
        /// <param name="addMetadataSchema"></param>
        /// <param name="cache"></param>
        /// <param name="name"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public static CalciteSchema CreateRootSchema(bool addMetadataSchema, bool cache, string name, org.apache.calcite.schema.Schema schema)
        {
            var root = cache
                ? new CachedCalciteSchema(null, schema, name)
                : new SimpleCalciteSchema(null, schema, name);

            if (addMetadataSchema)
                root.Add("metadata", MetadataSchema.Instance);

            return root;
        }

        /// <summary>
        /// Removes the given sub-schema.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveSubSchema(string name)
        {
            return _subschemaMap.Remove(name) != null;
        }

        /// <summary>
        /// Removes the given table.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveTable(string name)
        {
            return _tableMap.Remove(name) != null;
        }

        /// <summary>
        /// Removes the given function.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveFunction(string name)
        {
            var remove = _nullaryFunctionMap.Remove(name);
            if (remove == null)
                return false;

            _functionMap.Remove(name, remove);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveType(string name)
        {
            return _typeMap.Remove(name) != null;
        }

    }

}
