using System;
using System.Collections.Generic;

using com.google.common.collect;

using org.apache.calcite;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.schema.lookup;
using org.apache.calcite.util;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A port of Calcite's package-private <c>CachingCalciteSchema</c>, the <see cref="CalciteSchema"/>
    /// <c>CalciteSchema.createRootSchema</c> builds by default, with the ability to copy a tree of them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="CalciteSchema"/> keeps what was added to it by name (sub-schemas, tables, functions, types,
    /// lattices) in maps of its own, and those maps are written at run time: DDL adds and removes entries, and a
    /// recursive query registers its working table in the root while it runs. Calcite gives every connection
    /// a root of its own and never guards one against concurrent use. <see cref="CopyRoot"/> gives a connection
    /// maps of its own over the same <c>Schema</c> objects, which hold an adapter's clients and caches.
    /// </para>
    /// <para>
    /// The maps are <c>protected</c> on <see cref="CalciteSchema"/>, with no public accessor for the explicit
    /// entries alone, so a copy has to be made by a subclass. A tree is made entirely of this class when its
    /// root is: every other node is made by <see cref="add(string, Schema)"/>, <see cref="createSubSchema"/>
    /// or <see cref="snapshot"/>, which make this class. Calcite does not test a node for its concrete class.
    /// </para>
    /// </remarks>
    internal sealed class CachingCalciteSchema : CalciteSchema
    {

        /// <summary>
        /// A port of the package-private <c>CalciteConnectionImpl.RootSchema</c>: the schema at the root of a
        /// connection, whose expression is the data context's root schema.
        /// </summary>
        sealed class RootSchema : AbstractSchema
        {

            /// <inheritdoc />
            public override Expression getExpression(SchemaPlus? parentSchema, string name)
            {
                return Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
            }

        }

        /// <summary>
        /// A port of <c>CachingCalciteSchema.AbstractCached</c>: a value built on first use and kept while the
        /// schema's cache is enabled.
        /// </summary>
        /// <remarks>
        /// Unsynchronized, as Calcite's is: two threads reading at once may each build the value.
        /// </remarks>
        sealed class Cached
        {

            readonly CachingCalciteSchema _owner;
            readonly Func<NameSet> _build;
            NameSet? _t;
            bool _built;

            public Cached(CachingCalciteSchema owner, Func<NameSet> build)
            {
                _owner = owner;
                _build = build;
            }

            /// <summary>
            /// Returns the value, using the cached value if valid.
            /// </summary>
            /// <param name="now">The current time in milliseconds. Unused, as in Calcite.</param>
            /// <returns>The value.</returns>
            public NameSet Get(long now)
            {
                if (!_owner._cache)
                    return _build();

                if (!_built)
                    _t = _build();

                _built = true;
                return _t!;
            }

            /// <summary>
            /// Called when the schema's caching is enabled or disabled.
            /// </summary>
            /// <param name="now">The current time in milliseconds. Unused, as in Calcite.</param>
            /// <param name="enabled">Whether caching is now enabled.</param>
            public void Enable(long now, bool enabled)
            {
                if (!enabled)
                    _t = null;

                _built = false;
            }

        }

        /// <summary>
        /// The package-private <c>MetadataSchema.INSTANCE</c>, reached through the root Calcite builds with it.
        /// </summary>
        static readonly Schema MetadataSchemaInstance = CalciteSchema.createRootSchema(true, false).getSubSchema("metadata", true)!.schema;

        readonly java.util.concurrent.ConcurrentLinkedDeque _caches = new();
        readonly Cached _implicitFunctionCache;
        readonly Cached _implicitTypeCache;
        bool _cache = true;

        /// <summary>
        /// Creates a root schema, as <c>CalciteSchema.createRootSchema(addMetadataSchema, true, "")</c> does.
        /// </summary>
        /// <param name="addMetadataSchema">Whether to add a <c>metadata</c> schema holding the definitions of
        /// tables, columns and so on.</param>
        /// <returns>The root.</returns>
        public static CachingCalciteSchema CreateRoot(bool addMetadataSchema)
        {
            var root = new CachingCalciteSchema(null, new RootSchema(), "");
            if (addMetadataSchema)
                root.add("metadata", MetadataSchemaInstance);

            return root;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="parent">The parent schema, or <see langword="null"/> for a root.</param>
        /// <param name="schema">The schema this node wraps.</param>
        /// <param name="name">The name of this node.</param>
        CachingCalciteSchema(CalciteSchema? parent, Schema schema, string name) :
            this(parent, schema, name, null, null, null, null, null, null, null, null)
        {

        }

        CachingCalciteSchema(CalciteSchema? parent, Schema schema, string name,
            NameMap? subSchemaMap,
            NameMap? tableMap,
            NameMap? latticeMap,
            NameMap? typeMap,
            NameMultimap? functionMap,
            NameSet? functionNames,
            NameMap? nullaryFunctionMap,
            java.util.List? path) :
            base(parent, schema, name, subSchemaMap, tableMap, latticeMap, typeMap, functionMap, functionNames, nullaryFunctionMap, path)
        {
            _implicitFunctionCache = new Cached(this, () => NameSet.immutableCopyOf(this.schema.getFunctionNames()));
            _implicitTypeCache = new Cached(this, () => NameSet.immutableCopyOf(this.schema.getTypeNames()));
        }

        /// <inheritdoc />
        public override void setCache(bool cache)
        {
            if (cache == _cache)
                return;

            EnableCaches(cache);
            var now = java.lang.System.currentTimeMillis();
            _implicitFunctionCache.Enable(now, cache);
            _cache = cache;
        }

        /// <inheritdoc />
        protected override bool isCacheEnabled()
        {
            return _cache;
        }

        /// <inheritdoc />
        protected override CalciteSchema createSubSchema(Schema schema, string name)
        {
            return new CachingCalciteSchema(this, schema, name);
        }

        /// <inheritdoc />
        protected override Lookup enhanceLookup(Lookup lookup)
        {
            var snapshotLookup = new SnapshotLookup(lookup);
            _caches.add(snapshotLookup);
            return snapshotLookup;
        }

        /// <inheritdoc />
        public override CalciteSchema add(string name, Schema schema)
        {
            var calciteSchema = new CachingCalciteSchema(this, schema, name);
            subSchemaMap.put(name, calciteSchema);
            return calciteSchema;
        }

        /// <inheritdoc />
        protected override TypeEntry? getImplicitType(string name, bool caseSensitive)
        {
            var now = java.lang.System.currentTimeMillis();
            var implicitTypeNames = _implicitTypeCache.Get(now);
            for (var it = implicitTypeNames.range(name, caseSensitive).iterator(); it.hasNext();)
            {
                var typeName = (string)it.next();
                var type = schema.getType(typeName);
                if (type != null)
                    return typeEntry(name, type);
            }

            return null;
        }

        /// <inheritdoc />
        protected override void addImplicitFunctionsToBuilder(ImmutableList.Builder builder, string name, bool caseSensitive)
        {
            // Add implicit functions, case-insensitive.
            var now = java.lang.System.currentTimeMillis();
            var set = _implicitFunctionCache.Get(now);
            for (var it = set.range(name, caseSensitive).iterator(); it.hasNext();)
            {
                var functions = schema.getFunctions((string)it.next());
                if (functions != null)
                    builder.addAll(functions);
            }
        }

        /// <inheritdoc />
        protected override void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder builder)
        {
            // Add implicit functions, case-sensitive.
            var now = java.lang.System.currentTimeMillis();
            var set = _implicitFunctionCache.Get(now);
            builder.addAll(set.iterable());
        }

        /// <inheritdoc />
        protected override void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder builder)
        {
            // Add implicit types, case-sensitive.
            var now = java.lang.System.currentTimeMillis();
            var set = _implicitTypeCache.Get(now);
            builder.addAll(set.iterable());
        }

        /// <inheritdoc />
        protected override void addImplicitTablesBasedOnNullaryFunctionsToBuilder(ImmutableSortedMap.Builder builder)
        {
            var explicitTables = builder.build();

            var now = java.lang.System.currentTimeMillis();
            var set = _implicitFunctionCache.Get(now);
            for (var it = set.iterable().iterator(); it.hasNext();)
            {
                var s = (string)it.next();

                // explicit table wins.
                if (explicitTables.containsKey(s))
                    continue;

                for (var fit = schema.getFunctions(s).iterator(); fit.hasNext();)
                {
                    var function = (Function)fit.next();
                    if (function is TableMacro macro && function.getParameters().isEmpty())
                    {
                        var table = macro.apply(ImmutableList.of());
                        builder.put(s, table);
                    }
                }
            }
        }

        /// <inheritdoc />
        protected override TableEntry? getImplicitTableBasedOnNullaryFunction(string tableName, bool caseSensitive)
        {
            var now = java.lang.System.currentTimeMillis();
            var set = _implicitFunctionCache.Get(now);
            for (var it = set.range(tableName, caseSensitive).iterator(); it.hasNext();)
            {
                var s = (string)it.next();
                for (var fit = schema.getFunctions(s).iterator(); fit.hasNext();)
                {
                    var function = (Function)fit.next();
                    if (function is TableMacro macro && function.getParameters().isEmpty())
                    {
                        var table = macro.apply(ImmutableList.of());
                        return tableEntry(tableName, table);
                    }
                }
            }

            return null;
        }

        /// <inheritdoc />
        protected override CalciteSchema snapshot(CalciteSchema? parent, SchemaVersion version)
        {
            var snapshot = new CachingCalciteSchema(parent, schema.snapshot(version), name, null,
                tableMap, latticeMap, typeMap,
                functionMap, functionNames, nullaryFunctionMap, getPath());
            for (var it = subSchemaMap.map().values().iterator(); it.hasNext();)
            {
                var subSchema = (CachingCalciteSchema)it.next();
                var subSchemaSnapshot = subSchema.snapshot(snapshot, version);
                snapshot.subSchemaMap.put(subSchema.name, subSchemaSnapshot);
            }

            return snapshot;
        }

        /// <inheritdoc />
        public override bool removeTable(string name)
        {
            if (_cache)
            {
                EnableCaches(false);
                EnableCaches(true);
            }

            return base.removeTable(name);
        }

        /// <inheritdoc />
        public override bool removeFunction(string name)
        {
            if (_cache)
            {
                var now = java.lang.System.nanoTime();
                _implicitFunctionCache.Enable(now, false);
                _implicitFunctionCache.Enable(now, true);
            }

            return base.removeFunction(name);
        }

        void EnableCaches(bool cache)
        {
            for (var it = _caches.iterator(); it.hasNext();)
                ((SnapshotLookup)it.next()).enable(cache);
        }

        /// <summary>
        /// Returns a new root over the same <c>Schema</c> objects as this one, holding the same explicit
        /// sub-schemas, tables, functions, types and lattices, each sub-schema copied in turn.
        /// </summary>
        /// <returns>The copy, which shares no map with this root.</returns>
        /// <remarks>
        /// <para>
        /// This root is only read. Entries are written into the copy through the public <c>add</c> overloads,
        /// which keep the derived maps (function names, nullary functions) as Calcite keeps them. A sub-schema
        /// that a parent's <c>Schema</c> implies is not copied: the copy derives it again on first use, and
        /// caches it separately.
        /// </para>
        /// <para>
        /// The entries are this root's objects. A view macro holds the root it was defined against, so a view
        /// defined on this root is analysed against this root, not against the copy.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">This schema is not a root.</exception>
        public CachingCalciteSchema CopyRoot()
        {
            if (isRoot() == false)
                throw new InvalidOperationException("Only a root schema is copied.");

            var copy = new CachingCalciteSchema(null, schema, name);
            copy._cache = _cache;
            CopyInto(copy);
            return copy;
        }

        /// <summary>
        /// Copies this schema's explicit entries into another, sub-schemas first and recursively.
        /// </summary>
        void CopyInto(CachingCalciteSchema to)
        {
            for (var it = subSchemaMap.map().entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                var sub = (CachingCalciteSchema)entry.getValue();
                var subCopy = (CachingCalciteSchema)to.add((string)entry.getKey(), sub.schema);

                // a model's "cache": false is set on the sub-schema after it is added
                subCopy.setCache(sub._cache);
                sub.CopyInto(subCopy);
            }

            // adding a lattice adds its star table under the lattice's name, so the star tables are not
            // copied as tables as well
            var starTables = new HashSet<object>(ReferenceEqualityComparer.Instance);
            for (var it = latticeMap.map().entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                var lattice = (LatticeEntry)entry.getValue();
                starTables.Add(lattice.getStarTable());
                to.plus().add((string)entry.getKey(), lattice.getLattice());
            }

            for (var it = tableMap.map().entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                var table = (TableEntry)entry.getValue();
                if (starTables.Contains(table) == false)
                    to.add((string)entry.getKey(), table.getTable(), table.sqls);
            }

            for (var it = typeMap.map().entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                to.add((string)entry.getKey(), ((TypeEntry)entry.getValue()).getType());
            }

            // a name maps to every overload registered under it
            for (var it = functionMap.map().entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                for (var fit = ((java.util.List)entry.getValue()).iterator(); fit.hasNext();)
                    to.plus().add((string)entry.getKey(), ((FunctionEntry)fit.next()).getFunction());
            }
        }

        /// <summary>
        /// Returns whether this root holds exactly the tables another root holds, by name and by table.
        /// </summary>
        /// <param name="other">The root to compare with.</param>
        /// <returns><see langword="false"/> where a table was added, removed or replaced at either root.</returns>
        /// <remarks>
        /// A recursive query's working table is registered at the root and removed when the query is closed,
        /// so a copy whose statement was abandoned still holds it.
        /// </remarks>
        public bool HasSameTablesAs(CachingCalciteSchema other)
        {
            ArgumentNullException.ThrowIfNull(other);

            var actual = tableMap.map();
            var expected = other.tableMap.map();
            if (actual.size() != expected.size())
                return false;

            for (var it = actual.entrySet().iterator(); it.hasNext();)
            {
                var entry = (java.util.Map.Entry)it.next();
                if (expected.get(entry.getKey()) is not TableEntry match)
                    return false;
                if (ReferenceEquals(match.getTable(), ((TableEntry)entry.getValue()).getTable()) == false)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Visits the <c>Schema</c> of this node and of every explicit sub-schema below it, sub-schemas first.
        /// </summary>
        /// <param name="visit">Called with each node.</param>
        /// <remarks>
        /// Only explicit sub-schemas are visited: an implicit one belongs to its parent's <c>Schema</c>, which
        /// may answer a new object on every call.
        /// </remarks>
        public void VisitExplicit(Action<CachingCalciteSchema> visit)
        {
            ArgumentNullException.ThrowIfNull(visit);

            for (var it = subSchemaMap.map().values().iterator(); it.hasNext();)
                ((CachingCalciteSchema)it.next()).VisitExplicit(visit);

            visit(this);
        }

    }

}
