using System;

using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;

using Apache.Calcite.Data.Java;

namespace Apache.Calcite.Data.Schema
{

    /// <summary>
    /// Implementation of <see cref="SchemaPlus"/> based on a <see cref="CalciteSchema"/>.
    /// </summary>
    class SchemaPlusImpl : SchemaPlusBridge
    {

        readonly CalciteSchema _owner;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="owner"></param>
        public SchemaPlusImpl(CalciteSchema owner)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        }

        /// <summary>
        /// Gets the wrapped <see cref="CalciteSchema"/>.
        /// </summary>
        public CalciteSchema CalciteSchema => _owner;

        /// <inheritdoc />
        public SchemaPlus? getParentSchema()
        {
            return _owner.Parent == null ? null : _owner.Parent.Plus();
        }

        /// <inheritdoc />
        public string getName()
        {
            return _owner.Name;
        }

        /// <inheritdoc />
        public bool isMutable()
        {
            return _owner.Schema.IsMutable;
        }

        /// <inheritdoc />
        public void setCacheEnabled(bool cache)
        {
            _owner.SetCache(cache);
        }

        /// <inheritdoc />
        public bool isCacheEnabled()
        {
            return _owner.isCacheEnabled();
        }

        /// <inheritdoc />
        public org.apache.calcite.schema.Schema snapshot(SchemaVersion version)
        {
            throw new UnsupportedOperationException();
        }

        /// <inheritdoc />
        public Expression getExpression(SchemaPlus? parentSchema, string name)
        {
            return _owner.Schema.getExpression(parentSchema, name);
        }

        /// <inheritdoc />
        public Lookup tables()
        {
            return _owner.Tables.map((table, name)->table.getTable());
        }

        /// <inheritdoc />
        public Lookup subSchemas()
        {
            return _owner.Subschemas.map((schema, name)->schema.plus());
        }

        /// <inheritdoc />
        [Obsolete]
        public Table? getTable(string name)
        {
            var entry = _owner.GetTable(name, true);
            return entry == null ? null : entry.getTable();
        }

        /// <inheritdoc />
        [Obsolete]
        public Set getTableNames()
        {
            return _owner.GetTableNames(LikePattern.any());
        }

        /// <inheritdoc />
        public RelProtoDataType? getType(string name)
        {
            var entry = _owner.GetType(name, true);
            return entry == null ? null : entry.getType();
        }

        /// <inheritdoc />
        public Set getTypeNames()
        {
            return _owner.GetTypeNames();
        }

        /// <inheritdoc />
        public Collection getFunctions(string name)
        {
            return _owner.GetFunctions(name, true);
        }

        /// <inheritdoc />
        public NavigableSet getFunctionNames()
        {
            return _owner.GetFunctionNames();
        }

        /// <inheritdoc />
        [Obsolete]
        public SchemaPlus? getSubSchema(string name)
        {
            return subSchemas().get(name);
        }

        /// <inheritdoc />
        [Obsolete]
        public Set getSubSchemaNames()
        {
            return subSchemas().getNames(LikePattern.any());
        }

        /// <inheritdoc />
        public SchemaPlus add(string name, org.apache.calcite.schema.Schema schema)
        {
            var calciteSchema = _owner.Add(name, schema);
            return calciteSchema.plus();
        }

        /// <inheritdoc />
        public object unwrap(Class clazz)
        {
            if (clazz.isInstance(this))
                return clazz.cast(this);

            if (clazz.isInstance(_owner))
                return clazz.cast(_owner);

            if (clazz.isInstance(_owner.schema))
                return clazz.cast(_owner.schema);

            if (_owner.schema is Wrapper wrapper)
                return wrapper.unwrapOrThrow(clazz);

            throw new ClassCastException("not a " + clazz);
        }

        /// <inheritdoc />
        public void setPath(ImmutableList path)
        {
            _owner.path = path;
        }

        /// <inheritdoc />
        public void add(string name, org.apache.calcite.schema.Table table)
        {
            _owner.Add(name, table);
        }

        /// <inheritdoc />
        public bool removeTable(string name)
        {
            return _owner.RemoveTable(name);
        }

        /// <inheritdoc />
        public void add(string name, Function function)
        {
            _owner.Add(name, function);
        }

        /// <inheritdoc />
        public void add(string name, RelProtoDataType type)
        {
            _owner.Add(name, type);
        }

        /// <inheritdoc />
        public void add(string name, org.apache.calcite.materialize.Lattice lattice)
        {
            _owner.Add(name, lattice);
        }

    }

}
