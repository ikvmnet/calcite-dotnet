using System;

using Apache.Calcite.Data.Java;
using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.ClrRef;

using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;

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
        public override SchemaPlus? getParentSchema()
        {
            return _owner.Parent == null ? null : _owner.Parent.Plus();
        }

        /// <inheritdoc />
        public override string getName()
        {
            return _owner.Name;
        }

        /// <inheritdoc />
        public override bool isMutable()
        {
            return _owner.Schema.IsMutable;
        }

        /// <inheritdoc />
        public override void setCacheEnabled(bool cache)
        {
            _owner.SetCache(cache);
        }

        /// <inheritdoc />
        public override bool isCacheEnabled()
        {
            return _owner.isCacheEnabled();
        }

        /// <inheritdoc />
        public override org.apache.calcite.schema.Schema snapshot(SchemaVersion version)
        {
            throw new UnsupportedOperationException();
        }

        /// <inheritdoc />
        public override Expression getExpression(SchemaPlus? parentSchema, string name)
        {
            return _owner.Schema.GetExpression(parentSchema, name);
        }

        /// <inheritdoc />
        public override Lookup tables()
        {
            return _owner.Tables.Map<org.apache.calcite.schema.Table, TableRef, RefBinder<org.apache.calcite.schema.Table, TableRef>>((table, name) => table.Table).Underlying!;
        }

        /// <inheritdoc />
        public override Lookup subSchemas()
        {
            return _owner.Subschemas.Map((schema, name) => schema.Plus()).Underlying!;
        }

        /// <inheritdoc />
        [Obsolete]
        public override org.apache.calcite.schema.Table? getTable(string name)
        {
            return _owner.GetTable(name, true)?.Table.Underlying;
        }

        /// <inheritdoc />
        [Obsolete]
        public override Set getTableNames()
        {
            return _owner.GetTableNames(LikePattern.any()).AsClrRef() ?? throw new InvalidOperationException();
        }

        /// <inheritdoc />
        public override RelProtoDataType? getType(string name)
        {
            return _owner.GetType(name, true)?.Type;
        }

        /// <inheritdoc />
        public override Set getTypeNames()
        {
            return _owner.GetTypeNames().AsClrRef();
        }

        /// <inheritdoc />
        public override Collection getFunctions(string name)
        {
            return _owner.GetFunctions(name, true).AsClrRef();
        }

        /// <inheritdoc />
        public override NavigableSet getFunctionNames()
        {
            return _owner.GetFunctionNames().AsClrRef();
        }

        /// <inheritdoc />
        [Obsolete]
        public override SchemaPlus? getSubSchema(string name)
        {
            return subSchemas().get(name);
        }

        /// <inheritdoc />
        [Obsolete]
        public override Set getSubSchemaNames()
        {
            return subSchemas().getNames(LikePattern.any());
        }

        /// <inheritdoc />
        public override SchemaPlus add(string name, org.apache.calcite.schema.Schema schema)
        {
            var calciteSchema = _owner.Add(name, schema);
            return calciteSchema.plus();
        }

        /// <inheritdoc />
        public override object unwrap(Class clazz)
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
        public override void setPath(ImmutableList path)
        {
            _owner.path = path;
        }

        /// <inheritdoc />
        public override void add(string name, org.apache.calcite.schema.Table table)
        {
            _owner.Add(name, table.AsRef());
        }

        /// <inheritdoc />
        public override bool removeTable(string name)
        {
            return _owner.RemoveTable(name);
        }

        /// <inheritdoc />
        public override void add(string name, Function function)
        {
            _owner.Add(name, function.AsRef());
        }

        /// <inheritdoc />
        public override void add(string name, RelProtoDataType type)
        {
            _owner.Add(name, type);
        }

        /// <inheritdoc />
        public override void add(string name, org.apache.calcite.materialize.Lattice lattice)
        {
            _owner.Add(name, lattice);
        }

    }

}
