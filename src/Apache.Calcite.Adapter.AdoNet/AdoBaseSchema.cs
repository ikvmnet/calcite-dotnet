using java.util;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Base class for the adapter's Calcite schemas.
    /// </summary>
    /// <remarks>
    /// Table and sub-schema resolution goes through the <see cref="tables"/> and <see cref="subSchemas"/>
    /// lookups a derived class supplies. The schema declares no types and no functions, is not mutable, and
    /// returns itself as its snapshot.
    /// </remarks>
    public abstract class AdoBaseSchema : Schema
    {

        /// <inheritdoc />
        public abstract Lookup tables();

        /// <inheritdoc />
        public virtual Table getTable(string name)
        {
            return (Table)tables().get(name);
        }

        /// <inheritdoc />
        public virtual Set getTableNames()
        {
            return tables().getNames(LikePattern.any());
        }

        /// <inheritdoc />
        public abstract Lookup subSchemas();

        /// <inheritdoc />
        public virtual Schema getSubSchema(string name)
        {
            return (Schema)subSchemas().get(name);
        }

        /// <inheritdoc />
        public virtual Set getSubSchemaNames()
        {
            return subSchemas().getNames(LikePattern.any());
        }

        /// <inheritdoc />
        public virtual RelProtoDataType? getType(string name)
        {
            return null;
        }

        /// <inheritdoc />
        public virtual Set getTypeNames()
        {
            return Collections.emptySet();
        }

        /// <inheritdoc />
        public virtual Collection getFunctions(string name)
        {
            return Collections.emptyList();
        }

        /// <inheritdoc />
        public virtual Set getFunctionNames()
        {
            return Collections.emptySet();
        }

        /// <inheritdoc />
        public virtual Expression getExpression(SchemaPlus parentSchema, string name)
        {
            return Schemas.subSchemaExpression(parentSchema, name, GetType());
        }

        /// <inheritdoc />
        public virtual bool isMutable()
        {
            return false;
        }

        /// <inheritdoc />
        public virtual Schema snapshot(SchemaVersion version)
        {
            return this;
        }

    }

}
