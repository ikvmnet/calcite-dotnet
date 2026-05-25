using java.util;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Base class for Calcite <see cref="Schema"/> implementations backed by an ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// Provides default implementations of the Calcite <see cref="Schema"/> interface that delegate
    /// table and sub-schema resolution to the abstract <see cref="tables"/> and <see cref="subSchemas"/>
    /// lookups. Concrete types such as <see cref="AdoSchema"/> and <see cref="AdoDatabaseSchema"/>
    /// supply those lookups from an <see cref="AdoDataSource"/>.
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
