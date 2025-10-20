using java.util;

using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Provides an implementation of <see cref="SchemaFactory"/> for <see cref="AdoSchema"/>.
    /// </summary>
    public class AdoSchemaFactory : SchemaFactory
    {

        public static readonly AdoSchemaFactory Instance = new AdoSchemaFactory();

        /// <inheritdoc />
        public Schema create(SchemaPlus parentSchema, string name, Map operand)
        {
            return AdoSchema.Create(parentSchema, name, operand);
        }

    }

}
