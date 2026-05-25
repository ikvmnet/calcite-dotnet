using java.util;

using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A <see cref="SchemaFactory"/> that creates <see cref="AdoSchema"/> instances from a Calcite
    /// model configuration.
    /// </summary>
    /// <remarks>
    /// Register this factory in your Calcite model JSON by setting <c>"factory"</c> to the
    /// fully-qualified name of this class. Calcite will call <see cref="create"/> when it
    /// instantiates the schema, passing any operand properties from the model.
    /// </remarks>
    public class AdoSchemaFactory : SchemaFactory
    {

        /// <summary>
        /// Gets the singleton instance of <see cref="AdoSchemaFactory"/>.
        /// </summary>
        public static readonly AdoSchemaFactory Instance = new AdoSchemaFactory();

        /// <inheritdoc />
        public Schema create(SchemaPlus parentSchema, string name, Map operand)
        {
            return AdoSchema.Create(parentSchema, name, operand);
        }

    }

}
