using java.util;

using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The <see cref="SchemaFactory"/> a Calcite JSON model names to create an <see cref="AdoSchema"/>.
    /// </summary>
    /// <remarks>
    /// In a model, set <c>"factory"</c> to <c>cli.Apache.Calcite.Adapter.AdoNet.AdoSchemaFactory</c>, the name
    /// under which Java sees this class. The operands the model supplies are described on
    /// <see cref="AdoSchema.Create(SchemaPlus, string, Map)"/>.
    /// </remarks>
    public class AdoSchemaFactory : SchemaFactory
    {

        /// <summary>
        /// A shared instance.
        /// </summary>
        public static readonly AdoSchemaFactory Instance = new AdoSchemaFactory();

        /// <summary>
        /// Creates an <see cref="AdoSchema"/> from a model's operands.
        /// </summary>
        /// <param name="parentSchema">The schema the new schema is added to.</param>
        /// <param name="name">The name of the new schema.</param>
        /// <param name="operand">The model's operand map.</param>
        /// <returns>The new schema.</returns>
        /// <exception cref="AdoCalciteException">An operand is missing or names a type that cannot be loaded.</exception>
        public Schema create(SchemaPlus parentSchema, string name, Map operand)
        {
            return AdoSchema.Create(parentSchema, name, operand);
        }

    }

}
