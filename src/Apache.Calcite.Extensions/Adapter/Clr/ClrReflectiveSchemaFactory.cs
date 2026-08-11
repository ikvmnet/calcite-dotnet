using System;
using System.Reflection;

using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// Builds a <see cref="ClrReflectiveSchema"/> from the operands of a model file.
    /// </summary>
    /// <remarks>
    /// <c>ReflectiveSchema.Factory</c>. Two operands, as it has: <c>type</c>, the assembly-qualified name of
    /// the class to reflect over, and an optional <c>staticMethod</c> or <c>staticProperty</c> naming a
    /// member of it that answers the instance. Without either, the parameterless constructor is called.
    ///
    /// <para>Calcite's operand is called <c>class</c> and takes a name <c>Class.forName</c> resolves; this one
    /// is called <c>type</c> and takes a name <see cref="Type.GetType(string)"/> resolves, because the two
    /// name spaces are not the same one and a <c>cli.</c> prefixed name would be neither.</para>
    /// </remarks>
    public class ClrReflectiveSchemaFactory : SchemaFactory
    {

        /// <inheritdoc />
        public org.apache.calcite.schema.Schema create(SchemaPlus parentSchema, string name, java.util.Map operand)
        {
            ArgumentNullException.ThrowIfNull(operand);

            var typeName = (string?)operand.get("type")
                ?? throw new java.lang.IllegalArgumentException("Operand 'type' is required.");

            var type = Type.GetType(typeName, false)
                ?? throw new java.lang.IllegalArgumentException($"No type '{typeName}'.");

            return new ClrReflectiveSchema(Instance(type, operand));
        }

        const BindingFlags Static = BindingFlags.Public | BindingFlags.Static;

        /// <summary>
        /// Returns the object to reflect over.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="operand"></param>
        /// <returns></returns>
        static object Instance(Type type, java.util.Map operand)
        {
            if ((string?)operand.get("staticMethod") is string methodName)
            {
                var method = type.GetMethod(methodName, Static, [])
                    ?? throw new java.lang.IllegalArgumentException($"'{type}' has no static method '{methodName}()'.");

                return method.Invoke(null, null)
                    ?? throw new java.lang.IllegalStateException($"'{methodName}()' returned null.");
            }

            if ((string?)operand.get("staticProperty") is string propertyName)
            {
                var property = type.GetProperty(propertyName, Static)
                    ?? throw new java.lang.IllegalArgumentException($"'{type}' has no static property '{propertyName}'.");

                return property.GetValue(null)
                    ?? throw new java.lang.IllegalStateException($"'{propertyName}' returned null.");
            }

            return Activator.CreateInstance(type)
                ?? throw new java.lang.IllegalStateException($"'{type}' could not be created.");
        }

    }

}
