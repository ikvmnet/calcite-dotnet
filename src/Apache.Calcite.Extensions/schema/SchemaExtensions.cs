using System.Collections.Generic;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Extensions.Schema;

/// <summary>
/// Extensions applied to the <see cref="org.apache.calcite.schema.Schema"/> type.
/// </summary>
public static class SchemaExtensions
{

    extension(org.apache.calcite.schema.Schema self)
    {

        /// <summary>
        /// Gets the tables.
        /// </summary>
        public Lookup Tables => self.tables();

        /// <summary>
        /// Gets the subschemas.
        /// </summary>
        public Lookup Subschemas => self.subSchemas();

        /// <summary>
        /// Gets the type names.
        /// </summary>
        public IReadOnlySet<string> TypeNames => self.getTypeNames().AsRef<string>();

        /// <summary>
        /// Returns the names of the functions in this schema.
        /// </summary>
        /// <returns></returns>
        public IReadOnlySet<string> FunctionNames => self.getFunctionNames().AsRef<string>();

        /// <summary>
        /// Returns a list of functions in this schema with the given name, or an empty list if there is no such function.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public IReadOnlyCollection<Function> GetFunctions(string name) => self.getFunctions(name).AsRef<Function>();

        /// <summary>
        /// Returns the expression by which this schema can be referenced in generated code.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public Expression GetExpression(SchemaPlus? parentSchema, string name) => self.getExpression(parentSchema, name);

    }

}
