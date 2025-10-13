using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema;

public static class SchemaExtensions
{

    /// <summary>
    /// Returns a <see cref="SchemaRef"/> wrapping the specified <see cref="Schema"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static SchemaRef AsRef(this Schema self) => (SchemaRef)(IRef<Schema, SchemaRef>)self;

}
