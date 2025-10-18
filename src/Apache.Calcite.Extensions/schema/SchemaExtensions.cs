namespace org.apache.calcite.schema;

public static class SchemaExtensions
{

    /// <summary>
    /// Returns a <see cref="SchemaRef"/> wrapping the specified <see cref="Schema"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static SchemaRef AsRef(this Schema? self) => SchemaRef.Create(self);

    /// <summary>
    /// Returns a <see cref="TableRef"/> wrapping the specified <see cref="Table"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static TableRef AsRef(this Table? self) => TableRef.Create(self);

    /// <summary>
    /// Returns a <see cref="FunctionRef"/> wrapping the specified <see cref="Function"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static FunctionRef AsRef(this Function? self) => FunctionRef.Create(self);

}
