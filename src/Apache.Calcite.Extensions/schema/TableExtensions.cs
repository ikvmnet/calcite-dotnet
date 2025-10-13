using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema;

public static class TableExtensions
{

    /// <summary>
    /// Returns a <see cref="TableRef"/> wrapping the specified <see cref="Table"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static TableRef AsRef(this Table self) => (TableRef)(IRef<Table, TableRef>)self;

}
