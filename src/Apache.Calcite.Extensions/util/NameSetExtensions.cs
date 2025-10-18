namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="NameSet"/>.
/// </summary>
public static class NameSetExtensions
{

    /// <summary>
    /// Returns a <see cref="NameSetRef"/> wrapping the specified <see cref="NameSet"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameSetRef AsRef(this NameSet self)
        => NameSetRef.Create(self);

}
