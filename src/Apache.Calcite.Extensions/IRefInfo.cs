namespace org.apache.calcite.util;

/// <summary>
/// Describes how to convert a bound type to a cast type.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TRef"></typeparam>
public interface IRefInfo<T, TRef>
{

    /// <summary>
    /// Returns <c>true</c> if the given ref type is null.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract bool IsNull(TRef value);

    /// <summary>
    /// Converts the bind type to the cast type.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract T ToBind(TRef value);

    /// <summary>
    /// Returns <c>true</c> if the given type is null.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract bool IsNull(T value);

    /// <summary>
    /// Converts the cast type to the bind type.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract TRef ToCast(T value);

}
