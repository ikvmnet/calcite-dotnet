namespace org.apache.calcite.util;

/// <summary>
/// Implementation of <see cref="IRefInfo{TSource, TTarget}"/> for the same type.
/// </summary>
/// <typeparam name="T"></typeparam>
public readonly struct RefIdentityConf<T> : IRefInfo<T, T>
    where T : class
{

    /// <inheritdoc/>
    public static T? ToCast(T? value) => value;

    /// <inheritdoc/>
    public static T? ToBind(T? value) => value;

}
