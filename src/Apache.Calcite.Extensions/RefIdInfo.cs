namespace Apache.Calcite.Extensions;

/// <summary>
/// Implementation of <see cref="IRefInfo{TSource, TTarget}"/> for the same type.
/// </summary>
/// <typeparam name="T"></typeparam>
public readonly struct RefIdInfo<T> : IRefInfo<T, T>
{

    /// <inheritdoc/>
    public static T? Null => default;

    /// <inheritdoc/>
    public static T? RefNull => default;

    /// <inheritdoc/>
    public static bool IsNull(T? value) => value is null;

    /// <inheritdoc/>
    public static T? ToBind(T? value) => value;

    /// <inheritdoc/>
    public static T? ToCast(T? value) => value;

}
