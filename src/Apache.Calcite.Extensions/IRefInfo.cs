using System.Diagnostics.CodeAnalysis;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Describes how to convert a bound type to a cast type.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TRef"></typeparam>
public interface IRefInfo<T, TRef>
{

    /// <summary>
    /// Gets the null value for the type.
    /// </summary>
    public static abstract T? Null { get; }

    /// <summary>
    /// Gets the null value for the ref type.
    /// </summary>
    public static abstract TRef? RefNull { get; }

    /// <summary>
    /// Returns <c>true</c> if the given ref type is null.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract bool IsNull([NotNullWhen(false)] TRef? value);

    /// <summary>
    /// Converts the bind type to the cast type.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    [return: NotNullIfNotNull(nameof(value))]
    public static abstract T? ToBind(TRef? value);

    /// <summary>
    /// Returns <c>true</c> if the given type is null.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract bool IsNull([NotNullWhen(false)] T? value);

    /// <summary>
    /// Converts the cast type to the bind type.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    [return: NotNullIfNotNull(nameof(value))]
    public static abstract TRef? ToCast(T? value);

}
