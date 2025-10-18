using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="NameMap"/>.
/// </summary>
public static class NameMapExtensions
{

    /// <summary>
    /// Returns a <see cref="NameMapRef{TVTalue, TRef, TRefInfo}"/> wrapping the specified <see cref="NameMap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMapRef<T, T, RefIdInfo<T>> AsRef<T>(this NameMap self)
        where T : class
        => NameMapRef<T, T, RefIdInfo<T>>.Create(self);

    /// <summary>
    /// Returns a <see cref="NameMapRef{T, TRef, TRefInfo}"/> wrapping the specified <see cref="NameMap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMapRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this NameMap self)
        where T : class
        where TRef : struct, IRef<T, TRef>
        => NameMapRef<T, TRef, RefBinder<T, TRef>>.Create(self);

    /// <summary>
    /// Returns a <see cref="NameMapRef{T, TRef, TRefInfo}"/> wrapping the specified <see cref="NameMap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMapRef<T, TRef, TRefInfo> AsRef<T, TRef, TRefInfo>(this NameMap self)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
        => NameMapRef<T, TRef, TRefInfo>.Create(self);

}
