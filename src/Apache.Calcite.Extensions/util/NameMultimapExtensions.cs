using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="NameMultimap"/>.
/// </summary>
public static class NameMultimapExtensions
{

    /// <summary>
    /// Returns a <see cref="NameMultimapRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="NameMultimap"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMultimapRef<T, T, RefIdInfo<T>> AsRef<T>(this NameMultimap self)
        where T : class
        => NameMultimapRef<T, T, RefIdInfo<T>>.Create(self);

    /// <summary>
    /// Returns a <see cref="NameMultimapRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="NameMultimap"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMultimapRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this NameMultimap self)
        where T : class
        where TRef : struct, IRef<T, TRef>
        => NameMultimapRef<T, TRef, RefBinder<T, TRef>>.Create(self);

    /// <summary>
    /// Returns a <see cref="NameMultimapRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="NameMultimap"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMultimapRef<T, TRef, TInfo> AsRef<T, TRef, TInfo>(this NameMultimap self)
        where T : class
        where TInfo : IRefInfo<T, TRef>
        => NameMultimapRef<T, TRef, TInfo>.Create(self);

}
