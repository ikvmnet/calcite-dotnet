using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="LazyReference"/>.
/// </summary>
public static class LazyReferenceExtensions
{

    /// <summary>
    /// Returns a <see cref="LazyReferenceRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="LazyReference"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LazyReferenceRef<TValue, TValue, RefIdInfo<TValue>> AsRef<TValue>(this LazyReference? self)
        where TValue : class
        => LazyReferenceRef<TValue, TValue, RefIdInfo<TValue>>.Create(self);

    /// <summary>
    /// Returns a <see cref="LazyReferenceRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="LazyReference"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LazyReferenceRef<TValue, TValueRef, RefBinder<TValue,TValueRef>> AsRef<TValue, TValueRef>(this LazyReference? self)
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
        => LazyReferenceRef<TValue, TValueRef, RefBinder<TValue, TValueRef>>.Create(self);

    /// <summary>
    /// Returns a <see cref="LazyReferenceRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="LazyReference"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LazyReferenceRef<TValue, TValueRef, TValueInfo> AsRef<TValue, TValueRef, TValueInfo>(this LazyReference? self)
        where TValue : class
        where TValueInfo : IRefInfo<TValue, TValueRef>
        => LazyReferenceRef<TValue, TValueRef, TValueInfo>.Create(self);

}
