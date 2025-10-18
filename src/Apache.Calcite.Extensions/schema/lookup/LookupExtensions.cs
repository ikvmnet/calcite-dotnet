using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema.lookup;

/// <summary>
/// Various extension methods for working with <see cref="Lookup"/>.
/// </summary>
public static class LookupExtensions
{

    /// <summary>
    /// Returns a <see cref="LookupRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="Lookup"/>.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LookupRef<TValue, TValue, RefIdInfo<TValue>> AsRef<TValue>(this Lookup self)
        where TValue : class
        => LookupRef<TValue, TValue, RefIdInfo<TValue>>.Create(self);

    /// <summary>
    /// Returns a <see cref="LookupRef{TValue, TValueRef, TValueInfo}"/> wrapping the specified <see cref="Lookup"/>.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <typeparam name="TValueInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LookupRef<TValue, TValueRef, TValueInfo> AsRef<TValue, TValueRef, TValueInfo>(this Lookup self)
        where TValue : class
        where TValueInfo : IRefInfo<TValue, TValueRef>
        => LookupRef<TValue, TValueRef, TValueInfo>.Create(self);

}
