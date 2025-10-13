using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema.lookup;

/// <summary>
/// Various extension methods for working with <see cref="Lookup"/>.
/// </summary>
public static class LookupExtensions
{

    /// <summary>
    /// Returns a <see cref="LookupRef{TValue, TTypedValue}"/> wrapping the specified <see cref="Lookup"/>.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LookupRef<TValue, TValueRef> AsRef<TValue, TValueRef>(this Lookup self)
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
        => new LookupRef<TValue, TValueRef>(self);

}
