using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="LazyReference"/>.
/// </summary>
public static class LazyReferenceExtensions
{

    /// <summary>
    /// Returns a <see cref="LazyReferenceRef{TValue, TTypedValue}"/> wrapping the specified <see cref="LazyReference"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LazyReferenceRef<TValue, TTypedValue> AsRef<TValue, TTypedValue>(this LazyReference self)
        where TTypedValue : struct, IRef<TValue, TTypedValue>
        => (LazyReferenceRef<TValue, TTypedValue>)self;

}
