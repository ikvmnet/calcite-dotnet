using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="NameMultimap"/>.
/// </summary>
public static class NameMultimapExtensions
{

    /// <summary>
    /// Returns a <see cref="NameMultimapRef{TValue}"/> wrapping the specified <see cref="NameMultimap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMultimapRef<TValue> AsRef<TValue>(this NameMultimap self)
        => (NameMultimapRef<TValue>)self;

    /// <summary>
    /// Returns a <see cref="NameMultimapRef{TValue, TTypedValue}"/> wrapping the specified <see cref="NameMultimap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMultimapRef<TValue, TTypedValue> AsRef<TValue, TTypedValue>(this NameMultimap self)
        where TTypedValue : struct, IRef<TValue, TTypedValue>
        => (NameMultimapRef<TValue, TTypedValue>)self;

}
