using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Various extension methods for working with <see cref="NameMap"/>.
/// </summary>
public static class NameMapExtensions
{

    /// <summary>
    /// Returns a <see cref="NameMapRef{TValue, TTypedValue}"/> wrapping the specified <see cref="NameMap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMapRef<TValue> AsRef<TValue>(this NameMap self)
        => (NameMapRef<TValue>)self;

    /// <summary>
    /// Returns a <see cref="NameMapRef{TValue, TTypedValue}"/> wrapping the specified <see cref="NameMap"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static NameMapRef<TValue, TTypedValue> AsRef<TValue, TTypedValue>(this NameMap self)
        where TTypedValue : struct, IRef<TValue, TTypedValue>
        => (NameMapRef<TValue, TTypedValue>)self;

}
