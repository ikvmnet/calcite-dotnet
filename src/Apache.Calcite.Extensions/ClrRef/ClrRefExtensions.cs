using System.Collections.Generic;

using java.util;

namespace Apache.Calcite.Extensions.ClrRef;

/// <summary>
/// Extension methods that return Java-compatible wrappers for CLR types.
/// </summary>
public static class ClrRefExtensions
{

    /// <summary>
    /// Wraps the specified <see cref="IEnumerator{T}"/> in a <see cref="Iterator"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static ClrIterator<T> AsClrRef<T>(this IEnumerator<T> self)
        where T : class
    {
        return new ClrIterator<T>(self);
    }

    /// <summary>
    /// Wraps the specified <see cref="IList{T}"/> in a <see cref="List"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static ClrList<T> AsClrRef<T>(this IList<T> self)
        where T : class
    {
        return new ClrList<T>(self);
    }

    /// <summary>
    /// Wraps the specified <see cref="ISet{T}"/> in a <see cref="Set"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static ClrSet<T> AsClrRef<T>(this ISet<T> self)
        where T : class
    {
        return new ClrSet<T>(self);
    }

    /// <summary>
    /// Wraps the specified <see cref="ISet{T}"/> in a <see cref="Set"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static ClrReadOnlySet<T> AsClrRef<T>(this IReadOnlySet<T> self)
        where T : class
    {
        return new ClrReadOnlySet<T>(self);
    }

}
