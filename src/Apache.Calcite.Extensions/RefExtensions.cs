using System.Diagnostics.CodeAnalysis;

using java.lang;
using java.util;

namespace Apache.Calcite.Extensions;

public static class RefExtensions
{

    /// <summary>
    /// Returns a typed version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static TRef AsRef<T, TRef>(this T? self)
        where T : java.lang.Object
        where TRef : struct, IRef<T, TRef>
        => TRef.Create(self);

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static IterableRef<T, TRef, TInfo> AsRef<T, TRef, TInfo>(this Iterable? self)
        where T : class
        where TInfo : IRefInfo<T, TRef>
    {
        return new IterableRef<T, TRef, TInfo>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static IterableRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this Iterable? self)
        where T : class
        where TRef : struct, IRef<T, TRef>
    {
        return new IterableRef<T, TRef, RefBinder<T, TRef>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static IterableRef<T, T, RefIdInfo<T>> AsRef<T>(this Iterable? self)
        where T : class
    {
        return new IterableRef<T, T, RefIdInfo<T>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static CollectionRef<T, TRef, TRefInfo> AsRef<T, TRef, TRefInfo>(this Collection? self)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        return new CollectionRef<T, TRef, TRefInfo>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static CollectionRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this Collection? self)
        where T : class
        where TRef : struct, IRef<T, TRef>
    {
        return new CollectionRef<T, TRef, RefBinder<T, TRef>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static CollectionRef<T, T, RefIdInfo<T>> AsRef<T>(this Collection? self)
        where T : class
    {
        return new CollectionRef<T, T, RefIdInfo<T>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static ListRef<T, TRef, TRefInfo> AsRef<T, TRef, TRefInfo>(this List? self)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        return new ListRef<T, TRef, TRefInfo>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static ListRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this List? self)
        where T : class
        where TRef : struct, IRef<T, TRef>
    {
        return new ListRef<T, TRef, RefBinder<T, TRef>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>       /// <returns></returns>
    internal static ListRef<T, T, RefIdInfo<T>> AsRef<T>(this List? self)
        where T : class
    {
        return new ListRef<T, T, RefIdInfo<T>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static SetRef<T, TRef, TRefInfo> AsRef<T, TRef, TRefInfo>(this Set? self)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        return new SetRef<T, TRef, TRefInfo>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static SetRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this Set? self)
        where T : class
        where TRef : struct, IRef<T, TRef>
    {
        return new SetRef<T, TRef, RefBinder<T, TRef>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static SetRef<T, T, RefIdInfo<T>> AsRef<T>(this Set? self)
        where T : class
    {
        return new SetRef<T, T, RefIdInfo<T>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TKeyRef"></typeparam>
    /// <typeparam name="TKeyInfo"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <typeparam name="TValueInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static MapRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo> AsRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo>(this Map? self)
        where TKey : class
        where TKeyInfo : IRefInfo<TKey, TKeyRef>
        where TValue : class
        where TValueInfo : IRefInfo<TValue, TValueRef>
    {
        return new MapRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TKeyRef"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static MapRef<TKey, TKeyRef, RefBinder<TKey, TKeyRef>, TValue, TValueRef, RefBinder<TValue, TValueRef>> AsRef<TKey, TKeyRef, TValue, TValueRef>(this Map? self)
        where TKey : class
        where TKeyRef : struct, IRef<TKey, TKeyRef>
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {
        return new MapRef<TKey, TKeyRef, RefBinder<TKey, TKeyRef>, TValue, TValueRef, RefBinder<TValue, TValueRef>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    public static MapRef<TKey, TKey, RefIdInfo<TKey>, TValue, TValue, RefIdInfo<TValue>> AsRef<TKey, TValue>(this Map? self)
        where TKey : class
        where TValue : class
    {
        return new MapRef<TKey, TKey, RefIdInfo<TKey>, TValue, TValue, RefIdInfo<TValue>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static IteratorRef<T, TRef, TInfo> AsRef<T, TRef, TInfo>(this Iterator? self)
        where T : class
        where TInfo : IRefInfo<T, TRef>
    {
        return new IteratorRef<T, TRef, TInfo>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static IteratorRef<T, TRef, RefBinder<T, TRef>> AsRef<T, TRef>(this Iterator? self)
        where T : class
        where TRef : struct, IRef<T, TRef>
    {
        return new IteratorRef<T, TRef, RefBinder<T, TRef>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static IteratorRef<T, T, RefIdInfo<T>> AsRef<T>(this Iterator? self)
        where T : class
    {
        return new IteratorRef<T, T, RefIdInfo<T>>(self);
    }

    /// <summary>
    /// Returns a retyped version of the specified instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TInfo"></typeparam>
    /// <param name="self"></param>
    /// <returns></returns>
    internal static ComparatorRef<T, TRef, TInfo> AsRef<T, TRef, TInfo>(this Comparator? self)
        where T : class
        where TInfo : IRefInfo<T, TRef>
    {
        return new ComparatorRef<T, TRef, TInfo>(self);
    }

}
