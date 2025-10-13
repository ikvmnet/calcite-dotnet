using System;

using java.lang;
using java.util;

using org.apache.calcite.util;

namespace Apache.Calcite.Extensions
{

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
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TValueRef"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static IterableRef<TValue, TValueRef> AsRef<TValue, TValueRef>(this Iterable? self)
            where TValue : class
            where TValueRef : struct, IRef<TValue, TValueRef>
        {
            return new IterableRef<TValue, TValueRef>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TValueRef"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static CollectionRef<TValue, TValueRef> AsRef<TValue, TValueRef>(this Collection? self)
            where TValue : class
            where TValueRef : struct, IRef<TValue, TValueRef>
        {
            return new CollectionRef<TValue, TValueRef>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static CollectionRef<TValue> AsRef<TValue>(this Collection? self)
            where TValue : class
        {
            return new CollectionRef<TValue>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TValueRef"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static ListRef<TValue, TValueRef, TValueCast> AsRef<TValue, TValueRef, TValueCast>(this List? self)
            where TValue : class
            where TValueCast : IRefInfo<TValue, TValueRef>
        {
            throw new NotImplementedException();
            //return new ListRef<TValue, TValueRef>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TValueRef"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static SetRef<TValue, TValueRef> AsRef<TValue, TValueRef>(this Set? self)
            where TValue : class
            where TValueRef : struct, IRef<TValue, TValueRef>
        {
            return new SetRef<TValue, TValueRef>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static SetRef<TValue> AsRef<TValue>(this Set? self)
            where TValue : class
        {
            return new SetRef<TValue>(self);
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
        internal static MapRef<TKey, TKeyRef, TValue, TValueRef> AsRef<TKey, TKeyRef, TValue, TValueRef>(this Map? self)
            where TKey : class
            where TKeyRef : struct, IRef<TKey, TKeyRef>
            where TValue : class
            where TValueRef : struct, IRef<TValue, TValueRef>
        {
            return new MapRef<TKey, TKeyRef, TValue, TValueRef>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TValueRef"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static IteratorRef<TValue, TValueRef> AsRef<TValue, TValueRef>(this Iterator? self)
            where TValue : class
            where TValueRef : struct, IRef<TValue, TValueRef>
        {
            return new IteratorRef<TValue, TValueRef>(self);
        }

        /// <summary>
        /// Returns a retyped version of the specified instance.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        internal static IteratorRef<TValue> AsRef<TValue>(this Iterator? self)
            where TValue : class
        {
            return new IteratorRef<TValue>(self);
        }

    }

}
