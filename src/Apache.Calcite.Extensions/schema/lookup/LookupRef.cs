using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using java.util.function;

using org.apache.calcite.util;
using org.checkerframework.checker.units.qual;

namespace org.apache.calcite.schema.lookup;


/// <summary>
/// Various extension methods for <see cref="LookupRef{TValue, TValueRef, TValueInfo}"/>.
/// </summary>
public static class LookupRef
{

    /// <summary>
    /// Returns a <see cref="LookupRef{TValue, TValueRef,TValueInfo }"/> of the given <see cref="NameMapRef{TValue, TValueRef, TValueInfo}"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="map"></param>
    /// <returns></returns>
    public static LookupRef<T, TRef, TRefInfo> Of<T, TRef, TRefInfo>(NameMapRef<T, TRef, TRefInfo> map)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        return new LookupRef<T, TRef, TRefInfo>(Lookup.of(map.Underlying));
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue, TValueRef, TValueInfo}"/> instances.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="a"></param>
    /// <param name="b"></param>
    /// <returns></returns>
    public static LookupRef<T, TRef, TRefInfo> Concat<T, TRef, TRefInfo>(LookupRef<T, TRef, TRefInfo> a, LookupRef<T, TRef, TRefInfo> b)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        return Concat([a, b]);
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue, TValueRef, TValueInfo}"/> instances.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="lookups"></param>
    /// <returns></returns>
    public static LookupRef<T, TRef, TRefInfo> Concat<T, TRef, TRefInfo>(params ReadOnlySpan<LookupRef<T, TRef, TRefInfo>> lookups)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        var a = new Lookup?[lookups.Length];
        for (int i = 0; i < lookups.Length; i++)
            a[i] = lookups[i].Underlying;

        return new LookupRef<T, TRef, TRefInfo>(Lookup.concat(a));
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue, TValueRef, TValueInfo}"/> instances.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TRef"></typeparam>
    /// <typeparam name="TRefInfo"></typeparam>
    /// <param name="lookups"></param>
    /// <returns></returns>
    public static LookupRef<T, TRef, TRefInfo> Concat<T, TRef, TRefInfo>(params ReadOnlySpan<Lookup> lookups)
        where T : class
        where TRefInfo : IRefInfo<T, TRef>
    {
        return new LookupRef<T, TRef, TRefInfo>(Lookup.concat(lookups.ToArray()));
    }

    /// <summary>
    /// Gets the value with the specified name.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="lookup"></param>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public static T? Get<T>(LookupRef<T, T, RefIdInfo<T>> lookup, string name, bool caseSensitive)
        where T : class
    {
        if (caseSensitive)
            return lookup.Get(name);

        var obj = lookup.GetIgnoreCase(name);
        if (obj.IsNull)
            return RefIdInfo<T>.Null;

        return obj.Entity;
    }

    /// <summary>
    /// Gets the value with the specified name.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <typeparam name="TValueInfo"></typeparam>
    /// <param name="lookup"></param>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public static TValueRef? Get<T, TValueRef, TValueInfo>(LookupRef<T, TValueRef, TValueInfo> lookup, string name, bool caseSensitive)
        where T : class
        where TValueInfo : IRefInfo<T, TValueRef>
    {
        if (caseSensitive)
            return lookup.Get(name);

        var obj = lookup.GetIgnoreCase(name);
        if (obj.IsNull)
            return TValueInfo.RefNull;

        return obj.Entity;
    }

}

/// <summary>
/// Wrapper for <see cref="Lookup"/> that preserves types.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TRef"></typeparam>
/// <typeparam name="TRefInfo"></typeparam>
public readonly struct LookupRef<T, TRef, TRefInfo> :
    IRef<Lookup, LookupRef<T, TRef, TRefInfo>>
    where T : class
    where TRefInfo : IRefInfo<T, TRef>
{

    /// <inheritdoc />
    public static LookupRef<T, TRef, TRefInfo> Create(Lookup? value) => new LookupRef<T, TRef, TRefInfo>(value);

    readonly Lookup? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public LookupRef(Lookup? self)
    {
        _self = self;
    }

    /// <summary>
    /// Gets the real underlying lookup instance.
    /// </summary>
    public readonly Lookup? Underlying => _self;

    /// <inheritdoc />
    [MemberNotNullWhen(false, nameof(_self))]
    [MemberNotNullWhen(false, nameof(Underlying))]
    public readonly bool IsNull => _self == null;

    /// <summary>
    /// Throws if the reference is null.
    /// </summary>
    /// <exception cref="NullReferenceException"></exception>
    [MemberNotNull(nameof(_self))]
    readonly void ThrowOnNull()
    {
        if (IsNull)
            throw new NullReferenceException();
    }

    /// <summary>
    /// Gets the value with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TRef this[string key]
    {
        get
        {
            ThrowOnNull();
            return TRefInfo.ToCast((T)_self.get(key));
        }
    }

    /// <summary>
    /// Gets the value with the specified key, ignoring case.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TRef Get(string key)
    {
        ThrowOnNull();
        return TRefInfo.ToCast((T)_self.get(key));
    }

    /// <summary>
    /// Gets the value with the specified key, ignoring case.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly NamedRef<T, TRef, TRefInfo> GetIgnoreCase(string key)
    {
        ThrowOnNull();
        return new NamedRef<T, TRef, TRefInfo>(_self.getIgnoreCase(key));
    }

    /// <summary>
    /// Gets the names within the lookup.
    /// </summary>
    public readonly SetRef<string, string, RefIdInfo<string>> Names
    {
        get
        {
            ThrowOnNull();
            return GetNames(LikePattern.any());
        }
    }

    /// <summary>
    /// Gets the names that match the given pattern within the lookup.
    /// </summary>
    /// <param name="pattern"></param>
    /// <returns></returns>
    public readonly SetRef<string, string, RefIdInfo<string>> GetNames(LikePattern pattern)
    {
        ThrowOnNull();
        return _self.getNames(pattern).AsRef<string>();
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="mapper"></param>
    /// <returns></returns>
    public readonly LookupRef<TResult, TResult, RefIdInfo<TResult>> Map<TResult>(Func<TRef, string, TResult> mapper)
        where TResult : class
    {
        ThrowOnNull();
        return new LookupRef<TResult, TResult, RefIdInfo<TResult>>(_self.map(new DelegateFunction<T, string, TResult?>((entity, name) => mapper(TRefInfo.ToCast(entity), name))));
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="TResultRef"></typeparam>
    /// <typeparam name="TResultInfo"></typeparam>
    /// <param name="mapper"></param>
    /// <returns></returns>
    public readonly LookupRef<TResult, TResultRef, TResultInfo> Map<TResult, TResultRef, TResultInfo>(Func<TRef, string, TResultRef> mapper)
        where TResult : class
        where TResultInfo : IRefInfo<TResult, TResultRef>
    {
        ThrowOnNull();
        return new LookupRef<TResult, TResultRef, TResultInfo>(_self.map(new DelegateFunction<T, string, TResult?>((entity, name) => TResultInfo.ToBind(mapper(TRefInfo.ToCast(entity), name)))));
    }

    /// <inheritdoc/>
    public override readonly int GetHashCode()
    {
        ThrowOnNull();
        return _self.GetHashCode();
    }

    /// <inheritdoc/>
    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is LookupRef<T, TRef, TRefInfo> o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
