using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using java.util;
using java.util.function;

using org.apache.calcite.util;

namespace org.apache.calcite.schema.lookup;


/// <summary>
/// Various extension methods for <see cref="LookupRef{TValue, TTypedValue}"/>.
/// </summary>
public static class LookupRef
{

    /// <summary>
    /// Returns a <see cref="LookupRef{TValue}"/> of the given <see cref="NameMapRef{V}"/>.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="map"></param>
    /// <returns></returns>
    public static LookupRef<TValue> Of<TValue>(NameMapRef<TValue> map)
        where TValue : class
    {
        return new LookupRef<TValue>(Lookup.of(map.Underlying));
    }

    /// <summary>
    /// Returns a <see cref="LookupRef{TValue, TTypedValue}"/> of the given <see cref="NameMapRef{V}"/>.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <param name="map"></param>
    /// <returns></returns>
    public static LookupRef<TValue, TValueRef> Of<TValue, TValueRef>(NameMapRef<TValue, TValueRef> map)
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {
        return new LookupRef<TValue, TValueRef>(Lookup.of(map.Underlying));
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue}"/> instances.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="a"></param>
    /// <param name="b"></param>
    /// <returns></returns>
    public static LookupRef<TValue> Concat<TValue>(LookupRef<TValue> a, LookupRef<TValue> b)
        where TValue : class
    {
        return Concat<TValue>([a.Underlying!, b.Underlying!]);
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue, TTypedValue}"/> instances.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <param name="a"></param>
    /// <param name="b"></param>
    /// <returns></returns>
    public static LookupRef<TValue, TValueRef> Concat<TValue, TValueRef>(LookupRef<TValue, TValueRef> a, LookupRef<TValue, TValueRef> b)
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {
        return Concat<TValue, TValueRef>([a.Underlying!, b.Underlying!]);
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue}"/> instances.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="lookups"></param>
    /// <returns></returns>
    public static LookupRef<TValue> Concat<TValue>(params ReadOnlySpan<Lookup> lookups)
        where TValue : class
    {
        return new LookupRef<TValue>(Lookup.concat(lookups.ToArray()));
    }

    /// <summary>
    /// Concatinates the two <see cref="LookupRef{TValue, TTypedValue}"/> instances.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <param name="lookups"></param>
    /// <returns></returns>
    public static LookupRef<TValue, TValueRef> Concat<TValue, TValueRef>(params ReadOnlySpan<Lookup> lookups)
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {
        return new LookupRef<TValue, TValueRef>(Lookup.concat(lookups.ToArray()));
    }

    /// <summary>
    /// Gets the value with the specified name.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="lookup"></param>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public static TValue? Get<TValue>(LookupRef<TValue> lookup, string name, bool caseSensitive)
        where TValue : class
    {
        if (caseSensitive)
            return lookup.Get(name);

        var obj = lookup.GetIgnoreCase(name);
        if (obj.IsNull)
            return default;

        return obj.Entity;
    }

    /// <summary>
    /// Gets the value with the specified name.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <param name="lookup"></param>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public static TValueRef Get<TValue, TValueRef>(LookupRef<TValue, TValueRef> lookup, string name, bool caseSensitive)
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {
        if (caseSensitive)
            return lookup.Get(name);

        var obj = lookup.GetIgnoreCase(name);
        if (obj.IsNull)
            return new TValueRef();

        return obj.Entity;
    }

}

/// <summary>
/// Wrapper for <see cref="Lookup"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
public readonly struct LookupRef<TValue> :
    IRef<Lookup, LookupRef<TValue>>
    where TValue : class
{

    /// <inheritdoc />
    public static LookupRef<TValue> Create(Lookup? value) => new LookupRef<TValue>(value);

    readonly Lookup? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="lookup"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public LookupRef(Lookup? lookup)
    {
        _self = lookup;
    }

    /// <summary>
    /// Gets the real underlying lookup instance.
    /// </summary>
    public readonly Lookup? Underlying => _self;

    /// <inheritdoc />
    [MemberNotNullWhen(false, nameof(_self))]
    public readonly bool IsNull => _self == null;

    /// <summary>
    /// Throws if the reference is null.
    /// </summary>
    /// <exception cref="NullReferenceException"></exception>
    [MemberNotNull(nameof(_self))]
    void ThrowOnNull()
    {
        if (IsNull)
            throw new NullReferenceException();
    }

    /// <summary>
    /// Gets the value with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TValue? this[string key]
    {
        get
        {
            ThrowOnNull();
            return (TValue?)_self.get(key);
        }
    }

    /// <summary>
    /// Gets the value with the specified key, ignoring case.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TValue? Get(string key)
    {
        ThrowOnNull();
        return (TValue?)_self.get(key);
    }

    /// <summary>
    /// Gets the value with the specified key, ignoring case.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly NamedRef<TValue> GetIgnoreCase(string key)
    {
        ThrowOnNull();
        return new NamedRef<TValue>(_self.getIgnoreCase(key));
    }

    /// <summary>
    /// Gets the names of the lookup that match the specified pattern.
    /// </summary>
    /// <param name="pattern"></param>
    /// <returns></returns>
    public readonly IReadOnlySet<string> GetNames(LikePattern pattern)
    {
        ThrowOnNull();
        return (IReadOnlySet<string>)_self.getNames(pattern).AsSet<string>();
    }

    /// <summary>
    /// Gets the names within the lookup.
    /// </summary>
    public readonly IReadOnlySet<string> Names => GetNames(LikePattern.any());

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="mapper"></param>
    /// <returns></returns>
    public readonly LookupRef<TResult> Map<TResult>(Func<TValue, string, TResult> mapper)
        where TResult : class
    {
        ThrowOnNull();
        return new LookupRef<TResult>(_self.map(new DelegateFunction<TValue, string, TResult?>((entity, name) => mapper(entity, name))));
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="TResultRef"></typeparam>
    /// <param name="mapper"></param>
    /// <returns></returns>
    public readonly LookupRef<TResult, TResultRef> Map<TResult, TResultRef>(Func<TValue, string, TResultRef> mapper)
        where TResult : class
        where TResultRef : struct, IRef<TResult, TResultRef>
    {
        ThrowOnNull();
        return new LookupRef<TResult, TResultRef>(_self.map(new DelegateFunction<TValue, string, TResult?>((entity, name) => mapper(entity, name))));
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
        return obj is LookupRef<TValue> o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}

/// <summary>
/// Wrapper for <see cref="Lookup"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
public readonly struct LookupRef<TValue, TValueRef> :
    IRef<Lookup, LookupRef<TValue, TValueRef>>
    where TValue : class
    where TValueRef : struct, IRef<TValue, TValueRef>
{

    /// <inheritdoc />
    public static LookupRef<TValue, TValueRef> Create(Lookup? value) => new LookupRef<TValue, TValueRef>(value);

    readonly Lookup? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="lookup"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public LookupRef(Lookup? lookup)
    {
        _self = lookup;
    }

    /// <summary>
    /// Gets the real underlying lookup instance.
    /// </summary>
    public readonly Lookup? Underlying => _self;

    /// <inheritdoc />
    [MemberNotNullWhen(false, nameof(_self))]
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
    public readonly TValueRef this[string key]
    {
        get
        {
            ThrowOnNull();
            return (TValue?)_self.get(key);
        }
    }

    /// <summary>
    /// Gets the value with the specified key, ignoring case.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TValueRef Get(string key)
    {
        ThrowOnNull();
        return (TValue?)_self.get(key);
    }

    /// <summary>
    /// Gets the value with the specified key, ignoring case.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly NamedRef<TValue, TValueRef> GetIgnoreCase(string key)
    {
        ThrowOnNull();
        return new NamedRef<TValue, TValueRef>(_self.getIgnoreCase(key));
    }

    /// <summary>
    /// Gets the names within the lookup.
    /// </summary>
    public readonly ISet<string> Names
    {
        get
        {
            ThrowOnNull();
            return _self.getNames(LikePattern.any()).AsSet<string>();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="mapper"></param>
    /// <returns></returns>
    public readonly LookupRef<TResult> Map<TResult>(Func<TValue, string, TResult> mapper)
        where TResult : class
    {
        ThrowOnNull();
        return new LookupRef<TResult>(_self.map(new DelegateFunction<TValue, string, TResult?>((entity, name) => mapper(entity, name))));
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="TResultRef"></typeparam>
    /// <param name="mapper"></param>
    /// <returns></returns>
    public readonly LookupRef<TResult, TResultRef> Map<TResult, TResultRef>(Func<TValue, string, TResultRef> mapper)
        where TResult : class
        where TResultRef : struct, IRef<TResult, TResultRef>
    {
        ThrowOnNull();
        return new LookupRef<TResult, TResultRef>(_self.map(new DelegateFunction<TValue, string, TResult?>((entity, name) => mapper(entity, name))));
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
        return obj is LookupRef<TValue, TValueRef> o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
