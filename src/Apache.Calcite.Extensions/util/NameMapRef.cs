using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using java.util;

namespace org.apache.calcite.util;

/// <summary>
/// Wrapper for <see cref="NameMap"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
public readonly struct NameMapRef<TValue> : IRef<NameMap, NameMapRef<TValue>>
{

    public static bool operator ==(NameMapRef<TValue> a, NameMapRef<TValue> b) => a.Underlying == b.Underlying;

    public static bool operator !=(NameMapRef<TValue> a, NameMapRef<TValue> b) => a.Underlying != b.Underlying;

    public static implicit operator NameMap?(NameMapRef<TValue> self) => self.Underlying;

    public static implicit operator NameMapRef<TValue>(NameMap? self) => new NameMapRef<TValue>(self);

    readonly NameMap? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public NameMapRef(NameMap? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly NameMap? Underlying => _self;

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
    /// Puts a new key.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="v"></param>
    public readonly void Put(string name, TValue v)
    {
        ThrowOnNull();
        _self.put(name, v);
    }

    /// <summary>
    /// Returns <c>true</c> if the key exists.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly bool ContainsKey(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.containsKey(name, caseSensitive);
    }

    /// <summary>
    /// Removes an entry.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TValue Remove(string key)
    {
        ThrowOnNull();
        return (TValue)_self.remove(key);
    }

    /// <summary>
    /// Gets the underlying map instance.
    /// </summary>
    /// <returns></returns>
    public readonly IReadOnlyDictionary<string, TValue> Map
    {
        get
        {
            ThrowOnNull();
            return _self.map().AsDictionary<string, TValue>().AsReadOnly();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly IReadOnlyDictionary<string, TValue> Range(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.range(name, caseSensitive).AsDictionary<string, TValue>().AsReadOnly();
    }

    /// <inheritdoc />
    public override readonly int GetHashCode()
    {
        ThrowOnNull();
        return _self.hashCode();
    }

    /// <inheritdoc />
    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is NameMapRef<TValue> m ? _self.equals(m._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self.toString();
    }

}

/// <summary>
/// Wrapper for <see cref="NameMap"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TTypedValue"></typeparam>
public readonly struct NameMapRef<TValue, TTypedValue> : IRef<NameMap, NameMapRef<TValue, TTypedValue>>
    where TTypedValue : struct, IRef<TValue, TTypedValue>
{

    public static bool operator ==(NameMapRef<TValue, TTypedValue> a, NameMapRef<TValue, TTypedValue> b) => a.Underlying == b.Underlying;

    public static bool operator !=(NameMapRef<TValue, TTypedValue> a, NameMapRef<TValue, TTypedValue> b) => a.Underlying != b.Underlying;

    public static implicit operator NameMap?(NameMapRef<TValue, TTypedValue> self) => self.Underlying;

    public static implicit operator NameMapRef<TValue, TTypedValue>(NameMap? self) => new NameMapRef<TValue, TTypedValue>(self);

    public static implicit operator NameMapRef<TValue>(NameMapRef<TValue, TTypedValue> self) => new NameMapRef<TValue>(self.Underlying);

    public static implicit operator NameMapRef<TValue, TTypedValue>(NameMapRef<TValue> self) => new NameMapRef<TValue, TTypedValue>(self.Underlying);

    readonly NameMap? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public NameMapRef(NameMap? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly NameMap? Underlying => _self;

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
    /// Puts a new key.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="v"></param>
    public readonly void Put(string name, TValue v)
    {
        ThrowOnNull();
        _self.put(name, v);
    }

    /// <summary>
    /// Returns <c>true</c> if the key exists.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly bool ContainsKey(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.containsKey(name, caseSensitive);
    }

    /// <summary>
    /// Removes an entry.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public readonly TTypedValue Remove(string key)
    {
        ThrowOnNull();
        return (TValue)_self.remove(key);
    }

    /// <summary>
    /// Gets the underlying map instance.
    /// </summary>
    /// <returns></returns>
    public readonly IReadOnlyDictionary<string, TTypedValue> Map
    {
        get
        {
            ThrowOnNull();
            return _self.map().AsDictionary<string, TValue?>().AsRef<string, TValue, TTypedValue>();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly IReadOnlyDictionary<string, TTypedValue> Range(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.range(name, caseSensitive).AsDictionary<string, TValue?>().AsRef<string, TValue, TTypedValue>();
    }

    /// <inheritdoc />
    public override readonly int GetHashCode()
    {
        ThrowOnNull();
        return _self.hashCode();
    }

    /// <inheritdoc />
    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is NameMapRef<TValue, TTypedValue> m ? _self!.equals(m._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self!.toString();
    }

}
