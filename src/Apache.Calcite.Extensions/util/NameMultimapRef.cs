using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using Apache.Calcite.Extensions;

using java.util;

namespace org.apache.calcite.util;

/// <summary>
/// Wrapper for <see cref="NameMultimap"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
public readonly struct NameMultimapRef<TValue> :
    IRef<NameMultimap, NameMultimapRef<TValue>>
    where TValue : class
{

    /// <inheritdoc />
    public static NameMultimapRef<TValue> Create(NameMultimap? value) => new NameMultimapRef<TValue>(value);

    readonly NameMultimap? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public NameMultimapRef(NameMultimap? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly NameMultimap? Underlying => _self;

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
    /// Puts a new key.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="value"></param>
    public readonly void Put(string name, TValue value)
    {
        ThrowOnNull();
        _self.put(name, value);
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
    /// <param name="value"></param>
    /// <returns></returns>
    public readonly bool Remove(string key, TValue value)
    {
        ThrowOnNull();
        return _self.remove(key, value);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly IReadOnlyCollection<TValue> Range(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.range(name, caseSensitive).AsCollection<TValue>().ToList().AsReadOnly();
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

    /// <inheritdoc />
    public override readonly int GetHashCode() => _self!.hashCode();

    /// <inheritdoc />
    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is NameMultimapRef<TValue> m ? _self.equals(m._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self.toString();
    }

}

/// <summary>
/// Wrapper for <see cref="NameMultimap"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
public readonly struct NameMultimapRef<TValue, TValueRef> :
    IRef<NameMultimap, NameMultimapRef<TValue, TValueRef>>
    where TValue : class
    where TValueRef : struct, IRef<TValue, TValueRef>
{

    /// <inheritdoc />
    public static NameMultimapRef<TValue, TValueRef> Create(NameMultimap? value) => new NameMultimapRef<TValue, TValueRef>(value);

    readonly NameMultimap? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public NameMultimapRef(NameMultimap? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly NameMultimap? Underlying => _self;

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
    /// <param name="value"></param>
    public readonly void Put(string name, TValueRef value)
    {
        ThrowOnNull();
        _self.put(name, value.Underlying);
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
    /// <param name="value"></param>
    /// <returns></returns>
    public readonly bool Remove(string key, TValueRef value)
    {
        ThrowOnNull();
        return _self.remove(key, value.Underlying);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly IReadOnlyCollection<TValueRef> Range(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.range(name, caseSensitive).AsRef<TValue, TValueRef>();
    }

    /// <summary>
    /// Gets the underlying map instance.
    /// </summary>
    /// <returns></returns>
    public readonly IReadOnlyDictionary<string, TValueRef> Map
    {
        get
        {
            ThrowOnNull();
            return _self.map().AsRef<string, TValue, TValueRef>();
        }
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
        return obj is NameMultimapRef<TValue, TValueRef> m ? _self.equals(m._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self.toString();
    }

}