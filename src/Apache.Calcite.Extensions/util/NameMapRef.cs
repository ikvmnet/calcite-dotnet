using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Wrapper for <see cref="NameMap"/> that preserves types.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TRef"></typeparam>
/// <typeparam name="TRefInfo"></typeparam>
public readonly struct NameMapRef<T, TRef, TRefInfo> :
    IRef<NameMap, NameMapRef<T, TRef, TRefInfo>>
    where T : class
    where TRefInfo : IRefInfo<T, TRef>
{

    /// <inheritdoc />
    public static NameMapRef<T, TRef, TRefInfo> Create(NameMap? value) => new NameMapRef<T, TRef, TRefInfo>(value);

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
    public readonly void Put(string name, T v)
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
    public readonly TRef? Remove(string key)
    {
        ThrowOnNull();
        return TRefInfo.ToCast((T?)_self.remove(key));
    }

    /// <summary>
    /// Gets the underlying map instance.
    /// </summary>
    /// <returns></returns>
    public readonly MapRef<string, string, RefIdInfo<string>, T, TRef, TRefInfo> Map
    {
        get
        {
            ThrowOnNull();
            return _self.map().AsRef<string, string, RefIdInfo<string>, T, TRef, TRefInfo>();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly MapRef<string, string, RefIdInfo<string>, T, TRef, TRefInfo> Range(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.range(name, caseSensitive).AsRef<string, string, RefIdInfo<string>, T, TRef, TRefInfo>();
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
        return obj is NameMapRef<T, TRef, TRefInfo> m ? _self!.equals(m._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self!.toString();
    }

}
