using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Wrapper for <see cref="NameSet"/> that preserves types.
/// </summary>
public readonly struct NameSetRef :
    IRef<NameSet, NameSetRef>
{

    /// <inheritdoc />
    public static NameSetRef Create(NameSet? value) => new NameSetRef(value);

    readonly NameSet? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public NameSetRef(NameSet? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly NameSet? Underlying => _self;

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
    /// Adds a name to the set.
    /// </summary>
    /// <param name="name"></param>
    public readonly void Add(string name)
    {
        ThrowOnNull();
        _self.add(name);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly CollectionRef<string, string, RefIdInfo<string>> Range(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.range(name, caseSensitive).AsRef<string>();
    }

    /// <summary>
    /// Returns whether this set contains the given name, with a given case-sensitivity.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="caseSensitive"></param>
    /// <returns></returns>
    public readonly bool Contains(string name, bool caseSensitive)
    {
        ThrowOnNull();
        return _self.contains(name, caseSensitive);
    }

    /// <summary>
    /// Returns the contents as an iterable.
    /// </summary>
    /// <returns></returns>
    public readonly IterableRef<string, string, RefIdInfo<string>> Iterable()
    {
        ThrowOnNull();
        return _self.iterable().AsRef<string>();
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
        return obj is NameSetRef m ? _self!.equals(m._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self!.toString();
    }

}
