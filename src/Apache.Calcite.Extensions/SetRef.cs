using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Wraps a Java <see cref="Set"/> type in a CLR compatible accessor.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
/// <typeparam name="TValueInfo"></typeparam>
public readonly struct SetRef<TValue, TValueRef, TValueInfo> :
    ICollection<TValueRef>,
    IReadOnlyCollection<TValueRef>,
    ISet<TValueRef>,
    IReadOnlySet<TValueRef>,
    IRef<Set, SetRef<TValue, TValueRef, TValueInfo>>
    where TValue : class
    where TValueInfo : IRefInfo<TValue, TValueRef>
{

    /// <inheritdoc/>
    public static SetRef<TValue, TValueRef, TValueInfo> Create(Set? value) => new SetRef<TValue, TValueRef, TValueInfo>(value);

    readonly Set? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="set"></param>
    public SetRef(Set? set)
    {
        _self = set;
    }

    /// <inheritdoc />
    public readonly Set? Underlying => _self;

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

    /// <inheritdoc />
    public readonly bool Add(TValueRef item)
    {
        ThrowOnNull();
        return _self.add(TValueInfo.ToBind(item));
    }

    /// <inheritdoc />
    public readonly void ExceptWith(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();

        if (other is IRef<Collection> c)
            _self.removeAll(c.Underlying);
        else
            throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly void IntersectWith(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();

        if (other is IRef<Collection> c)
            _self.retainAll(c.Underlying);
        else
            throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly bool IsProperSubsetOf(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly bool IsProperSupersetOf(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly bool IsSubsetOf(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly bool IsSupersetOf(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly bool Overlaps(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly bool SetEquals(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly void SymmetricExceptWith(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public readonly void UnionWith(IEnumerable<TValueRef> other)
    {
        ThrowOnNull();

        if (other is IRef<Collection> c)
            _self.addAll(c.Underlying);
        else
            throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public readonly int Count => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Count;

    /// <inheritdoc/>
    public readonly bool IsReadOnly => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).IsReadOnly;

    readonly void ICollection<TValueRef>.Add(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Add(item);

    /// <inheritdoc/>
    public readonly void Clear() => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Clear();

    /// <inheritdoc/>
    public readonly bool Contains(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Contains(item);

    /// <inheritdoc/>
    public readonly void CopyTo(TValueRef[] array, int arrayIndex) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).CopyTo(array, arrayIndex);

    /// <inheritdoc/>
    public readonly bool Remove(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Remove(item);

    /// <inheritdoc/>
    public readonly IEnumerator<TValueRef> GetEnumerator() => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

}
