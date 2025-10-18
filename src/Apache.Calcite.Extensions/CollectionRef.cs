using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Wraps a Java <see cref="Collection"/> type in a CLR compatible accessor.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TRef"></typeparam>
/// <typeparam name="TRefInfo"></typeparam>
public readonly struct CollectionRef<T, TRef, TRefInfo> :
    IEnumerable<TRef>,
    ICollection<TRef>,
    IReadOnlyCollection<TRef>,
    IRef<Collection, CollectionRef<T, TRef, TRefInfo>>
    where T : class
    where TRefInfo : IRefInfo<T, TRef>
{

    /// <inheritdoc/>
    public static CollectionRef<T, TRef, TRefInfo> Create(Collection? value) => new CollectionRef<T, TRef, TRefInfo>(value);

    readonly Collection? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="collection"></param>
    public CollectionRef(Collection? collection)
    {
        _self = collection;
    }

    /// <inheritdoc />
    public readonly Collection? Underlying => _self;

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
    public readonly int Count
    {
        get
        {
            ThrowOnNull();
            return _self.size();
        }
    }

    /// <inheritdoc />
    public readonly bool IsReadOnly
    {
        get
        {
            ThrowOnNull();
            return false;
        }
    }

    /// <inheritdoc />
    public readonly void Add(TRef item)
    {
        ThrowOnNull();
        _self.add(TRefInfo.ToBind(item));
    }

    /// <inheritdoc />
    public readonly void Clear()
    {
        ThrowOnNull();
        _self.clear();
    }

    /// <inheritdoc />
    public readonly bool Contains(TRef item)
    {
        ThrowOnNull();

        if (TRefInfo.IsNull(item))
            throw new ArgumentNullException(nameof(item));

        return _self.contains(TRefInfo.ToBind(item));
    }

    /// <inheritdoc />
    public readonly void CopyTo(TRef?[] array, int arrayIndex)
    {
        ThrowOnNull();

        if (array == null)
            throw new ArgumentNullException(nameof(array));

        var i = _self.iterator();
        var j = 0;
        while (i.hasNext())
            array[arrayIndex + j] = TRefInfo.ToCast((T?)i.next());
    }

    /// <inheritdoc />
    public readonly bool Remove(TRef item)
    {
        ThrowOnNull();
        return _self.remove(TRefInfo.ToBind(item));
    }

    /// <inheritdoc />
    public readonly IteratorRef<T, TRef, TRefInfo> GetEnumerator()
    {
        ThrowOnNull();
        return new IteratorRef<T, TRef, TRefInfo>(_self.iterator());
    }

    /// <inheritdoc />
    readonly IEnumerator<TRef> IEnumerable<TRef>.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <inheritdoc />
    readonly IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

}
