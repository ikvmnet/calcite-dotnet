using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Wraps a Java <see cref="List"/> type in a CLR compatible accessor.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
/// <typeparam name="TValueInfo"></typeparam>
public readonly struct ListRef<TValue, TValueRef, TValueInfo> :
    ICollection<TValueRef>,
    IReadOnlyCollection<TValueRef>,
    IList<TValueRef>,
    IReadOnlyList<TValueRef>,
    IRef<List, ListRef<TValue, TValueRef, TValueInfo>>
    where TValue : class
    where TValueInfo : IRefInfo<TValue, TValueRef>
{

    /// <inheritdoc/>
    public static ListRef<TValue, TValueRef, TValueInfo> Create(List? value) => new ListRef<TValue, TValueRef, TValueInfo>(value);

    readonly List? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public ListRef(List? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    [MemberNotNullWhen(false, nameof(_self))]
    public readonly bool IsNull => _self == null;

    /// <inheritdoc />
    public readonly List? Underlying => _self;

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
    public TValueRef this[int index]
    {
        get
        {
            ThrowOnNull();
            return TValueInfo.ToCast((TValue)_self.get(index));
        }
        set
        {
            ThrowOnNull();
            _self.set(index, TValueInfo.ToBind(value));
        }
    }

    /// <inheritdoc />
    public int IndexOf(TValueRef item)
    {
        ThrowOnNull();
        return _self.indexOf(item);
    }

    /// <inheritdoc />
    public void Insert(int index, TValueRef item)
    {
        ThrowOnNull();
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public void RemoveAt(int index)
    {
        ThrowOnNull();
        _self.remove(index);
    }


    /// <inheritdoc />
    public int Count => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Count;

    /// <inheritdoc />
    public bool IsReadOnly => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).IsReadOnly;

    /// <inheritdoc />
    public void Add(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Add(item);

    /// <inheritdoc />
    public void Clear() => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Clear();

    /// <inheritdoc />
    public bool Contains(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Contains(item);

    /// <inheritdoc />
    public void CopyTo(TValueRef[] array, int arrayIndex) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).CopyTo(array, arrayIndex);

    /// <inheritdoc />
    public bool Remove(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).Remove(item);

    /// <inheritdoc />
    public IteratorRef<TValue, TValueRef, TValueInfo> GetEnumerator() => new CollectionRef<TValue, TValueRef, TValueInfo>(_self).GetEnumerator();

    /// <inheritdoc />
    IEnumerator<TValueRef> IEnumerable<TValueRef>.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <inheritdoc />
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

}
