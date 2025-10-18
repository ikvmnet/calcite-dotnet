using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Reference to a <see cref="Comparator"/>.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
/// <typeparam name="TValueInfo"></typeparam>
public readonly struct ComparatorRef<TValue, TValueRef, TValueInfo> :
    IComparer<TValueRef>
    where TValue : class
    where TValueInfo : IRefInfo<TValue, TValueRef>
{

    readonly Comparator? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public ComparatorRef(Comparator? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    [MemberNotNullWhen(false, nameof(_self))]
    public readonly bool IsNull => _self == null;

    /// <inheritdoc />
    public readonly Comparator? Underlying => _self;

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
    public int Compare(TValueRef? x, TValueRef? y)
    {
        ThrowOnNull();
        return _self.compare(TValueInfo.ToBind(x), TValueInfo.ToBind(y));
    }

}
