using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Wrapper around a <see cref="IEnumerator"/> that retypes the values.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
/// <typeparam name="TValueInfo"></typeparam>
public struct IteratorRef<TValue, TValueRef, TValueInfo> :
    IEnumerator<TValueRef>,
    IRef<Iterator, IteratorRef<TValue, TValueRef, TValueInfo>>
    where TValue : class
    where TValueInfo : IRefInfo<TValue, TValueRef>
{

    /// <inheritdoc/>
    public static IteratorRef<TValue, TValueRef, TValueInfo> Create(Iterator? value) => new IteratorRef<TValue, TValueRef, TValueInfo>(value);

    readonly Iterator? _self;
    TValue? _current;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public IteratorRef(Iterator? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly Iterator? Underlying => _self;

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
    public readonly TValueRef Current
    {
        get
        {
            ThrowOnNull();
            return TValueInfo.ToCast(_current)!;
        }
    }

    /// <inheritdoc />
    readonly object? IEnumerator.Current => Current;

    /// <inheritdoc />
    public readonly void Dispose()
    {

    }

    /// <inheritdoc />
    public bool MoveNext()
    {
        ThrowOnNull();

        if (_self.hasNext())
        {
            _current = (TValue)_self.next();
            return true;
        }
        else
        {
            return false;
        }
    }

    /// <inheritdoc />
    public void Reset()
    {
        throw new NotSupportedException();
    }

}
