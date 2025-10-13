using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using java.util.function;

namespace org.apache.calcite.util;

/// <summary>
/// Wrapper for <see cref="LazyReference"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
/// <typeparam name="TValueConv"></typeparam>
public readonly struct LazyReferenceRef<TValue, TValueRef, TValueConv> :
    IRef<LazyReference, LazyReferenceRef<TValue, TValueRef, TValueConv>>
    where TValue : class
    where TValueConv : IRefInfo<TValue, TValueRef>
{

    /// <inheritdoc/>
    public static LazyReferenceRef<TValue, TValueRef, TValueConv> Create(LazyReference? value) => new LazyReferenceRef<TValue, TValueRef, TValueConv>(value);

    readonly LazyReference? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public LazyReferenceRef(LazyReference? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly LazyReference? Underlying => _self;

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
    /// 
    /// </summary>
    /// <param name="supplier"></param>
    /// <returns></returns>
    public readonly TValueRef? GetOrCompute(Func<TValueRef> supplier)
    {
        ThrowOnNull();
        return TValueConv.ToCast(
            (TValue?)_self.getOrCompute(
                new DelegateSupplier<TValue?>(()
                    => TValueConv.ToBind(supplier()))));
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        ThrowOnNull();
        return _self.GetHashCode();
    }

    /// <inheritdoc/>
    public override bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is LazyReferenceRef<TValue, TValueRef, TValueConv> o ? _self.equals(o._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string ToString()
    {
        ThrowOnNull();
        return _self.toString();
    }

}
