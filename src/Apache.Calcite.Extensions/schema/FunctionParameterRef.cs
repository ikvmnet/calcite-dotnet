using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema;

/// <summary>
/// Wrapper for <see cref="Function"/> that preserves types.
/// </summary>
public readonly struct FunctionParameterRef : IRef<FunctionParameter, FunctionParameterRef>
{

    /// <inheritdoc />
    public static FunctionParameterRef Create(FunctionParameter? function) => new FunctionParameterRef(function);

    readonly FunctionParameter? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public FunctionParameterRef(FunctionParameter? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly FunctionParameter? Underlying => _self;

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

    /// <inheritdoc/>
    public override readonly int GetHashCode()
    {
        ThrowOnNull();
        return _self.GetHashCode();
    }

    /// <inheritdoc/>
    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is FunctionParameterRef o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
