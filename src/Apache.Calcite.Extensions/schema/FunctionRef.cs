using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema;

/// <summary>
/// Wrapper for <see cref="Function"/> that preserves types.
/// </summary>
public readonly struct FunctionRef : IRef<Function, FunctionRef>
{

    /// <inheritdoc />
    public static FunctionRef Create(Function? function) => new FunctionRef(function);

    readonly Function? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public FunctionRef(Function? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly Function? Underlying => _self;

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
    /// Returns the parameters of this function.
    /// </summary>
    public readonly ListRef<FunctionParameter, FunctionParameterRef, RefBinder<FunctionParameter, FunctionParameterRef>> Parameters
    {
        get
        {
            ThrowOnNull();
            return _self.getParameters().AsRef<FunctionParameter, FunctionParameterRef, RefBinder<FunctionParameter, FunctionParameterRef>>();
        }
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
        return obj is FunctionRef o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
