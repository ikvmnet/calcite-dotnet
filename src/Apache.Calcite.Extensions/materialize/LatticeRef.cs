using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using org.apache.calcite.schema;

namespace org.apache.calcite.materialize;

/// <summary>
/// Wrapper for <see cref="Lattice"/> that preserves types.
/// </summary>
public readonly struct LatticeRef : IRef<Lattice, LatticeRef>
{

    /// <inheritdoc />
    public static LatticeRef Create(Lattice? table) => new LatticeRef(table);

    readonly Lattice? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public LatticeRef(Lattice? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly Lattice? Underlying => _self;

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
    public readonly TableRef CreateStarTable()
    {
        ThrowOnNull();
        return _self.createStarTable().AsRef();
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
        return obj is LatticeRef o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
