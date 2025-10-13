using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema;

/// <summary>
/// Wrapper for <see cref="Table"/> that preserves types.
/// </summary>
public readonly struct TableRef : IRef<Table, TableRef>
{

    /// <inheritdoc />
    public static TableRef Create(Table? table) => new TableRef(table);

    /// <summary>
    /// Returns a <see cref="TableRef"/> wrapping the specified <see cref="Table"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static TableRef Of(Schema self) => (TableRef)(IRef<Table, TableRef>)self;

    readonly Table? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public TableRef(Table? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly Table? Underlying => _self;

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
    public override int GetHashCode()
    {
        ThrowOnNull();
        return _self.GetHashCode();
    }

    /// <inheritdoc/>
    public override bool Equals([NotNullWhen(true)] object? obj)
    {
        ThrowOnNull();
        return obj is TableRef o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
