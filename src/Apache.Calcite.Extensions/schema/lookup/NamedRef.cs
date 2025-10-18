using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema.lookup;

/// <summary>
/// Wrapper for <see cref="Named"/> that preserves types.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TRef"></typeparam>
/// <typeparam name="TInfo"></typeparam>
public readonly struct NamedRef<T, TRef, TInfo> :
    IRef<Named, NamedRef<T, TRef, TInfo>>
    where T : class
    where TInfo : IRefInfo<T, TRef>
{

    /// <inheritdoc />
    public static NamedRef<T, TRef, TInfo> Create(Named? value) => new NamedRef<T, TRef, TInfo>(value);

    readonly Named? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="named"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public NamedRef(Named? named)
    {
        _self = named;
    }

    /// <inheritdoc />
    public readonly Named? Underlying => _self;

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
    /// Gets the name of the entity.
    /// </summary>
    public readonly string Name
    {
        get
        {
            ThrowOnNull();
            return _self.name();
        }
    }

    /// <summary>
    /// Gets the entity.
    /// </summary>
    public readonly TRef Entity
    {
        get
        {
            ThrowOnNull();
            return TInfo.ToCast((T)_self.entity());
        }
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
        return obj is NamedRef<T, TRef, TInfo> o ? _self.equals(o._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
