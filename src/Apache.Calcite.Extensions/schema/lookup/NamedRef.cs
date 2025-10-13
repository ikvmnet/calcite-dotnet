using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

namespace org.apache.calcite.schema.lookup;

/// <summary>
/// Wrapper for <see cref="Named"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
public readonly struct NamedRef<TValue, TValueRef> :
    IRef<Named, NamedRef<TValue, TValueRef>>
    where TValue : class
    where TValueRef : struct, IRef<TValue, TValueRef>
{

    /// <inheritdoc />
    public static NamedRef<TValue, TValueRef> Create(Named? value) => new NamedRef<TValue, TValueRef>(value);

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
    public readonly TValueRef Entity
    {
        get
        {
            ThrowOnNull();
            return (TValue)_self.entity();
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
        return obj is NamedRef<TValue, TValueRef> o ? _self.equals(o._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}

/// <summary>
/// Wrapper for <see cref="Named"/> that preserves types.
/// </summary>
/// <typeparam name="TValue"></typeparam>
public readonly struct NamedRef<TValue> :
    IRef<Named, NamedRef<TValue>>
    where TValue : class
{

    /// <inheritdoc />
    public static NamedRef<TValue> Create(Named? value) => new NamedRef<TValue>(value);

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
    public readonly TValue Entity
    {
        get
        {
            ThrowOnNull();
            return (TValue)_self!.entity();
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
        return obj is NamedRef<TValue> o ? _self.equals(o._self) : _self.equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }
}