using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using org.apache.calcite.schema.lookup;

namespace org.apache.calcite.schema;

/// <summary>
/// .NET API surface for <see cref="Schema"/>.
/// </summary>
public readonly struct SchemaRef : IRef<Schema, SchemaRef>
{

    /// <inheritdoc />
    public static SchemaRef Create(Schema? schema) => new SchemaRef(schema);

    readonly Schema? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public SchemaRef(Schema? self)
    {
        _self = self;
    }

    /// <inheritdoc />
    public readonly Schema? Underlying => _self;

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
    /// Gets whether this schema is mutable.
    /// </summary>
    public readonly bool IsMutable
    {
        get
        {
            ThrowOnNull();
            return _self.isMutable();
        }
    }

    /// <summary>
    /// Gets the tables.
    /// </summary>
    public readonly LookupRef<Table, TableRef> Tables
    {
        get
        {
            ThrowOnNull();
            return _self.tables().AsRef<Table, TableRef>();
        }
    }

    /// <summary>
    /// Gets the subschemas.
    /// </summary>
    public readonly LookupRef<Schema, SchemaRef> Subschemas
    {
        get
        {
            ThrowOnNull();
            return _self.subSchemas().AsRef<Schema, SchemaRef>();
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
        return obj is SchemaRef o ? _self.Equals(o._self) : _self.Equals(obj);
    }

    /// <inheritdoc />
    public override readonly string? ToString()
    {
        ThrowOnNull();
        return _self.ToString();
    }

}
