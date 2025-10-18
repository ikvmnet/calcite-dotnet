using System;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Extensions;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
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
    public readonly LookupRef<Table, TableRef, RefBinder<Table, TableRef>> Tables
    {
        get
        {
            ThrowOnNull();
            return _self.tables().AsRef<Table, TableRef, RefBinder<Table, TableRef>>();
        }
    }

    /// <summary>
    /// Gets the subschemas.
    /// </summary>
    public readonly LookupRef<Schema, SchemaRef, RefBinder<Schema, SchemaRef>> Subschemas
    {
        get
        {
            ThrowOnNull();
            return _self.subSchemas().AsRef<Schema, SchemaRef, RefBinder<Schema, SchemaRef>>();
        }
    }

    /// <summary>
    /// Gets the named type.
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public readonly RelProtoDataType GetType(string name)
    {
        ThrowOnNull();
        return _self.getType(name);
    }

    /// <summary>
    /// Gets the type names.
    /// </summary>
    public readonly SetRef<string, string, RefIdInfo<string>> TypeNames
    {
        get
        {
            ThrowOnNull();
            return _self.getTypeNames().AsRef<string>();
        }
    }

    /// <summary>
    /// Returns a list of functions in this schema with the given name, or an empty list if there is no such function.
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public readonly CollectionRef<Function, FunctionRef, RefBinder<Function, FunctionRef>> GetFunctions(string name)
    {
        ThrowOnNull();
        return _self.getFunctions(name).AsRef<Function, FunctionRef, RefBinder<Function, FunctionRef>>();
    }

    /// <summary>
    /// Returns the names of the functions in this schema.
    /// </summary>
    /// <returns></returns>
    public readonly SetRef<string, string, RefIdInfo<string>> FunctionNames
    {
        get
        {
            ThrowOnNull();
            return _self.getFunctionNames().AsRef<string>();
        }
    }

    /// <summary>
    /// Returns the expression by which this schema can be referenced in generated code.
    /// </summary>
    /// <param name="parentSchema"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    public readonly Expression GetExpression(SchemaPlus? parentSchema, string name)
    {
        ThrowOnNull();
        return _self.getExpression(parentSchema, name);
    }

    /// <summary>
    /// Returns the snapshot of this schema as of the specified time. The contents of the schema snapshot should not change over time.
    /// </summary>
    /// <param name="version"></param>
    /// <returns></returns>
    public readonly SchemaRef Snapshot(SchemaVersion version)
    {
        ThrowOnNull();
        return _self.snapshot(version).AsRef();
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
