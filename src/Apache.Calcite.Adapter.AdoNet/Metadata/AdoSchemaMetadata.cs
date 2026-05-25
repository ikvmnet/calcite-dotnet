namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Describes a schema returned by <see cref="AdoDatabaseMetadata.GetSchemas"/>.
    /// </summary>
    /// <param name="Name">The schema name as it appears in the database.</param>
    public readonly record struct AdoSchemaMetadata(
        string Name
    )
    {



    }

}
