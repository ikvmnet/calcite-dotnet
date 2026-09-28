namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// A schema, as <see cref="AdoDatabaseMetadata.GetSchemas"/> returns it.
    /// </summary>
    /// <param name="Name">The schema name as it appears in the database.</param>
    public readonly record struct AdoSchemaMetadata(
        string Name
    )
    {



    }

}
