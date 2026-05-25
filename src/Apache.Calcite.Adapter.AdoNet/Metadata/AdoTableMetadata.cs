namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Describes a table returned by <see cref="AdoDatabaseMetadata.GetTables"/>.
    /// </summary>
    /// <param name="DatabaseName">The database that owns this table, or <see langword="null"/> if the database is the default.</param>
    /// <param name="SchemaName">The schema that owns this table, or <see langword="null"/> if the schema is the default.</param>
    /// <param name="Name">The table name as it appears in the database.</param>
    public readonly record struct AdoTableMetadata(
        string? DatabaseName,
        string? SchemaName,
        string Name
    )
    {



    }

}
