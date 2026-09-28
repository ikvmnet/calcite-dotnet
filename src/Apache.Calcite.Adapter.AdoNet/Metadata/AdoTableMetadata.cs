namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// A table, as <see cref="AdoDatabaseMetadata.GetTables"/> returns it.
    /// </summary>
    /// <param name="DatabaseName">The table's database, or <see langword="null"/> where the provider has none. The table
    /// name is qualified with it in generated SQL.</param>
    /// <param name="SchemaName">The table's schema, or <see langword="null"/> where the provider has none. The table
    /// name is qualified with it in generated SQL.</param>
    /// <param name="Name">The table name as it appears in the database.</param>
    public readonly record struct AdoTableMetadata(
        string? DatabaseName,
        string? SchemaName,
        string Name
    )
    {



    }

}
