namespace Apache.Calcite.Adapter.Ado.Metadata
{

    /// <summary>
    /// Describes a database table.
    /// </summary>
    /// <param name="DatabaseName"></param>
    /// <param name="SchemaName"></param>
    /// <param name="Name"></param>
    public readonly record struct AdoTableMetadata(
        string? DatabaseName,
        string? SchemaName,
        string Name
    )
    {



    }

}