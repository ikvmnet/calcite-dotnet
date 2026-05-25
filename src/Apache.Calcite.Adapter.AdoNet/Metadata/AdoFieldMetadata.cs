using System.Data;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Describes a single column returned by <see cref="AdoDatabaseMetadata.GetFields"/>.
    /// </summary>
    /// <param name="Name">The column name as it appears in the database.</param>
    /// <param name="DbType">The ADO.NET data type of the column.</param>
    /// <param name="Size">The maximum length for character columns, or <see langword="null"/> if not applicable.</param>
    /// <param name="Precision">The total number of digits for numeric columns, or <see langword="null"/> if not applicable.</param>
    /// <param name="Scale">The number of digits to the right of the decimal point for numeric columns, or <see langword="null"/> if not applicable.</param>
    /// <param name="Nullable"><see langword="true"/> if the column accepts <c>NULL</c> values; otherwise <see langword="false"/>.</param>
    public readonly record struct AdoFieldMetadata(
        string Name,
        DbType DbType,
        int? Size,
        int? Precision,
        int? Scale,
        bool Nullable
    )
    {



    }

}
