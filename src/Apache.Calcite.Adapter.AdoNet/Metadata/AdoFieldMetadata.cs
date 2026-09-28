using System.Data;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// A column, as <see cref="AdoDatabaseMetadata.GetFields"/> returns it. The adapter maps it to a Calcite type
    /// by <paramref name="DbType"/>; <see cref="System.Data.DbType.Object"/> maps to <c>OTHER</c>, whose values pass
    /// through unchanged, and suits a column of a type with no other mapping.
    /// </summary>
    /// <param name="Name">The column name as it appears in the database.</param>
    /// <param name="DbType">The column's type.</param>
    /// <param name="Size">The maximum length of a character or binary column, or <see langword="null"/> where there
    /// is none.</param>
    /// <param name="Precision">The precision of a numeric column, or <see langword="null"/> where there is none.</param>
    /// <param name="Scale">The scale of a numeric column, or <see langword="null"/> where there is none.</param>
    /// <param name="Nullable">Whether the column accepts <c>NULL</c>.</param>
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
