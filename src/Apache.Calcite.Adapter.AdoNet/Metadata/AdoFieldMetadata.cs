using System.Data;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Describes a database field.
    /// </summary>
    /// <param name="Name"></param>
    /// <param name="DbType"></param>
    /// <param name="Size"></param>
    /// <param name="Precision"></param>
    /// <param name="Scale"></param>
    /// <param name="Nullable"></param>
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
