using System;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Describes a single column in a Calcite result set.
    /// </summary>
    internal sealed class CalciteColumn
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteColumn"/> class.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="clrType">The .NET type the column's values are read as.</param>
        /// <param name="providerTypeName">The Calcite SQL type name.</param>
        /// <param name="allowDbNull">Whether the column is nullable.</param>
        public CalciteColumn(string name, Type clrType, string providerTypeName, bool allowDbNull)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            ClrType = clrType ?? throw new ArgumentNullException(nameof(clrType));
            ProviderTypeName = providerTypeName ?? throw new ArgumentNullException(nameof(providerTypeName));
            AllowDbNull = allowDbNull;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the .NET type the column's values are read as.
        /// </summary>
        public Type ClrType { get; }

        /// <summary>
        /// Gets the Calcite SQL type name of the column.
        /// </summary>
        public string ProviderTypeName { get; }

        /// <summary>
        /// Gets a value indicating whether the column allows nulls.
        /// </summary>
        public bool AllowDbNull { get; }

    }

}
