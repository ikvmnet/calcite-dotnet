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
        /// <param name="name"></param>
        /// <param name="clrType"></param>
        /// <param name="providerTypeName"></param>
        /// <param name="allowDbNull"></param>
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
        /// Gets the CLR type that values in this column will be materialized as.
        /// </summary>
        public Type ClrType { get; }

        /// <summary>
        /// Gets the Calcite-side provider type name for this column.
        /// </summary>
        public string ProviderTypeName { get; }

        /// <summary>
        /// Gets a value indicating whether the column allows nulls.
        /// </summary>
        public bool AllowDbNull { get; }

    }

}
