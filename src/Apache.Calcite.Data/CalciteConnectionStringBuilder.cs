using System;
using System.Collections;
using System.Data.Common;

using java.util;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Strongly-typed JDBC connection string builder.
    /// </summary>
    public partial class CalciteConnectionStringBuilder : DbConnectionStringBuilder
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnectionStringBuilder()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnectionStringBuilder(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Writes the configured connection information to a <see cref="Properties"/>.
        /// </summary>
        /// <param name="properties"></param>
        public void WriteTo(Properties properties)
        {
            ArgumentNullException.ThrowIfNull(properties);
            foreach (DictionaryEntry kvp in this)
                properties.put(kvp.Key, kvp.Value);
        }

        /// <summary>
        /// Gets or sets the user to use when opening the connection.
        /// </summary>
        public string? Model
        {
            get => ContainsKey("model") ? this["model"] as string : null;
            set => this["model"] = value;
        }

    }

}
