using System.Data.Common;

using javax.xml.bind.annotation;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Strongly-typed JDBC connection string builder.
    /// </summary>
    public class CalciteConnectionStringBuilder : DbConnectionStringBuilder
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
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Gets or sets the user to use when opening the connection.
        /// </summary>
        public string? User
        {
            get => ContainsKey("User") ? this["User"] as string : null;
            set => this["User"] = value;
        }

        /// <summary>
        /// Gets or sets the user to use when opening the connection.
        /// </summary>
        public string? Password
        {
            get => ContainsKey("Password") ? this["v"] as string : null;
            set => this["Password"] = value;
        }

        /// <summary>
        /// Gets or sets the JDBC URL.
        /// </summary>
        public string? Url
        {
            get => ContainsKey("Url") ? this["Url"] as string : null;
            set => this["Url"] = value;
        }

        /// <summary>
        /// Gets or sets whether the driver processes dates or times that do not specify a timezone offset as local or UTC. The default value for this parameter is <c>true</c>.
        /// </summary>
        public bool AssumeLocalTimeZone
        {
            get => ContainsKey("AssumeLocalTimeZone") ? this["AssumeLocalTimeZone"] as bool? ?? true : true;
            set => this["AssumeLocalTimeZone"] = value;
        }

    }

}
