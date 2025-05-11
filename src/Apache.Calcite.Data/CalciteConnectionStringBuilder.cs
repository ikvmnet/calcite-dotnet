using System.Data.Common;

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

    }

}
