using System.Data.Common;

namespace Apache.Calcite.AdoNet
{

    /// <summary>
    /// Represents a set of methods for creating instances of the Apache Calcite ADO.NET provider's classes.
    /// </summary>
    public sealed class CalciteProviderFactory : DbProviderFactory
    {

        /// <summary>
        /// Gets the singleton instance of <see cref="CalciteProviderFactory"/>.
        /// </summary>
        public static readonly CalciteProviderFactory Instance = new();

        CalciteProviderFactory()
        {

        }

        /// <inheritdoc />
        public override DbConnection CreateConnection() => new CalciteConnection();

        /// <inheritdoc />
        public override DbCommand CreateCommand() => new CalciteCommand();

        /// <inheritdoc />
        public override DbParameter CreateParameter() => new CalciteParameter();

        /// <inheritdoc />
        public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new CalciteConnectionStringBuilder();

    }

}
