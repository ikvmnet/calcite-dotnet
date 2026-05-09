using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a set of methods for creating instances of the Apache Calcite ADO.NET provider's
    /// implementation of the data source classes. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Use the <see cref="Instance"/> field directly, or register it with
    /// <c>DbProviderFactories.RegisterFactory</c> under an invariant name such as
    /// <c>Apache.Calcite.Data</c>.
    /// </remarks>
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
