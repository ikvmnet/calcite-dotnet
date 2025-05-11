using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Provides a set of methods for creating instances of the JDBC database provider classes.
    /// </summary>
    public class CalciteProviderFactory : DbProviderFactory
    {

        /// <summary>
        /// Returns a new instance of <see cref="CalciteCommand"/>.
        /// </summary>
        /// <returns></returns>
        public override DbCommand CreateCommand()
        {
            return new CalciteCommand();
        }

        /// <summary>
        /// Returns a new instance of <see cref="CalciteCommandBuilder"/>.
        /// </summary>
        /// <returns></returns>
        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new CalciteCommandBuilder();
        }

        /// <summary>
        /// Returns a new instance of <see cref="CalciteConnection"/>.
        /// </summary>
        /// <returns></returns>
        public override DbConnection CreateConnection()
        {
            return new CalciteConnection();
        }

        /// <summary>
        /// Returns a new instance of <see cref="CalciteConnectionStringBuilder"/>.
        /// </summary>
        /// <returns></returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new CalciteConnectionStringBuilder();
        }

        /// <summary>
        /// Returns a new instance of <see cref="CalciteDataAdapter"/>.
        /// </summary>
        /// <returns></returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new CalciteDataAdapter();
        }

        /// <summary>
        /// Returns a new instance of <see cref="CalciteParameter"/>.
        /// </summary>
        /// <returns></returns>
        public override DbParameter CreateParameter()
        {
            return new CalciteParameter();
        }

    }

}
