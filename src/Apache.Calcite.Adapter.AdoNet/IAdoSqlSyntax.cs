using org.apache.calcite.rel.type;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// How a driver names query parameters, and a rewrite applied to each generated statement before it is
    /// written as text.
    /// </summary>
    /// <remarks>
    /// This belongs to the driver rather than the server: SqlClient and the ODBC driver can reach the same SQL
    /// Server and still name parameters differently (<c>@P0</c> and <c>?</c>). The dialect, which describes the
    /// server, is supplied separately by <see cref="Metadata.AdoDatabaseMetadata.Dialect"/>.
    /// </remarks>
    public interface IAdoSqlSyntax
    {

        /// <summary>
        /// Returns the text written in the statement for the parameter at a position, which is also the
        /// <see cref="System.Data.Common.DbParameter.ParameterName"/> it is bound under.
        /// </summary>
        /// <param name="index">The zero-based position of the parameter in the statement.</param>
        /// <returns>The parameter's name.</returns>
        /// <remarks>
        /// Defaults to <c>@P</c> followed by <paramref name="index"/>, which SqlClient accepts. A driver that binds by
        /// position, as ODBC and OLE DB do, returns <c>?</c>; the parameters are then added in order.
        /// </remarks>
        string GetParameterName(int index) => "@P" + index;

        /// <summary>
        /// Returns the statement to write, given the one Calcite generated.
        /// </summary>
        /// <param name="statement">The statement Calcite generated from the plan.</param>
        /// <param name="dialect">The dialect the statement is written in.</param>
        /// <param name="typeFactory">The type factory, for a rewrite that has to name a type.</param>
        /// <returns>The statement to write. The default returns <paramref name="statement"/> unchanged.</returns>
        /// <remarks>
        /// For a correction Calcite's unparsing does not make because the node in question consults no dialect.
        /// The SQL Server syntax uses it to write each <c>UUID</c> literal as a cast of its text.
        /// </remarks>
        SqlNode Rewrite(SqlNode statement, SqlDialect dialect, RelDataTypeFactory typeFactory) => statement;

    }

}
