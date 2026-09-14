using org.apache.calcite.rel.type;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Says how a driver names a query parameter, and gives it the last say on the SQL a statement unparses to.
    /// </summary>
    /// <remarks>
    /// Deliberately not the dialect. A dialect is a property of the <i>server</i> — what SQL it understands
    /// — and Calcite already models it. This is a property of the <i>driver</i>: two providers reaching the
    /// same server disagree, since SqlClient binds <c>@P0</c> where ODBC to that same server binds <c>?</c>.
    /// The two travel together but neither derives from the other, so they are passed side by side rather
    /// than one carrying the other.
    /// </remarks>
    public interface IAdoSqlSyntax
    {

        /// <summary>
        /// Returns the name the driver binds the parameter at the given position by.
        /// </summary>
        /// <param name="index">The zero-based position of the parameter within the statement.</param>
        /// <returns>A parameter name suitable for inclusion in a SQL statement.</returns>
        /// <remarks>
        /// <para>
        /// Calcite writes a parameter as a bare <c>?</c> and records its ordinal, because JDBC binds by
        /// position. ADO.NET matches a <see cref="System.Data.Common.DbParameter"/> to the command text by
        /// name, so the marker has to be written as the name that will be bound.
        /// </para>
        /// <para>
        /// <c>@P0</c> by default, which SqlClient, MySql and several others accept. A driver that spells it
        /// differently overrides this.
        /// </para>
        /// </remarks>
        string GetParameterName(int index) => "@P" + index;

        /// <summary>
        /// Returns the statement to unparse, given the one Calcite produced — the last chance to rewrite the SQL
        /// tree before it becomes text.
        /// </summary>
        /// <param name="statement">The statement Calcite translated the plan to.</param>
        /// <param name="dialect">The dialect the statement is being written for.</param>
        /// <param name="typeFactory">The type factory, for a rewrite that has to name a type.</param>
        /// <returns>The statement to unparse, unchanged by default.</returns>
        /// <remarks>
        /// A generic seam for the corrections a driver's SQL needs that Calcite's own unparse does not make — where a
        /// node unparses the same for every dialect because it consults none, and the result is wrong for one of them.
        /// The default returns the statement untouched; a provider that needs a rewrite overrides this. It is not the
        /// dialect's job made the driver's: the dialect is where such a thing belongs, and this is where it can be done
        /// until the dialect can.
        /// </remarks>
        SqlNode Rewrite(SqlNode statement, SqlDialect dialect, RelDataTypeFactory typeFactory) => statement;

    }

}
